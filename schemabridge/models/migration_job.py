"""Define the vocabulary and stage order for background migration jobs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from schemabridge.models.transport import BatchTransportProgress
from schemabridge.models.workflow import AuditActorType


class MigrationJobStatus(str, Enum):
    """Describe the overall condition of a background migration job."""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


class MigrationJobStage(str, Enum):
    """Describe the latest pipeline stage reached by a migration job."""

    QUEUED = "QUEUED"
    PREPARING = "PREPARING"
    STAGING = "STAGING"
    TRANSFORMING = "TRANSFORMING"
    EXECUTING = "EXECUTING"
    CLEANING_UP = "CLEANING_UP"
    VALIDATING = "VALIDATING"
    COMPLETED = "COMPLETED"


ALLOWED_JOB_STAGE_TRANSITIONS: dict[
    MigrationJobStage, frozenset[MigrationJobStage]
] = {
    MigrationJobStage.QUEUED: frozenset({MigrationJobStage.PREPARING}),
    MigrationJobStage.PREPARING: frozenset({MigrationJobStage.STAGING}),
    MigrationJobStage.STAGING: frozenset({MigrationJobStage.TRANSFORMING}),
    MigrationJobStage.TRANSFORMING: frozenset({MigrationJobStage.EXECUTING}),
    MigrationJobStage.EXECUTING: frozenset({MigrationJobStage.CLEANING_UP}),
    MigrationJobStage.CLEANING_UP: frozenset({MigrationJobStage.VALIDATING}),
    MigrationJobStage.VALIDATING: frozenset({MigrationJobStage.COMPLETED}),
    MigrationJobStage.COMPLETED: frozenset(),
}


_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")


def _positive(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} is invalid.")


def _text(value: str, name: str, limit: int = 256) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > limit or "\x00" in value:
        raise ValueError(f"{name} is invalid.")


def _utc(value: datetime | None, name: str, *, required: bool = True) -> None:
    if value is None and not required:
        return
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timezone.utc.utcoffset(value)
    ):
        raise ValueError(f"{name} must be UTC.")


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class MigrationJob:
    """Bind one background run to exact workflow inputs and lifecycle state."""

    job_id: UUID
    workflow_id: UUID
    expected_workflow_version: int
    source_discovery_artifact_version: int
    approved_mapping_artifact_version: int
    source_profile_id: str
    target_profile_id: str
    batch_size: int
    timeout_seconds: int
    job_fingerprint: str
    status: MigrationJobStatus
    stage: MigrationJobStage
    queued_at: datetime
    actor_type: AuditActorType
    idempotency_key: str
    actor_reference: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: int | None = None
    failure_category: str | None = None
    batch_progress: BatchTransportProgress | None = None
    progress_updated_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.job_id, UUID) or not isinstance(self.workflow_id, UUID):
            raise TypeError("job identifiers must be UUIDs.")
        for name in (
            "expected_workflow_version",
            "source_discovery_artifact_version",
            "approved_mapping_artifact_version",
            "batch_size",
            "timeout_seconds",
        ):
            _positive(getattr(self, name), name)
        if self.batch_size > 10_000:
            raise ValueError("batch_size exceeds the job limit.")
        _text(self.source_profile_id, "source_profile_id")
        _text(self.target_profile_id, "target_profile_id")
        if not _HASH.fullmatch(self.job_fingerprint):
            raise ValueError("job_fingerprint is invalid.")
        if not isinstance(self.status, MigrationJobStatus):
            raise TypeError("status is invalid.")
        if not isinstance(self.stage, MigrationJobStage):
            raise TypeError("stage is invalid.")
        _utc(self.queued_at, "queued_at")
        _utc(self.started_at, "started_at", required=False)
        _utc(self.completed_at, "completed_at", required=False)
        _utc(self.progress_updated_at, "progress_updated_at", required=False)
        if self.started_at is not None and self.started_at < self.queued_at:
            raise ValueError("started_at precedes queued_at.")
        if self.completed_at is not None and (
            self.started_at is None or self.completed_at < self.started_at
        ):
            raise ValueError("completed_at is invalid.")
        if self.duration_ms is not None and (
            isinstance(self.duration_ms, bool)
            or not isinstance(self.duration_ms, int)
            or self.duration_ms < 0
        ):
            raise ValueError("duration_ms is invalid.")
        if not isinstance(self.actor_type, AuditActorType):
            raise TypeError("actor_type is invalid.")
        _text(self.idempotency_key, "idempotency_key", 128)
        if self.actor_reference is not None:
            _text(self.actor_reference, "actor_reference")
        if self.failure_category is not None and not _CODE.fullmatch(self.failure_category):
            raise ValueError("failure_category is invalid.")
        if self.batch_progress is not None and not isinstance(
            self.batch_progress,
            BatchTransportProgress,
        ):
            raise TypeError("batch_progress is invalid.")
        if (self.batch_progress is None) != (self.progress_updated_at is None):
            raise ValueError("batch progress and its timestamp must appear together.")
        if self.batch_progress is not None:
            progress_stages = {
                MigrationJobStage.STAGING,
                MigrationJobStage.TRANSFORMING,
                MigrationJobStage.EXECUTING,
                MigrationJobStage.CLEANING_UP,
                MigrationJobStage.VALIDATING,
                MigrationJobStage.COMPLETED,
            }
            if (
                self.stage not in progress_stages
                or self.started_at is None
                or self.progress_updated_at < self.started_at
                or (
                    self.completed_at is not None
                    and self.progress_updated_at > self.completed_at
                )
            ):
                raise ValueError("batch progress timing or stage is inconsistent.")

        if self.status is MigrationJobStatus.QUEUED:
            valid = (
                self.stage is MigrationJobStage.QUEUED
                and self.started_at is None
                and self.completed_at is None
                and self.duration_ms is None
                and self.failure_category is None
            )
        elif self.status is MigrationJobStatus.RUNNING:
            valid = (
                self.stage not in {MigrationJobStage.QUEUED, MigrationJobStage.COMPLETED}
                and self.started_at is not None
                and self.completed_at is None
                and self.duration_ms is None
                and self.failure_category is None
            )
        elif self.status is MigrationJobStatus.SUCCEEDED:
            valid = (
                self.stage is MigrationJobStage.COMPLETED
                and self.started_at is not None
                and self.completed_at is not None
                and self.duration_ms is not None
                and self.failure_category is None
            )
        else:
            valid = (
                self.stage not in {MigrationJobStage.QUEUED, MigrationJobStage.COMPLETED}
                and self.started_at is not None
                and self.completed_at is not None
                and self.duration_ms is not None
                and self.failure_category is not None
            )
        if not valid:
            raise ValueError("job lifecycle fields are inconsistent.")

    def __repr__(self) -> str:
        return (
            "MigrationJob("
            f"job_id={self.job_id!r}, "
            f"status={self.status.value!r}, "
            f"stage={self.stage.value!r})"
        )


__all__ = [
    "ALLOWED_JOB_STAGE_TRANSITIONS",
    "MigrationJob",
    "MigrationJobStage",
    "MigrationJobStatus",
]
