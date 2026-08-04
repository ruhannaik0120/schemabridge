"""Safe durable models for one workflow-scoped target execution attempt."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from models.workflow import AuditActorType


class MigrationExecutionAttemptStatus(str, Enum):
    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_ROLLED_BACK = "FAILED_ROLLED_BACK"
    OUTCOME_UNCERTAIN = "OUTCOME_UNCERTAIN"


class MigrationTransactionOutcome(str, Enum):
    COMMITTED = "COMMITTED"
    ROLLED_BACK = "ROLLED_BACK"
    UNKNOWN = "UNKNOWN"


_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")


def _utc(value: datetime | None, name: str, *, required: bool = True) -> None:
    if value is None and not required:
        return
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timezone.utc.utcoffset(value)
    ):
        raise ValueError(f"{name} must be UTC.")


def _positive(value: int, name: str, *, allow_zero: bool = False) -> None:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} is invalid.")


def _text(value: str, name: str, limit: int = 256) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > limit
        or "\x00" in value
    ):
        raise ValueError(f"{name} is invalid.")


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class MigrationExecutionAttempt:
    attempt_id: UUID
    workflow_id: UUID
    approved_mapping_artifact_version: int
    transformation_preview_artifact_version: int
    target_profile_id: str
    execution_fingerprint: str
    status: MigrationExecutionAttemptStatus
    timeout_seconds: int
    claimed_at: datetime
    actor_type: AuditActorType
    idempotency_key: str
    actor_reference: str | None = None
    running_at: datetime | None = None
    completed_at: datetime | None = None
    evidence_artifact_id: UUID | None = None
    failure_category: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.attempt_id, UUID) or not isinstance(self.workflow_id, UUID):
            raise TypeError("execution attempt identifiers must be UUIDs.")
        _positive(
            self.approved_mapping_artifact_version,
            "approved_mapping_artifact_version",
        )
        _positive(
            self.transformation_preview_artifact_version,
            "transformation_preview_artifact_version",
        )
        _positive(self.timeout_seconds, "timeout_seconds")
        _text(self.target_profile_id, "target_profile_id")
        if not _HASH.fullmatch(self.execution_fingerprint):
            raise ValueError("execution_fingerprint is invalid.")
        if not isinstance(self.status, MigrationExecutionAttemptStatus):
            raise TypeError("status is invalid.")
        _utc(self.claimed_at, "claimed_at")
        if not isinstance(self.actor_type, AuditActorType):
            raise TypeError("actor_type is invalid.")
        _text(self.idempotency_key, "idempotency_key", 128)
        _utc(self.running_at, "running_at", required=False)
        _utc(self.completed_at, "completed_at", required=False)
        if self.running_at is not None and self.running_at < self.claimed_at:
            raise ValueError("running_at precedes claimed_at.")
        if (
            self.completed_at is not None
            and self.running_at is not None
            and self.completed_at < self.running_at
        ):
            raise ValueError("completed_at precedes running_at.")
        if self.actor_reference is not None:
            _text(self.actor_reference, "actor_reference")
        if self.failure_category is not None and not _CODE.fullmatch(
            self.failure_category
        ):
            raise ValueError("failure_category is invalid.")
        if self.evidence_artifact_id is not None and not isinstance(
            self.evidence_artifact_id, UUID
        ):
            raise TypeError("evidence_artifact_id must be a UUID.")
        if self.status is MigrationExecutionAttemptStatus.CLAIMED:
            valid = (
                self.running_at is None
                and self.completed_at is None
                and self.evidence_artifact_id is None
                and self.failure_category is None
            )
        elif self.status is MigrationExecutionAttemptStatus.RUNNING:
            valid = (
                self.running_at is not None
                and self.completed_at is None
                and self.evidence_artifact_id is None
                and self.failure_category is None
            )
        else:
            valid = (
                self.running_at is not None
                and self.completed_at is not None
                and self.evidence_artifact_id is not None
            )
        if not valid:
            raise ValueError("execution attempt lifecycle fields are inconsistent.")

    def __repr__(self) -> str:
        return (
            "MigrationExecutionAttempt("
            f"attempt_id={self.attempt_id!r}, status={self.status.value!r})"
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class MigrationExecutionEvidence:
    attempt_id: UUID
    workflow_id: UUID
    status: MigrationExecutionAttemptStatus
    statement_count: int
    affected_rows: int | None
    target_relation: tuple[str | None, str, str]
    target_profile_id: str
    connector_type: str
    started_at: datetime
    completed_at: datetime
    duration_ms: int
    transaction_outcome: MigrationTransactionOutcome
    approved_mapping_artifact_version: int
    transformation_preview_artifact_version: int
    execution_fingerprint: str
    failure_category: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.attempt_id, UUID) or not isinstance(self.workflow_id, UUID):
            raise TypeError("execution evidence identifiers must be UUIDs.")
        if self.status not in {
            MigrationExecutionAttemptStatus.SUCCEEDED,
            MigrationExecutionAttemptStatus.FAILED_ROLLED_BACK,
            MigrationExecutionAttemptStatus.OUTCOME_UNCERTAIN,
        }:
            raise ValueError("execution evidence status must be terminal.")
        if self.statement_count != 1:
            raise ValueError("execution evidence must describe one statement.")
        if self.affected_rows is not None:
            _positive(self.affected_rows, "affected_rows", allow_zero=True)
        if not isinstance(self.target_relation, tuple) or len(self.target_relation) != 3:
            raise TypeError("target_relation must have three components.")
        if self.target_relation[0] is not None:
            _text(self.target_relation[0], "target_database")
        _text(self.target_relation[1], "target_schema")
        _text(self.target_relation[2], "target_table")
        _text(self.target_profile_id, "target_profile_id")
        _text(self.connector_type, "connector_type", 64)
        _utc(self.started_at, "started_at")
        _utc(self.completed_at, "completed_at")
        if self.completed_at < self.started_at:
            raise ValueError("completed_at precedes started_at.")
        _positive(self.duration_ms, "duration_ms", allow_zero=True)
        if not isinstance(self.transaction_outcome, MigrationTransactionOutcome):
            raise TypeError("transaction_outcome is invalid.")
        _positive(
            self.approved_mapping_artifact_version,
            "approved_mapping_artifact_version",
        )
        _positive(
            self.transformation_preview_artifact_version,
            "transformation_preview_artifact_version",
        )
        if not _HASH.fullmatch(self.execution_fingerprint):
            raise ValueError("execution_fingerprint is invalid.")
        if self.failure_category is not None and not _CODE.fullmatch(
            self.failure_category
        ):
            raise ValueError("failure_category is invalid.")


__all__ = [
    "MigrationExecutionAttempt",
    "MigrationExecutionAttemptStatus",
    "MigrationExecutionEvidence",
    "MigrationTransactionOutcome",
]
