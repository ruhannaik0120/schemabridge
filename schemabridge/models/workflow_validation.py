"""Define the durable lifecycle of one workflow-scoped validation run.

The run binds execution evidence, an approved mapping, generated validation
SQL, and source/target profiles to a single fingerprint.  Field invariants make
claimed, running, successful, review-required, and uncertain states mutually
consistent before they reach persistence.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from schemabridge.models.workflow import AuditActorType


class WorkflowValidationRunStatus(str, Enum):
    """Track the claim, execution, and terminal result of validation."""

    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    OUTCOME_UNCERTAIN = "OUTCOME_UNCERTAIN"


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
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must be UTC.")


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class WorkflowValidationRun:
    """Represent one durable validation claim and its optional evidence artifact."""

    run_id: UUID
    workflow_id: UUID
    execution_attempt_id: UUID
    execution_evidence_artifact_version: int
    approved_mapping_artifact_version: int
    validation_preview_artifact_version: int
    source_profile_id: str
    target_profile_id: str
    validation_fingerprint: str
    status: WorkflowValidationRunStatus
    timeout_seconds: int
    claimed_at: datetime
    actor_type: AuditActorType
    idempotency_key: str
    actor_reference: str | None = None
    running_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: int | None = None
    evidence_artifact_id: UUID | None = None
    failure_category: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, UUID) or not isinstance(self.workflow_id, UUID) or not isinstance(self.execution_attempt_id, UUID):
            raise TypeError("validation run identifiers must be UUIDs.")
        for name in (
            "execution_evidence_artifact_version",
            "approved_mapping_artifact_version",
            "validation_preview_artifact_version",
            "timeout_seconds",
        ):
            _positive(getattr(self, name), name)
        _text(self.source_profile_id, "source_profile_id")
        _text(self.target_profile_id, "target_profile_id")
        if not _HASH.fullmatch(self.validation_fingerprint):
            raise ValueError("validation_fingerprint is invalid.")
        if not isinstance(self.status, WorkflowValidationRunStatus):
            raise TypeError("status is invalid.")
        _utc(self.claimed_at, "claimed_at")
        _utc(self.running_at, "running_at", required=False)
        _utc(self.completed_at, "completed_at", required=False)
        if self.running_at is not None and self.running_at < self.claimed_at:
            raise ValueError("running_at precedes claimed_at.")
        if self.completed_at is not None and self.running_at is not None and self.completed_at < self.running_at:
            raise ValueError("completed_at precedes running_at.")
        if self.duration_ms is not None:
            if isinstance(self.duration_ms, bool) or not isinstance(self.duration_ms, int) or self.duration_ms < 0:
                raise ValueError("duration_ms is invalid.")
        if not isinstance(self.actor_type, AuditActorType):
            raise TypeError("actor_type is invalid.")
        _text(self.idempotency_key, "idempotency_key", 128)
        if self.actor_reference is not None:
            _text(self.actor_reference, "actor_reference")
        if self.evidence_artifact_id is not None and not isinstance(self.evidence_artifact_id, UUID):
            raise TypeError("evidence_artifact_id must be a UUID.")
        if self.failure_category is not None and not _CODE.fullmatch(self.failure_category):
            raise ValueError("failure_category is invalid.")
        if self.status is WorkflowValidationRunStatus.CLAIMED:
            valid = self.running_at is None and self.completed_at is None and self.duration_ms is None and self.evidence_artifact_id is None and self.failure_category is None
        elif self.status is WorkflowValidationRunStatus.RUNNING:
            valid = self.running_at is not None and self.completed_at is None and self.duration_ms is None and self.evidence_artifact_id is None and self.failure_category is None
        elif self.status is WorkflowValidationRunStatus.OUTCOME_UNCERTAIN:
            valid = self.running_at is not None and self.completed_at is not None and self.duration_ms is not None and self.evidence_artifact_id is None and self.failure_category is not None
        else:
            valid = self.running_at is not None and self.completed_at is not None and self.duration_ms is not None and self.evidence_artifact_id is not None and self.failure_category is None
        if not valid:
            raise ValueError("validation run lifecycle fields are inconsistent.")

    def __repr__(self) -> str:
        return f"WorkflowValidationRun(run_id={self.run_id!r}, status={self.status.value!r})"


__all__ = ["WorkflowValidationRun", "WorkflowValidationRunStatus"]
