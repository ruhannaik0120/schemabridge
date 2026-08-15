"""Define durable claims and sanitized evidence for staging-table loading."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from schemabridge.models.transport import TransportRelation
from schemabridge.models.workflow import AuditActorType


class WorkflowTransportAttemptStatus(str, Enum):
    """Track one source-to-staging operation from claim to final outcome."""

    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_CLEANED_UP = "FAILED_CLEANED_UP"
    OUTCOME_UNCERTAIN = "OUTCOME_UNCERTAIN"


_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")


def _positive(value: int, name: str, *, allow_zero: bool = False) -> None:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
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
class WorkflowTransportAttempt:
    """Represent the durable single-runner claim for one staging load."""

    attempt_id: UUID
    workflow_id: UUID
    source_discovery_artifact_version: int
    approved_mapping_artifact_version: int
    source_profile_id: str
    target_profile_id: str
    staging_relation: TransportRelation
    batch_size: int
    timeout_seconds: int
    transport_fingerprint: str
    status: WorkflowTransportAttemptStatus
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
            raise TypeError("transport attempt identifiers must be UUIDs.")
        _positive(self.source_discovery_artifact_version, "source_discovery_artifact_version")
        _positive(self.approved_mapping_artifact_version, "approved_mapping_artifact_version")
        _text(self.source_profile_id, "source_profile_id")
        _text(self.target_profile_id, "target_profile_id")
        if not isinstance(self.staging_relation, TransportRelation):
            raise TypeError("staging_relation must be a TransportRelation.")
        _positive(self.batch_size, "batch_size")
        _positive(self.timeout_seconds, "timeout_seconds")
        if not _HASH.fullmatch(self.transport_fingerprint):
            raise ValueError("transport_fingerprint is invalid.")
        if not isinstance(self.status, WorkflowTransportAttemptStatus):
            raise TypeError("status is invalid.")
        _utc(self.claimed_at, "claimed_at")
        _utc(self.running_at, "running_at", required=False)
        _utc(self.completed_at, "completed_at", required=False)
        if self.running_at is not None and self.running_at < self.claimed_at:
            raise ValueError("running_at precedes claimed_at.")
        if self.completed_at is not None and (
            self.running_at is None or self.completed_at < self.running_at
        ):
            raise ValueError("completed_at is invalid.")
        if not isinstance(self.actor_type, AuditActorType):
            raise TypeError("actor_type is invalid.")
        _text(self.idempotency_key, "idempotency_key", 128)
        if self.actor_reference is not None:
            _text(self.actor_reference, "actor_reference")
        if self.evidence_artifact_id is not None and not isinstance(self.evidence_artifact_id, UUID):
            raise TypeError("evidence_artifact_id must be a UUID.")
        if self.failure_category is not None and not _CODE.fullmatch(self.failure_category):
            raise ValueError("failure_category is invalid.")

        if self.status is WorkflowTransportAttemptStatus.CLAIMED:
            valid = self.running_at is None and self.completed_at is None
        elif self.status is WorkflowTransportAttemptStatus.RUNNING:
            valid = self.running_at is not None and self.completed_at is None
        else:
            valid = self.running_at is not None and self.completed_at is not None
        if self.status is WorkflowTransportAttemptStatus.SUCCEEDED:
            valid = valid and self.evidence_artifact_id is not None and self.failure_category is None
        elif self.status in {
            WorkflowTransportAttemptStatus.FAILED_CLEANED_UP,
            WorkflowTransportAttemptStatus.OUTCOME_UNCERTAIN,
        }:
            valid = valid and self.evidence_artifact_id is None and self.failure_category is not None
        else:
            valid = valid and self.evidence_artifact_id is None and self.failure_category is None
        if not valid:
            raise ValueError("transport attempt lifecycle fields are inconsistent.")

    def __repr__(self) -> str:
        return f"WorkflowTransportAttempt(attempt_id={self.attempt_id!r}, status={self.status.value!r})"


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowTransportEvidence:
    """Store row-free evidence for one successful source-to-staging load."""

    attempt_id: UUID
    workflow_id: UUID
    source_relation: TransportRelation
    staging_relation: TransportRelation
    source_profile_id: str
    target_profile_id: str
    batch_size: int
    batch_count: int
    column_count: int
    rows_read: int
    rows_written: int
    started_at: datetime
    completed_at: datetime
    duration_ms: int
    source_discovery_artifact_version: int
    approved_mapping_artifact_version: int
    transport_fingerprint: str

    def __post_init__(self) -> None:
        if not isinstance(self.attempt_id, UUID) or not isinstance(self.workflow_id, UUID):
            raise TypeError("transport evidence identifiers must be UUIDs.")
        if not isinstance(self.source_relation, TransportRelation) or not isinstance(
            self.staging_relation, TransportRelation
        ):
            raise TypeError("transport evidence relations are invalid.")
        _text(self.source_profile_id, "source_profile_id")
        _text(self.target_profile_id, "target_profile_id")
        _positive(self.batch_size, "batch_size")
        _positive(self.batch_count, "batch_count", allow_zero=True)
        _positive(self.column_count, "column_count")
        _positive(self.rows_read, "rows_read", allow_zero=True)
        _positive(self.rows_written, "rows_written", allow_zero=True)
        if self.rows_read != self.rows_written:
            raise ValueError("transport evidence row counts must match.")
        if (self.batch_count == 0) != (self.rows_read == 0):
            raise ValueError("transport evidence batch count is inconsistent.")
        _utc(self.started_at, "started_at")
        _utc(self.completed_at, "completed_at")
        if self.completed_at < self.started_at:
            raise ValueError("completed_at precedes started_at.")
        _positive(self.duration_ms, "duration_ms", allow_zero=True)
        _positive(self.source_discovery_artifact_version, "source_discovery_artifact_version")
        _positive(self.approved_mapping_artifact_version, "approved_mapping_artifact_version")
        if not _HASH.fullmatch(self.transport_fingerprint):
            raise ValueError("transport_fingerprint is invalid.")


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowStagingCleanupEvidence:
    """Prove that one managed staging table was removed after commit."""

    workflow_id: UUID
    transport_attempt_id: UUID
    execution_attempt_id: UUID
    staging_relation: TransportRelation
    target_profile_id: str
    started_at: datetime
    completed_at: datetime
    duration_ms: int
    cleanup_fingerprint: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.workflow_id, "workflow_id"),
            (self.transport_attempt_id, "transport_attempt_id"),
            (self.execution_attempt_id, "execution_attempt_id"),
        ):
            if not isinstance(value, UUID):
                raise TypeError(f"{name} must be a UUID.")
        if not isinstance(self.staging_relation, TransportRelation):
            raise TypeError("staging_relation must be a TransportRelation.")
        if not self.staging_relation.object_name.startswith("SB_STAGE_"):
            raise ValueError("staging_relation is not managed by SchemaBridge.")
        _text(self.target_profile_id, "target_profile_id")
        _utc(self.started_at, "started_at")
        _utc(self.completed_at, "completed_at")
        if self.completed_at < self.started_at:
            raise ValueError("completed_at precedes started_at.")
        _positive(self.duration_ms, "duration_ms", allow_zero=True)
        if not _HASH.fullmatch(self.cleanup_fingerprint):
            raise ValueError("cleanup_fingerprint is invalid.")


__all__ = [
    "WorkflowTransportAttempt",
    "WorkflowTransportAttemptStatus",
    "WorkflowTransportEvidence",
    "WorkflowStagingCleanupEvidence",
]
