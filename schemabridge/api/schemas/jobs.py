"""Strict HTTP contracts for background migration job submission and status."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import Field, StringConstraints

from schemabridge.models.migration_job import MigrationJobStage, MigrationJobStatus
from schemabridge.models.workflow import AuditActorType

from .common import ApiSchema
from .migrations import NonNegativeInt, PositiveInt, SafeCode
from .workflows import ActorReference, ProfileId


JobBatchSize = Annotated[int, Field(strict=True, gt=0, le=10_000)]
Fingerprint = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
IdempotencyKey = Annotated[str, StringConstraints(min_length=1, max_length=128)]
Percentage = Annotated[int, Field(strict=True, ge=0, le=100)]


class MigrationJobCreateRequest(ApiSchema):
    """Accept only caller-controlled options for one approved workflow job."""

    expected_version: PositiveInt
    source_discovery_artifact_version: PositiveInt
    approved_mapping_artifact_version: PositiveInt
    batch_size: JobBatchSize
    timeout_seconds: PositiveInt
    actor_type: AuditActorType = AuditActorType.USER
    actor_reference: ActorReference | None = None


class MigrationJobBatchProgressSchema(ApiSchema):
    """Expose safe cumulative counts without exposing transported row values."""

    batches_completed: NonNegativeInt
    rows_read: NonNegativeInt
    rows_written: NonNegativeInt
    total_rows_estimate: NonNegativeInt | None
    estimated_percent_complete: Percentage | None


class MigrationJobSchema(ApiSchema):
    """Expose safe durable job identity, lineage, configuration, and state."""

    job_id: UUID
    workflow_id: UUID
    expected_workflow_version: PositiveInt
    source_discovery_artifact_version: PositiveInt
    approved_mapping_artifact_version: PositiveInt
    source_profile_id: ProfileId
    target_profile_id: ProfileId
    batch_size: JobBatchSize
    timeout_seconds: PositiveInt
    job_fingerprint: Fingerprint
    status: MigrationJobStatus
    stage: MigrationJobStage
    queued_at: datetime
    actor_type: AuditActorType
    idempotency_key: IdempotencyKey
    actor_reference: ActorReference | None
    started_at: datetime | None
    completed_at: datetime | None
    duration_ms: NonNegativeInt | None
    failure_category: SafeCode | None
    batch_progress: MigrationJobBatchProgressSchema | None = None
    progress_updated_at: datetime | None = None


class MigrationJobCreateResponse(ApiSchema):
    """Tell callers whether this request created or replayed the returned job."""

    job: MigrationJobSchema
    created: bool


__all__ = [
    "MigrationJobCreateRequest",
    "MigrationJobCreateResponse",
    "MigrationJobBatchProgressSchema",
    "MigrationJobSchema",
]
