"""Convert background migration job domain objects into public API schemas."""

from __future__ import annotations

from schemabridge.api.schemas.jobs import (
    MigrationJobBatchProgressSchema,
    MigrationJobSchema,
)
from schemabridge.models.migration_job import MigrationJob


def migration_job_to_api(value: MigrationJob) -> MigrationJobSchema:
    """Copy only the explicitly declared safe job fields into the API model."""

    progress = (
        MigrationJobBatchProgressSchema(
            batches_completed=value.batch_progress.batches_completed,
            rows_read=value.batch_progress.rows_read,
            rows_written=value.batch_progress.rows_written,
            total_rows_estimate=value.batch_progress.total_rows_estimate,
            estimated_percent_complete=(
                value.batch_progress.estimated_percent_complete
            ),
        )
        if value.batch_progress is not None
        else None
    )
    return MigrationJobSchema(
        job_id=value.job_id,
        workflow_id=value.workflow_id,
        expected_workflow_version=value.expected_workflow_version,
        source_discovery_artifact_version=value.source_discovery_artifact_version,
        approved_mapping_artifact_version=value.approved_mapping_artifact_version,
        source_profile_id=value.source_profile_id,
        target_profile_id=value.target_profile_id,
        batch_size=value.batch_size,
        timeout_seconds=value.timeout_seconds,
        job_fingerprint=value.job_fingerprint,
        status=value.status,
        stage=value.stage,
        queued_at=value.queued_at,
        actor_type=value.actor_type,
        idempotency_key=value.idempotency_key,
        actor_reference=value.actor_reference,
        started_at=value.started_at,
        completed_at=value.completed_at,
        duration_ms=value.duration_ms,
        failure_category=value.failure_category,
        batch_progress=progress,
        progress_updated_at=value.progress_updated_at,
    )


__all__ = ["migration_job_to_api"]
