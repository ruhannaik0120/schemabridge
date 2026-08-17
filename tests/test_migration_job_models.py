"""Verify the vocabulary and legal stage order of migration jobs."""

from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from schemabridge.models.transport import BatchTransportProgress
from schemabridge.models.migration_job import (
    ALLOWED_JOB_STAGE_TRANSITIONS,
    MigrationJob,
    MigrationJobStage,
    MigrationJobStatus,
)
from schemabridge.models.workflow import AuditActorType


NOW = datetime(2026, 8, 16, tzinfo=timezone.utc)
JOB_ID = UUID("11111111-2222-3333-4444-555555555555")
WORKFLOW_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


def _job(**overrides) -> MigrationJob:
    values = {
        "job_id": JOB_ID,
        "workflow_id": WORKFLOW_ID,
        "expected_workflow_version": 5,
        "source_discovery_artifact_version": 1,
        "approved_mapping_artifact_version": 4,
        "source_profile_id": "mysql-source",
        "target_profile_id": "snowflake-target",
        "batch_size": 500,
        "timeout_seconds": 30,
        "job_fingerprint": "a" * 64,
        "status": MigrationJobStatus.QUEUED,
        "stage": MigrationJobStage.QUEUED,
        "queued_at": NOW,
        "actor_type": AuditActorType.USER,
        "idempotency_key": "queue-migration-1",
    }
    values.update(overrides)
    return MigrationJob(**values)


def test_job_status_values_are_stable_for_persistence_and_apis() -> None:
    assert [status.value for status in MigrationJobStatus] == [
        "QUEUED",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
        "REVIEW_REQUIRED",
        "RECOVERY_REQUIRED",
    ]


def test_job_stages_follow_the_real_pipeline_order() -> None:
    ordered_stages = (
        MigrationJobStage.QUEUED,
        MigrationJobStage.PREPARING,
        MigrationJobStage.STAGING,
        MigrationJobStage.TRANSFORMING,
        MigrationJobStage.EXECUTING,
        MigrationJobStage.CLEANING_UP,
        MigrationJobStage.VALIDATING,
        MigrationJobStage.COMPLETED,
    )

    for current, following in zip(ordered_stages, ordered_stages[1:]):
        assert ALLOWED_JOB_STAGE_TRANSITIONS[current] == frozenset({following})


def test_completed_stage_is_terminal() -> None:
    assert ALLOWED_JOB_STAGE_TRANSITIONS[MigrationJobStage.COMPLETED] == frozenset()


def test_queued_job_is_bound_to_exact_non_secret_inputs() -> None:
    job = _job()

    assert job.expected_workflow_version == 5
    assert job.approved_mapping_artifact_version == 4
    assert job.source_profile_id == "mysql-source"
    assert {field.name for field in fields(job)}.isdisjoint(
        {"password", "credentials", "source_rows", "sql"}
    )


def test_running_job_requires_a_start_time_and_active_stage() -> None:
    running = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.PREPARING,
        started_at=NOW + timedelta(seconds=1),
    )

    assert running.started_at == NOW + timedelta(seconds=1)
    with pytest.raises(ValueError, match="lifecycle"):
        replace(_job(), status=MigrationJobStatus.RUNNING)


def test_successful_job_requires_completed_stage_and_timing() -> None:
    succeeded = replace(
        _job(),
        status=MigrationJobStatus.SUCCEEDED,
        stage=MigrationJobStage.COMPLETED,
        started_at=NOW + timedelta(seconds=1),
        completed_at=NOW + timedelta(seconds=11),
        duration_ms=10_000,
    )

    assert succeeded.duration_ms == 10_000
    with pytest.raises(ValueError, match="lifecycle"):
        replace(succeeded, stage=MigrationJobStage.VALIDATING)


@pytest.mark.parametrize(
    "status",
    [
        MigrationJobStatus.FAILED,
        MigrationJobStatus.REVIEW_REQUIRED,
        MigrationJobStatus.RECOVERY_REQUIRED,
    ],
)
def test_unsuccessful_job_requires_a_sanitized_failure_category(status) -> None:
    unsuccessful = replace(
        _job(),
        status=status,
        stage=MigrationJobStage.STAGING,
        started_at=NOW + timedelta(seconds=1),
        completed_at=NOW + timedelta(seconds=2),
        duration_ms=1_000,
        failure_category="REMOTE_OPERATION_FAILED",
    )

    assert unsuccessful.failure_category == "REMOTE_OPERATION_FAILED"
    with pytest.raises(ValueError, match="lifecycle"):
        replace(unsuccessful, failure_category=None)


def test_job_rejects_invalid_hash_and_non_utc_timestamp() -> None:
    with pytest.raises(ValueError, match="job_fingerprint"):
        _job(job_fingerprint="not-a-hash")
    with pytest.raises(ValueError, match="UTC"):
        _job(queued_at=datetime(2026, 8, 16))


def test_running_staging_job_can_hold_one_coherent_progress_snapshot() -> None:
    progress = BatchTransportProgress(
        batches_completed=2,
        rows_read=4,
        rows_written=4,
        total_rows_estimate=5,
    )
    job = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.STAGING,
        started_at=NOW + timedelta(seconds=1),
        batch_progress=progress,
        progress_updated_at=NOW + timedelta(seconds=2),
    )

    assert job.batch_progress == progress
    assert job.batch_progress.estimated_percent_complete == 80


def test_job_requires_progress_value_and_timestamp_to_appear_together() -> None:
    progress = BatchTransportProgress(
        batches_completed=1,
        rows_read=2,
        rows_written=2,
    )

    with pytest.raises(ValueError, match="appear together"):
        _job(batch_progress=progress)
    with pytest.raises(ValueError, match="appear together"):
        _job(progress_updated_at=NOW)


@pytest.mark.parametrize(
    "stage",
    [MigrationJobStage.QUEUED, MigrationJobStage.PREPARING],
)
def test_job_rejects_batch_progress_before_staging(stage) -> None:
    progress = BatchTransportProgress(
        batches_completed=1,
        rows_read=2,
        rows_written=2,
    )

    with pytest.raises(ValueError, match="timing or stage"):
        _job(
            status=(
                MigrationJobStatus.QUEUED
                if stage is MigrationJobStage.QUEUED
                else MigrationJobStatus.RUNNING
            ),
            stage=stage,
            started_at=(
                None
                if stage is MigrationJobStage.QUEUED
                else NOW + timedelta(seconds=1)
            ),
            batch_progress=progress,
            progress_updated_at=NOW + timedelta(seconds=2),
        )


def test_job_rejects_progress_timestamp_before_job_start() -> None:
    progress = BatchTransportProgress(
        batches_completed=1,
        rows_read=2,
        rows_written=2,
    )

    with pytest.raises(ValueError, match="timing or stage"):
        _job(
            status=MigrationJobStatus.RUNNING,
            stage=MigrationJobStage.STAGING,
            started_at=NOW + timedelta(seconds=2),
            batch_progress=progress,
            progress_updated_at=NOW + timedelta(seconds=1),
        )
