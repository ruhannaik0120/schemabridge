"""Verify explicit conversion from migration jobs to safe HTTP schemas."""

from dataclasses import fields, replace
from datetime import timedelta

from schemabridge.api.adapters.jobs import migration_job_to_api
from schemabridge.api.schemas.jobs import MigrationJobSchema
from schemabridge.models.migration_job import MigrationJobStage, MigrationJobStatus
from schemabridge.models.transport import BatchTransportProgress
from tests.test_migration_job_repository import NOW, _job


def test_job_adapter_copies_every_declared_api_field() -> None:
    job = _job()

    result = migration_job_to_api(job)

    assert isinstance(result, MigrationJobSchema)
    for field in fields(job):
        if field.name == "batch_progress":
            continue
        assert getattr(result, field.name) == getattr(job, field.name)


def test_job_adapter_response_contains_no_secret_or_business_data_fields() -> None:
    payload = migration_job_to_api(_job()).model_dump(mode="json")

    assert all(
        name not in payload
        for name in ("password", "credentials", "sql", "parameters", "source_rows")
    )


def test_job_adapter_exposes_safe_progress_counts_and_calculated_percentage() -> None:
    job = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.STAGING,
        started_at=NOW,
        batch_progress=BatchTransportProgress(
            batches_completed=2,
            rows_read=400,
            rows_written=400,
            total_rows_estimate=1_000,
        ),
        progress_updated_at=NOW + timedelta(seconds=2),
    )

    payload = migration_job_to_api(job).model_dump(mode="json")

    assert payload["batch_progress"] == {
        "batches_completed": 2,
        "rows_read": 400,
        "rows_written": 400,
        "total_rows_estimate": 1_000,
        "estimated_percent_complete": 40,
    }
    assert payload["progress_updated_at"] == "2026-08-16T00:00:02Z"
