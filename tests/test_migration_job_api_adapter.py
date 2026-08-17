"""Verify explicit conversion from migration jobs to safe HTTP schemas."""

from dataclasses import fields

from schemabridge.api.adapters.jobs import migration_job_to_api
from schemabridge.api.schemas.jobs import MigrationJobSchema
from tests.test_migration_job_repository import _job


def test_job_adapter_copies_every_declared_api_field() -> None:
    job = _job()

    result = migration_job_to_api(job)

    assert isinstance(result, MigrationJobSchema)
    for field in fields(job):
        assert getattr(result, field.name) == getattr(job, field.name)


def test_job_adapter_response_contains_no_secret_or_business_data_fields() -> None:
    payload = migration_job_to_api(_job()).model_dump(mode="json")

    assert all(
        name not in payload
        for name in ("password", "credentials", "sql", "parameters", "source_rows")
    )
