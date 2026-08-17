"""Verify strict public schemas for background migration jobs."""

from datetime import datetime, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from schemabridge.api.schemas.jobs import (
    MigrationJobCreateRequest,
    MigrationJobCreateResponse,
    MigrationJobSchema,
)
from schemabridge.models.migration_job import MigrationJobStage, MigrationJobStatus
from schemabridge.models.workflow import AuditActorType


def _request(**overrides):
    values = {
        "expected_version": 5,
        "source_discovery_artifact_version": 1,
        "approved_mapping_artifact_version": 4,
        "batch_size": 500,
        "timeout_seconds": 30,
    }
    values.update(overrides)
    return MigrationJobCreateRequest(**values)


def _job_schema():
    return MigrationJobSchema(
        job_id=UUID(int=1),
        workflow_id=UUID(int=2),
        expected_workflow_version=5,
        source_discovery_artifact_version=1,
        approved_mapping_artifact_version=4,
        source_profile_id="mysql-source",
        target_profile_id="snowflake-target",
        batch_size=500,
        timeout_seconds=30,
        job_fingerprint="a" * 64,
        status=MigrationJobStatus.QUEUED,
        stage=MigrationJobStage.QUEUED,
        queued_at=datetime(2026, 8, 16, tzinfo=timezone.utc),
        actor_type=AuditActorType.USER,
        idempotency_key="create-job-1",
        actor_reference=None,
        started_at=None,
        completed_at=None,
        duration_ms=None,
        failure_category=None,
    )


def test_create_request_accepts_only_caller_controlled_fields() -> None:
    request = _request()

    assert request.actor_type is AuditActorType.USER
    assert request.batch_size == 500


@pytest.mark.parametrize(
    "unexpected",
    [
        {"job_id": str(UUID(int=1))},
        {"status": "SUCCEEDED"},
        {"source_profile_id": "attacker-selected-profile"},
        {"job_fingerprint": "a" * 64},
    ],
)
def test_create_request_rejects_server_controlled_fields(unexpected) -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        _request(**unexpected)


@pytest.mark.parametrize("batch_size", [0, -1, 10_001, True])
def test_create_request_rejects_unsafe_batch_sizes(batch_size) -> None:
    with pytest.raises(ValidationError):
        _request(batch_size=batch_size)


def test_response_exposes_safe_job_and_replay_indicator() -> None:
    response = MigrationJobCreateResponse(job=_job_schema(), created=False)
    payload = response.model_dump(mode="json")

    assert payload["job"]["status"] == "QUEUED"
    assert payload["created"] is False
    assert all(name not in payload["job"] for name in ("password", "sql", "source_rows"))
