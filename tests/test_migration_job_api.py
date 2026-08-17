"""Verify the HTTP boundary for durable queued migration jobs."""

from dataclasses import replace
from datetime import timedelta
from uuid import UUID

from fastapi.testclient import TestClient

from schemabridge.api.app import create_app
from schemabridge.api.dependencies import get_migration_job_submission_service
from schemabridge.models.migration_job import MigrationJobStage, MigrationJobStatus
from schemabridge.models.transport import BatchTransportProgress
from tests.test_migration_job_repository import NOW, WORKFLOW_ID
from tests.test_migration_job_service import _service


CREATE_URL = f"/api/v1/migrations/workflows/{WORKFLOW_ID}/jobs"


def _payload() -> dict:
    return {
        "expected_version": 5,
        "source_discovery_artifact_version": 1,
        "approved_mapping_artifact_version": 4,
        "batch_size": 500,
        "timeout_seconds": 30,
        "actor_type": "USER",
        "actor_reference": "reviewer-1",
    }


def _client():
    service, _repository = _service()
    app = create_app()
    app.dependency_overrides[get_migration_job_submission_service] = lambda: service
    return TestClient(app)


def test_create_replay_and_get_job() -> None:
    with _client() as client:
        created = client.post(
            CREATE_URL,
            json=_payload(),
            headers={"Idempotency-Key": "create-background-job-1"},
        )
        replay = client.post(
            CREATE_URL,
            json=_payload(),
            headers={"Idempotency-Key": "create-background-job-1"},
        )
        retrieved = client.get(f"/api/v1/migrations/jobs/{created.json()['job']['job_id']}")

    assert created.status_code == 201
    assert created.json()["created"] is True
    assert created.json()["job"]["status"] == "QUEUED"
    assert created.json()["job"]["stage"] == "QUEUED"
    assert created.json()["job"]["source_profile_id"] == "mysql-source"
    assert created.json()["job"]["target_profile_id"] == "snowflake-target"
    assert replay.status_code == 200
    assert replay.json()["created"] is False
    assert replay.json()["job"] == created.json()["job"]
    assert retrieved.status_code == 200
    assert retrieved.json() == created.json()["job"]


def test_create_rejects_missing_header_and_server_controlled_fields() -> None:
    with _client() as client:
        missing_header = client.post(CREATE_URL, json=_payload())
        invalid_payload = _payload() | {"status": "RUNNING"}
        controlled_field = client.post(
            CREATE_URL,
            json=invalid_payload,
            headers={"Idempotency-Key": "invalid-job"},
        )

    assert missing_header.status_code == 422
    assert controlled_field.status_code == 422


def test_get_job_exposes_current_durable_batch_progress() -> None:
    service, repository = _service()
    job, _created = service.create(
        WORKFLOW_ID,
        expected_version=5,
        source_discovery_artifact_version=1,
        approved_mapping_artifact_version=4,
        batch_size=500,
        timeout_seconds=30,
        idempotency_key="progress-job",
    )
    repository._jobs[job.job_id] = replace(
        job,
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.STAGING,
        started_at=NOW,
        batch_progress=BatchTransportProgress(
            batches_completed=2,
            rows_read=1_000,
            rows_written=1_000,
            total_rows_estimate=2_000,
        ),
        progress_updated_at=NOW + timedelta(seconds=2),
    )
    app = create_app()
    app.dependency_overrides[get_migration_job_submission_service] = lambda: service

    with TestClient(app) as client:
        response = client.get(f"/api/v1/migrations/jobs/{job.job_id}")

    assert response.status_code == 200
    assert response.json()["batch_progress"] == {
        "batches_completed": 2,
        "rows_read": 1_000,
        "rows_written": 1_000,
        "total_rows_estimate": 2_000,
        "estimated_percent_complete": 50,
    }


def test_second_active_job_and_missing_job_use_stable_errors() -> None:
    with _client() as client:
        first = client.post(
            CREATE_URL,
            json=_payload(),
            headers={"Idempotency-Key": "create-background-job-1"},
        )
        conflict = client.post(
            CREATE_URL,
            json=_payload(),
            headers={"Idempotency-Key": "create-background-job-2"},
        )
        missing = client.get(f"/api/v1/migrations/jobs/{UUID(int=99)}")

    assert first.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "MIGRATION_JOB_ALREADY_ACTIVE"
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "MIGRATION_JOB_NOT_FOUND"
    assert "persistence" not in missing.text.lower()


def test_openapi_describes_job_submission_and_read_routes() -> None:
    schema = create_app().openapi()

    create_operation = schema["paths"]["/api/v1/migrations/workflows/{workflow_id}/jobs"]["post"]
    get_operation = schema["paths"]["/api/v1/migrations/jobs/{job_id}"]["get"]
    assert create_operation["operationId"] == "migration_job_create"
    assert any(parameter["name"] == "Idempotency-Key" for parameter in create_operation["parameters"])
    assert get_operation["operationId"] == "migration_job_get"
    job_properties = schema["components"]["schemas"]["MigrationJobSchema"]["properties"]
    progress_properties = schema["components"]["schemas"]["MigrationJobBatchProgressSchema"]["properties"]
    assert "batch_progress" in job_properties
    assert "progress_updated_at" in job_properties
    assert "estimated_percent_complete" in progress_properties
