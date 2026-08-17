"""Verify the HTTP boundary for durable queued migration jobs."""

from uuid import UUID

from fastapi.testclient import TestClient

from schemabridge.api.app import create_app
from schemabridge.api.dependencies import get_migration_job_submission_service
from tests.test_migration_job_repository import WORKFLOW_ID
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
