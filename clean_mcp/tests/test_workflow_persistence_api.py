"""Focused HTTP tests for durable workflow persistence integration."""

from __future__ import annotations

from copy import deepcopy
import importlib
from uuid import UUID

from fastapi.testclient import TestClient

from api.app import create_app
from api.config import ApiSettings
from api.dependencies import get_workflow_repository
from persistence.config import ControlPlaneConfig
from services.workflow_persistence import WorkflowPersistenceService
from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from tests.test_migration_api import _json_table, _workflow_tables


BASE = "/api/v1/migrations/workflows"


def _relation(table) -> dict:
    return {
        "catalog_name": table.catalog_name,
        "schema_name": table.schema_name,
        "object_name": table.object_name,
        "system": table.system,
    }


def _create_payload() -> dict:
    source, target = _workflow_tables()
    return {
        "display_name": "Durable migration",
        "source_profile_id": "pg-source",
        "target_profile_id": "sf-target",
        "source_relation": _relation(source),
        "target_relation": _relation(target),
        "actor_type": "USER",
        "actor_reference": "reviewer-1",
    }


def _app_with_repository(repository: InMemoryWorkflowRepository):
    app = create_app()
    app.dependency_overrides[get_workflow_repository] = lambda: repository
    return app


def _create(client: TestClient, *, key: str = "create-1", payload: dict | None = None):
    return client.post(
        BASE,
        json=payload or _create_payload(),
        headers={"Idempotency-Key": key, "X-Request-ID": "request-create"},
    )


def test_create_persists_and_survives_service_reconstruction_and_http_retrieval() -> None:
    repository = InMemoryWorkflowRepository()
    with TestClient(_app_with_repository(repository)) as client:
        created = _create(client)
        assert created.status_code == 201, created.text
        workflow_id = created.json()["workflow_id"]
        retrieved = client.get(f"{BASE}/{workflow_id}")

    reconstructed = WorkflowPersistenceService(repository).get_workflow(UUID(workflow_id))
    assert retrieved.status_code == 200
    assert retrieved.json() == created.json()
    assert str(reconstructed.workflow_id) == workflow_id
    assert reconstructed.source_relation.object_name == _create_payload()["source_relation"]["object_name"]


def test_create_and_transition_idempotency_concurrency_and_invalid_transition_errors() -> None:
    repository = InMemoryWorkflowRepository()
    with TestClient(_app_with_repository(repository)) as client:
        first = _create(client)
        replay = _create(client)
        assert replay.status_code == 201 and replay.json() == first.json()
        workflow_id = first.json()["workflow_id"]

        invalid = client.post(
            f"{BASE}/{workflow_id}/transitions",
            json={"expected_version": 1, "new_status": "MAPPING_APPROVED"},
            headers={"Idempotency-Key": "invalid-transition"},
        )
        discovered = client.post(
            f"{BASE}/{workflow_id}/transitions",
            json={"expected_version": 1, "new_status": "DISCOVERED"},
            headers={"Idempotency-Key": "discover"},
        )
        transition_replay = client.post(
            f"{BASE}/{workflow_id}/transitions",
            json={"expected_version": 1, "new_status": "DISCOVERED"},
            headers={"Idempotency-Key": "discover"},
        )
        stale = client.post(
            f"{BASE}/{workflow_id}/transitions",
            json={"expected_version": 1, "new_status": "MAPPING_PROPOSED"},
            headers={"Idempotency-Key": "stale"},
        )
        reused = client.post(
            f"{BASE}/{workflow_id}/transitions",
            json={"expected_version": 2, "new_status": "MAPPING_PROPOSED"},
            headers={"Idempotency-Key": "discover"},
        )

    assert invalid.status_code == 409 and invalid.json()["error"]["code"] == "INVALID_WORKFLOW_TRANSITION"
    assert discovered.status_code == 200 and discovered.json()["version"] == 2
    assert transition_replay.json() == discovered.json()
    assert stale.status_code == 409 and stale.json()["error"]["code"] == "WORKFLOW_VERSION_CONFLICT"
    assert reused.status_code == 409 and reused.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"
    assert len(repository.list_audit_events(UUID(workflow_id))) == 2


def test_typed_artifact_persistence_version_replay_listing_and_audit_order() -> None:
    repository = InMemoryWorkflowRepository()
    source, _ = _workflow_tables()
    command = {
        "artifact_type": "SOURCE_DISCOVERY",
        "expected_version": 1,
        "payload": _json_table(source),
        "actor_type": "SERVICE",
        "actor_reference": "schema-discovery",
    }
    with TestClient(_app_with_repository(repository)) as client:
        workflow_id = _create(client).json()["workflow_id"]
        first = client.post(
            f"{BASE}/{workflow_id}/artifacts",
            json=command,
            headers={"Idempotency-Key": "source-discovery", "X-Request-ID": "artifact-request"},
        )
        replay = client.post(
            f"{BASE}/{workflow_id}/artifacts",
            json=command,
            headers={"Idempotency-Key": "source-discovery"},
        )
        changed = deepcopy(command)
        changed["payload"]["comment"] = "changed canonical content"
        conflict = client.post(
            f"{BASE}/{workflow_id}/artifacts",
            json=changed,
            headers={"Idempotency-Key": "source-discovery"},
        )
        next_version = deepcopy(command)
        next_version["expected_version"] = 2
        second = client.post(
            f"{BASE}/{workflow_id}/artifacts",
            json=next_version,
            headers={"Idempotency-Key": "source-rediscovery"},
        )
        unsupported = client.post(
            f"{BASE}/{workflow_id}/artifacts",
            json={"artifact_type": "ARBITRARY_JSON", "expected_version": 3, "payload": {}},
            headers={"Idempotency-Key": "unsupported"},
        )
        artifacts = client.get(f"{BASE}/{workflow_id}/artifacts?offset=0&limit=10")
        audit = client.get(f"{BASE}/{workflow_id}/audit-events?offset=0&limit=10")

    assert first.status_code == 201, first.text
    assert first.json()["workflow"]["version"] == 2
    assert first.json()["artifact"]["artifact_version"] == 1
    assert replay.json() == first.json()
    assert conflict.status_code == 409 and conflict.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"
    assert second.status_code == 201 and second.json()["artifact"]["artifact_version"] == 2
    assert unsupported.status_code == 422
    assert len(artifacts.json()["items"]) == 2
    assert artifacts.json()["items"][0]["payload"]["object_name"] == source.object_name
    assert [item["sequence_number"] for item in audit.json()["items"]] == [1, 2, 3]
    assert [item["event_type"] for item in audit.json()["items"]] == ["WORKFLOW_CREATED", "ARTIFACT_APPENDED", "ARTIFACT_APPENDED"]
    assert audit.json()["items"][1]["request_id"] == "artifact-request"
    assert audit.json()["items"][1]["actor_reference"] == "schema-discovery"


def test_not_found_malformed_and_persistence_failures_are_stable_and_redacted() -> None:
    missing = UUID(int=999)
    repository = InMemoryWorkflowRepository()
    with TestClient(_app_with_repository(repository)) as client:
        not_found = client.get(f"{BASE}/{missing}", headers={"X-Request-ID": "missing-request"})
        malformed = client.post(BASE, json=_create_payload())
        repository.fail_audit = True
        failed = _create(client, key="failing-create")

    assert not_found.status_code == 404
    assert not_found.json()["error"] == {
        "code": "WORKFLOW_NOT_FOUND",
        "message": "The requested workflow is unavailable.",
        "request_id": "missing-request",
    }
    assert malformed.status_code == 422
    assert failed.status_code == 503 and failed.json()["error"]["code"] == "WORKFLOW_PERSISTENCE_FAILED"
    rendered = f"{not_found.text} {failed.text}".casefold()
    assert all(value not in rendered for value in ("postgresql://", "password", "select ", "traceback"))


def test_disabled_control_plane_has_no_hidden_fallback_repository() -> None:
    app = create_app(ApiSettings(control_plane=ControlPlaneConfig(dsn="", enabled=False)))
    with TestClient(app) as client:
        response = _create(client)
        assert app.state.workflow_repository is None
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "CONTROL_PLANE_UNAVAILABLE"
    assert app.state.workflow_repository is None


def test_lifespan_builds_closes_and_isolates_configured_repositories(monkeypatch) -> None:
    class ClosingRepository(InMemoryWorkflowRepository):
        def __init__(self):
            super().__init__()
            self.closed = False

        def close(self):
            self.closed = True

    repositories: list[ClosingRepository] = []

    def factory(config):
        assert config.enabled is True
        repository = ClosingRepository()
        repositories.append(repository)
        return repository

    app_module = importlib.import_module("api.app")
    monkeypatch.setattr(app_module, "build_workflow_repository", factory)
    settings = ApiSettings(control_plane=ControlPlaneConfig(dsn="postgresql://integration-placeholder"))
    first = create_app(settings)
    second = create_app(settings)
    with TestClient(first) as first_client, TestClient(second) as second_client:
        created = _create(first_client)
        workflow_id = created.json()["workflow_id"]
        isolated = second_client.get(f"{BASE}/{workflow_id}")
        assert first.state.workflow_repository is repositories[0]
        assert second.state.workflow_repository is repositories[1]

    assert isolated.status_code == 404
    assert len(repositories) == 2 and all(repository.closed for repository in repositories)
    assert first.state.workflow_repository is second.state.workflow_repository is None


def test_openapi_exposes_typed_durable_workflow_contract_without_secret_fields() -> None:
    schema = create_app().openapi()
    expected = {
        BASE,
        f"{BASE}/{{workflow_id}}",
        f"{BASE}/{{workflow_id}}/transitions",
        f"{BASE}/{{workflow_id}}/artifacts",
        f"{BASE}/{{workflow_id}}/audit-events",
    }
    assert expected.issubset(schema["paths"])
    assert schema["paths"][BASE]["post"]["parameters"][0]["name"] == "Idempotency-Key"
    rendered = str(schema).casefold()
    assert all(value not in rendered for value in ("dsn", "password", "connection_string", "raw_sql"))
