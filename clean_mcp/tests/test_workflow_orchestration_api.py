"""End-to-end HTTP tests for durable Stage 6C planning orchestration."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from uuid import UUID

from fastapi.testclient import TestClient

from api.app import create_app
from api.dependencies import get_schema_discovery_service, get_workflow_repository
from models.workflow import MigrationAuditEventType, MigrationWorkflowStatus
from persistence.errors import WorkflowPersistenceError
from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from tests.test_migration_api import _json_table, _workflow_tables
from tests.test_workflow_persistence_api import BASE, _create_payload


def _application(
    repository: InMemoryWorkflowRepository,
    *,
    failing_discovery: bool = False,
    changing_discovery: bool = False,
):
    source, target = _workflow_tables()
    discovery_calls = 0

    class Connector:
        def __init__(self, metadata):
            self.metadata = metadata

        def get_table_metadata(self, **_kwargs):
            nonlocal discovery_calls
            if failing_discovery:
                raise RuntimeError("driver SQL and password must remain private")
            discovery_calls += 1
            if changing_discovery:
                return replace(self.metadata, comment=f"discovery-call-{discovery_calls}")
            return self.metadata

    def resolver(profile_id: str):
        return Connector(source if profile_id == "pg-source" else target)

    app = create_app()
    app.dependency_overrides[get_workflow_repository] = lambda: repository
    app.dependency_overrides[get_schema_discovery_service] = lambda: resolver
    return app


def _mutate(client: TestClient, path: str, payload: dict, key: str):
    return client.post(
        path,
        json=payload,
        headers={"Idempotency-Key": key, "X-Request-ID": f"request-{key}"},
    )


def _create(client: TestClient) -> dict:
    response = _mutate(client, BASE, _create_payload(), "create")
    assert response.status_code == 201, response.text
    return response.json()


def _discover_pair(client: TestClient, workflow: dict) -> tuple[dict, dict]:
    workflow_id = workflow["workflow_id"]
    source = _mutate(
        client,
        f"{BASE}/{workflow_id}/discover-source",
        {"expected_version": workflow["version"], "actor_type": "SERVICE"},
        "discover-source",
    )
    assert source.status_code == 201, source.text
    target = _mutate(
        client,
        f"{BASE}/{workflow_id}/discover-target",
        {"expected_version": source.json()["workflow"]["version"], "actor_type": "SERVICE"},
        "discover-target",
    )
    assert target.status_code == 201, target.text
    return source.json(), target.json()


def _mapping(client: TestClient, workflow_id: str, expected_version: int) -> dict:
    response = _mutate(
        client,
        f"{BASE}/{workflow_id}/mapping-proposals",
        {"expected_version": expected_version, "actor_type": "SERVICE"},
        "mapping-proposal",
    )
    assert response.status_code == 201, response.text
    return response.json()


def _decisions() -> list[dict]:
    return [
        {
            "source_column": "first_name",
            "target_column": "full_name",
            "status": "APPROVED",
            "reviewer_note": "Reviewed name composition.",
            "transformation": {
                "expression_type": "CONCAT",
                "source_columns": ["first_name", "last_name"],
                "separator": " ",
            },
        },
        {"source_column": "last_name", "status": "REJECTED"},
        {
            "source_column": "age",
            "target_column": "age",
            "status": "APPROVED",
            "transformation": {
                "expression_type": "DIRECT_COPY",
                "source_columns": ["age"],
            },
        },
    ]


def _approve(client: TestClient, workflow_id: str, expected_version: int, mapping_version: int, *, key: str = "mapping-approval") -> dict:
    response = _mutate(
        client,
        f"{BASE}/{workflow_id}/mapping-approvals",
        {
            "expected_version": expected_version,
            "mapping_artifact_version": mapping_version,
            "decisions": _decisions(),
            "actor_type": "USER",
            "actor_reference": "reviewer-1",
        },
        key,
    )
    assert response.status_code == 201, response.text
    return response.json()


def test_complete_planning_workflow_automatically_persists_artifacts_states_and_audit() -> None:
    repository = InMemoryWorkflowRepository()
    with TestClient(_application(repository, changing_discovery=True)) as client:
        created = _create(client)
        source, target = _discover_pair(client, created)
        workflow_id = created["workflow_id"]
        proposed = _mapping(client, workflow_id, target["workflow"]["version"])
        approved = _approve(
            client,
            workflow_id,
            proposed["workflow"]["version"],
            proposed["artifact"]["artifact_version"],
        )
        preview = _mutate(
            client,
            f"{BASE}/{workflow_id}/transformation-previews",
            {
                "expected_version": approved["workflow"]["version"],
                "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                "staging_database": "stage",
                "staging_schema": "landing",
                "staging_table": "source_people",
                "statement_type": "SELECT",
                "actor_type": "SERVICE",
            },
            "transformation-preview",
        )
        artifacts = client.get(f"{BASE}/{workflow_id}/artifacts?limit=20").json()["items"]
        audit = client.get(f"{BASE}/{workflow_id}/audit-events?limit=20").json()["items"]

    assert source["workflow"]["status"] == "DRAFT"
    assert target["workflow"]["status"] == "DISCOVERED"
    assert proposed["workflow"]["status"] == "MAPPING_PROPOSED"
    assert approved["workflow"]["status"] == "MAPPING_APPROVED"
    assert preview.status_code == 201, preview.text
    assert preview.json()["workflow"]["status"] == "EXECUTION_READY"
    assert preview.json()["result"]["preview_only"] is True
    assert [item["artifact_type"] for item in artifacts] == [
        "SOURCE_DISCOVERY",
        "TARGET_DISCOVERY",
        "MAPPING_PLAN",
        "APPROVED_MAPPING_PLAN",
        "TRANSFORMATION_PREVIEW",
    ]
    assert [item["artifact_version"] for item in artifacts] == [1, 2, 3, 4, 5]
    assert [item["sequence_number"] for item in audit] == list(range(1, 11))
    assert sum(item["event_type"] == "ARTIFACT_APPENDED" for item in audit) == 5
    assert audit[-1]["event_type"] == "STATUS_CHANGED"


def test_idempotent_replay_conflicting_reuse_and_optimistic_version_are_enforced() -> None:
    repository = InMemoryWorkflowRepository()
    with TestClient(_application(repository)) as client:
        created = _create(client)
        workflow_id = created["workflow_id"]
        command = {"expected_version": 1, "actor_type": "SERVICE"}
        first = _mutate(client, f"{BASE}/{workflow_id}/discover-source", command, "source")
        replay = _mutate(client, f"{BASE}/{workflow_id}/discover-source", command, "source")
        stale = _mutate(
            client,
            f"{BASE}/{workflow_id}/discover-target",
            {"expected_version": 1, "actor_type": "SERVICE"},
            "target-stale",
        )
        changed = _mutate(
            client,
            f"{BASE}/{workflow_id}/discover-source",
            {"expected_version": 2, "actor_type": "SERVICE"},
            "source",
        )
        artifacts = client.get(f"{BASE}/{workflow_id}/artifacts").json()["items"]
        audit = client.get(f"{BASE}/{workflow_id}/audit-events").json()["items"]

    assert first.status_code == replay.status_code == 201
    assert replay.json()["artifact"] == first.json()["artifact"]
    assert replay.json()["result"] == first.json()["result"]
    assert stale.status_code == 409 and stale.json()["error"]["code"] == "WORKFLOW_VERSION_CONFLICT"
    assert changed.status_code == 409 and changed.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"
    assert len(artifacts) == 1 and len(audit) == 2


def test_approval_replay_survives_later_artifact_versions_without_duplication() -> None:
    repository = InMemoryWorkflowRepository()
    with TestClient(_application(repository)) as client:
        created = _create(client)
        _, target = _discover_pair(client, created)
        workflow_id = created["workflow_id"]
        proposed = _mapping(client, workflow_id, target["workflow"]["version"])
        approved = _approve(
            client,
            workflow_id,
            proposed["workflow"]["version"],
            proposed["artifact"]["artifact_version"],
        )
        later = _mutate(
            client,
            f"{BASE}/{workflow_id}/artifacts",
            {
                "artifact_type": "MAPPING_PLAN",
                "expected_version": approved["workflow"]["version"],
                "payload": proposed["result"],
            },
            "later-mapping",
        )
        replay = _approve(
            client,
            workflow_id,
            proposed["workflow"]["version"],
            proposed["artifact"]["artifact_version"],
        )
        artifacts = client.get(f"{BASE}/{workflow_id}/artifacts").json()["items"]

    assert later.status_code == 201
    assert replay["artifact"] == approved["artifact"]
    assert replay["result"] == approved["result"]
    assert len(artifacts) == 5


def test_approval_rejects_missing_and_stale_mapping_references_and_preview_requires_approval() -> None:
    repository = InMemoryWorkflowRepository()
    source_table, _ = _workflow_tables()
    with TestClient(_application(repository)) as client:
        created = _create(client)
        _, target = _discover_pair(client, created)
        workflow_id = created["workflow_id"]
        proposed = _mapping(client, workflow_id, target["workflow"]["version"])
        missing = _mutate(
            client,
            f"{BASE}/{workflow_id}/mapping-approvals",
            {
                "expected_version": proposed["workflow"]["version"],
                "mapping_artifact_version": 999,
                "decisions": _decisions(),
            },
            "missing-mapping",
        )
        blocked = _mutate(
            client,
            f"{BASE}/{workflow_id}/transformation-previews",
            {
                "expected_version": proposed["workflow"]["version"],
                "approved_mapping_artifact_version": proposed["artifact"]["artifact_version"],
                "staging_database": "stage",
                "staging_schema": "landing",
                "staging_table": "source",
                "statement_type": "SELECT",
            },
            "blocked-preview",
        )
        duplicate_plan = _mutate(
            client,
            f"{BASE}/{workflow_id}/artifacts",
            {
                "artifact_type": "MAPPING_PLAN",
                "expected_version": proposed["workflow"]["version"],
                "payload": proposed["result"],
            },
            "new-mapping-version",
        )
        stale = _mutate(
            client,
            f"{BASE}/{workflow_id}/mapping-approvals",
            {
                "expected_version": duplicate_plan.json()["workflow"]["version"],
                "mapping_artifact_version": proposed["artifact"]["artifact_version"],
                "decisions": _decisions(),
            },
            "stale-mapping",
        )

    assert missing.status_code == 409 and missing.json()["error"]["code"] == "REQUIRED_WORKFLOW_ARTIFACT_MISSING"
    assert blocked.status_code == 409 and blocked.json()["error"]["code"] == "MAPPING_APPROVAL_REQUIRED"
    assert duplicate_plan.status_code == 201, duplicate_plan.text
    assert stale.status_code == 409 and stale.json()["error"]["code"] == "STALE_WORKFLOW_ARTIFACT"
    assert source_table.object_name not in f"{missing.text} {stale.text}"


def test_connector_and_atomic_transition_failure_do_not_partially_advance_workflow(monkeypatch) -> None:
    failing_repository = InMemoryWorkflowRepository()
    with TestClient(_application(failing_repository, failing_discovery=True)) as client:
        created = _create(client)
        workflow_id = created["workflow_id"]
        failed = _mutate(
            client,
            f"{BASE}/{workflow_id}/discover-source",
            {"expected_version": 1},
            "failed-source",
        )
        current = client.get(f"{BASE}/{workflow_id}").json()
        artifacts = client.get(f"{BASE}/{workflow_id}/artifacts").json()["items"]
    assert failed.status_code == 502 and failed.json()["error"]["code"] == "WORKFLOW_DISCOVERY_FAILED"
    assert current["status"] == "DRAFT" and current["version"] == 1 and artifacts == []
    assert "password" not in failed.text.casefold() and "driver" not in failed.text.casefold()

    repository = InMemoryWorkflowRepository()
    with TestClient(_application(repository)) as client:
        created = _create(client)
        source, _ = _discover_pair_source_only(client, created)
        original_event = repository._event

        def fail_transition_event(event):
            if event.event_type is MigrationAuditEventType.STATUS_CHANGED:
                raise WorkflowPersistenceError()
            return original_event(event)

        monkeypatch.setattr(repository, "_event", fail_transition_event)
        target = _mutate(
            client,
            f"{BASE}/{created['workflow_id']}/discover-target",
            {"expected_version": source["workflow"]["version"]},
            "target-atomic-failure",
        )
        current = client.get(f"{BASE}/{created['workflow_id']}").json()
        artifacts = client.get(f"{BASE}/{created['workflow_id']}/artifacts").json()["items"]
        audit = client.get(f"{BASE}/{created['workflow_id']}/audit-events").json()["items"]
    assert target.status_code == 503
    assert current["status"] == "DRAFT" and current["version"] == 2
    assert [item["artifact_type"] for item in artifacts] == ["SOURCE_DISCOVERY"]
    assert [item["event_type"] for item in audit] == ["WORKFLOW_CREATED", "ARTIFACT_APPENDED"]


def _discover_pair_source_only(client: TestClient, workflow: dict) -> tuple[dict, None]:
    source = _mutate(
        client,
        f"{BASE}/{workflow['workflow_id']}/discover-source",
        {"expected_version": workflow["version"]},
        "source-before-atomic-failure",
    )
    assert source.status_code == 201, source.text
    return source.json(), None


def test_reconstructed_application_continues_from_persisted_artifacts_and_stateless_routes_remain() -> None:
    repository = InMemoryWorkflowRepository()
    first_app = _application(repository)
    with TestClient(first_app) as client:
        created = _create(client)
        source, _ = _discover_pair_source_only(client, created)

    second_app = _application(repository)
    with TestClient(second_app) as client:
        target = _mutate(
            client,
            f"{BASE}/{created['workflow_id']}/discover-target",
            {"expected_version": source["workflow"]["version"]},
            "target-after-reconstruction",
        )
        proposed = _mapping(
            client, created["workflow_id"], target.json()["workflow"]["version"]
        )
        source_table, target_table = _workflow_tables()
        stateless = client.post(
            "/api/v1/migrations/mappings/suggest",
            json={"source": _json_table(source_table), "target": _json_table(target_table)},
        )

    assert target.status_code == 201 and target.json()["workflow"]["status"] == "DISCOVERED"
    assert proposed["artifact"]["artifact_type"] == "MAPPING_PLAN"
    assert stateless.status_code == 200
    assert repository.get_workflow(UUID(created["workflow_id"])).status is MigrationWorkflowStatus.MAPPING_PROPOSED
