"""Credential-free end-to-end test through the production workflow boundaries."""

from fastapi.testclient import TestClient

from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from tests.test_workflow_execution_api import FakeExecutor
from tests.test_workflow_persistence_api import BASE
from tests.test_workflow_validation_api import (
    FakeValidationExecutor,
    _app,
    _executed,
    _mutate,
    _payload,
)


def test_complete_durable_workflow_replays_without_repeating_remote_boundaries() -> None:
    repository = InMemoryWorkflowRepository()
    migration = FakeExecutor()
    validation = FakeValidationExecutor()

    with TestClient(_app(repository, migration, validation)) as client:
        created, approved, preview, executed = _executed(client)
        workflow_id = created["workflow_id"]
        validation_payload = _payload(approved, executed)
        validated = _mutate(
            client,
            f"{BASE}/{workflow_id}/validate",
            validation_payload,
            "end-to-end-validation",
        )
        replay = _mutate(
            client,
            f"{BASE}/{workflow_id}/validate",
            validation_payload,
            "end-to-end-validation",
        )
        retrieved = client.get(f"{BASE}/{workflow_id}")
        artifacts = client.get(f"{BASE}/{workflow_id}/artifacts?limit=50").json()["items"]
        audit = client.get(f"{BASE}/{workflow_id}/audit-events?limit=50").json()["items"]

    assert validated.status_code == replay.status_code == 201
    assert retrieved.status_code == 200
    assert validated.json() == replay.json()
    assert retrieved.json() == validated.json()["workflow"]
    assert retrieved.json()["status"] == "VALIDATED"
    assert preview["result"]["statement_type"] == "INSERT_SELECT"
    assert executed["result"]["status"] == "SUCCEEDED"
    assert validated.json()["result"]["validation_report"]["status"] == "PASSED"
    assert migration.invocations == validation.invocations == 1

    assert [item["artifact_type"] for item in artifacts] == [
        "SOURCE_DISCOVERY",
        "TARGET_DISCOVERY",
        "MAPPING_PLAN",
        "APPROVED_MAPPING_PLAN",
        "TRANSFORMATION_PREVIEW",
        "EXECUTION_EVIDENCE",
        "VALIDATION_PREVIEW",
        "VALIDATION_EXECUTION_REPORT",
    ]
    assert [item["sequence_number"] for item in audit] == list(range(1, 18))
    assert [item["event_type"] for item in audit] == [
        "WORKFLOW_CREATED",
        "ARTIFACT_APPENDED",
        "ARTIFACT_APPENDED",
        "STATUS_CHANGED",
        "ARTIFACT_APPENDED",
        "STATUS_CHANGED",
        "ARTIFACT_APPENDED",
        "STATUS_CHANGED",
        "ARTIFACT_APPENDED",
        "STATUS_CHANGED",
        "STATUS_CHANGED",
        "ARTIFACT_APPENDED",
        "STATUS_CHANGED",
        "ARTIFACT_APPENDED",
        "STATUS_CHANGED",
        "ARTIFACT_APPENDED",
        "STATUS_CHANGED",
    ]

    replacement_migration = FakeExecutor()
    replacement_validation = FakeValidationExecutor()
    with TestClient(_app(repository, replacement_migration, replacement_validation)) as client:
        reconstructed = client.get(f"{BASE}/{workflow_id}")
        reconstructed_replay = _mutate(
            client,
            f"{BASE}/{workflow_id}/validate",
            validation_payload,
            "end-to-end-validation",
        )

    assert reconstructed.status_code == 200
    assert reconstructed.json() == retrieved.json()
    assert reconstructed_replay.status_code == 201
    assert reconstructed_replay.json() == validated.json()
    assert replacement_migration.invocations == replacement_validation.invocations == 0
