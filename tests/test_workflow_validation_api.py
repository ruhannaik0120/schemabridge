"""Focused Stage 6E tests for durable post-execution validation."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from fastapi.testclient import TestClient

from schemabridge.api.dependencies import (
    get_migration_execution_service,
    get_target_execution_registry,
    get_validation_execution_service,
)
from schemabridge.models.validation import MigrationValidationExecutionReport, MigrationValidationStatus, ValidationExecutionStatus
from schemabridge.models.mapping import SqlDialect
from schemabridge.persistence.errors import WorkflowPersistenceError
from schemabridge.services.migration_execution import TargetExecutionDisposition, TargetExecutionResult
from schemabridge.services.reconciliation import reconcile_validation_results
from schemabridge.services.validation_sql import compile_validation_sql
from schemabridge.target_execution import TargetExecutionAdapter, TargetExecutionRegistry
from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from tests.test_workflow_execution_api import FakeExecutor, _execution_payload, _ready
from tests.test_workflow_orchestration_api import _application, _create, _mutate
from tests.test_workflow_persistence_api import BASE


class FakeValidationExecutor:
    def __init__(self, *, mismatch: bool = False, fail: bool = False, reported_plan_version: int | None = None, started: Event | None = None, release: Event | None = None) -> None:
        self.mismatch = mismatch
        self.fail = fail
        self.reported_plan_version = reported_plan_version
        self.started = started
        self.release = release
        self.invocations = 0
        self.requests = []
        self.plans = []

    def resolve_dialects(self, **_kwargs):
        return SqlDialect.POSTGRESQL, SqlDialect.SNOWFLAKE

    def run(self, request):
        self.invocations += 1
        self.requests.append(request)
        plan = compile_validation_sql(
            request.approved_mapping_plan,
            source_schema=request.source_schema,
            source_table=request.source_table,
            target_database=request.target_database,
            target_schema=request.target_schema,
            target_table=request.target_table,
            source_dialect=SqlDialect.POSTGRESQL,
            target_dialect=SqlDialect.SNOWFLAKE,
        )
        self.plans.append(plan)
        if self.started is not None:
            self.started.set()
        if self.release is not None:
            assert self.release.wait(timeout=10)
        if self.fail:
            raise RuntimeError("password=hunter2 host=private.example SQL SELECT secret")
        source = {alias: 3 for alias in plan[0].metric_aliases}
        target = dict(source)
        if self.mismatch:
            target["row_count"] = 4
        report = reconcile_validation_results(
            plan[0],
            plan[1],
            approved_plan_version=(
                request.approved_mapping_plan.version
                if self.reported_plan_version is None
                else self.reported_plan_version
            ),
            source_metrics=source,
            target_metrics=target,
        )
        return MigrationValidationExecutionReport(
            source_profile_id=request.source_profile_id,
            target_profile_id=request.target_profile_id,
            source_sql_summary=plan[0],
            target_sql_summary=plan[1],
            validation_report=report,
            source_execution_status=ValidationExecutionStatus.SUCCEEDED,
            target_execution_status=ValidationExecutionStatus.SUCCEEDED,
        )


def _app(repository, migration_executor, validation_executor):
    app = _application(repository)
    app.dependency_overrides[get_migration_execution_service] = lambda: migration_executor
    app.dependency_overrides[get_validation_execution_service] = lambda: validation_executor
    if isinstance(migration_executor, TargetExecutionAdapter):
        app.dependency_overrides[get_target_execution_registry] = lambda: (
            TargetExecutionRegistry((migration_executor,))
        )
    return app


def _executed(client: TestClient, migration_executor: FakeExecutor | None = None):
    created, approved, preview = _ready(client)
    response = _mutate(
        client,
        f"{BASE}/{created['workflow_id']}/execute",
        _execution_payload(approved, preview),
        "execute",
    )
    assert response.status_code == 201, response.text
    return created, approved, preview, response.json()


def _payload(approved: dict, executed: dict) -> dict:
    return {
        "expected_version": executed["workflow"]["version"],
        "execution_evidence_artifact_version": executed["artifact"]["artifact_version"],
        "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
        "source_profile_id": "pg-source",
        "target_profile_id": "sf-target",
        "timeout_seconds": 15,
        "actor_type": "SERVICE",
        "actor_reference": "validation-runner",
    }


def test_validation_is_blocked_before_execution_and_rejects_sql_or_missing_evidence() -> None:
    repository = InMemoryWorkflowRepository()
    migration, validation = FakeExecutor(), FakeValidationExecutor()
    with TestClient(_app(repository, migration, validation)) as client:
        created = _create(client)
        blocked = _mutate(
            client,
            f"{BASE}/{created['workflow_id']}/validate",
            {
                "expected_version": created["version"],
                "execution_evidence_artifact_version": 1,
                "approved_mapping_artifact_version": 1,
                "source_profile_id": "pg-source",
                "target_profile_id": "sf-target",
            },
            "blocked",
        )
        arbitrary = _mutate(
            client,
            f"{BASE}/{created['workflow_id']}/validate",
            {
                "expected_version": created["version"],
                "execution_evidence_artifact_version": 1,
                "approved_mapping_artifact_version": 1,
                "source_profile_id": "pg-source",
                "target_profile_id": "sf-target",
                "sql": "DROP TABLE confidential",
            },
            "arbitrary",
        )
        client_evidence = _mutate(
            client,
            f"{BASE}/{created['workflow_id']}/artifacts",
            {
                "artifact_type": "VALIDATION_EXECUTION_REPORT",
                "expected_version": created["version"],
                "payload": {},
            },
            "client-evidence",
        )
        _, approved, _, executed = _executed(client)
        missing = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", _payload(approved, executed) | {"execution_evidence_artifact_version": 999}, "missing")

    assert blocked.status_code == 409 and blocked.json()["error"]["code"] == "WORKFLOW_NOT_READY_FOR_VALIDATION"
    assert arbitrary.status_code == 422
    assert client_evidence.status_code == 422
    assert missing.status_code == 409 and missing.json()["error"]["code"] == "REQUIRED_WORKFLOW_ARTIFACT_MISSING"
    assert validation.invocations == 0


def test_success_persists_plan_reconciliation_evidence_audits_and_exact_replay() -> None:
    repository = InMemoryWorkflowRepository()
    migration, validation = FakeExecutor(), FakeValidationExecutor()
    with TestClient(_app(repository, migration, validation)) as client:
        created, approved, _, executed = _executed(client)
        payload = _payload(approved, executed)
        first = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload, "validate")
        replay = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload, "validate")
        artifacts = client.get(f"{BASE}/{created['workflow_id']}/artifacts?limit=20").json()["items"]
        audit = client.get(f"{BASE}/{created['workflow_id']}/audit-events?limit=40").json()["items"]

    assert first.status_code == replay.status_code == 201
    assert first.json() == replay.json()
    body = first.json()
    assert body["workflow"]["status"] == "VALIDATED"
    assert body["run"]["status"] == "SUCCEEDED"
    assert body["result"]["validation_report"]["status"] == "PASSED"
    assert body["plan_artifact"]["artifact_type"] == "VALIDATION_PREVIEW"
    assert body["evidence_artifact"]["artifact_type"] == "VALIDATION_EXECUTION_REPORT"
    assert [item["artifact_type"] for item in artifacts[-2:]] == ["VALIDATION_PREVIEW", "VALIDATION_EXECUTION_REPORT"]
    assert validation.invocations == 1 and migration.invocations == 1
    assert validation.requests[0].source_profile_id == "pg-source"
    assert validation.requests[0].target_profile_id == "sf-target"
    assert validation.plans[0][0].sql.lstrip().upper().startswith("SELECT")
    assert validation.plans[0][1].sql.lstrip().upper().startswith("SELECT")
    assert all(token not in (validation.plans[0][0].sql + validation.plans[0][1].sql).upper() for token in ("INSERT ", "UPDATE ", "DELETE ", "BEGIN ", "COMMIT ", "ROLLBACK "))
    assert len(audit) >= 17


def test_wrong_approved_plan_version_is_quarantined_not_persisted() -> None:
    repository = InMemoryWorkflowRepository()
    migration, validation = FakeExecutor(), FakeValidationExecutor(reported_plan_version=2)
    with TestClient(_app(repository, migration, validation)) as client:
        created, approved, _, executed = _executed(client)
        response = _mutate(
            client,
            f"{BASE}/{created['workflow_id']}/validate",
            _payload(approved, executed),
            "wrong-plan-version",
        )
        workflow = client.get(f"{BASE}/{created['workflow_id']}").json()
        artifacts = client.get(
            f"{BASE}/{created['workflow_id']}/artifacts?limit=20"
        ).json()["items"]

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "VALIDATION_OUTCOME_UNCERTAIN"
    assert workflow["status"] == "VALIDATION_RECOVERY_REQUIRED"
    assert artifacts[-1]["artifact_type"] == "VALIDATION_PREVIEW"
    assert all(
        item["artifact_type"] != "VALIDATION_EXECUTION_REPORT"
        for item in artifacts
    )


def test_mismatch_is_review_required_and_not_a_connector_failure() -> None:
    repository = InMemoryWorkflowRepository()
    migration, validation = FakeExecutor(), FakeValidationExecutor(mismatch=True)
    with TestClient(_app(repository, migration, validation)) as client:
        created, approved, _, executed = _executed(client)
        response = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", _payload(approved, executed), "mismatch")

    assert response.status_code == 201
    assert response.json()["workflow"]["status"] == "VALIDATION_REVIEW_REQUIRED"
    assert response.json()["run"]["status"] == "REVIEW_REQUIRED"
    assert response.json()["result"]["validation_report"]["status"] == "FAILED"
    assert response.json()["result"]["validation_report"]["mismatched_count"] == 1


def test_connector_failure_is_sanitized_quarantined_and_never_replayed() -> None:
    repository = InMemoryWorkflowRepository()
    migration, validation = FakeExecutor(), FakeValidationExecutor(fail=True)
    with TestClient(_app(repository, migration, validation)) as client:
        created, approved, _, executed = _executed(client)
        payload = _payload(approved, executed)
        first = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload, "failure")
        replay = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload, "failure")
        workflow = client.get(f"{BASE}/{created['workflow_id']}").json()
        artifacts = client.get(f"{BASE}/{created['workflow_id']}/artifacts?limit=20").json()["items"]
        audit = client.get(f"{BASE}/{created['workflow_id']}/audit-events?limit=40").json()["items"]
        combined = json.dumps({"responses": [first.json(), replay.json()], "artifacts": artifacts, "audit": audit}).casefold()

    assert first.status_code == replay.status_code == 502
    assert first.json()["error"]["code"] == "VALIDATION_OUTCOME_UNCERTAIN"
    assert workflow["status"] == "VALIDATION_RECOVERY_REQUIRED"
    assert validation.invocations == 1 and migration.invocations == 1
    assert artifacts[-1]["artifact_type"] == "VALIDATION_PREVIEW"
    assert all(secret not in combined for secret in ("hunter2", "private.example", "select secret"))


def test_idempotency_version_stale_profile_and_concurrent_claims_are_enforced() -> None:
    repository = InMemoryWorkflowRepository()
    started, release = Event(), Event()
    migration, validation = FakeExecutor(), FakeValidationExecutor(started=started, release=release)
    with TestClient(_app(repository, migration, validation)) as client:
        created, approved, _, executed = _executed(client)
        payload = _payload(approved, executed)
        wrong_profile = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload | {"source_profile_id": "other-source"}, "wrong-profile")
        stale_evidence = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload | {"execution_evidence_artifact_version": approved["artifact"]["artifact_version"]}, "stale-evidence")
        with TestClient(_app(repository, migration, validation)) as second:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(_mutate, client, f"{BASE}/{created['workflow_id']}/validate", payload, "validate")
                assert started.wait(timeout=10)
                current = second.get(f"{BASE}/{created['workflow_id']}").json()
                concurrent = _mutate(second, f"{BASE}/{created['workflow_id']}/validate", payload | {"expected_version": current["version"]}, "concurrent")
                release.set()
                first = future.result(timeout=10)
        changed = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload | {"timeout_seconds": 14}, "validate")
        stale_version = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload, "stale-version")

    assert wrong_profile.status_code == 409 and wrong_profile.json()["error"]["code"] == "WORKFLOW_NOT_READY_FOR_VALIDATION"
    assert stale_evidence.status_code == 409 and stale_evidence.json()["error"]["code"] == "STALE_WORKFLOW_ARTIFACT"
    assert concurrent.status_code == 409 and concurrent.json()["error"]["code"] == "VALIDATION_ALREADY_IN_PROGRESS"
    assert first.status_code == 201
    assert changed.status_code == 409 and changed.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"
    assert stale_version.status_code == 409 and stale_version.json()["error"]["code"] == "WORKFLOW_VERSION_CONFLICT"
    assert validation.invocations == 1 and migration.invocations == 1


def test_successful_replay_reconstructs_after_service_restart_without_queries() -> None:
    repository = InMemoryWorkflowRepository()
    migration, validation = FakeExecutor(), FakeValidationExecutor()
    with TestClient(_app(repository, migration, validation)) as client:
        created, approved, _, executed = _executed(client)
        payload = _payload(approved, executed)
        first = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload, "restart")
    replacement_migration, replacement_validation = FakeExecutor(), FakeValidationExecutor()
    with TestClient(_app(repository, replacement_migration, replacement_validation)) as client:
        replay = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload, "restart")
    assert replay.status_code == 201 and replay.json() == first.json()
    assert replacement_validation.invocations == 0 and replacement_migration.invocations == 0


def test_interrupted_claim_before_queries_resumes_once() -> None:
    repository = InMemoryWorkflowRepository()
    migration, validation = FakeExecutor(), FakeValidationExecutor()
    with TestClient(_app(repository, migration, validation)) as client:
        created, approved, _, executed = _executed(client)
        payload = _payload(approved, executed)
        original = repository.mark_validation_running
        repository.mark_validation_running = lambda *_args, **_kwargs: (_ for _ in ()).throw(WorkflowPersistenceError())
        interrupted = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload, "resume")
        repository.mark_validation_running = original
        resumed = _mutate(client, f"{BASE}/{created['workflow_id']}/validate", payload, "resume")

    assert interrupted.status_code == 503
    assert interrupted.json()["error"]["code"] == "WORKFLOW_PERSISTENCE_FAILED"
    assert resumed.status_code == 201 and resumed.json()["workflow"]["status"] == "VALIDATED"
    assert validation.invocations == 1 and migration.invocations == 1


def test_execution_recovery_required_cannot_enter_validation() -> None:
    repository = InMemoryWorkflowRepository()
    migration = FakeExecutor([TargetExecutionResult(TargetExecutionDisposition.OUTCOME_UNCERTAIN, failure_category="TARGET_OUTCOME_UNCERTAIN")])
    validation = FakeValidationExecutor()
    with TestClient(_app(repository, migration, validation)) as client:
        created, approved, preview = _ready(client)
        execution = _mutate(client, f"{BASE}/{created['workflow_id']}/execute", _execution_payload(approved, preview), "uncertain-execution")
        workflow = client.get(f"{BASE}/{created['workflow_id']}").json()
        evidence = client.get(f"{BASE}/{created['workflow_id']}/artifacts?limit=20").json()["items"][-1]
        blocked = _mutate(
            client,
            f"{BASE}/{created['workflow_id']}/validate",
            {
                "expected_version": workflow["version"],
                "execution_evidence_artifact_version": evidence["artifact_version"],
                "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                "source_profile_id": "pg-source",
                "target_profile_id": "sf-target",
            },
            "blocked-recovery",
        )

    assert execution.status_code == 409 and workflow["status"] == "EXECUTION_RECOVERY_REQUIRED"
    assert blocked.status_code == 409 and blocked.json()["error"]["code"] == "EXECUTION_OUTCOME_UNCERTAIN"
    assert validation.invocations == 0 and migration.invocations == 1
