"""Focused Stage 6D API tests for safe durable migration execution."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Event
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from api.dependencies import get_migration_execution_service
from models.mapping import GeneratedTransformationSql, TransformationStatementType
from models.execution import MigrationExecutionAttempt, MigrationExecutionAttemptStatus
from models.workflow import AuditActorType
from persistence.serialization import request_hash
from services.migration_execution import (
    PreparedMigrationTarget,
    ProfileBoundMigrationExecutionService,
    TargetExecutionDisposition,
    TargetExecutionResult,
)
from services.workflow_execution import WorkflowExecutionOrchestrator
from services.workflow_persistence import WorkflowPersistenceService
from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from tests.test_workflow_orchestration_api import (
    _application,
    _approve,
    _create,
    _discover_pair,
    _mapping,
    _mutate,
)
from tests.test_workflow_persistence_api import BASE


class FakeExecutor:
    def __init__(
        self,
        outcomes: list[TargetExecutionResult] | None = None,
        *,
        started: Event | None = None,
        release: Event | None = None,
    ) -> None:
        self.outcomes = list(
            outcomes
            or [TargetExecutionResult(TargetExecutionDisposition.SUCCEEDED, 3)]
        )
        self.started = started
        self.release = release
        self.invocations = 0
        self.prepared_profiles: list[str] = []
        self.previews: list[GeneratedTransformationSql] = []

    @staticmethod
    def validate_preview(preview: GeneratedTransformationSql) -> None:
        assert preview.statement_type is TransformationStatementType.INSERT_SELECT

    def prepare(
        self,
        profile_id: str,
        *,
        target_database: str | None,
        target_system: str,
        timeout_seconds: int | None,
    ) -> PreparedMigrationTarget:
        assert target_system == "snowflake"
        assert target_database is not None
        self.prepared_profiles.append(profile_id)
        return PreparedMigrationTarget(
            profile_id,
            target_database,
            "snowflake",
            min(timeout_seconds or 30, 30),
            self,
        )

    def execute(
        self,
        _target: PreparedMigrationTarget,
        preview: GeneratedTransformationSql,
    ) -> TargetExecutionResult:
        self.invocations += 1
        self.previews.append(preview)
        if self.started is not None:
            self.started.set()
        if self.release is not None:
            assert self.release.wait(timeout=10)
        return self.outcomes.pop(0)


def _app(repository: InMemoryWorkflowRepository, executor: object):
    app = _application(repository)
    app.dependency_overrides[get_migration_execution_service] = lambda: executor
    return app


def _ready(client: TestClient) -> tuple[dict, dict, dict]:
    created = _create(client)
    _, target = _discover_pair(client, created)
    proposed = _mapping(
        client, created["workflow_id"], target["workflow"]["version"]
    )
    approved = _approve(
        client,
        created["workflow_id"],
        proposed["workflow"]["version"],
        proposed["artifact"]["artifact_version"],
    )
    preview = _mutate(
        client,
        f"{BASE}/{created['workflow_id']}/transformation-previews",
        {
            "expected_version": approved["workflow"]["version"],
            "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
            "staging_database": "stage",
            "staging_schema": "landing",
            "staging_table": "source_people",
            "statement_type": "INSERT_SELECT",
            "actor_type": "SERVICE",
        },
        "execution-preview",
    )
    assert preview.status_code == 201, preview.text
    return created, approved, preview.json()


def _execution_payload(approved: dict, preview: dict) -> dict:
    return {
        "expected_version": preview["workflow"]["version"],
        "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
        "transformation_preview_artifact_version": preview["artifact"]["artifact_version"],
        "target_profile_id": "sf-target",
        "timeout_seconds": 15,
        "actor_type": "SERVICE",
        "actor_reference": "migration-runner",
    }


def test_success_executes_persisted_compiler_output_once_and_reconstructs_after_restart() -> None:
    repository = InMemoryWorkflowRepository()
    executor = FakeExecutor()
    with TestClient(_app(repository, executor)) as client:
        created, approved, preview = _ready(client)
        workflow_id = created["workflow_id"]
        payload = _execution_payload(approved, preview)
        bypass = _mutate(
            client,
            f"{BASE}/{workflow_id}/transitions",
            {
                "expected_version": preview["workflow"]["version"],
                "new_status": "EXECUTING",
            },
            "bypass-execution-claim",
        )
        first = _mutate(client, f"{BASE}/{workflow_id}/execute", payload, "execute")
        replay = _mutate(client, f"{BASE}/{workflow_id}/execute", payload, "execute")
        artifacts = client.get(f"{BASE}/{workflow_id}/artifacts?limit=20").json()["items"]
        audit = client.get(f"{BASE}/{workflow_id}/audit-events?limit=30").json()["items"]

    assert bypass.status_code == 409
    assert bypass.json()["error"]["code"] == "INVALID_WORKFLOW_TRANSITION"
    assert first.status_code == replay.status_code == 201
    assert first.json() == replay.json()
    assert executor.invocations == 1
    assert executor.prepared_profiles == ["sf-target"]
    assert executor.previews[0].sql == preview["result"]["sql"]
    assert first.json()["workflow"]["status"] == "EXECUTED"
    assert first.json()["result"]["status"] == "SUCCEEDED"
    assert first.json()["result"]["transaction_outcome"] == "COMMITTED"
    assert first.json()["result"]["affected_rows"] == 3
    assert artifacts[-1]["artifact_type"] == "EXECUTION_EVIDENCE"
    assert len(artifacts) == 6 and len(audit) == 13

    replacement_executor = FakeExecutor()
    with TestClient(_app(repository, replacement_executor)) as client:
        reconstructed = _mutate(
            client, f"{BASE}/{workflow_id}/execute", payload, "execute"
        )
    assert reconstructed.status_code == 201
    assert reconstructed.json() == first.json()
    assert replacement_executor.invocations == 0


def test_execution_requires_approval_and_a_persisted_preview_and_accepts_no_sql() -> None:
    repository = InMemoryWorkflowRepository()
    executor = FakeExecutor()
    with TestClient(_app(repository, executor)) as client:
        created = _create(client)
        workflow_id = created["workflow_id"]
        blocked = _mutate(
            client,
            f"{BASE}/{workflow_id}/execute",
            {
                "expected_version": created["version"],
                "approved_mapping_artifact_version": 1,
                "transformation_preview_artifact_version": 2,
                "target_profile_id": "sf-target",
            },
            "blocked",
        )
        _, target = _discover_pair(client, created)
        proposed = _mapping(client, workflow_id, target["workflow"]["version"])
        approved = _approve(
            client,
            workflow_id,
            proposed["workflow"]["version"],
            proposed["artifact"]["artifact_version"],
        )
        missing = _mutate(
            client,
            f"{BASE}/{workflow_id}/execute",
            {
                "expected_version": approved["workflow"]["version"],
                "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                "transformation_preview_artifact_version": 999,
                "target_profile_id": "sf-target",
            },
            "missing",
        )
        arbitrary = _mutate(
            client,
            f"{BASE}/{workflow_id}/execute",
            {
                "expected_version": approved["workflow"]["version"],
                "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                "transformation_preview_artifact_version": 999,
                "target_profile_id": "sf-target",
                "sql": "DROP TABLE secret_table",
            },
            "arbitrary",
        )

    assert blocked.status_code == 409
    assert blocked.json()["error"]["code"] == "MAPPING_APPROVAL_REQUIRED"
    assert missing.status_code == 409
    assert missing.json()["error"]["code"] == "REQUIRED_WORKFLOW_ARTIFACT_MISSING"
    assert arbitrary.status_code == 422
    assert executor.invocations == 0


def test_stale_references_and_altered_persisted_sql_are_rejected_before_execution() -> None:
    repository = InMemoryWorkflowRepository()
    executor = FakeExecutor()
    with TestClient(_app(repository, executor)) as client:
        created, approved, preview = _ready(client)
        workflow_id = created["workflow_id"]
        duplicate_preview = _mutate(
            client,
            f"{BASE}/{workflow_id}/artifacts",
            {
                "artifact_type": "TRANSFORMATION_PREVIEW",
                "expected_version": preview["workflow"]["version"],
                "payload": preview["result"],
            },
            "duplicate-preview",
        )
        stale_preview = _mutate(
            client,
            f"{BASE}/{workflow_id}/execute",
            _execution_payload(approved, preview)
            | {"expected_version": duplicate_preview.json()["workflow"]["version"]},
            "stale-preview",
        )
        malicious_payload = dict(preview["result"])
        malicious_payload["sql"] = 'DELETE FROM "ANALYTICS"."PUBLIC"."PEOPLE"'
        altered = _mutate(
            client,
            f"{BASE}/{workflow_id}/artifacts",
            {
                "artifact_type": "TRANSFORMATION_PREVIEW",
                "expected_version": duplicate_preview.json()["workflow"]["version"],
                "payload": malicious_payload,
            },
            "altered-preview",
        )
        unsafe = _mutate(
            client,
            f"{BASE}/{workflow_id}/execute",
            {
                "expected_version": altered.json()["workflow"]["version"],
                "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                "transformation_preview_artifact_version": altered.json()["artifact"]["artifact_version"],
                "target_profile_id": "sf-target",
            },
            "unsafe",
        )
        duplicate_mapping = _mutate(
            client,
            f"{BASE}/{workflow_id}/artifacts",
            {
                "artifact_type": "APPROVED_MAPPING_PLAN",
                "expected_version": altered.json()["workflow"]["version"],
                "payload": approved["result"],
            },
            "duplicate-approved",
        )
        stale_mapping = _mutate(
            client,
            f"{BASE}/{workflow_id}/execute",
            {
                "expected_version": duplicate_mapping.json()["workflow"]["version"],
                "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                "transformation_preview_artifact_version": altered.json()["artifact"]["artifact_version"],
                "target_profile_id": "sf-target",
            },
            "stale-mapping",
        )

    assert stale_preview.status_code == 409
    assert stale_preview.json()["error"]["code"] == "STALE_WORKFLOW_ARTIFACT"
    assert unsafe.status_code == 409
    assert unsafe.json()["error"]["code"] == "UNSAFE_GENERATED_STATEMENT"
    assert stale_mapping.status_code == 409
    assert stale_mapping.json()["error"]["code"] == "STALE_WORKFLOW_ARTIFACT"
    assert executor.invocations == 0


def test_idempotency_conflict_optimistic_conflict_and_concurrent_claim_do_not_duplicate() -> None:
    repository = InMemoryWorkflowRepository()
    started, release = Event(), Event()
    executor = FakeExecutor(started=started, release=release)
    with TestClient(_app(repository, executor)) as client:
        created, approved, preview = _ready(client)
        workflow_id = created["workflow_id"]
        payload = _execution_payload(approved, preview)
        with TestClient(_app(repository, executor)) as concurrent_client:
            with ThreadPoolExecutor(max_workers=1) as pool:
                first_future = pool.submit(
                    _mutate, client, f"{BASE}/{workflow_id}/execute", payload, "execute"
                )
                assert started.wait(timeout=10)
                current = concurrent_client.get(f"{BASE}/{workflow_id}").json()
                concurrent = _mutate(
                    concurrent_client,
                    f"{BASE}/{workflow_id}/execute",
                    payload | {"expected_version": current["version"]},
                    "concurrent",
                )
                release.set()
                first = first_future.result(timeout=10)
        changed = _mutate(
            client,
            f"{BASE}/{workflow_id}/execute",
            payload | {"timeout_seconds": 14},
            "execute",
        )
        stale = _mutate(
            client,
            f"{BASE}/{workflow_id}/execute",
            payload,
            "new-stale-command",
        )

    assert first.status_code == 201, first.text
    assert concurrent.status_code == 409
    assert concurrent.json()["error"]["code"] == "EXECUTION_ALREADY_IN_PROGRESS"
    assert changed.status_code == 409
    assert changed.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "WORKFLOW_VERSION_CONFLICT"
    assert executor.invocations == 1


def test_confirmed_rollback_is_persisted_and_allows_an_explicit_retry() -> None:
    repository = InMemoryWorkflowRepository()
    executor = FakeExecutor(
        [
            TargetExecutionResult(
                TargetExecutionDisposition.CONFIRMED_FAILED_ROLLED_BACK,
                failure_category="TARGET_CONSTRAINT_FAILURE",
            ),
            TargetExecutionResult(TargetExecutionDisposition.SUCCEEDED, 2),
        ]
    )
    with TestClient(_app(repository, executor)) as client:
        created, approved, preview = _ready(client)
        workflow_id = created["workflow_id"]
        payload = _execution_payload(approved, preview)
        failed = _mutate(client, f"{BASE}/{workflow_id}/execute", payload, "failed")
        after_failure = client.get(f"{BASE}/{workflow_id}").json()
        evidence = client.get(f"{BASE}/{workflow_id}/artifacts?limit=20").json()["items"][-1]
        retry = _mutate(
            client,
            f"{BASE}/{workflow_id}/execute",
            payload | {"expected_version": after_failure["version"]},
            "retry",
        )

    assert failed.status_code == 502
    assert failed.json()["error"]["code"] == "EXECUTION_CONFIRMED_FAILED"
    assert after_failure["status"] == "EXECUTION_READY"
    assert evidence["payload"]["status"] == "FAILED_ROLLED_BACK"
    assert evidence["payload"]["transaction_outcome"] == "ROLLED_BACK"
    assert retry.status_code == 201 and retry.json()["workflow"]["status"] == "EXECUTED"
    assert executor.invocations == 2


def test_uncertain_outcome_is_sanitized_persisted_and_never_retried() -> None:
    repository = InMemoryWorkflowRepository()

    class UnsafeQueryService:
        def migration_execution_context(self, _timeout):
            return {
                "profile_id": "sf-target",
                "db_type": "snowflake",
                "database": 'Data.B"ase',
                "timeout_seconds": 15,
                "write_enabled": True,
                "connector_type": "snowflake",
            }

        def execute_query(self, **_kwargs):
            raise RuntimeError("password=hunter2 host=private.example raw SQL failure")

    executor = ProfileBoundMigrationExecutionService(lambda _profile: UnsafeQueryService())
    with TestClient(_app(repository, executor)) as client:
        created, approved, preview = _ready(client)
        workflow_id = created["workflow_id"]
        payload = _execution_payload(approved, preview)
        first = _mutate(client, f"{BASE}/{workflow_id}/execute", payload, "uncertain")
        replay = _mutate(client, f"{BASE}/{workflow_id}/execute", payload, "uncertain")
        workflow = client.get(f"{BASE}/{workflow_id}").json()
        artifacts = client.get(f"{BASE}/{workflow_id}/artifacts?limit=20").json()["items"]
        audit = client.get(f"{BASE}/{workflow_id}/audit-events?limit=30").json()["items"]
        combined = json.dumps({"responses": [first.json(), replay.json()], "artifacts": artifacts, "audit": audit}).casefold()

    assert first.status_code == replay.status_code == 409
    assert first.json()["error"]["code"] == "EXECUTION_OUTCOME_UNCERTAIN"
    assert workflow["status"] == "EXECUTION_RECOVERY_REQUIRED"
    assert artifacts[-1]["payload"]["status"] == "OUTCOME_UNCERTAIN"
    assert artifacts[-1]["payload"]["transaction_outcome"] == "UNKNOWN"
    assert all(secret not in combined for secret in ("hunter2", "private.example", "raw sql failure"))


def test_profile_write_opt_in_is_required_before_an_attempt_is_claimed() -> None:
    repository = InMemoryWorkflowRepository()

    class ReadOnlyQueryService:
        def migration_execution_context(self, _timeout):
            return {
                "profile_id": "sf-target",
                "db_type": "snowflake",
                "database": 'Data.B"ase',
                "timeout_seconds": 15,
                "write_enabled": False,
                "connector_type": "snowflake",
            }

    executor = ProfileBoundMigrationExecutionService(lambda _profile: ReadOnlyQueryService())
    with TestClient(_app(repository, executor)) as client:
        created, approved, preview = _ready(client)
        response = _mutate(
            client,
            f"{BASE}/{created['workflow_id']}/execute",
            _execution_payload(approved, preview),
            "read-only",
        )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "TARGET_PROFILE_NOT_WRITE_CAPABLE"
    assert repository._attempts == {}


def test_interrupted_claim_before_target_execution_is_safely_resumed_once() -> None:
    repository = InMemoryWorkflowRepository()
    executor = FakeExecutor()
    with TestClient(_app(repository, executor)) as client:
        created, approved, preview = _ready(client)
        workflow_id = UUID(created["workflow_id"])
        payload = _execution_payload(approved, preview)
        command_hash = WorkflowExecutionOrchestrator._command_hash(
            workflow_id,
            expected_version=payload["expected_version"],
            approved_mapping_artifact_version=payload[
                "approved_mapping_artifact_version"
            ],
            transformation_preview_artifact_version=payload[
                "transformation_preview_artifact_version"
            ],
            target_profile_id=payload["target_profile_id"],
            timeout_seconds=payload["timeout_seconds"],
        )
        approved_artifact = repository.get_artifact(
            workflow_id, payload["approved_mapping_artifact_version"]
        )
        preview_artifact = repository.get_artifact(
            workflow_id, payload["transformation_preview_artifact_version"]
        )
        fingerprint = request_hash(
            "WORKFLOW_EXECUTION_FINGERPRINT",
            {
                "workflow_id": workflow_id,
                "approved_mapping_hash": approved_artifact.payload_sha256,
                "preview_hash": preview_artifact.payload_sha256,
                "target_profile_id": payload["target_profile_id"],
            },
        )
        attempt = MigrationExecutionAttempt(
            attempt_id=uuid4(),
            workflow_id=workflow_id,
            approved_mapping_artifact_version=payload[
                "approved_mapping_artifact_version"
            ],
            transformation_preview_artifact_version=payload[
                "transformation_preview_artifact_version"
            ],
            target_profile_id="sf-target",
            execution_fingerprint=fingerprint,
            status=MigrationExecutionAttemptStatus.CLAIMED,
            timeout_seconds=15,
            claimed_at=datetime.now(timezone.utc),
            actor_type=AuditActorType.SERVICE,
            idempotency_key="resume",
            actor_reference="migration-runner",
        )
        claimed, _, created_attempt = WorkflowPersistenceService(
            repository
        ).claim_execution_attempt(
            workflow_id,
            payload["expected_version"],
            attempt,
            command_hash=command_hash,
            idempotency_key="resume",
            actor_type=AuditActorType.SERVICE,
            actor_reference="migration-runner",
            request_id="interrupted-before-target",
        )
        assert claimed.status.value == "EXECUTING" and created_attempt is True
        resumed = _mutate(
            client, f"{BASE}/{workflow_id}/execute", payload, "resume"
        )

    assert resumed.status_code == 201, resumed.text
    assert resumed.json()["result"]["status"] == "SUCCEEDED"
    assert executor.invocations == 1
