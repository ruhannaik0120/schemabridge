"""Prove that workflow orchestration routes targets by registered capability."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from fastapi.testclient import TestClient

from schemabridge.api.app import create_app
from schemabridge.api.dependencies import (
    get_migration_execution_service,
    get_schema_discovery_service,
    get_target_execution_registry,
    get_workflow_repository,
)
from schemabridge.models.mapping import (
    GeneratedTransformationSql,
    SqlDialect,
    TransformationStatementType,
)
from schemabridge.services.migration_execution import (
    PreparedMigrationTarget,
    TargetExecutionDisposition,
    TargetExecutionResult,
)
from schemabridge.target_execution import (
    PostgreSqlTargetExecutionAdapter,
    SnowflakeTargetTransformationCompiler,
    TargetExecutionCapabilities,
    TargetExecutionRegistry,
)
from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from tests.test_migration_api import _workflow_tables
from tests.test_workflow_orchestration_api import (
    _approve,
    _discover_pair,
    _mapping,
    _mutate,
)
from tests.test_workflow_persistence_api import BASE, _create_payload


class FakePostgreSqlTargetAdapter:
    """Test double proving that the core does not require a Snowflake target."""

    database_type = "postgresql"
    dialect = SqlDialect.POSTGRESQL
    capabilities = TargetExecutionCapabilities(
        supports_select_preview=True,
        supports_insert_select_preview=True,
        supports_insert_select_execution=True,
    )

    def __init__(self) -> None:
        self.compiler = FakePostgreSqlCompiler()
        self.executions = 0

    @staticmethod
    def validate_preview(preview: GeneratedTransformationSql) -> None:
        assert preview.dialect is SqlDialect.POSTGRESQL
        assert preview.statement_type is TransformationStatementType.INSERT_SELECT

    def execute(
        self,
        target: PreparedMigrationTarget,
        preview: GeneratedTransformationSql,
    ) -> TargetExecutionResult:
        assert target.connector_type == "postgresql"
        self.validate_preview(preview)
        self.executions += 1
        return TargetExecutionResult(TargetExecutionDisposition.SUCCEEDED, 2)


class FakePostgreSqlCompiler:
    """Test-only compiler; production PostgreSQL SQL support is future work."""

    def __init__(self) -> None:
        self._compatible_test_compiler = SnowflakeTargetTransformationCompiler()

    def compile_select(self, plan, *, staging_relation, source_alias="src"):
        preview = self._compatible_test_compiler.compile_select(
            plan,
            staging_relation=staging_relation,
            source_alias=source_alias,
        )
        return replace(preview, dialect=SqlDialect.POSTGRESQL)

    def compile_insert_select(self, plan, *, staging_relation, source_alias="src"):
        preview = self._compatible_test_compiler.compile_insert_select(
            plan,
            staging_relation=staging_relation,
            source_alias=source_alias,
        )
        return replace(preview, dialect=SqlDialect.POSTGRESQL)


class FakePostgreSqlTargetPreparer:
    """Record the generic target identity passed across the execution boundary."""

    def __init__(self, *, service: object | None = None) -> None:
        self.prepared_systems: list[str] = []
        self.service = service or object()

    def prepare(
        self,
        profile_id: str,
        *,
        target_database: str | None,
        target_system: str,
        timeout_seconds: int | None,
    ) -> PreparedMigrationTarget:
        assert profile_id == "pg-target"
        assert target_database is not None
        assert target_system == "postgresql"
        self.prepared_systems.append(target_system)
        return PreparedMigrationTarget(
            profile_id=profile_id,
            database=target_database,
            connector_type=target_system,
            timeout_seconds=timeout_seconds or 30,
            service=self.service,
        )


class FakePostgreSqlWriteService:
    """Record the controlled write call without using a real database."""

    def __init__(self, *, rows_affected: int = 2) -> None:
        self.rows_affected = rows_affected
        self.calls: list[dict] = []

    def execute_migration_statement(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(rows_affected=self.rows_affected)


def _application(
    repository: InMemoryWorkflowRepository,
    adapter: object,
    preparer: FakePostgreSqlTargetPreparer,
):
    source, snowflake_target = _workflow_tables()
    postgresql_target = replace(snowflake_target, system="postgresql")

    class Connector:
        def __init__(self, metadata):
            self.metadata = metadata

        def get_table_metadata(self, **_kwargs):
            return self.metadata

    def resolver(profile_id: str):
        return Connector(source if profile_id == "pg-source" else postgresql_target)

    app = create_app()
    app.dependency_overrides[get_workflow_repository] = lambda: repository
    app.dependency_overrides[get_schema_discovery_service] = lambda: resolver
    app.dependency_overrides[get_migration_execution_service] = lambda: preparer
    app.dependency_overrides[get_target_execution_registry] = lambda: (
        TargetExecutionRegistry((adapter,))
    )
    return app


def _ready_postgresql_workflow(client: TestClient) -> tuple[dict, dict, dict]:
    payload = _create_payload()
    payload["target_profile_id"] = "pg-target"
    payload["target_relation"]["system"] = "postgresql"

    created_response = _mutate(client, BASE, payload, "create-postgresql-target")
    assert created_response.status_code == 201, created_response.text
    created = created_response.json()
    _, discovered_target = _discover_pair(client, created)
    proposed = _mapping(
        client,
        created["workflow_id"],
        discovered_target["workflow"]["version"],
    )
    approved = _approve(
        client,
        created["workflow_id"],
        proposed["workflow"]["version"],
        proposed["artifact"]["artifact_version"],
        key="approve-postgresql-target",
    )
    preview_response = _mutate(
        client,
        f"{BASE}/{created['workflow_id']}/transformation-previews",
        {
            "expected_version": approved["workflow"]["version"],
            "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
            "staging_database": "Data.B\"ase",
            "staging_schema": "landing",
            "staging_table": "source_people",
            "statement_type": "INSERT_SELECT",
            "actor_type": "SERVICE",
        },
        "preview-postgresql-target",
    )
    assert preview_response.status_code == 201, preview_response.text
    return created, approved, preview_response.json()


def test_planning_and_execution_route_through_registered_postgresql_target() -> None:
    repository = InMemoryWorkflowRepository()
    adapter = FakePostgreSqlTargetAdapter()
    preparer = FakePostgreSqlTargetPreparer()

    with TestClient(_application(repository, adapter, preparer)) as client:
        created, approved, preview = _ready_postgresql_workflow(client)
        execution_response = _mutate(
            client,
            f"{BASE}/{created['workflow_id']}/execute",
            {
                "expected_version": preview["workflow"]["version"],
                "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                "transformation_preview_artifact_version": preview["artifact"]["artifact_version"],
                "target_profile_id": "pg-target",
                "timeout_seconds": 15,
                "actor_type": "SERVICE",
            },
            "execute-postgresql-target",
        )

    assert preview["result"]["dialect"] == "POSTGRESQL"
    assert execution_response.status_code == 201, execution_response.text
    assert execution_response.json()["workflow"]["status"] == "EXECUTED"
    assert execution_response.json()["result"]["affected_rows"] == 2
    assert preparer.prepared_systems == ["postgresql"]
    assert adapter.executions == 1


def test_real_postgresql_execution_uses_the_durable_workflow_and_controlled_service() -> None:
    repository = InMemoryWorkflowRepository()
    adapter = PostgreSqlTargetExecutionAdapter()
    service = FakePostgreSqlWriteService()
    preparer = FakePostgreSqlTargetPreparer(service=service)

    with TestClient(_application(repository, adapter, preparer)) as client:
        created, approved, preview = _ready_postgresql_workflow(client)
        execution_response = _mutate(
            client,
            f"{BASE}/{created['workflow_id']}/execute",
            {
                "expected_version": preview["workflow"]["version"],
                "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                "transformation_preview_artifact_version": preview["artifact"]["artifact_version"],
                "target_profile_id": "pg-target",
                "timeout_seconds": 15,
                "actor_type": "SERVICE",
            },
            "execute-real-postgresql-preview",
        )

    assert preview["result"]["dialect"] == "POSTGRESQL"
    assert '"Data.B""ase"."' not in preview["result"]["sql"]
    assert execution_response.status_code == 201, execution_response.text
    assert execution_response.json()["workflow"]["status"] == "EXECUTED"
    assert execution_response.json()["result"]["affected_rows"] == 2
    assert preparer.prepared_systems == ["postgresql"]
    assert service.calls == [
        {
            "sql": preview["result"]["sql"],
                "parameters": (" ",),
            "database": "Data.B\"ase",
            "timeout_seconds": 15,
        }
    ]
