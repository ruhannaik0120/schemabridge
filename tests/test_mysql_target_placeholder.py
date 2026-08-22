"""Verify MySQL can safely preview a transformation before writes are enabled."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from schemabridge.api.app import create_app
from schemabridge.api.dependencies import get_target_execution_registry
from schemabridge.models.mapping import SqlDialect
from schemabridge.models.transport import TransportRelation
from schemabridge.persistence.errors import WorkflowUnsafeGeneratedStatementError
from schemabridge.services.transformation_sql import InvalidTransformationPlanError
from schemabridge.target_execution import (
    MySqlTargetExecutionAdapter,
    TargetExecutionAdapter,
    TargetExecutionDisposition,
    TargetTransformationCompiler,
)
from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from tests.test_migration_api import _workflow_tables
from tests.test_transformation_sql import _approved
from tests.test_workflow_orchestration_api import _approve, _discover_pair, _mapping, _mutate
from tests.test_workflow_persistence_api import BASE, _create_payload


MYSQL_DATABASE = "mysql_lab"
STAGING = TransportRelation(
    catalog_name=MYSQL_DATABASE,
    schema_name=MYSQL_DATABASE,
    object_name="people_stage`; DROP TABLE people;--",
)


def _mysql_plan():
    plan = _approved()
    return replace(
        plan,
        target_table=replace(
            plan.target_table,
            catalog_name=MYSQL_DATABASE,
            schema_name=MYSQL_DATABASE,
            system="mysql",
        ),
    )


def test_mysql_target_is_registered_for_previews_and_execution() -> None:
    registry = get_target_execution_registry()
    adapter = registry.resolve("MYSQL")

    assert isinstance(adapter, MySqlTargetExecutionAdapter)
    assert isinstance(adapter, TargetExecutionAdapter)
    assert isinstance(adapter.compiler, TargetTransformationCompiler)
    assert adapter.dialect is SqlDialect.MYSQL
    assert adapter.capabilities.supports_select_preview is True
    assert adapter.capabilities.supports_insert_select_preview is True
    assert adapter.capabilities.supports_insert_select_execution is True


def test_mysql_compiler_uses_database_table_relations_and_bound_parameters() -> None:
    adapter = MySqlTargetExecutionAdapter()
    preview = adapter.compiler.compile_insert_select(
        _mysql_plan(), staging_relation=STAGING
    )

    assert preview.dialect is SqlDialect.MYSQL
    assert preview.parameters == (" ",)
    assert "INSERT INTO `mysql_lab`.`people`" in preview.sql
    assert "FROM `mysql_lab`.`people_stage``; DROP TABLE people;--` AS `src`" in preview.sql
    assert '"mysql_lab"' not in preview.sql


class FakeMySqlService:
    def __init__(self, *, rows_affected=4, error=None) -> None:
        self.rows_affected = rows_affected
        self.error = error
        self.calls: list[dict] = []

    def execute_migration_statement(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(rows_affected=self.rows_affected)


def _target(service, *, database=MYSQL_DATABASE):
    from schemabridge.services.migration_execution import PreparedMigrationTarget

    return PreparedMigrationTarget(
        profile_id="mysql-target",
        database=database,
        connector_type="mysql",
        timeout_seconds=20,
        service=service,
    )


def test_mysql_adapter_validates_and_calls_the_controlled_service() -> None:
    adapter = MySqlTargetExecutionAdapter()
    preview = adapter.compiler.compile_insert_select(
        _mysql_plan(), staging_relation=STAGING
    )

    adapter.validate_preview(preview)
    unsafe_preview = replace(
        preview, sql="INSERT INTO people SELECT 1; DELETE FROM people"
    )
    with pytest.raises(WorkflowUnsafeGeneratedStatementError):
        adapter.validate_preview(unsafe_preview)

    service = FakeMySqlService()
    result = adapter.execute(_target(service), preview)

    assert result.disposition is TargetExecutionDisposition.SUCCEEDED
    assert result.affected_rows == 4
    assert service.calls == [{
        "sql": preview.sql,
        "parameters": preview.parameters,
        "database": MYSQL_DATABASE,
        "timeout_seconds": 20,
    }]


def test_mysql_adapter_rejects_wrong_target_and_marks_connection_loss_uncertain() -> None:
    adapter = MySqlTargetExecutionAdapter()
    preview = adapter.compiler.compile_insert_select(
        _mysql_plan(), staging_relation=STAGING
    )

    with pytest.raises(WorkflowUnsafeGeneratedStatementError):
        adapter.execute(_target(FakeMySqlService(), database="other"), preview)

    interrupted = adapter.execute(
        _target(FakeMySqlService(error=RuntimeError("driver lost connection"))),
        preview,
    )
    assert interrupted.disposition is TargetExecutionDisposition.OUTCOME_UNCERTAIN
    assert interrupted.failure_category == "TARGET_EXECUTION_INTERRUPTED"


def test_mysql_compiler_rejects_staging_from_another_database() -> None:
    adapter = MySqlTargetExecutionAdapter()
    wrong_database = TransportRelation(
        catalog_name="other_database",
        schema_name="other_database",
        object_name="people_stage",
    )

    with pytest.raises(InvalidTransformationPlanError):
        adapter.compiler.compile_select(_mysql_plan(), staging_relation=wrong_database)


def test_mysql_compiler_rejects_relations_with_different_catalog_and_schema() -> None:
    adapter = MySqlTargetExecutionAdapter()
    invalid_mysql_relation = TransportRelation(
        catalog_name=MYSQL_DATABASE,
        schema_name="another_schema",
        object_name="people_stage",
    )

    with pytest.raises(InvalidTransformationPlanError):
        adapter.compiler.compile_select(_mysql_plan(), staging_relation=invalid_mysql_relation)


def test_mysql_target_workflow_can_persist_a_preview_but_not_claim_execution() -> None:
    """Prove the normal API reaches EXECUTION_READY without enabling a write."""

    repository = InMemoryWorkflowRepository()
    source, discovered_target = _workflow_tables()
    discovered_target = replace(
        discovered_target,
        catalog_name=MYSQL_DATABASE,
        schema_name=MYSQL_DATABASE,
        system="mysql",
        columns=tuple(
            replace(
                column,
                catalog_name=MYSQL_DATABASE,
                schema_name=MYSQL_DATABASE,
            )
            for column in discovered_target.columns
        ),
    )

    class Connector:
        def __init__(self, metadata):
            self.metadata = metadata

        def get_table_metadata(self, **_kwargs):
            return self.metadata

    app = create_app()
    from schemabridge.api.dependencies import (
        get_schema_discovery_service,
        get_workflow_repository,
    )

    app.dependency_overrides[get_workflow_repository] = lambda: repository
    app.dependency_overrides[get_schema_discovery_service] = lambda: (
        lambda profile_id: Connector(
            source if profile_id == "pg-source" else discovered_target
        )
    )
    payload = _create_payload()
    payload["target_profile_id"] = "mysql-target"
    payload["target_relation"] = {
        "catalog_name": MYSQL_DATABASE,
        "schema_name": MYSQL_DATABASE,
        "object_name": discovered_target.object_name,
        "system": "mysql",
    }

    with TestClient(app) as client:
        created = _mutate(client, BASE, payload, "create-mysql-preview")
        assert created.status_code == 201, created.text
        created_json = created.json()
        _, discovered = _discover_pair(client, created_json)
        proposed = _mapping(
            client,
            created_json["workflow_id"],
            discovered["workflow"]["version"],
        )
        approved = _approve(
            client,
            created_json["workflow_id"],
            proposed["workflow"]["version"],
            proposed["artifact"]["artifact_version"],
            key="approve-mysql-preview",
        )
        preview = _mutate(
            client,
            f"{BASE}/{created_json['workflow_id']}/transformation-previews",
            {
                "expected_version": approved["workflow"]["version"],
                "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                "staging_database": MYSQL_DATABASE,
                "staging_schema": MYSQL_DATABASE,
                "staging_table": "people_stage",
                "statement_type": "INSERT_SELECT",
            },
            "preview-mysql-target",
        )

    assert preview.status_code == 201, preview.text
    preview_json = preview.json()
    assert preview_json["workflow"]["status"] == "EXECUTION_READY"
    assert preview_json["result"]["dialect"] == "MYSQL"
    assert "INSERT INTO `mysql_lab`" in preview_json["result"]["sql"]
