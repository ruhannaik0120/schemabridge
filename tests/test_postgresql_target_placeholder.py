"""Verify PostgreSQL target previews and controlled execution behavior."""

import pytest
from types import SimpleNamespace

from schemabridge.api.dependencies import get_target_execution_registry
from schemabridge.models.mapping import GeneratedTransformationSql, SqlDialect
from schemabridge.models.transport import TransportRelation
from schemabridge.persistence.errors import WorkflowUnsafeGeneratedStatementError
from schemabridge.services.transformation_sql import InvalidTransformationPlanError
from schemabridge.target_execution import (
    PostgreSqlTargetExecutionAdapter,
    TargetExecutionAdapter,
    TargetTransformationCompiler,
    UnsupportedTargetOperationError,
)
from schemabridge.services.migration_execution import (
    PreparedMigrationTarget,
    TargetExecutionDisposition,
)
from tests.test_transformation_sql import _approved


STAGING = TransportRelation(
    catalog_name="catalog",
    schema_name="landing",
    object_name='people_stage"; DROP TABLE x;--',
)


class FakePostgreSqlService:
    def __init__(self, *, rows_affected=4, error=None) -> None:
        self.rows_affected = rows_affected
        self.error = error
        self.calls: list[dict] = []

    def execute_migration_statement(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(rows_affected=self.rows_affected)


def _target(service, *, database="catalog") -> PreparedMigrationTarget:
    return PreparedMigrationTarget(
        profile_id="postgresql-target",
        database=database,
        connector_type="postgresql",
        timeout_seconds=20,
        service=service,
    )


def test_postgresql_target_is_registered_for_previews_and_execution() -> None:
    registry = get_target_execution_registry()
    adapter = registry.resolve("POSTGRESQL")

    assert isinstance(adapter, PostgreSqlTargetExecutionAdapter)
    assert isinstance(adapter, TargetExecutionAdapter)
    assert isinstance(adapter.compiler, TargetTransformationCompiler)
    assert adapter.dialect is SqlDialect.POSTGRESQL
    assert adapter.capabilities.supports_select_preview is True
    assert adapter.capabilities.supports_insert_select_preview is True
    assert adapter.capabilities.supports_insert_select_execution is True


def test_postgresql_compiler_uses_two_part_relations_and_bound_parameters() -> None:
    adapter = PostgreSqlTargetExecutionAdapter()
    preview = adapter.compiler.compile_insert_select(
        _approved(), staging_relation=STAGING
    )

    assert preview.dialect is SqlDialect.POSTGRESQL
    assert preview.parameters == (" ",)
    assert 'INSERT INTO "schema"."people"' in preview.sql
    assert 'FROM "landing"."people_stage""; DROP TABLE x;--" AS "src"' in preview.sql
    assert '"catalog"."schema"' not in preview.sql


def test_postgresql_compiler_rejects_a_staging_relation_from_another_database() -> None:
    adapter = PostgreSqlTargetExecutionAdapter()
    wrong_database = TransportRelation(
        catalog_name="other_database",
        schema_name="landing",
        object_name="people_stage",
    )

    with pytest.raises(InvalidTransformationPlanError):
        adapter.compiler.compile_select(_approved(), staging_relation=wrong_database)


def test_postgresql_adapter_validates_and_connects_to_the_controlled_service() -> None:
    adapter = PostgreSqlTargetExecutionAdapter()
    preview = adapter.compiler.compile_insert_select(
        _approved(), staging_relation=STAGING
    )

    adapter.validate_preview(preview)

    unsafe = GeneratedTransformationSql(
        dialect=SqlDialect.POSTGRESQL,
        statement_type=preview.statement_type,
        sql="INSERT INTO public.people SELECT 1; DELETE FROM public.people",
        parameters=(),
        source_relation=preview.source_relation,
        target_relation=preview.target_relation,
        source_columns=preview.source_columns,
        target_columns=preview.target_columns,
        approved_plan_version=preview.approved_plan_version,
    )

    with pytest.raises(WorkflowUnsafeGeneratedStatementError):
        adapter.validate_preview(unsafe)

    service = FakePostgreSqlService()
    result = adapter.execute(_target(service), preview)

    assert result.disposition is TargetExecutionDisposition.SUCCEEDED
    assert result.affected_rows == 4
    assert service.calls == [
        {
            "sql": preview.sql,
            "parameters": preview.parameters,
            "database": "catalog",
            "timeout_seconds": 20,
        }
    ]


def test_postgresql_adapter_rejects_wrong_target_and_classifies_interruption() -> None:
    adapter = PostgreSqlTargetExecutionAdapter()
    preview = adapter.compiler.compile_insert_select(
        _approved(), staging_relation=STAGING
    )

    with pytest.raises(WorkflowUnsafeGeneratedStatementError):
        adapter.execute(_target(FakePostgreSqlService(), database="other"), preview)

    interrupted = adapter.execute(
        _target(FakePostgreSqlService(error=RuntimeError("private driver failure"))),
        preview,
    )
    assert interrupted.disposition is TargetExecutionDisposition.OUTCOME_UNCERTAIN
    assert interrupted.failure_category == "TARGET_EXECUTION_INTERRUPTED"
