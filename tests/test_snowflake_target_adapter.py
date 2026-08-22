"""Verify Snowflake fits the target skeleton without changing generated SQL."""

import pytest
from types import SimpleNamespace

from schemabridge.models.mapping import GeneratedTransformationSql
from schemabridge.models.transport import TransportRelation
from schemabridge.persistence.errors import WorkflowUnsafeGeneratedStatementError
from schemabridge.services.migration_execution import (
    PreparedMigrationTarget,
    TargetExecutionDisposition,
)
from schemabridge.services.transformation_sql import (
    InvalidTransformationPlanError,
    SnowflakeTransformationSqlCompiler,
)
from schemabridge.target_execution import (
    SnowflakeTargetExecutionAdapter,
    SnowflakeTargetTransformationCompiler,
    TargetExecutionAdapter,
    TargetExecutionRegistry,
    TargetTransformationCompiler,
)
from tests.test_transformation_sql import _approved


STAGING = TransportRelation(
    catalog_name="stage db",
    schema_name="schema.with.dot",
    object_name='x"; DROP;--',
)


class TargetService:
    def __init__(self, *, rows_affected=7, error=None):
        self.rows_affected = rows_affected
        self.error = error
        self.calls = []

    def execute_migration_statement(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(rows_affected=self.rows_affected)


def _target(service):
    return PreparedMigrationTarget(
        profile_id="snowflake-target",
        database="catalog",
        connector_type="snowflake",
        timeout_seconds=20,
        service=service,
    )


def test_neutral_compiler_preserves_exact_snowflake_output() -> None:
    plan = _approved()
    existing = SnowflakeTransformationSqlCompiler().compile_insert_select(
        plan,
        staging_database=STAGING.catalog_name,
        staging_schema=STAGING.schema_name,
        staging_table=STAGING.object_name,
    )

    adapted = SnowflakeTargetTransformationCompiler().compile_insert_select(
        plan,
        staging_relation=STAGING,
    )

    assert adapted == existing


def test_snowflake_compiler_rejects_a_relation_without_a_database() -> None:
    relation = TransportRelation(
        catalog_name=None,
        schema_name="public",
        object_name="stage_table",
    )

    with pytest.raises(InvalidTransformationPlanError):
        SnowflakeTargetTransformationCompiler().compile_select(
            _approved(),
            staging_relation=relation,
        )


def test_adapter_validates_and_executes_through_the_prepared_target() -> None:
    service = TargetService()
    adapter = SnowflakeTargetExecutionAdapter()
    preview = adapter.compiler.compile_insert_select(
        _approved(),
        staging_relation=STAGING,
    )

    adapter.validate_preview(preview)
    result = adapter.execute(_target(service), preview)

    assert result.disposition is TargetExecutionDisposition.SUCCEEDED
    assert result.affected_rows == 7
    assert service.calls == [
        {
            "sql": preview.sql,
            "parameters": preview.parameters,
            "database": "catalog",
            "timeout_seconds": 20,
        }
    ]


def test_adapter_rejects_wrong_dialect_and_classifies_driver_interruption() -> None:
    adapter = SnowflakeTargetExecutionAdapter()
    preview = adapter.compiler.compile_insert_select(
        _approved(),
        staging_relation=STAGING,
    )
    unsafe = GeneratedTransformationSql(
        dialect=preview.dialect,
        statement_type=preview.statement_type,
        sql="SELECT 1",
        parameters=(),
        source_relation=preview.source_relation,
        target_relation=preview.target_relation,
        source_columns=preview.source_columns,
        target_columns=preview.target_columns,
        approved_plan_version=preview.approved_plan_version,
    )

    with pytest.raises(WorkflowUnsafeGeneratedStatementError):
        adapter.validate_preview(unsafe)

    interrupted = adapter.execute(
        _target(TargetService(error=RuntimeError("private driver failure"))),
        preview,
    )
    assert interrupted.disposition is TargetExecutionDisposition.OUTCOME_UNCERTAIN
    assert interrupted.failure_category == "TARGET_EXECUTION_INTERRUPTED"


def test_snowflake_adapter_is_structural_and_registry_resolvable() -> None:
    adapter = SnowflakeTargetExecutionAdapter()
    registry = TargetExecutionRegistry((adapter,))

    assert isinstance(adapter.compiler, TargetTransformationCompiler)
    assert isinstance(adapter, TargetExecutionAdapter)
    assert registry.resolve("SNOWFLAKE") is adapter
