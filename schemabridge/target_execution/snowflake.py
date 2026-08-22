"""Adapt existing Snowflake compilation and execution to the target skeleton."""

from __future__ import annotations

from schemabridge.models.mapping import (
    ApprovedTableMappingPlan,
    GeneratedTransformationSql,
    SqlDialect,
    TransformationStatementType,
)
from schemabridge.models.transport import TransportRelation
from schemabridge.services.transformation_sql import (
    InvalidTransformationPlanError,
    SnowflakeTransformationSqlCompiler,
)
from schemabridge.persistence.errors import WorkflowUnsafeGeneratedStatementError
from .base import (
    PreparedMigrationTarget,
    TargetExecutionCapabilities,
    TargetExecutionDisposition,
    TargetExecutionResult,
)
from schemabridge.validation.sql_guard import validate_query


class SnowflakeTargetTransformationCompiler:
    """Accept neutral staging relations and reuse the proven Snowflake compiler."""

    def __init__(
        self,
        compiler: SnowflakeTransformationSqlCompiler | None = None,
    ) -> None:
        self._compiler = compiler or SnowflakeTransformationSqlCompiler()

    @staticmethod
    def _parts(relation: TransportRelation) -> tuple[str, str, str]:
        if (
            not isinstance(relation, TransportRelation)
            or relation.catalog_name is None
        ):
            raise InvalidTransformationPlanError("Invalid transformation plan.")
        return (
            relation.catalog_name,
            relation.schema_name,
            relation.object_name,
        )

    def compile_select(
        self,
        plan: ApprovedTableMappingPlan,
        *,
        staging_relation: TransportRelation,
        source_alias: str = "src",
    ) -> GeneratedTransformationSql:
        database, schema, table = self._parts(staging_relation)
        return self._compiler.compile_select(
            plan,
            staging_database=database,
            staging_schema=schema,
            staging_table=table,
            source_alias=source_alias,
        )

    def compile_insert_select(
        self,
        plan: ApprovedTableMappingPlan,
        *,
        staging_relation: TransportRelation,
        source_alias: str = "src",
    ) -> GeneratedTransformationSql:
        database, schema, table = self._parts(staging_relation)
        return self._compiler.compile_insert_select(
            plan,
            staging_database=database,
            staging_schema=schema,
            staging_table=table,
            source_alias=source_alias,
        )


class SnowflakeTargetExecutionAdapter:
    """Own Snowflake-specific SQL validation and sanitized remote execution."""

    database_type = "snowflake"
    dialect = SqlDialect.SNOWFLAKE
    capabilities = TargetExecutionCapabilities(
        supports_select_preview=True,
        supports_insert_select_preview=True,
        supports_insert_select_execution=True,
    )

    def __init__(
        self,
        *,
        compiler: SnowflakeTargetTransformationCompiler | None = None,
    ) -> None:
        self.compiler = compiler or SnowflakeTargetTransformationCompiler()

    def validate_preview(self, preview: GeneratedTransformationSql) -> None:
        if (
            not isinstance(preview, GeneratedTransformationSql)
            or preview.dialect is not self.dialect
            or preview.statement_type is not TransformationStatementType.INSERT_SELECT
            or not preview.sql.lstrip().upper().startswith("INSERT INTO ")
        ):
            raise WorkflowUnsafeGeneratedStatementError()
        valid, _reason = validate_query(preview.sql, self.database_type)
        if not valid:
            raise WorkflowUnsafeGeneratedStatementError()

    def execute(
        self,
        target: PreparedMigrationTarget,
        preview: GeneratedTransformationSql,
    ) -> TargetExecutionResult:
        self.validate_preview(preview)
        if preview.target_relation[0] != target.database:
            raise WorkflowUnsafeGeneratedStatementError()
        try:
            response = target.service.execute_migration_statement(
                sql=preview.sql,
                parameters=preview.parameters,
                database=target.database,
                timeout_seconds=target.timeout_seconds,
            )
        except Exception:
            # A driver exception cannot prove whether the remote transaction
            # committed, so an automatic retry could duplicate target rows.
            return TargetExecutionResult(
                TargetExecutionDisposition.OUTCOME_UNCERTAIN,
                failure_category="TARGET_EXECUTION_INTERRUPTED",
            )
        rows = getattr(response, "rows_affected", None)
        affected_rows = (
            rows
            if isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0
            else None
        )
        return TargetExecutionResult(
            TargetExecutionDisposition.SUCCEEDED,
            affected_rows=affected_rows,
        )


__all__ = [
    "SnowflakeTargetExecutionAdapter",
    "SnowflakeTargetTransformationCompiler",
]
