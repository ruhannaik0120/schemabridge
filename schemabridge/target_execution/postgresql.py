"""PostgreSQL-specific rendering and execution for the shared target skeleton."""

from __future__ import annotations

from typing import Any

from schemabridge.models.mapping import GeneratedTransformationSql, SqlDialect, TransformationStatementType
from schemabridge.models.metadata import CanonicalType
from schemabridge.persistence.errors import WorkflowUnsafeGeneratedStatementError
from schemabridge.validation.sql_guard import validate_query

from .base import (
    PreparedMigrationTarget,
    TargetExecutionCapabilities,
    TargetExecutionDisposition,
    TargetExecutionResult,
)
from .dialect_compiler import DialectTransformationCompiler


_POSTGRESQL_TYPES = {
    CanonicalType.BOOLEAN: "BOOLEAN", CanonicalType.INTEGER: "BIGINT",
    CanonicalType.DECIMAL: "NUMERIC", CanonicalType.FLOAT: "DOUBLE PRECISION",
    CanonicalType.STRING: "TEXT", CanonicalType.DATE: "DATE", CanonicalType.TIME: "TIME",
    CanonicalType.TIMESTAMP: "TIMESTAMP", CanonicalType.TIMESTAMP_TZ: "TIMESTAMPTZ",
    CanonicalType.BINARY: "BYTEA", CanonicalType.SEMI_STRUCTURED: "JSONB",
}


def _quote_identifier(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, str) or not value or "\x00" in value:
        from schemabridge.services.transformation_sql import InvalidTransformationPlanError
        raise InvalidTransformationPlanError("Invalid transformation plan.")
    return '"' + value.replace('"', '""') + '"'


def _relation(catalog: Any, schema: Any, table: Any) -> str:
    """PostgreSQL uses ``schema.table`` within its selected profile database."""

    _quote_identifier(catalog)
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


class PostgreSqlTargetTransformationCompiler(DialectTransformationCompiler):
    """Compile approved mappings with PostgreSQL type and quoting rules."""

    def __init__(self) -> None:
        super().__init__(
            dialect=SqlDialect.POSTGRESQL,
            canonical_types=_POSTGRESQL_TYPES,
            quote_identifier=_quote_identifier,
            render_relation=_relation,
            require_matching_catalog=True,
        )


class PostgreSqlTargetExecutionAdapter:
    """Own PostgreSQL SQL previews and controlled target execution."""

    database_type = "postgresql"
    dialect = SqlDialect.POSTGRESQL
    capabilities = TargetExecutionCapabilities(True, True, True)

    def __init__(self) -> None:
        self.compiler = PostgreSqlTargetTransformationCompiler()

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

    def execute(self, target: PreparedMigrationTarget, preview: GeneratedTransformationSql) -> TargetExecutionResult:
        self.validate_preview(preview)
        if preview.target_relation[0] != target.database:
            raise WorkflowUnsafeGeneratedStatementError()
        try:
            response = target.service.execute_migration_statement(
                sql=preview.sql, parameters=preview.parameters,
                database=target.database, timeout_seconds=target.timeout_seconds,
            )
        except Exception:
            return TargetExecutionResult(TargetExecutionDisposition.OUTCOME_UNCERTAIN, failure_category="TARGET_EXECUTION_INTERRUPTED")
        rows = getattr(response, "rows_affected", None)
        return TargetExecutionResult(
            TargetExecutionDisposition.SUCCEEDED,
            affected_rows=rows if isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0 else None,
        )


__all__ = ["PostgreSqlTargetExecutionAdapter", "PostgreSqlTargetTransformationCompiler"]
