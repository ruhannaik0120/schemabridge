"""MySQL-specific SQL previews within the shared target-adapter skeleton."""

from __future__ import annotations

from typing import Any

from schemabridge.models.mapping import GeneratedTransformationSql, SqlDialect
from schemabridge.models.metadata import CanonicalType
from schemabridge.persistence.errors import WorkflowUnsafeGeneratedStatementError
from schemabridge.services.transformation_sql import InvalidTransformationPlanError
from schemabridge.validation.sql_guard import validate_query

from .base import (
    PreparedMigrationTarget,
    TargetExecutionCapabilities,
    TargetExecutionDisposition,
    TargetExecutionResult,
)
from .dialect_compiler import DialectTransformationCompiler


_MYSQL_TYPES = {
    CanonicalType.BOOLEAN: "BOOLEAN",
    CanonicalType.INTEGER: "BIGINT",
    CanonicalType.DECIMAL: "DECIMAL",
    CanonicalType.FLOAT: "DOUBLE",
    CanonicalType.STRING: "TEXT",
    CanonicalType.DATE: "DATE",
    CanonicalType.TIME: "TIME",
    CanonicalType.TIMESTAMP: "DATETIME",
    CanonicalType.BINARY: "LONGBLOB",
    CanonicalType.SEMI_STRUCTURED: "JSON",
}


def _quote_identifier(value: Any) -> str:
    """Render one MySQL identifier without ever treating it as SQL syntax."""

    if (
        isinstance(value, bool)
        or not isinstance(value, str)
        or not value
        or "\x00" in value
        or len(value) > 64
    ):
        raise InvalidTransformationPlanError("Invalid transformation plan.")
    return "`" + value.replace("`", "``") + "`"


def _relation(catalog: Any, schema: Any, table: Any) -> str:
    """MySQL calls a database a schema, so both canonical fields must agree."""

    if catalog != schema:
        raise InvalidTransformationPlanError("Invalid transformation plan.")
    return f"{_quote_identifier(catalog)}.{_quote_identifier(table)}"


class MySqlTargetTransformationCompiler(DialectTransformationCompiler):
    """Compile approved mappings using MySQL quoting and type names."""

    def __init__(self) -> None:
        super().__init__(
            dialect=SqlDialect.MYSQL,
            canonical_types=_MYSQL_TYPES,
            quote_identifier=_quote_identifier,
            render_relation=_relation,
            require_matching_catalog=True,
        )


class MySqlTargetExecutionAdapter:
    """Own MySQL SQL validation and controlled target execution."""

    database_type = "mysql"
    dialect = SqlDialect.MYSQL
    capabilities = TargetExecutionCapabilities(
        supports_select_preview=True,
        supports_insert_select_preview=True,
        supports_insert_select_execution=True,
    )

    def __init__(self) -> None:
        self.compiler = MySqlTargetTransformationCompiler()

    def validate_preview(self, preview: GeneratedTransformationSql) -> None:
        if (
            not isinstance(preview, GeneratedTransformationSql)
            or preview.dialect is not self.dialect
            or preview.statement_type.value != "INSERT_SELECT"
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
            return TargetExecutionResult(
                TargetExecutionDisposition.OUTCOME_UNCERTAIN,
                failure_category="TARGET_EXECUTION_INTERRUPTED",
            )
        rows = getattr(response, "rows_affected", None)
        return TargetExecutionResult(
            TargetExecutionDisposition.SUCCEEDED,
            affected_rows=(
                rows
                if isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0
                else None
            ),
        )


__all__ = ["MySqlTargetExecutionAdapter", "MySqlTargetTransformationCompiler"]
