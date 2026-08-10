"""Profile-bound execution of one compiler-produced Snowflake transformation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable

from schemabridge.models.mapping import GeneratedTransformationSql, SqlDialect, TransformationStatementType
from schemabridge.persistence.errors import (
    WorkflowTargetProfileNotWriteCapableError,
    WorkflowTargetProfileUnavailableError,
    WorkflowUnsafeGeneratedStatementError,
    WorkflowUnsupportedExecutionConnectorError,
)
from schemabridge.validation.sql_guard import validate_query


class TargetExecutionDisposition(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    CONFIRMED_FAILED_ROLLED_BACK = "CONFIRMED_FAILED_ROLLED_BACK"
    OUTCOME_UNCERTAIN = "OUTCOME_UNCERTAIN"


@dataclass(frozen=True, slots=True)
class PreparedMigrationTarget:
    profile_id: str
    database: str
    connector_type: str
    timeout_seconds: int
    service: object


@dataclass(frozen=True, slots=True)
class TargetExecutionResult:
    disposition: TargetExecutionDisposition
    affected_rows: int | None = None
    failure_category: str | None = None


class ProfileBoundMigrationExecutionService:
    """Resolve a write-enabled target profile and discard unsafe driver output."""

    def __init__(self, database_service_factory: Callable[[str], object]) -> None:
        self.database_service_factory = database_service_factory

    def prepare(
        self,
        profile_id: str,
        *,
        target_database: str | None,
        target_system: str,
        timeout_seconds: int | None,
    ) -> PreparedMigrationTarget:
        try:
            service = self.database_service_factory(profile_id)
            context = service.migration_execution_context(timeout_seconds)
        except Exception:
            raise WorkflowTargetProfileUnavailableError() from None
        if context.get("profile_id") != profile_id:
            raise WorkflowTargetProfileUnavailableError()
        if context.get("write_enabled") is not True:
            raise WorkflowTargetProfileNotWriteCapableError()
        if (
            str(context.get("db_type", "")).casefold() != "snowflake"
            or target_system.casefold() != "snowflake"
        ):
            raise WorkflowUnsupportedExecutionConnectorError()
        configured_database = context.get("database")
        if (
            not isinstance(configured_database, str)
            or target_database is None
            or configured_database != target_database
        ):
            raise WorkflowTargetProfileUnavailableError()
        effective_timeout = context.get("timeout_seconds")
        if (
            isinstance(effective_timeout, bool)
            or not isinstance(effective_timeout, int)
            or effective_timeout <= 0
        ):
            raise WorkflowTargetProfileUnavailableError()
        connector_type = context.get("connector_type")
        if not isinstance(connector_type, str) or not connector_type:
            raise WorkflowUnsupportedExecutionConnectorError()
        return PreparedMigrationTarget(
            profile_id=profile_id,
            database=configured_database,
            connector_type=connector_type,
            timeout_seconds=effective_timeout,
            service=service,
        )

    @staticmethod
    def validate_preview(preview: GeneratedTransformationSql) -> None:
        if (
            not isinstance(preview, GeneratedTransformationSql)
            or preview.dialect is not SqlDialect.SNOWFLAKE
            or preview.statement_type is not TransformationStatementType.INSERT_SELECT
            or not preview.sql.lstrip().upper().startswith("INSERT INTO ")
        ):
            raise WorkflowUnsafeGeneratedStatementError()
        valid, _ = validate_query(preview.sql, "snowflake")
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
    "PreparedMigrationTarget",
    "ProfileBoundMigrationExecutionService",
    "TargetExecutionDisposition",
    "TargetExecutionResult",
]
