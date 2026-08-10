"""Controlled execution of paired PostgreSQL and Snowflake validation queries."""

from __future__ import annotations

from models.mapping import SqlDialect
from models.validation import (
    MigrationValidationExecutionReport,
    MigrationValidationExecutionRequest,
    ValidationExecutionStatus,
)
from services.database_service import get_database_service
from services.reconciliation import reconcile_validation_results
from services.validation_sql import compile_validation_sql


class ValidationApprovalRequiredError(ValueError):
    pass


class ValidationExecutionError(ValueError):
    pass


class MalformedValidationExecutionResultError(ValueError):
    pass


class MigrationValidationExecutionService:
    def __init__(self, database_service_factory=None):
        self.database_service_factory = database_service_factory

    def run(
        self,
        request: MigrationValidationExecutionRequest,
    ) -> MigrationValidationExecutionReport:
        if (
            not isinstance(request, MigrationValidationExecutionRequest)
            or request.explicitly_approved is not True
        ):
            raise ValidationApprovalRequiredError("Validation approval is required.")
        source_sql, target_sql = compile_validation_sql(
            request.approved_mapping_plan,
            source_schema=request.source_schema,
            source_table=request.source_table,
            target_database=request.target_database,
            target_schema=request.target_schema,
            target_table=request.target_table,
        )
        if (
            source_sql.dialect is not SqlDialect.POSTGRESQL
            or target_sql.dialect is not SqlDialect.SNOWFLAKE
        ):
            raise ValidationExecutionError("Validation compilation failed.")
        forbidden = (
            "INSERT ",
            "UPDATE ",
            "DELETE ",
            "MERGE ",
            "CREATE ",
            "ALTER ",
            "DROP ",
            "BEGIN ",
            "COMMIT ",
            "ROLLBACK ",
        )
        for generated in (source_sql, target_sql):
            normalized = generated.sql.lstrip().upper()
            if not normalized.startswith("SELECT") or any(
                word in normalized for word in forbidden
            ):
                raise ValidationExecutionError("Validation compilation failed.")

        resolver = self.database_service_factory or get_database_service
        try:
            source = resolver(request.source_profile_id)
            context = getattr(source, "validation_execution_context", None)
            source_context = (
                context(request.timeout_seconds) if callable(context) else None
            )
        except Exception:
            raise ValidationExecutionError(
                "Validation source profile unavailable."
            ) from None
        if source_context is not None and (
            source_context.get("profile_id") != request.source_profile_id
            or str(source_context.get("db_type", "")).casefold()
            not in {"postgres", "postgresql"}
        ):
            raise ValidationExecutionError(
                "Validation source connector is unsupported."
            )
        try:
            source_result = source.execute_validation_query(
                sql=source_sql.sql,
                parameters=source_sql.parameters,
                timeout_seconds=request.timeout_seconds,
            )
        except Exception:
            raise ValidationExecutionError(
                "Validation source execution failed."
            ) from None

        try:
            target = resolver(request.target_profile_id)
            context = getattr(target, "validation_execution_context", None)
            target_context = (
                context(request.timeout_seconds) if callable(context) else None
            )
        except Exception:
            raise ValidationExecutionError(
                "Validation target profile unavailable."
            ) from None
        if target_context is not None and (
            target_context.get("profile_id") != request.target_profile_id
            or str(target_context.get("db_type", "")).casefold() != "snowflake"
        ):
            raise ValidationExecutionError(
                "Validation target connector is unsupported."
            )
        try:
            target_result = target.execute_validation_query(
                sql=target_sql.sql,
                parameters=target_sql.parameters,
                timeout_seconds=request.timeout_seconds,
            )
        except Exception:
            raise ValidationExecutionError(
                "Validation target execution failed."
            ) from None

        def row(result) -> dict[str, object]:
            rows = getattr(result, "rows", None)
            columns = getattr(result, "columns", None)
            if not isinstance(rows, (tuple, list)) or not isinstance(
                columns, (tuple, list)
            ):
                raise MalformedValidationExecutionResultError(
                    "Malformed validation execution result."
                )
            if len(rows) != 1:
                raise MalformedValidationExecutionResultError(
                    "Malformed validation execution result."
                )
            names = [str(key).casefold() for key in columns]
            if len(set(names)) != len(names):
                raise MalformedValidationExecutionResultError(
                    "Malformed validation execution result."
                )
            value = rows[0]
            if isinstance(value, dict):
                return {str(key).casefold(): item for key, item in value.items()}
            if isinstance(value, (tuple, list)) and len(value) == len(columns):
                return dict(zip(names, value))
            raise MalformedValidationExecutionResultError(
                "Malformed validation execution result."
            )

        report = reconcile_validation_results(
            source_sql,
            target_sql,
            source_metrics=row(source_result),
            target_metrics=row(target_result),
        )
        return MigrationValidationExecutionReport(
            source_profile_id=request.source_profile_id,
            target_profile_id=request.target_profile_id,
            source_sql_summary=source_sql,
            target_sql_summary=target_sql,
            validation_report=report,
            source_execution_status=ValidationExecutionStatus.SUCCEEDED,
            target_execution_status=ValidationExecutionStatus.SUCCEEDED,
        )
