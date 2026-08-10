"""Execute paired generated validation queries and reconcile their metrics.

The service resolves source and target profiles independently, verifies the
expected connector dialects, runs read-only aggregate queries, and normalizes
their single rows before reconciliation.  Driver exceptions are replaced with
sanitized domain failures.
"""

from __future__ import annotations

from schemabridge.models.mapping import SqlDialect
from schemabridge.models.validation import (
    MigrationValidationExecutionReport,
    MigrationValidationExecutionRequest,
    ValidationExecutionStatus,
)
from schemabridge.services.database_service import get_database_service
from schemabridge.services.reconciliation import reconcile_validation_results
from schemabridge.services.validation_sql import compile_validation_sql


class ValidationApprovalRequiredError(ValueError):
    """Raised when a caller has not explicitly authorized validation execution."""


class ValidationExecutionError(ValueError):
    """Raised when compilation, profile resolution, or remote execution fails."""


class MalformedValidationExecutionResultError(ValueError):
    """Raised when an aggregate query does not return one unambiguous row."""


class MigrationValidationExecutionService:
    """Run a generated PostgreSQL/Snowflake validation pair synchronously."""

    def __init__(self, database_service_factory=None):
        """Accept an optional profile resolver for dependency injection and tests."""

        self.database_service_factory = database_service_factory

    def run(
        self,
        request: MigrationValidationExecutionRequest,
    ) -> MigrationValidationExecutionReport:
        """Execute an approved request and return reconciled aggregate evidence.

        Raises:
            ValidationApprovalRequiredError: If explicit approval is absent.
            ValidationExecutionError: If compilation, profiles, or either query
                cannot be completed safely.
            MalformedValidationExecutionResultError: If either query does not
                produce exactly one aggregate row with unique column names.
        """

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
        # Resolve and execute each side separately because the databases have
        # different profiles, dialects, permissions, and failure domains.
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
            """Normalize one aggregate row into a case-insensitive metric map."""

            rows = getattr(result, "rows", None)
            columns = getattr(result, "columns", None)
            if not isinstance(rows, (tuple, list)) or not isinstance(
                columns, (tuple, list)
            ):
                raise MalformedValidationExecutionResultError(
                    "Malformed validation execution result."
                )
            # Generated aggregate queries have no GROUP BY and therefore must
            # produce exactly one row; accepting more would misalign evidence.
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
