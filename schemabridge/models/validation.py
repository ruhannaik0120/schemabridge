"""Define generated validation plans, metric results, and execution reports.

The models form the immutable contract between SQL generation, profile-bound
query execution, reconciliation, workflow artifacts, and API adapters.  They
carry aggregate evidence only and never contain source or target business rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from schemabridge.models.mapping import ApprovedTableMappingPlan, SqlDialect
from schemabridge.models.metadata import _MetadataModel, _json_value


class ValidationCheckType(str, Enum):
    """Identify the aggregate measured by a paired validation check."""

    ROW_COUNT = "ROW_COUNT"
    NULL_COUNT = "NULL_COUNT"
    DISTINCT_COUNT = "DISTINCT_COUNT"


class ValidationStatus(str, Enum):
    """Classify one source/target metric comparison."""

    MATCH = "MATCH"
    MISMATCH = "MISMATCH"
    UNAVAILABLE = "UNAVAILABLE"


class MigrationValidationStatus(str, Enum):
    """Summarize the complete reconciled check set."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    INCOMPLETE = "INCOMPLETE"


class ValidationExecutionStatus(str, Enum):
    """Record whether one side of paired validation completed."""

    NOT_STARTED = "NOT_STARTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True, kw_only=True)
class MigrationValidationExecutionRequest(_MetadataModel):
    """Authorize and locate one paired validation execution."""

    source_profile_id: str
    target_profile_id: str
    approved_mapping_plan: ApprovedTableMappingPlan
    source_schema: str
    source_table: str
    target_database: str
    target_schema: str
    target_table: str
    timeout_seconds: int | None = None
    explicitly_approved: bool = False

    def __post_init__(self):
        if not all(
            isinstance(value, str) and value
            for value in (
                self.source_profile_id,
                self.target_profile_id,
                self.source_schema,
                self.source_table,
                self.target_database,
                self.target_schema,
                self.target_table,
            )
        ):
            raise ValueError("Invalid validation execution request.")
        if self.timeout_seconds is not None and (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, int)
            or self.timeout_seconds <= 0
        ):
            raise ValueError("Invalid validation execution request.")


@dataclass(frozen=True, slots=True, kw_only=True)
class MigrationValidationExecutionReport(_MetadataModel):
    """Bundle generated SQL summaries, reconciliation, and side statuses."""

    source_profile_id: str
    target_profile_id: str
    source_sql_summary: GeneratedValidationSql
    target_sql_summary: GeneratedValidationSql
    validation_report: MigrationValidationReport
    source_execution_status: ValidationExecutionStatus
    target_execution_status: ValidationExecutionStatus
    warnings: tuple[str, ...] = ()

    def to_dict(self):
        return _json_value(
            {name: getattr(self, name) for name in self.__dataclass_fields__}
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidationCheckDefinition(_MetadataModel):
    """Bind one check ID to matching source and target metric aliases."""

    check_id: str
    check_type: ValidationCheckType
    source_column: str | None
    target_column: str | None
    source_metric_alias: str
    target_metric_alias: str

    def to_dict(self):
        return _json_value(
            {name: getattr(self, name) for name in self.__dataclass_fields__}
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class GeneratedValidationSql(_MetadataModel):
    """Carry one generated aggregate query and its reconciliation contract."""

    dialect: SqlDialect
    sql: str
    parameters: tuple[object, ...]
    relation: tuple[str, ...]
    metric_aliases: tuple[str, ...]
    checks: tuple[ValidationCheckDefinition, ...]
    warnings: tuple[str, ...] = ()

    def __post_init__(self):
        if (
            not self.sql.strip()
            or len(set(self.metric_aliases)) != len(self.metric_aliases)
            or len({item.check_id for item in self.checks}) != len(self.checks)
        ):
            raise ValueError("Invalid validation SQL.")

    def to_dict(self):
        return _json_value(
            {name: getattr(self, name) for name in self.__dataclass_fields__}
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidationCheckResult(_MetadataModel):
    """Record the normalized values and status of one reconciled check."""

    check_id: str
    check_type: ValidationCheckType
    source_value: int | None
    target_value: int | None
    status: ValidationStatus
    difference: int | None
    source_column: str | None
    target_column: str | None

    def to_dict(self):
        return _json_value(
            {name: getattr(self, name) for name in self.__dataclass_fields__}
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class MigrationValidationReport(_MetadataModel):
    """Summarize all paired checks for one source and target relation."""

    source_table: tuple[str, ...]
    target_table: tuple[str, ...]
    check_results: tuple[ValidationCheckResult, ...]
    status: MigrationValidationStatus
    matched_count: int
    mismatched_count: int
    unavailable_count: int
    warnings: tuple[str, ...]
    approved_plan_version: int

    def to_dict(self):
        return _json_value(
            {name: getattr(self, name) for name in self.__dataclass_fields__}
        )
