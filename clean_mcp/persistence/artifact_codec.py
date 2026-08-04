"""Typed rehydration for canonical workflow artifacts used by orchestration."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from uuid import UUID

from models.discovery import (
    CheckConstraintMetadata,
    ConstraintType,
    CoverageStatus,
    DatabaseObjectType,
    DiscoveryCoverage,
    ForeignKeyMetadata,
    KeyConstraintMetadata,
    ObjectPersistence,
    TableMetadata,
)
from models.mapping import (
    ApprovedTableMappingPlan,
    ColumnCompatibility,
    ColumnMappingApproval,
    ColumnMappingSuggestion,
    MappingApprovalStatus,
    MappingDecision,
    MappingEvidence,
    GeneratedTransformationSql,
    SqlDialect,
    TableMappingIdentity,
    TableMappingPlan,
    TransformationExpression,
    TransformationExpressionType,
    TransformationStatementType,
)
from models.metadata import CanonicalType, ColumnMetadata
from models.execution import (
    MigrationExecutionAttemptStatus,
    MigrationExecutionEvidence,
    MigrationTransactionOutcome,
)
from models.validation import (
    GeneratedValidationSql,
    MigrationValidationExecutionReport,
    MigrationValidationReport,
    MigrationValidationStatus,
    ValidationCheckDefinition,
    ValidationCheckResult,
    ValidationCheckType,
    ValidationExecutionStatus,
    ValidationStatus,
)
from models.workflow import WorkflowArtifact, WorkflowArtifactType
from persistence.errors import WorkflowArtifactValidationError


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise WorkflowArtifactValidationError()
    return value


def _tuple(value: object) -> tuple:
    if not isinstance(value, list):
        raise WorkflowArtifactValidationError()
    return tuple(value)


def _column(value: object) -> ColumnMetadata:
    data = dict(_mapping(value))
    data["canonical_type"] = CanonicalType(data["canonical_type"])
    if data.get("element_canonical_type") is not None:
        data["element_canonical_type"] = CanonicalType(data["element_canonical_type"])
    data["vendor_metadata"] = {}
    return ColumnMetadata(**data)


def _key(value: object) -> KeyConstraintMetadata:
    data = dict(_mapping(value))
    data["constraint_type"] = ConstraintType(data["constraint_type"])
    data["columns"] = _tuple(data["columns"])
    data["vendor_metadata"] = {}
    return KeyConstraintMetadata(**data)


def _foreign_key(value: object) -> ForeignKeyMetadata:
    data = dict(_mapping(value))
    data["local_columns"] = _tuple(data["local_columns"])
    data["referenced_columns"] = _tuple(data["referenced_columns"])
    data["vendor_metadata"] = {}
    return ForeignKeyMetadata(**data)


def _check(value: object) -> CheckConstraintMetadata:
    data = dict(_mapping(value))
    data["vendor_metadata"] = {}
    return CheckConstraintMetadata(**data)


def _coverage(value: object) -> DiscoveryCoverage:
    data = dict(_mapping(value))
    for name in DiscoveryCoverage.__dataclass_fields__:
        if name != "warnings":
            data[name] = CoverageStatus(data[name])
    data["warnings"] = _tuple(data.get("warnings", []))
    return DiscoveryCoverage(**data)


def table_metadata_from_artifact(artifact: WorkflowArtifact) -> TableMetadata:
    if artifact.artifact_type not in {
        WorkflowArtifactType.SOURCE_DISCOVERY,
        WorkflowArtifactType.TARGET_DISCOVERY,
    }:
        raise WorkflowArtifactValidationError()
    try:
        data = dict(_mapping(json.loads(artifact.payload.decode("utf-8"))))
        data["object_type"] = DatabaseObjectType(data["object_type"])
        data["persistence"] = ObjectPersistence(data["persistence"])
        data["columns"] = tuple(_column(item) for item in _tuple(data["columns"]))
        primary = data.get("primary_key")
        data["primary_key"] = _key(primary) if primary is not None else None
        data["unique_constraints"] = tuple(
            _key(item) for item in _tuple(data.get("unique_constraints", []))
        )
        data["foreign_keys"] = tuple(
            _foreign_key(item) for item in _tuple(data.get("foreign_keys", []))
        )
        data["check_constraints"] = tuple(
            _check(item) for item in _tuple(data.get("check_constraints", []))
        )
        data["coverage"] = _coverage(data["coverage"])
        data["vendor_metadata"] = {}
        return TableMetadata(**data)
    except WorkflowArtifactValidationError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise WorkflowArtifactValidationError() from None


def _identity(value: object) -> TableMappingIdentity:
    return TableMappingIdentity(**dict(_mapping(value)))


def _evidence(value: object) -> MappingEvidence:
    return MappingEvidence(**dict(_mapping(value)))


def _suggestion(value: object) -> ColumnMappingSuggestion:
    data = dict(_mapping(value))
    data["compatibility"] = ColumnCompatibility(data["compatibility"])
    data["decision"] = MappingDecision(data["decision"])
    data["evidence"] = tuple(_evidence(item) for item in _tuple(data["evidence"]))
    return ColumnMappingSuggestion(**data)


def mapping_plan_from_artifact(artifact: WorkflowArtifact) -> TableMappingPlan:
    if artifact.artifact_type is not WorkflowArtifactType.MAPPING_PLAN:
        raise WorkflowArtifactValidationError()
    try:
        data = dict(_mapping(json.loads(artifact.payload.decode("utf-8"))))
        data["source_table"] = _identity(data["source_table"])
        data["target_table"] = _identity(data["target_table"])
        data["suggestions"] = tuple(
            _suggestion(item) for item in _tuple(data["suggestions"])
        )
        for name in (
            "unmatched_source_columns",
            "unmatched_target_columns",
            "ambiguous_source_columns",
            "warnings",
        ):
            data[name] = _tuple(data.get(name, []))
        return TableMappingPlan(**data)
    except WorkflowArtifactValidationError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise WorkflowArtifactValidationError() from None


def _expression(value: object | None) -> TransformationExpression | None:
    if value is None:
        return None
    data = dict(_mapping(value))
    data["expression_type"] = TransformationExpressionType(data["expression_type"])
    data["source_columns"] = _tuple(data.get("source_columns", []))
    data["arguments"] = tuple(_expression(item) for item in _tuple(data.get("arguments", [])))
    if data.get("target_canonical_type") is not None:
        data["target_canonical_type"] = CanonicalType(data["target_canonical_type"])
    return TransformationExpression(**data)


def _approval(value: object) -> ColumnMappingApproval:
    data = dict(_mapping(value))
    data["status"] = MappingApprovalStatus(data["status"])
    data["original_compatibility"] = ColumnCompatibility(data["original_compatibility"])
    data["compatibility"] = ColumnCompatibility(data["compatibility"])
    data["original_evidence"] = tuple(
        _evidence(item) for item in _tuple(data["original_evidence"])
    )
    data["transformation"] = _expression(data.get("transformation"))
    return ColumnMappingApproval(**data)


def approved_mapping_plan_from_artifact(
    artifact: WorkflowArtifact,
) -> ApprovedTableMappingPlan:
    if artifact.artifact_type is not WorkflowArtifactType.APPROVED_MAPPING_PLAN:
        raise WorkflowArtifactValidationError()
    try:
        data = dict(_mapping(json.loads(artifact.payload.decode("utf-8"))))
        data["source_table"] = _identity(data["source_table"])
        data["target_table"] = _identity(data["target_table"])
        data["approvals"] = tuple(_approval(item) for item in _tuple(data["approvals"]))
        data["approved_mappings"] = tuple(
            _approval(item) for item in _tuple(data["approved_mappings"])
        )
        for name in (
            "rejected_source_columns",
            "unmatched_source_columns",
            "unmatched_target_columns",
            "warnings",
        ):
            data[name] = _tuple(data.get(name, []))
        return ApprovedTableMappingPlan(**data)
    except WorkflowArtifactValidationError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise WorkflowArtifactValidationError() from None


def transformation_sql_from_artifact(
    artifact: WorkflowArtifact,
) -> GeneratedTransformationSql:
    if artifact.artifact_type is not WorkflowArtifactType.TRANSFORMATION_PREVIEW:
        raise WorkflowArtifactValidationError()
    try:
        data = dict(_mapping(json.loads(artifact.payload.decode("utf-8"))))
        data["dialect"] = SqlDialect(data["dialect"])
        data["statement_type"] = TransformationStatementType(data["statement_type"])
        for name in (
            "parameters",
            "source_relation",
            "target_relation",
            "source_columns",
            "target_columns",
            "warnings",
        ):
            data[name] = _tuple(data.get(name, []))
        return GeneratedTransformationSql(**data)
    except WorkflowArtifactValidationError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise WorkflowArtifactValidationError() from None


def execution_evidence_from_artifact(
    artifact: WorkflowArtifact,
) -> MigrationExecutionEvidence:
    if artifact.artifact_type is not WorkflowArtifactType.EXECUTION_EVIDENCE:
        raise WorkflowArtifactValidationError()
    try:
        data = dict(_mapping(json.loads(artifact.payload.decode("utf-8"))))
        data["attempt_id"] = UUID(data["attempt_id"])
        data["workflow_id"] = UUID(data["workflow_id"])
        data["status"] = MigrationExecutionAttemptStatus(data["status"])
        data["target_relation"] = _tuple(data["target_relation"])
        data["started_at"] = datetime.fromisoformat(
            str(data["started_at"]).replace("Z", "+00:00")
        )
        data["completed_at"] = datetime.fromisoformat(
            str(data["completed_at"]).replace("Z", "+00:00")
        )
        data["transaction_outcome"] = MigrationTransactionOutcome(
            data["transaction_outcome"]
        )
        return MigrationExecutionEvidence(**data)
    except WorkflowArtifactValidationError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise WorkflowArtifactValidationError() from None


def _validation_check(value: object) -> ValidationCheckDefinition:
    data = dict(_mapping(value))
    data["check_type"] = ValidationCheckType(data["check_type"])
    return ValidationCheckDefinition(**data)


def _validation_sql(value: object) -> GeneratedValidationSql:
    data = dict(_mapping(value))
    data["dialect"] = SqlDialect(data["dialect"])
    for name in ("parameters", "relation", "metric_aliases", "warnings"):
        data[name] = _tuple(data.get(name, []))
    data["checks"] = tuple(_validation_check(item) for item in _tuple(data["checks"]))
    return GeneratedValidationSql(**data)


def validation_preview_from_artifact(
    artifact: WorkflowArtifact,
) -> tuple[GeneratedValidationSql, GeneratedValidationSql]:
    if artifact.artifact_type is not WorkflowArtifactType.VALIDATION_PREVIEW:
        raise WorkflowArtifactValidationError()
    try:
        data = _tuple(json.loads(artifact.payload.decode("utf-8")))
        if len(data) != 2:
            raise WorkflowArtifactValidationError()
        return _validation_sql(data[0]), _validation_sql(data[1])
    except WorkflowArtifactValidationError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise WorkflowArtifactValidationError() from None


def _validation_result(value: object) -> ValidationCheckResult:
    data = dict(_mapping(value))
    data["check_type"] = ValidationCheckType(data["check_type"])
    data["status"] = ValidationStatus(data["status"])
    return ValidationCheckResult(**data)


def _validation_report(value: object) -> MigrationValidationReport:
    data = dict(_mapping(value))
    data["source_table"] = _tuple(data["source_table"])
    data["target_table"] = _tuple(data["target_table"])
    data["check_results"] = tuple(_validation_result(item) for item in _tuple(data["check_results"]))
    data["status"] = MigrationValidationStatus(data["status"])
    data["warnings"] = _tuple(data.get("warnings", []))
    return MigrationValidationReport(**data)


def validation_execution_report_from_artifact(
    artifact: WorkflowArtifact,
) -> MigrationValidationExecutionReport:
    if artifact.artifact_type is not WorkflowArtifactType.VALIDATION_EXECUTION_REPORT:
        raise WorkflowArtifactValidationError()
    try:
        data = dict(_mapping(json.loads(artifact.payload.decode("utf-8"))))
        data["source_sql_summary"] = _validation_sql(data["source_sql_summary"])
        data["target_sql_summary"] = _validation_sql(data["target_sql_summary"])
        data["validation_report"] = _validation_report(data["validation_report"])
        data["source_execution_status"] = ValidationExecutionStatus(data["source_execution_status"])
        data["target_execution_status"] = ValidationExecutionStatus(data["target_execution_status"])
        data["warnings"] = _tuple(data.get("warnings", []))
        return MigrationValidationExecutionReport(**data)
    except WorkflowArtifactValidationError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise WorkflowArtifactValidationError() from None


__all__ = [
    "approved_mapping_plan_from_artifact",
    "execution_evidence_from_artifact",
    "mapping_plan_from_artifact",
    "table_metadata_from_artifact",
    "transformation_sql_from_artifact",
    "validation_execution_report_from_artifact",
    "validation_preview_from_artifact",
]
