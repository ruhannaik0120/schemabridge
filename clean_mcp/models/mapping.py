"""Immutable, explainable schema-mapping suggestion models."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import Enum

from models.metadata import CanonicalType, _MetadataModel, _json_value, _validate_identifier


class ColumnCompatibility(str, Enum):
    EXACT = "EXACT"
    SAFE = "SAFE"
    LOSSY = "LOSSY"
    INCOMPATIBLE = "INCOMPATIBLE"
    UNKNOWN = "UNKNOWN"


class MappingDecision(str, Enum):
    SUGGESTED = "SUGGESTED"
    AMBIGUOUS = "AMBIGUOUS"
    UNMATCHED = "UNMATCHED"


class MappingApprovalStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    OVERRIDDEN = "OVERRIDDEN"


class TransformationExpressionType(str, Enum):
    SOURCE_COLUMN = "SOURCE_COLUMN"
    LITERAL = "LITERAL"
    CAST = "CAST"
    CONCAT = "CONCAT"
    COALESCE = "COALESCE"
    DIRECT_COPY = "DIRECT_COPY"


class SqlDialect(str, Enum):
    SNOWFLAKE = "SNOWFLAKE"
    POSTGRESQL = "POSTGRESQL"


class TransformationStatementType(str, Enum):
    SELECT = "SELECT"
    INSERT_SELECT = "INSERT_SELECT"


def _validate_fixed_code(value: str, field_name: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", value) is None:
        raise ValueError(f"{field_name} must be a fixed safe code.")


@dataclass(frozen=True, slots=True, kw_only=True)
class MappingEvidence(_MetadataModel):
    code: str
    explanation: str
    contribution: float | None = None

    def __post_init__(self) -> None:
        _validate_fixed_code(self.code, "code")
        if not isinstance(self.explanation, str) or not self.explanation:
            raise ValueError("explanation must be a non-empty string.")
        if self.contribution is not None:
            if isinstance(self.contribution, bool) or not isinstance(self.contribution, (int, float)):
                raise TypeError("contribution must be a finite number or None.")
            if not math.isfinite(float(self.contribution)):
                raise ValueError("contribution must be finite.")
            object.__setattr__(self, "contribution", float(self.contribution))

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class TableMappingIdentity(_MetadataModel):
    catalog_name: str | None
    schema_name: str
    table_name: str
    system: str

    def __post_init__(self) -> None:
        _validate_identifier(self.catalog_name, "catalog_name", required=False)
        _validate_identifier(self.schema_name, "schema_name", required=True)
        _validate_identifier(self.table_name, "table_name", required=True)
        _validate_identifier(self.system, "system", required=True)

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class ColumnMappingSuggestion(_MetadataModel):
    source_column: str
    target_column: str | None
    confidence: float
    compatibility: ColumnCompatibility
    decision: MappingDecision
    evidence: tuple[MappingEvidence, ...]

    def __post_init__(self) -> None:
        _validate_identifier(self.source_column, "source_column", required=True)
        _validate_identifier(self.target_column, "target_column", required=False)
        if isinstance(self.confidence, bool) or not isinstance(self.confidence, (int, float)):
            raise TypeError("confidence must be a finite number.")
        if not math.isfinite(float(self.confidence)) or not 0.0 <= float(self.confidence) <= 1.0:
            raise ValueError("confidence must be between 0.0 and 1.0.")
        object.__setattr__(self, "confidence", float(self.confidence))
        if not isinstance(self.compatibility, ColumnCompatibility):
            raise TypeError("compatibility must be a ColumnCompatibility.")
        if not isinstance(self.decision, MappingDecision):
            raise TypeError("decision must be a MappingDecision.")
        if not isinstance(self.evidence, tuple) or not all(
            isinstance(item, MappingEvidence) for item in self.evidence
        ):
            raise TypeError("evidence must be a tuple of MappingEvidence values.")
        if self.decision is MappingDecision.SUGGESTED and self.target_column is None:
            raise ValueError("suggested mappings require a target_column.")
        if self.decision is not MappingDecision.SUGGESTED and self.target_column is not None:
            raise ValueError("only suggested mappings may expose a target_column.")

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class TransformationExpression(_MetadataModel):
    expression_type: TransformationExpressionType
    source_columns: tuple[str, ...] = ()
    literal_value: str | int | float | bool | None = None
    target_canonical_type: CanonicalType | None = None
    arguments: tuple["TransformationExpression", ...] = ()
    separator: str | None = None
    nullable: bool | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.expression_type, TransformationExpressionType):
            raise TypeError("expression_type must be a TransformationExpressionType.")
        if not isinstance(self.source_columns, tuple):
            raise TypeError("source_columns must be a tuple.")
        for column in self.source_columns:
            _validate_identifier(column, "source_columns", required=True)
        if len(set(self.source_columns)) != len(self.source_columns):
            raise ValueError("source_columns must be unique.")
        if self.target_canonical_type is not None and not isinstance(
            self.target_canonical_type, CanonicalType
        ):
            raise TypeError("target_canonical_type must be a CanonicalType or None.")
        if not isinstance(self.arguments, tuple) or not all(
            isinstance(item, TransformationExpression) for item in self.arguments
        ):
            raise TypeError("arguments must be a tuple of TransformationExpression values.")
        if self.separator is not None and not isinstance(self.separator, str):
            raise TypeError("separator must be a string or None.")
        if self.nullable is not None and not isinstance(self.nullable, bool):
            raise TypeError("nullable must be a boolean or None.")
        if self.description is not None and not isinstance(self.description, str):
            raise TypeError("description must be a string or None.")
        if isinstance(self.literal_value, (list, tuple, dict, set, frozenset)):
            raise TypeError("literal_value must be a safe scalar or None.")
        if isinstance(self.literal_value, float) and not math.isfinite(self.literal_value):
            raise ValueError("literal_value must be finite.")
        kind = self.expression_type
        if kind is TransformationExpressionType.DIRECT_COPY:
            valid = len(self.source_columns) == 1 and not self.arguments and self.literal_value is None
        elif kind is TransformationExpressionType.CAST:
            valid = (
                len(self.source_columns) == 1
                and self.target_canonical_type is not None
                and not self.arguments
                and self.literal_value is None
            )
        elif kind is TransformationExpressionType.CONCAT:
            valid = len(self.source_columns) >= 2 and not self.arguments and self.literal_value is None
        elif kind is TransformationExpressionType.COALESCE:
            valid = len(self.source_columns) + len(self.arguments) >= 2 and self.literal_value is None
        elif kind is TransformationExpressionType.LITERAL:
            valid = not self.source_columns and not self.arguments
        else:  # SOURCE_COLUMN
            valid = len(self.source_columns) == 1 and not self.arguments and self.literal_value is None
        if not valid:
            raise ValueError("Transformation expression fields are invalid for its expression_type.")

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class MappingReviewDecision(_MetadataModel):
    source_column: str
    status: MappingApprovalStatus
    target_column: str | None = None
    reviewer_note: str | None = None
    override_reason: str | None = None
    transformation: TransformationExpression | None = None

    def __post_init__(self) -> None:
        _validate_identifier(self.source_column, "source_column", required=True)
        _validate_identifier(self.target_column, "target_column", required=False)
        if not isinstance(self.status, MappingApprovalStatus):
            raise TypeError("status must be a MappingApprovalStatus.")
        for field_name in ("reviewer_note", "override_reason"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{field_name} must be a string or None.")
        if self.transformation is not None and not isinstance(
            self.transformation, TransformationExpression
        ):
            raise TypeError("transformation must be a TransformationExpression or None.")
        if self.status is MappingApprovalStatus.REJECTED and self.transformation is not None:
            raise ValueError("rejected mappings cannot include a transformation.")
        if self.status is MappingApprovalStatus.OVERRIDDEN and not (
            isinstance(self.override_reason, str) and self.override_reason.strip()
        ):
            raise ValueError("overridden mappings require a non-empty override_reason.")

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class ColumnMappingApproval(_MetadataModel):
    source_column: str
    target_column: str | None
    status: MappingApprovalStatus
    original_confidence: float
    original_compatibility: ColumnCompatibility
    original_evidence: tuple[MappingEvidence, ...]
    compatibility: ColumnCompatibility
    target_ordinal_position: int | None = None
    reviewer_note: str | None = None
    override_reason: str | None = None
    transformation: TransformationExpression | None = None

    def __post_init__(self) -> None:
        _validate_identifier(self.source_column, "source_column", required=True)
        _validate_identifier(self.target_column, "target_column", required=False)
        if not isinstance(self.status, MappingApprovalStatus):
            raise TypeError("status must be a MappingApprovalStatus.")
        if isinstance(self.original_confidence, bool) or not isinstance(
            self.original_confidence, (int, float)
        ):
            raise TypeError("original_confidence must be a finite number.")
        if not math.isfinite(float(self.original_confidence)) or not 0.0 <= float(
            self.original_confidence
        ) <= 1.0:
            raise ValueError("original_confidence must be between 0.0 and 1.0.")
        object.__setattr__(self, "original_confidence", float(self.original_confidence))
        if not isinstance(self.original_compatibility, ColumnCompatibility):
            raise TypeError("original_compatibility must be a ColumnCompatibility.")
        if not isinstance(self.compatibility, ColumnCompatibility):
            raise TypeError("compatibility must be a ColumnCompatibility.")
        if self.target_ordinal_position is not None and (
            isinstance(self.target_ordinal_position, bool)
            or not isinstance(self.target_ordinal_position, int)
            or self.target_ordinal_position < 0
        ):
            raise ValueError("target_ordinal_position must be a non-negative integer or None.")
        if not isinstance(self.original_evidence, tuple) or not all(
            isinstance(item, MappingEvidence) for item in self.original_evidence
        ):
            raise TypeError("original_evidence must be a tuple of MappingEvidence values.")
        for field_name in ("reviewer_note", "override_reason"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{field_name} must be a string or None.")
        if self.transformation is not None and not isinstance(
            self.transformation, TransformationExpression
        ):
            raise TypeError("transformation must be a TransformationExpression or None.")
        if self.status is MappingApprovalStatus.APPROVED and self.target_column is None:
            raise ValueError("approved mappings require a target_column.")
        if self.status is MappingApprovalStatus.REJECTED:
            if self.transformation is not None or self.target_column is not None:
                raise ValueError("rejected mappings cannot include a target or transformation.")
        if self.status is MappingApprovalStatus.OVERRIDDEN:
            if self.target_column is None or not (
                isinstance(self.override_reason, str) and self.override_reason.strip()
            ):
                raise ValueError("overridden mappings require a target_column and non-empty override_reason.")
        if (
            self.status is MappingApprovalStatus.APPROVED
            and self.compatibility is ColumnCompatibility.INCOMPATIBLE
        ):
            raise ValueError("incompatible mappings require OVERRIDDEN status.")

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class TableMappingPlan(_MetadataModel):
    source_table: TableMappingIdentity
    target_table: TableMappingIdentity
    suggestions: tuple[ColumnMappingSuggestion, ...]
    unmatched_source_columns: tuple[str, ...]
    unmatched_target_columns: tuple[str, ...]
    ambiguous_source_columns: tuple[str, ...]
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.source_table, TableMappingIdentity) or not isinstance(
            self.target_table, TableMappingIdentity
        ):
            raise TypeError("source_table and target_table must be TableMappingIdentity values.")
        if not isinstance(self.suggestions, tuple) or not all(
            isinstance(item, ColumnMappingSuggestion) for item in self.suggestions
        ):
            raise TypeError("suggestions must be a tuple of ColumnMappingSuggestion values.")
        sources = tuple(item.source_column for item in self.suggestions)
        targets = tuple(
            item.target_column
            for item in self.suggestions
            if item.decision is MappingDecision.SUGGESTED
        )
        if len(set(sources)) != len(sources) or len(set(targets)) != len(targets):
            raise ValueError("final mapping assignments must be one-to-one.")
        for field_name in (
            "unmatched_source_columns",
            "unmatched_target_columns",
            "ambiguous_source_columns",
        ):
            values = getattr(self, field_name)
            if not isinstance(values, tuple) or len(set(values)) != len(values):
                raise ValueError(f"{field_name} must be a tuple of unique column names.")
            for value in values:
                _validate_identifier(value, field_name, required=True)
        if not isinstance(self.warnings, tuple):
            raise TypeError("warnings must be a tuple.")
        for warning in self.warnings:
            _validate_fixed_code(warning, "warnings")
        if self.warnings != tuple(sorted(set(self.warnings))):
            raise ValueError("warnings must be sorted and deduplicated.")

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class ApprovedTableMappingPlan(_MetadataModel):
    source_table: TableMappingIdentity
    target_table: TableMappingIdentity
    approvals: tuple[ColumnMappingApproval, ...]
    approved_mappings: tuple[ColumnMappingApproval, ...]
    rejected_source_columns: tuple[str, ...]
    unmatched_source_columns: tuple[str, ...]
    unmatched_target_columns: tuple[str, ...]
    warnings: tuple[str, ...] = ()
    version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.source_table, TableMappingIdentity) or not isinstance(
            self.target_table, TableMappingIdentity
        ):
            raise TypeError("source_table and target_table must be TableMappingIdentity values.")
        if not isinstance(self.approvals, tuple) or not all(
            isinstance(item, ColumnMappingApproval) for item in self.approvals
        ):
            raise TypeError("approvals must be a tuple of ColumnMappingApproval values.")
        sources = tuple(item.source_column for item in self.approvals)
        if len(set(sources)) != len(sources):
            raise ValueError("approvals must contain each source column at most once.")
        approved = tuple(
            item
            for item in self.approvals
            if item.status in {MappingApprovalStatus.APPROVED, MappingApprovalStatus.OVERRIDDEN}
        )
        if self.approved_mappings != approved:
            raise ValueError("approved_mappings must exactly reflect approved approvals.")
        targets = tuple(item.target_column for item in approved)
        if len(set(targets)) != len(targets):
            raise ValueError("approved mappings must not reuse a target column.")
        for field_name in (
            "rejected_source_columns",
            "unmatched_source_columns",
            "unmatched_target_columns",
        ):
            values = getattr(self, field_name)
            if not isinstance(values, tuple) or len(set(values)) != len(values):
                raise ValueError(f"{field_name} must be a tuple of unique column names.")
            for value in values:
                _validate_identifier(value, field_name, required=True)
        if not isinstance(self.warnings, tuple):
            raise TypeError("warnings must be a tuple.")
        for warning in self.warnings:
            _validate_fixed_code(warning, "warnings")
        if self.warnings != tuple(sorted(set(self.warnings))):
            raise ValueError("warnings must be sorted and deduplicated.")
        if isinstance(self.version, bool) or not isinstance(self.version, int) or self.version < 1:
            raise ValueError("version must be a positive integer.")

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class GeneratedTransformationSql(_MetadataModel):
    dialect: SqlDialect
    statement_type: TransformationStatementType
    sql: str
    parameters: tuple[object, ...]
    source_relation: tuple[str, str, str]
    target_relation: tuple[str | None, str, str]
    source_columns: tuple[str, ...]
    target_columns: tuple[str, ...]
    approved_plan_version: int
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.dialect, SqlDialect) or not isinstance(self.statement_type, TransformationStatementType):
            raise TypeError("dialect and statement_type must be SQL enums.")
        if not isinstance(self.sql, str) or not self.sql.strip():
            raise ValueError("sql must be non-empty.")
        if not isinstance(self.parameters, tuple):
            raise TypeError("parameters must be a tuple.")
        for value in self.parameters:
            if not _safe_literal(value):
                raise TypeError("parameters must contain safe scalar literals.")
        for relation in (self.source_relation, self.target_relation):
            if not isinstance(relation, tuple) or len(relation) != 3:
                raise TypeError("relations must be three-component tuples.")
        for field_name in ("source_columns", "target_columns"):
            values = getattr(self, field_name)
            if not isinstance(values, tuple):
                raise TypeError(f"{field_name} must be a tuple.")
            for value in values:
                _validate_identifier(value, field_name, required=True)
        if isinstance(self.approved_plan_version, bool) or not isinstance(self.approved_plan_version, int) or self.approved_plan_version < 1:
            raise ValueError("approved_plan_version must be positive.")
        if self.warnings != tuple(sorted(set(self.warnings))):
            raise ValueError("warnings must be sorted and deduplicated.")

    def to_dict(self) -> dict[str, object]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


def _safe_literal(value: object) -> bool:
    from datetime import date, datetime, time
    from decimal import Decimal
    return (
        value is None
        or isinstance(value, (str, bool, date, datetime, time))
        or (isinstance(value, int) and not isinstance(value, bool))
        or (isinstance(value, float) and math.isfinite(value))
        or (isinstance(value, Decimal) and value.is_finite())
    )


__all__ = [
    "ColumnCompatibility",
    "ColumnMappingApproval",
    "ColumnMappingSuggestion",
    "ApprovedTableMappingPlan",
    "MappingApprovalStatus",
    "MappingDecision",
    "MappingEvidence",
    "MappingReviewDecision",
    "TableMappingIdentity",
    "TableMappingPlan",
    "TransformationExpression",
    "TransformationExpressionType",
    "GeneratedTransformationSql",
    "SqlDialect",
    "TransformationStatementType",
]
