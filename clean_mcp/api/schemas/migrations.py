"""Strict versioned transport contracts for migration workflows."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AfterValidator, Field, StrictBool, StrictFloat, StrictInt, StringConstraints

from models.discovery import ConstraintType, CoverageStatus, DatabaseObjectType, ObjectPersistence
from models.mapping import (
    ColumnCompatibility,
    MappingApprovalStatus,
    MappingDecision,
    SqlDialect,
    TransformationExpressionType,
    TransformationStatementType,
)
from models.metadata import CanonicalType
from models.validation import (
    MigrationValidationStatus,
    ValidationCheckType,
    ValidationExecutionStatus,
    ValidationStatus,
)

from .common import ApiSchema

def _exact_identifier(value: str) -> str:
    if not value.strip() or "\x00" in value:
        raise ValueError("Identifier text is invalid.")
    return value


Identifier = Annotated[
    str,
    StringConstraints(min_length=1, max_length=512),
    AfterValidator(_exact_identifier),
]
OptionalText = Annotated[str, StringConstraints(max_length=4096)]
SafeCode = Annotated[str, StringConstraints(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")]
Confidence = Annotated[StrictFloat, Field(ge=0.0, le=1.0, allow_inf_nan=False)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
SafeScalar = str | StrictBool | StrictInt | StrictFloat | Decimal | date | datetime | time | None


class ColumnMetadataSchema(ApiSchema):
    catalog_name: Identifier | None
    schema_name: Identifier | None
    table_name: Identifier
    column_name: Identifier
    ordinal_position: NonNegativeInt | None
    native_type: OptionalText | None
    canonical_type: CanonicalType
    nullable: StrictBool | None
    character_length: NonNegativeInt | None
    numeric_precision: NonNegativeInt | None
    numeric_scale: StrictInt | None
    datetime_precision: NonNegativeInt | None
    is_primary_key: StrictBool | None = None
    is_foreign_key: StrictBool | None = None
    default_expression: OptionalText | None = None
    comment: OptionalText | None = None
    collation: OptionalText | None = None
    is_identity: StrictBool | None = None
    identity_generation: OptionalText | None = None
    is_auto_increment: StrictBool | None = None
    is_generated: StrictBool | None = None
    generation_expression: OptionalText | None = None
    is_unique_key: StrictBool | None = None
    array_dimensions: NonNegativeInt | None = None
    element_native_type: OptionalText | None = None
    element_canonical_type: CanonicalType | None = None


class KeyConstraintSchema(ApiSchema):
    name: Identifier | None
    constraint_type: ConstraintType
    columns: tuple[Identifier, ...]
    is_enforced: StrictBool | None = None
    is_validated: StrictBool | None = None
    is_rely: StrictBool | None = None
    is_deferrable: StrictBool | None = None
    initially_deferred: StrictBool | None = None
    comment: OptionalText | None = None


class ForeignKeySchema(ApiSchema):
    name: Identifier | None
    local_columns: tuple[Identifier, ...]
    referenced_catalog: Identifier | None
    referenced_schema: Identifier | None
    referenced_table: Identifier
    referenced_columns: tuple[Identifier, ...]
    match_option: OptionalText | None = None
    update_rule: OptionalText | None = None
    delete_rule: OptionalText | None = None
    is_enforced: StrictBool | None = None
    is_validated: StrictBool | None = None
    is_rely: StrictBool | None = None
    is_deferrable: StrictBool | None = None
    initially_deferred: StrictBool | None = None
    comment: OptionalText | None = None


class CheckConstraintSchema(ApiSchema):
    name: Identifier | None
    expression: OptionalText
    is_enforced: StrictBool | None = None
    is_validated: StrictBool | None = None
    is_rely: StrictBool | None = None
    comment: OptionalText | None = None


class DiscoveryCoverageSchema(ApiSchema):
    columns: CoverageStatus
    primary_key: CoverageStatus
    unique_constraints: CoverageStatus
    foreign_keys: CoverageStatus
    check_constraints: CoverageStatus
    comments: CoverageStatus
    estimated_row_count: CoverageStatus
    view_definition: CoverageStatus
    partitioning: CoverageStatus
    clustering: CoverageStatus
    warnings: tuple[SafeCode, ...] = ()


class TableMetadataSchema(ApiSchema):
    catalog_name: Identifier | None
    schema_name: Identifier
    object_name: Identifier
    system: Identifier
    object_type: DatabaseObjectType
    persistence: ObjectPersistence
    owner: OptionalText | None = None
    comment: OptionalText | None = None
    estimated_row_count: NonNegativeInt | None = None
    is_system_managed: StrictBool | None = None
    columns: tuple[ColumnMetadataSchema, ...]
    primary_key: KeyConstraintSchema | None = None
    unique_constraints: tuple[KeyConstraintSchema, ...] = ()
    foreign_keys: tuple[ForeignKeySchema, ...] = ()
    check_constraints: tuple[CheckConstraintSchema, ...] = ()
    view_definition: OptionalText | None = None
    clustering_expression: OptionalText | None = None
    is_partitioned: StrictBool | None = None
    partitioning_expression: OptionalText | None = None
    coverage: DiscoveryCoverageSchema


class DiscoveryRequest(ApiSchema):
    profile_id: Identifier
    schema_name: Identifier
    table_name: Identifier
    database_name: Identifier | None = None


class TableIdentitySchema(ApiSchema):
    catalog_name: Identifier | None
    schema_name: Identifier
    table_name: Identifier
    system: Identifier


class MappingEvidenceSchema(ApiSchema):
    code: SafeCode
    explanation: OptionalText
    contribution: StrictFloat | None = None


class ColumnMappingSuggestionSchema(ApiSchema):
    source_column: Identifier
    target_column: Identifier | None
    confidence: Confidence
    compatibility: ColumnCompatibility
    decision: MappingDecision
    evidence: tuple[MappingEvidenceSchema, ...]


class TableMappingPlanSchema(ApiSchema):
    source_table: TableIdentitySchema
    target_table: TableIdentitySchema
    suggestions: tuple[ColumnMappingSuggestionSchema, ...]
    unmatched_source_columns: tuple[Identifier, ...]
    unmatched_target_columns: tuple[Identifier, ...]
    ambiguous_source_columns: tuple[Identifier, ...]
    warnings: tuple[SafeCode, ...] = ()


class MappingSuggestionRequest(ApiSchema):
    source: TableMetadataSchema
    target: TableMetadataSchema


class TransformationExpressionSchema(ApiSchema):
    expression_type: TransformationExpressionType
    source_columns: tuple[Identifier, ...] = ()
    literal_value: SafeScalar = None
    target_canonical_type: CanonicalType | None = None
    arguments: tuple["TransformationExpressionSchema", ...] = ()
    separator: OptionalText | None = None
    nullable: StrictBool | None = None
    description: OptionalText | None = None


class MappingReviewDecisionSchema(ApiSchema):
    source_column: Identifier
    status: MappingApprovalStatus
    target_column: Identifier | None = None
    reviewer_note: OptionalText | None = None
    override_reason: OptionalText | None = None
    transformation: TransformationExpressionSchema | None = None


class MappingApprovalRequest(ApiSchema):
    plan: TableMappingPlanSchema
    source: TableMetadataSchema
    target: TableMetadataSchema
    decisions: tuple[MappingReviewDecisionSchema, ...]


class ColumnMappingApprovalSchema(ApiSchema):
    source_column: Identifier
    target_column: Identifier | None
    status: MappingApprovalStatus
    original_confidence: Confidence
    original_compatibility: ColumnCompatibility
    original_evidence: tuple[MappingEvidenceSchema, ...]
    compatibility: ColumnCompatibility
    target_ordinal_position: NonNegativeInt | None = None
    reviewer_note: OptionalText | None = None
    override_reason: OptionalText | None = None
    transformation: TransformationExpressionSchema | None = None


class ApprovedTableMappingPlanSchema(ApiSchema):
    source_table: TableIdentitySchema
    target_table: TableIdentitySchema
    approvals: tuple[ColumnMappingApprovalSchema, ...]
    approved_mappings: tuple[ColumnMappingApprovalSchema, ...]
    rejected_source_columns: tuple[Identifier, ...]
    unmatched_source_columns: tuple[Identifier, ...]
    unmatched_target_columns: tuple[Identifier, ...]
    warnings: tuple[SafeCode, ...] = ()
    version: PositiveInt = 1


class TransformationPreviewRequest(ApiSchema):
    approved_plan: ApprovedTableMappingPlanSchema
    staging_database: Identifier
    staging_schema: Identifier
    staging_table: Identifier
    statement_type: TransformationStatementType


class GeneratedTransformationSqlSchema(ApiSchema):
    preview_only: Literal[True] = True
    dialect: SqlDialect
    statement_type: TransformationStatementType
    sql: str
    parameters: tuple[SafeScalar, ...]
    source_relation: tuple[Identifier, Identifier, Identifier]
    target_relation: tuple[Identifier | None, Identifier, Identifier]
    source_columns: tuple[Identifier, ...]
    target_columns: tuple[Identifier, ...]
    approved_plan_version: PositiveInt
    warnings: tuple[SafeCode, ...] = ()


class ValidationPreviewRequest(ApiSchema):
    approved_plan: ApprovedTableMappingPlanSchema
    source_schema: Identifier
    source_table: Identifier
    target_database: Identifier
    target_schema: Identifier
    target_table: Identifier


class ValidationCheckDefinitionSchema(ApiSchema):
    check_id: Identifier
    check_type: ValidationCheckType
    source_column: Identifier | None
    target_column: Identifier | None
    source_metric_alias: Identifier
    target_metric_alias: Identifier


class GeneratedValidationSqlSchema(ApiSchema):
    dialect: SqlDialect
    sql: str
    parameters: tuple[SafeScalar, ...]
    relation: tuple[Identifier, ...]
    metric_aliases: tuple[Identifier, ...]
    checks: tuple[ValidationCheckDefinitionSchema, ...]
    warnings: tuple[SafeCode, ...] = ()


class ValidationPreviewResponse(ApiSchema):
    preview_only: Literal[True] = True
    source: GeneratedValidationSqlSchema
    target: GeneratedValidationSqlSchema


class ValidationExecutionRequestSchema(ValidationPreviewRequest):
    source_profile_id: Identifier
    target_profile_id: Identifier
    timeout_seconds: PositiveInt | None = None
    explicitly_approved: StrictBool


class ValidationCheckResultSchema(ApiSchema):
    check_id: Identifier
    check_type: ValidationCheckType
    source_value: NonNegativeInt | None
    target_value: NonNegativeInt | None
    status: ValidationStatus
    difference: StrictInt | None
    source_column: Identifier | None
    target_column: Identifier | None


class MigrationValidationReportSchema(ApiSchema):
    source_table: tuple[Identifier, ...]
    target_table: tuple[Identifier, ...]
    check_results: tuple[ValidationCheckResultSchema, ...]
    status: MigrationValidationStatus
    matched_count: NonNegativeInt
    mismatched_count: NonNegativeInt
    unavailable_count: NonNegativeInt
    warnings: tuple[SafeCode, ...]
    approved_plan_version: PositiveInt


class ValidationExecutionResponse(ApiSchema):
    source_sql_summary: GeneratedValidationSqlSchema
    target_sql_summary: GeneratedValidationSqlSchema
    validation_report: MigrationValidationReportSchema
    source_execution_status: ValidationExecutionStatus
    target_execution_status: ValidationExecutionStatus
    warnings: tuple[SafeCode, ...] = ()


TransformationExpressionSchema.model_rebuild()
