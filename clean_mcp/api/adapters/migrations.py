"""Lossless conversions between strict API schemas and immutable domain models."""

from __future__ import annotations

from models.discovery import (
    CheckConstraintMetadata,
    DiscoveryCoverage,
    ForeignKeyMetadata,
    KeyConstraintMetadata,
    TableMetadata,
)
from models.mapping import (
    ApprovedTableMappingPlan,
    ColumnMappingApproval,
    ColumnMappingSuggestion,
    GeneratedTransformationSql,
    MappingEvidence,
    MappingReviewDecision,
    TableMappingIdentity,
    TableMappingPlan,
    TransformationExpression,
)
from models.metadata import ColumnMetadata
from models.validation import (
    GeneratedValidationSql,
    MigrationValidationExecutionReport,
    MigrationValidationExecutionRequest,
    MigrationValidationReport,
    ValidationCheckDefinition,
    ValidationCheckResult,
)

from ..schemas.migrations import (
    ApprovedTableMappingPlanSchema,
    CheckConstraintSchema,
    ColumnMappingApprovalSchema,
    ColumnMappingSuggestionSchema,
    ColumnMetadataSchema,
    DiscoveryCoverageSchema,
    ForeignKeySchema,
    GeneratedTransformationSqlSchema,
    GeneratedValidationSqlSchema,
    KeyConstraintSchema,
    MappingEvidenceSchema,
    MappingReviewDecisionSchema,
    MigrationValidationReportSchema,
    TableIdentitySchema,
    TableMappingPlanSchema,
    TableMetadataSchema,
    TransformationExpressionSchema,
    ValidationCheckDefinitionSchema,
    ValidationCheckResultSchema,
    ValidationExecutionRequestSchema,
    ValidationExecutionResponse,
)


def column_to_api(value: ColumnMetadata) -> ColumnMetadataSchema:
    return ColumnMetadataSchema(**{
        name: getattr(value, name)
        for name in ColumnMetadataSchema.model_fields
    })


def column_to_domain(value: ColumnMetadataSchema) -> ColumnMetadata:
    return ColumnMetadata(**value.model_dump(mode="python"), vendor_metadata={})


def key_to_api(value: KeyConstraintMetadata) -> KeyConstraintSchema:
    return KeyConstraintSchema(**{name: getattr(value, name) for name in KeyConstraintSchema.model_fields})


def key_to_domain(value: KeyConstraintSchema) -> KeyConstraintMetadata:
    return KeyConstraintMetadata(**value.model_dump(mode="python"), vendor_metadata={})


def foreign_key_to_api(value: ForeignKeyMetadata) -> ForeignKeySchema:
    return ForeignKeySchema(**{name: getattr(value, name) for name in ForeignKeySchema.model_fields})


def foreign_key_to_domain(value: ForeignKeySchema) -> ForeignKeyMetadata:
    return ForeignKeyMetadata(**value.model_dump(mode="python"), vendor_metadata={})


def check_to_api(value: CheckConstraintMetadata) -> CheckConstraintSchema:
    return CheckConstraintSchema(**{name: getattr(value, name) for name in CheckConstraintSchema.model_fields})


def check_to_domain(value: CheckConstraintSchema) -> CheckConstraintMetadata:
    return CheckConstraintMetadata(**value.model_dump(mode="python"), vendor_metadata={})


def coverage_to_api(value: DiscoveryCoverage) -> DiscoveryCoverageSchema:
    return DiscoveryCoverageSchema(**{name: getattr(value, name) for name in DiscoveryCoverageSchema.model_fields})


def coverage_to_domain(value: DiscoveryCoverageSchema) -> DiscoveryCoverage:
    return DiscoveryCoverage(**value.model_dump(mode="python"))


def table_to_api(value: TableMetadata) -> TableMetadataSchema:
    return TableMetadataSchema(
        **{
            name: getattr(value, name)
            for name in TableMetadataSchema.model_fields
            if name not in {"columns", "primary_key", "unique_constraints", "foreign_keys", "check_constraints", "coverage"}
        },
        columns=tuple(column_to_api(item) for item in value.columns),
        primary_key=key_to_api(value.primary_key) if value.primary_key is not None else None,
        unique_constraints=tuple(key_to_api(item) for item in value.unique_constraints),
        foreign_keys=tuple(foreign_key_to_api(item) for item in value.foreign_keys),
        check_constraints=tuple(check_to_api(item) for item in value.check_constraints),
        coverage=coverage_to_api(value.coverage),
    )


def table_to_domain(value: TableMetadataSchema) -> TableMetadata:
    scalar = value.model_dump(
        mode="python",
        exclude={"columns", "primary_key", "unique_constraints", "foreign_keys", "check_constraints", "coverage"},
    )
    return TableMetadata(
        **scalar,
        columns=tuple(column_to_domain(item) for item in value.columns),
        primary_key=key_to_domain(value.primary_key) if value.primary_key is not None else None,
        unique_constraints=tuple(key_to_domain(item) for item in value.unique_constraints),
        foreign_keys=tuple(foreign_key_to_domain(item) for item in value.foreign_keys),
        check_constraints=tuple(check_to_domain(item) for item in value.check_constraints),
        coverage=coverage_to_domain(value.coverage),
        vendor_metadata={},
    )


def identity_to_api(value: TableMappingIdentity) -> TableIdentitySchema:
    return TableIdentitySchema(**value.to_dict())


def identity_to_domain(value: TableIdentitySchema) -> TableMappingIdentity:
    return TableMappingIdentity(**value.model_dump(mode="python"))


def evidence_to_api(value: MappingEvidence) -> MappingEvidenceSchema:
    return MappingEvidenceSchema(**value.to_dict())


def evidence_to_domain(value: MappingEvidenceSchema) -> MappingEvidence:
    return MappingEvidence(**value.model_dump(mode="python"))


def suggestion_to_api(value: ColumnMappingSuggestion) -> ColumnMappingSuggestionSchema:
    return ColumnMappingSuggestionSchema(
        source_column=value.source_column,
        target_column=value.target_column,
        confidence=value.confidence,
        compatibility=value.compatibility,
        decision=value.decision,
        evidence=tuple(evidence_to_api(item) for item in value.evidence),
    )


def suggestion_to_domain(value: ColumnMappingSuggestionSchema) -> ColumnMappingSuggestion:
    return ColumnMappingSuggestion(
        **value.model_dump(mode="python", exclude={"evidence"}),
        evidence=tuple(evidence_to_domain(item) for item in value.evidence),
    )


def plan_to_api(value: TableMappingPlan) -> TableMappingPlanSchema:
    return TableMappingPlanSchema(
        source_table=identity_to_api(value.source_table),
        target_table=identity_to_api(value.target_table),
        suggestions=tuple(suggestion_to_api(item) for item in value.suggestions),
        unmatched_source_columns=value.unmatched_source_columns,
        unmatched_target_columns=value.unmatched_target_columns,
        ambiguous_source_columns=value.ambiguous_source_columns,
        warnings=value.warnings,
    )


def plan_to_domain(value: TableMappingPlanSchema) -> TableMappingPlan:
    return TableMappingPlan(
        source_table=identity_to_domain(value.source_table),
        target_table=identity_to_domain(value.target_table),
        suggestions=tuple(suggestion_to_domain(item) for item in value.suggestions),
        unmatched_source_columns=tuple(value.unmatched_source_columns),
        unmatched_target_columns=tuple(value.unmatched_target_columns),
        ambiguous_source_columns=tuple(value.ambiguous_source_columns),
        warnings=tuple(value.warnings),
    )


def expression_to_api(value: TransformationExpression | None) -> TransformationExpressionSchema | None:
    if value is None:
        return None
    return TransformationExpressionSchema(
        expression_type=value.expression_type,
        source_columns=value.source_columns,
        literal_value=value.literal_value,
        target_canonical_type=value.target_canonical_type,
        arguments=tuple(expression_to_api(item) for item in value.arguments),  # type: ignore[arg-type]
        separator=value.separator,
        nullable=value.nullable,
        description=value.description,
    )


def expression_to_domain(value: TransformationExpressionSchema | None) -> TransformationExpression | None:
    if value is None:
        return None
    return TransformationExpression(
        **value.model_dump(mode="python", exclude={"arguments"}),
        arguments=tuple(expression_to_domain(item) for item in value.arguments),  # type: ignore[arg-type]
    )


def decision_to_domain(value: MappingReviewDecisionSchema) -> MappingReviewDecision:
    return MappingReviewDecision(
        **value.model_dump(mode="python", exclude={"transformation"}),
        transformation=expression_to_domain(value.transformation),
    )


def approval_to_api(value: ColumnMappingApproval) -> ColumnMappingApprovalSchema:
    return ColumnMappingApprovalSchema(
        source_column=value.source_column,
        target_column=value.target_column,
        status=value.status,
        original_confidence=value.original_confidence,
        original_compatibility=value.original_compatibility,
        original_evidence=tuple(evidence_to_api(item) for item in value.original_evidence),
        compatibility=value.compatibility,
        target_ordinal_position=value.target_ordinal_position,
        reviewer_note=value.reviewer_note,
        override_reason=value.override_reason,
        transformation=expression_to_api(value.transformation),
    )


def approval_to_domain(value: ColumnMappingApprovalSchema) -> ColumnMappingApproval:
    return ColumnMappingApproval(
        **value.model_dump(mode="python", exclude={"original_evidence", "transformation"}),
        original_evidence=tuple(evidence_to_domain(item) for item in value.original_evidence),
        transformation=expression_to_domain(value.transformation),
    )


def approved_plan_to_api(value: ApprovedTableMappingPlan) -> ApprovedTableMappingPlanSchema:
    return ApprovedTableMappingPlanSchema(
        source_table=identity_to_api(value.source_table),
        target_table=identity_to_api(value.target_table),
        approvals=tuple(approval_to_api(item) for item in value.approvals),
        approved_mappings=tuple(approval_to_api(item) for item in value.approved_mappings),
        rejected_source_columns=value.rejected_source_columns,
        unmatched_source_columns=value.unmatched_source_columns,
        unmatched_target_columns=value.unmatched_target_columns,
        warnings=value.warnings,
        version=value.version,
    )


def approved_plan_to_domain(value: ApprovedTableMappingPlanSchema) -> ApprovedTableMappingPlan:
    return ApprovedTableMappingPlan(
        source_table=identity_to_domain(value.source_table),
        target_table=identity_to_domain(value.target_table),
        approvals=tuple(approval_to_domain(item) for item in value.approvals),
        approved_mappings=tuple(approval_to_domain(item) for item in value.approved_mappings),
        rejected_source_columns=tuple(value.rejected_source_columns),
        unmatched_source_columns=tuple(value.unmatched_source_columns),
        unmatched_target_columns=tuple(value.unmatched_target_columns),
        warnings=tuple(value.warnings),
        version=value.version,
    )


def transformation_sql_to_api(value: GeneratedTransformationSql) -> GeneratedTransformationSqlSchema:
    return GeneratedTransformationSqlSchema(**value.to_dict())


def check_definition_to_api(value: ValidationCheckDefinition) -> ValidationCheckDefinitionSchema:
    return ValidationCheckDefinitionSchema(**value.to_dict())


def validation_sql_to_api(value: GeneratedValidationSql) -> GeneratedValidationSqlSchema:
    return GeneratedValidationSqlSchema(
        dialect=value.dialect,
        sql=value.sql,
        parameters=value.parameters,
        relation=value.relation,
        metric_aliases=value.metric_aliases,
        checks=tuple(check_definition_to_api(item) for item in value.checks),
        warnings=value.warnings,
    )


def execution_request_to_domain(value: ValidationExecutionRequestSchema) -> MigrationValidationExecutionRequest:
    return MigrationValidationExecutionRequest(
        source_profile_id=value.source_profile_id,
        target_profile_id=value.target_profile_id,
        approved_mapping_plan=approved_plan_to_domain(value.approved_plan),
        source_schema=value.source_schema,
        source_table=value.source_table,
        target_database=value.target_database,
        target_schema=value.target_schema,
        target_table=value.target_table,
        timeout_seconds=value.timeout_seconds,
        explicitly_approved=value.explicitly_approved,
    )


def check_result_to_api(value: ValidationCheckResult) -> ValidationCheckResultSchema:
    return ValidationCheckResultSchema(**value.to_dict())


def validation_report_to_api(value: MigrationValidationReport) -> MigrationValidationReportSchema:
    return MigrationValidationReportSchema(
        source_table=value.source_table,
        target_table=value.target_table,
        check_results=tuple(check_result_to_api(item) for item in value.check_results),
        status=value.status,
        matched_count=value.matched_count,
        mismatched_count=value.mismatched_count,
        unavailable_count=value.unavailable_count,
        warnings=value.warnings,
        approved_plan_version=value.approved_plan_version,
    )


def execution_report_to_api(value: MigrationValidationExecutionReport) -> ValidationExecutionResponse:
    return ValidationExecutionResponse(
        source_sql_summary=validation_sql_to_api(value.source_sql_summary),
        target_sql_summary=validation_sql_to_api(value.target_sql_summary),
        validation_report=validation_report_to_api(value.validation_report),
        source_execution_status=value.source_execution_status,
        target_execution_status=value.target_execution_status,
        warnings=value.warnings,
    )


__all__ = [name for name in globals() if name.endswith(("_to_api", "_to_domain"))]
