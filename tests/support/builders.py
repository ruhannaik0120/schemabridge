"""Canonical metadata and approved-mapping builders for tests."""

from __future__ import annotations

from schemabridge.models.discovery import (
    CoverageStatus,
    DatabaseObjectType,
    DiscoveryCoverage,
    ObjectPersistence,
    TableMetadata,
)
from schemabridge.models.mapping import (
    MappingApprovalStatus,
    MappingReviewDecision,
    TransformationExpression,
    TransformationExpressionType,
)
from schemabridge.models.metadata import CanonicalType, ColumnMetadata
from schemabridge.models.validation import MigrationValidationExecutionRequest
from schemabridge.mapping.approval import MappingApprovalService
from schemabridge.mapping.suggestions import SchemaMappingService


def mapping_column(
    name: str,
    canonical_type: CanonicalType = CanonicalType.STRING,
    ordinal: int = 1,
    *,
    nullable: bool | None = False,
) -> ColumnMetadata:
    return ColumnMetadata(
        catalog_name="catalog",
        schema_name="schema",
        table_name="table",
        column_name=name,
        ordinal_position=ordinal,
        native_type="native",
        canonical_type=canonical_type,
        nullable=nullable,
        character_length=100 if canonical_type is CanonicalType.STRING else None,
        numeric_precision=None,
        numeric_scale=None,
        datetime_precision=None,
        vendor_metadata={"password": "secret", "safe": "value"},
    )


def mapping_table(name: str, *columns: ColumnMetadata) -> TableMetadata:
    coverage = DiscoveryCoverage(
        columns=CoverageStatus.COMPLETE,
        primary_key=CoverageStatus.COMPLETE,
        unique_constraints=CoverageStatus.COMPLETE,
        foreign_keys=CoverageStatus.COMPLETE,
        check_constraints=CoverageStatus.COMPLETE,
        comments=CoverageStatus.COMPLETE,
        estimated_row_count=CoverageStatus.COMPLETE,
        view_definition=CoverageStatus.NOT_APPLICABLE,
        partitioning=CoverageStatus.NOT_APPLICABLE,
        clustering=CoverageStatus.NOT_APPLICABLE,
    )
    return TableMetadata(
        catalog_name="catalog",
        schema_name="schema",
        object_name=name,
        system="test",
        object_type=DatabaseObjectType.TABLE,
        persistence=ObjectPersistence.PERMANENT,
        columns=tuple(columns),
        coverage=coverage,
        vendor_metadata={"credential": "secret"},
    )


def approved_mapping_plan():
    source = mapping_table(
        "source",
        mapping_column("first_name", ordinal=1),
        mapping_column("last_name", ordinal=2),
        mapping_column("age", canonical_type=CanonicalType.INTEGER, ordinal=3),
    )
    target = mapping_table(
        "people",
        mapping_column("full_name", ordinal=1),
        mapping_column("age", canonical_type=CanonicalType.INTEGER, ordinal=2),
    )
    plan = SchemaMappingService().suggest(source, target)
    return MappingApprovalService().apply(
        plan,
        source=source,
        target=target,
        decisions=(
            MappingReviewDecision(
                source_column="first_name",
                target_column="full_name",
                status=MappingApprovalStatus.APPROVED,
                transformation=TransformationExpression(
                    expression_type=TransformationExpressionType.CONCAT,
                    source_columns=("first_name", "last_name"),
                    separator=" ",
                ),
            ),
            MappingReviewDecision(
                source_column="age",
                status=MappingApprovalStatus.APPROVED,
                transformation=TransformationExpression(
                    expression_type=TransformationExpressionType.DIRECT_COPY,
                    source_columns=("age",),
                ),
            ),
        ),
    )


def validation_execution_request(
    explicitly_approved: bool = True,
    *,
    plan=None,
) -> MigrationValidationExecutionRequest:
    return MigrationValidationExecutionRequest(
        source_profile_id="pg",
        target_profile_id="sf",
        approved_mapping_plan=plan or approved_mapping_plan(),
        source_schema="public",
        source_table="people",
        target_database="db",
        target_schema="schema",
        target_table="people",
        timeout_seconds=9,
        explicitly_approved=explicitly_approved,
    )
