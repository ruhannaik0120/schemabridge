"""Tests for conservative primary-key strict-validation eligibility."""

from dataclasses import replace

from schemabridge.models.discovery import (
    ConstraintType,
    CoverageStatus,
    KeyConstraintMetadata,
)
from schemabridge.models.mapping import (
    MappingApprovalStatus,
    MappingReviewDecision,
    TransformationExpression,
    TransformationExpressionType,
)
from schemabridge.models.metadata import CanonicalType
from schemabridge.validation.primary_key import assess_primary_key_reconciliation
from tests.support.builders import mapping_column, mapping_table


def _primary_key(*columns: str) -> KeyConstraintMetadata:
    return KeyConstraintMetadata(
        name="pk_test",
        constraint_type=ConstraintType.PRIMARY_KEY,
        columns=columns,
        vendor_metadata={},
    )


def _tables():
    source = replace(
        mapping_table(
            "source", mapping_column("id"), mapping_column("name", ordinal=2)
        ),
        primary_key=_primary_key("id"),
    )
    target = replace(
        mapping_table(
            "target", mapping_column("person_id"), mapping_column("name", ordinal=2)
        ),
        primary_key=_primary_key("person_id"),
    )
    return source, target


def _plan(source, target):
    from schemabridge.mapping.approval import MappingApprovalService
    from schemabridge.mapping.suggestions import SchemaMappingService

    proposed = SchemaMappingService().suggest(source, target)
    return MappingApprovalService().apply(
        proposed,
        source=source,
        target=target,
        decisions=(
            MappingReviewDecision(
                source_column="id",
                target_column="person_id",
                status=MappingApprovalStatus.APPROVED,
                transformation=TransformationExpression(
                    expression_type=TransformationExpressionType.DIRECT_COPY,
                    source_columns=("id",),
                ),
            ),
            MappingReviewDecision(
                source_column="name",
                target_column="name",
                status=MappingApprovalStatus.APPROVED,
                transformation=TransformationExpression(
                    expression_type=TransformationExpressionType.DIRECT_COPY,
                    source_columns=("name",),
                ),
            ),
        ),
    )


def test_matching_direct_primary_keys_are_eligible():
    source, target = _tables()
    result = assess_primary_key_reconciliation(
        _plan(source, target), source_table=source, target_table=target
    )

    assert result.eligible is True
    assert result.source_key_columns == ("id",)
    assert result.target_key_columns == ("person_id",)
    assert result.reason is None


def test_missing_primary_key_metadata_is_not_eligible():
    source, target = _tables()
    source = replace(source, coverage=replace(source.coverage, primary_key=CoverageStatus.PARTIAL))

    result = assess_primary_key_reconciliation(
        _plan(source, target), source_table=source, target_table=target
    )

    assert result.eligible is False
    assert result.reason == "PRIMARY_KEY_METADATA_INCOMPLETE"


def test_transformed_primary_key_is_not_eligible():
    source, target = _tables()
    plan = _plan(source, target)
    approvals = list(plan.approvals)
    approvals[0] = replace(
        approvals[0],
        transformation=TransformationExpression(
            expression_type=TransformationExpressionType.CAST,
            source_columns=("id",),
            target_canonical_type=CanonicalType.STRING,
        ),
    )
    plan = replace(
        plan,
        approvals=tuple(approvals),
        approved_mappings=tuple(approvals),
    )

    result = assess_primary_key_reconciliation(
        plan, source_table=source, target_table=target
    )

    assert result.eligible is False
    assert result.reason == "PRIMARY_KEY_MAPPING_INCOMPLETE"


def test_different_target_primary_key_is_not_eligible():
    source, target = _tables()
    target = replace(target, primary_key=_primary_key("name"))

    result = assess_primary_key_reconciliation(
        _plan(source, target), source_table=source, target_table=target
    )

    assert result.eligible is False
    assert result.reason == "PRIMARY_KEY_MAPPING_MISMATCH"
