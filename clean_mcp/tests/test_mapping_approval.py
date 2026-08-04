"""Focused tests for manual mapping approval and transformation expressions."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from models.discovery import CoverageStatus, DatabaseObjectType, DiscoveryCoverage, ObjectPersistence, TableMetadata
from models.mapping import (
    ColumnCompatibility,
    ColumnMappingApproval,
    MappingApprovalStatus,
    MappingReviewDecision,
    TransformationExpression,
    TransformationExpressionType,
)
from models.metadata import CanonicalType, ColumnMetadata
from services.mapping_approval import MappingApprovalService
from services.schema_mapping import SchemaMappingService


def _column(
    name: str,
    canonical_type: CanonicalType = CanonicalType.STRING,
    ordinal: int = 1,
    *,
    nullable: bool | None = False,
) -> ColumnMetadata:
    return ColumnMetadata(
        catalog_name="catalog", schema_name="schema", table_name="table", column_name=name,
        ordinal_position=ordinal, native_type="native", canonical_type=canonical_type,
        nullable=nullable, character_length=100 if canonical_type is CanonicalType.STRING else None,
        numeric_precision=None, numeric_scale=None, datetime_precision=None,
        vendor_metadata={"password": "secret", "safe": "value"},
    )


def _table(name: str, *columns: ColumnMetadata) -> TableMetadata:
    coverage = DiscoveryCoverage(
        columns=CoverageStatus.COMPLETE, primary_key=CoverageStatus.COMPLETE,
        unique_constraints=CoverageStatus.COMPLETE, foreign_keys=CoverageStatus.COMPLETE,
        check_constraints=CoverageStatus.COMPLETE, comments=CoverageStatus.COMPLETE,
        estimated_row_count=CoverageStatus.COMPLETE, view_definition=CoverageStatus.NOT_APPLICABLE,
        partitioning=CoverageStatus.NOT_APPLICABLE, clustering=CoverageStatus.NOT_APPLICABLE,
    )
    return TableMetadata(
        catalog_name="catalog", schema_name="schema", object_name=name, system="test",
        object_type=DatabaseObjectType.TABLE, persistence=ObjectPersistence.PERMANENT,
        columns=tuple(columns), coverage=coverage, vendor_metadata={"credential": "secret"},
    )


def _inputs(source_columns=None, target_columns=None):
    source = _table("source", *(source_columns or [_column("name")]))
    target = _table("target", *(target_columns or [_column("name")]))
    return source, target, SchemaMappingService().suggest(source, target)


def test_default_review_plan_is_human_in_the_loop_and_preserves_stage_4a_evidence():
    source, target, plan = _inputs()
    review = MappingApprovalService().create_review_plan(plan)
    assert all(item.status is MappingApprovalStatus.PENDING for item in review.approvals)
    assert review.approved_mappings == ()
    assert review.approvals[0].original_evidence == plan.suggestions[0].evidence
    assert review.source_table.table_name == source.object_name
    assert review.target_table.table_name == target.object_name


def test_approve_exact_mapping_and_reject_mapping():
    source, target, plan = _inputs([_column("name"), _column("discard", ordinal=2)], [_column("name")])
    approved = MappingApprovalService().apply(
        plan, source=source, target=target,
        decisions=(
            MappingReviewDecision(source_column="name", status=MappingApprovalStatus.APPROVED),
            MappingReviewDecision(source_column="discard", status=MappingApprovalStatus.REJECTED),
        ),
    )
    assert [(item.source_column, item.target_column) for item in approved.approved_mappings] == [("name", "name")]
    assert approved.rejected_source_columns == ("discard",)
    assert next(item for item in approved.approvals if item.source_column == "discard").target_column is None


def test_override_incompatible_mapping_requires_and_preserves_reason():
    source, target, plan = _inputs(
        [_column("enabled", CanonicalType.BOOLEAN)], [_column("enabled", CanonicalType.STRING)]
    )
    approved = MappingApprovalService().apply(
        plan, source=source, target=target,
        decisions=(MappingReviewDecision(
            source_column="enabled", target_column="enabled", status=MappingApprovalStatus.OVERRIDDEN,
            override_reason="Reviewer confirmed conversion policy.",
        ),),
    )
    item = approved.approved_mappings[0]
    assert item.status is MappingApprovalStatus.OVERRIDDEN
    assert item.compatibility is ColumnCompatibility.INCOMPATIBLE
    assert item.original_compatibility is ColumnCompatibility.INCOMPATIBLE
    with pytest.raises(ValueError, match="override_reason"):
        MappingReviewDecision(source_column="enabled", target_column="enabled", status=MappingApprovalStatus.OVERRIDDEN)
    with pytest.raises(ValueError, match="incompatible"):
        MappingApprovalService().apply(
            plan, source=source, target=target,
            decisions=(MappingReviewDecision(
                source_column="enabled", target_column="enabled", status=MappingApprovalStatus.APPROVED,
            ),),
        )


def test_manual_safe_remapping_recalculates_effective_compatibility_without_fuzzy_matching():
    source, target, plan = _inputs(
        [_column("given_name")], [_column("wrong_name"), _column("full_name", ordinal=2)]
    )
    approved = MappingApprovalService().apply(
        plan, source=source, target=target,
        decisions=(MappingReviewDecision(
            source_column="given_name", target_column="full_name", status=MappingApprovalStatus.APPROVED,
        ),),
    )
    item = approved.approved_mappings[0]
    assert item.target_column == "full_name"
    assert item.compatibility is ColumnCompatibility.EXACT
    assert item.original_compatibility is plan.suggestions[0].compatibility


def test_duplicate_source_decision_target_reuse_and_unknown_references_are_rejected():
    source, target, plan = _inputs(
        [_column("first"), _column("second", ordinal=2)], [_column("target")]
    )
    service = MappingApprovalService()
    with pytest.raises(ValueError, match="duplicate"):
        service.apply(plan, source=source, target=target, decisions=(
            MappingReviewDecision(source_column="first", target_column="target", status=MappingApprovalStatus.APPROVED),
            MappingReviewDecision(source_column="first", status=MappingApprovalStatus.REJECTED),
        ))
    with pytest.raises(ValueError, match="reuse"):
        service.apply(plan, source=source, target=target, decisions=(
            MappingReviewDecision(source_column="first", target_column="target", status=MappingApprovalStatus.APPROVED),
            MappingReviewDecision(source_column="second", target_column="target", status=MappingApprovalStatus.APPROVED),
        ))
    with pytest.raises(ValueError, match="unknown source"):
        service.apply(plan, source=source, target=target, decisions=(
            MappingReviewDecision(source_column="missing", status=MappingApprovalStatus.REJECTED),
        ))
    with pytest.raises(ValueError, match="unknown target"):
        service.apply(plan, source=source, target=target, decisions=(
            MappingReviewDecision(source_column="first", target_column="missing", status=MappingApprovalStatus.APPROVED),
        ))
    with pytest.raises(ValueError, match="unknown target"):
        service.apply(plan, source=source, target=target, decisions=(
            MappingReviewDecision(source_column="first", target_column="missing", status=MappingApprovalStatus.REJECTED),
        ))


def test_transformation_expression_validation_rules():
    direct = TransformationExpression(
        expression_type=TransformationExpressionType.DIRECT_COPY, source_columns=("source",)
    )
    cast = TransformationExpression(
        expression_type=TransformationExpressionType.CAST, source_columns=("source",),
        target_canonical_type=CanonicalType.INTEGER,
    )
    concat = TransformationExpression(
        expression_type=TransformationExpressionType.CONCAT, source_columns=("first", "last"), separator=" "
    )
    coalesce = TransformationExpression(
        expression_type=TransformationExpressionType.COALESCE, source_columns=("first", "second"), nullable=True
    )
    literal = TransformationExpression(
        expression_type=TransformationExpressionType.LITERAL, literal_value="fixed", nullable=False
    )
    assert direct.source_columns == ("source",)
    assert cast.target_canonical_type is CanonicalType.INTEGER
    assert concat.separator == " "
    assert coalesce.nullable is True
    assert literal.literal_value == "fixed"
    with pytest.raises(ValueError):
        TransformationExpression(expression_type=TransformationExpressionType.DIRECT_COPY, source_columns=("a", "b"))
    with pytest.raises(ValueError):
        TransformationExpression(expression_type=TransformationExpressionType.CAST, source_columns=("a",))
    with pytest.raises(ValueError):
        TransformationExpression(expression_type=TransformationExpressionType.CONCAT, source_columns=("a",))
    with pytest.raises(ValueError):
        TransformationExpression(expression_type=TransformationExpressionType.COALESCE, source_columns=("a",))
    with pytest.raises(ValueError):
        TransformationExpression(expression_type=TransformationExpressionType.LITERAL, source_columns=("a",))


def test_concat_workflow_validates_all_sources_and_leaves_other_item_pending():
    source, target, plan = _inputs(
        [_column("first_name", ordinal=1), _column("last_name", ordinal=2), _column("age", CanonicalType.INTEGER, 3)],
        [_column("full_name", ordinal=1), _column("age", CanonicalType.INTEGER, 2)],
    )
    concat = TransformationExpression(
        expression_type=TransformationExpressionType.CONCAT,
        source_columns=("first_name", "last_name"), separator=" ", nullable=False,
        description="Combine reviewed name components.",
    )
    approved = MappingApprovalService().apply(
        plan, source=source, target=target,
        decisions=(
            MappingReviewDecision(source_column="first_name", target_column="full_name", status=MappingApprovalStatus.APPROVED, transformation=concat),
            MappingReviewDecision(source_column="age", status=MappingApprovalStatus.APPROVED,
                transformation=TransformationExpression(expression_type=TransformationExpressionType.DIRECT_COPY, source_columns=("age",))),
        ),
    )
    assert [(item.source_column, item.target_column) for item in approved.approved_mappings] == [("first_name", "full_name"), ("age", "age")]
    assert next(item for item in approved.approvals if item.source_column == "last_name").status is MappingApprovalStatus.PENDING
    bad = TransformationExpression(expression_type=TransformationExpressionType.DIRECT_COPY, source_columns=("missing",))
    with pytest.raises(ValueError, match="unknown source"):
        MappingApprovalService().apply(
            plan, source=source, target=target,
            decisions=(MappingReviewDecision(source_column="first_name", target_column="full_name", status=MappingApprovalStatus.APPROVED, transformation=bad),),
        )


def test_ordering_immutability_and_safe_serialization_preserve_original_plan():
    source, target, plan = _inputs(
        [_column("later", ordinal=2), _column("first", ordinal=1)],
        [_column("later", ordinal=2), _column("first", ordinal=1)],
    )
    before = plan.to_dict()
    approved = MappingApprovalService().apply(
        plan, source=source, target=target,
        decisions=(MappingReviewDecision(source_column="later", status=MappingApprovalStatus.APPROVED),),
    )
    assert [item.source_column for item in approved.approvals] == ["first", "later"]
    assert plan.to_dict() == before
    with pytest.raises(FrozenInstanceError):
        approved.approvals[0].status = MappingApprovalStatus.APPROVED  # type: ignore[misc]
    assert "password" not in str(approved.to_dict()).casefold()
    assert "credential" not in str(approved.to_dict()).casefold()
