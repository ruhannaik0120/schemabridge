"""Focused tests for deterministic schema-mapping suggestions."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from models.discovery import CoverageStatus, DatabaseObjectType, DiscoveryCoverage, ObjectPersistence, TableMetadata
from models.mapping import (
    ColumnCompatibility,
    ColumnMappingSuggestion,
    MappingDecision,
    MappingEvidence,
    TableMappingIdentity,
    TableMappingPlan,
)
from models.metadata import CanonicalType, ColumnMetadata
from services.schema_mapping import SchemaMappingService


def _column(
    name: str,
    canonical_type: CanonicalType = CanonicalType.INTEGER,
    ordinal: int | None = 1,
    *,
    nullable: bool | None = False,
    length: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> ColumnMetadata:
    return ColumnMetadata(
        catalog_name="catalog",
        schema_name="schema",
        table_name="table",
        column_name=name,
        ordinal_position=ordinal,
        native_type="safe native type",
        canonical_type=canonical_type,
        nullable=nullable,
        character_length=length,
        numeric_precision=precision,
        numeric_scale=scale,
        datetime_precision=None,
        vendor_metadata={"password": "redacted", "safe": "value"},
    )


def _table(name: str, *columns: ColumnMetadata) -> TableMetadata:
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
        vendor_metadata={"credential": "never serialize"},
    )


def _plan(source_columns, target_columns) -> TableMappingPlan:
    return SchemaMappingService().suggest(_table("source", *source_columns), _table("target", *target_columns))


def _suggestion(plan: TableMappingPlan, source: str) -> ColumnMappingSuggestion:
    return next(item for item in plan.suggestions if item.source_column == source)


@pytest.mark.parametrize(
    ("source_name", "target_name", "evidence"),
    [
        ("customer_id", "customer_id", "EXACT_NATIVE_NAME"),
        ("customer_id", "CUSTOMER_ID", "CASE_INSENSITIVE_NAME"),
        ("firstName", "first_name", "NORMALIZED_NAME"),
        ("postal-code", "postal_code", "NORMALIZED_NAME"),
        ("Account Number", "account_number", "NORMALIZED_NAME"),
    ],
)
def test_name_matching_is_explainable_and_suggests_safe_pairs(source_name, target_name, evidence):
    plan = _plan([_column(source_name)], [_column(target_name)])
    suggestion = plan.suggestions[0]
    assert suggestion.target_column == target_name
    assert suggestion.decision is MappingDecision.SUGGESTED
    assert suggestion.compatibility is ColumnCompatibility.EXACT
    assert suggestion.confidence >= 0.72
    assert evidence in [item.code for item in suggestion.evidence]


def test_integer_widening_and_date_to_timestamp_are_safe():
    plan = _plan(
        [_column("identifier", CanonicalType.INTEGER, precision=10), _column("created", CanonicalType.DATE, 2)],
        [_column("identifier", CanonicalType.DECIMAL, precision=12), _column("created", CanonicalType.TIMESTAMP, 2)],
    )
    assert all(item.decision is MappingDecision.SUGGESTED for item in plan.suggestions)
    assert all(item.compatibility is ColumnCompatibility.SAFE for item in plan.suggestions)


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (_column("amount", CanonicalType.DECIMAL, precision=20, scale=4), _column("amount", CanonicalType.DECIMAL, precision=10, scale=2)),
        (_column("label", CanonicalType.STRING, length=100), _column("label", CanonicalType.STRING, length=50)),
    ],
)
def test_precision_and_length_narrowing_are_lossy_and_never_suggested(source, target):
    suggestion = _plan([source], [target]).suggestions[0]
    assert suggestion.compatibility is ColumnCompatibility.LOSSY
    assert suggestion.decision is MappingDecision.AMBIGUOUS
    assert suggestion.target_column is None


def test_string_length_widening_is_safe_for_assignment():
    suggestion = _plan(
        [_column("label", CanonicalType.STRING, length=50)],
        [_column("label", CanonicalType.STRING, length=100)],
    ).suggestions[0]
    assert suggestion.decision is MappingDecision.SUGGESTED
    assert "LENGTH_OR_PRECISION_COMPATIBLE" in [item.code for item in suggestion.evidence]


def test_incompatible_same_name_is_an_ambiguous_diagnostic_not_assignment():
    suggestion = _plan(
        [_column("enabled", CanonicalType.BOOLEAN)],
        [_column("enabled", CanonicalType.STRING)],
    ).suggestions[0]
    assert suggestion.decision is MappingDecision.AMBIGUOUS
    assert suggestion.compatibility is ColumnCompatibility.INCOMPATIBLE
    assert suggestion.target_column is None
    assert "EXACT_NATIVE_NAME" in [item.code for item in suggestion.evidence]
    assert "INCOMPATIBLE_TYPE" in [item.code for item in suggestion.evidence]


def test_nullable_source_to_non_nullable_target_is_not_auto_suggested():
    suggestion = _plan(
        [_column("value", nullable=True)],
        [_column("value", nullable=False)],
    ).suggestions[0]
    assert suggestion.decision is MappingDecision.AMBIGUOUS
    assert "NULLABILITY_MISMATCH" in [item.code for item in suggestion.evidence]


def test_ordinal_proximity_is_weak_evidence_only():
    suggestion = _plan([_column("alpha", ordinal=1)], [_column("beta", ordinal=1)]).suggestions[0]
    assert suggestion.decision is MappingDecision.AMBIGUOUS
    assert suggestion.confidence < 0.72
    assert "ORDINAL_PROXIMITY" in [item.code for item in suggestion.evidence]


def test_duplicate_normalized_targets_and_tied_candidates_are_ambiguous():
    plan = _plan(
        [_column("customer_id")],
        [_column("customerId"), _column("customer-id", ordinal=2)],
    )
    suggestion = plan.suggestions[0]
    assert suggestion.decision is MappingDecision.AMBIGUOUS
    assert suggestion.target_column is None
    assert plan.ambiguous_source_columns == ("customer_id",)
    assert set(plan.unmatched_target_columns) == {"customerId", "customer-id"}


def test_mutual_best_prevents_two_sources_from_receiving_one_target():
    plan = _plan(
        [_column("customerId", ordinal=1), _column("customer_id", ordinal=2)],
        [_column("customer_id", ordinal=1)],
    )
    suggested = [item for item in plan.suggestions if item.decision is MappingDecision.SUGGESTED]
    assert [(item.source_column, item.target_column) for item in suggested] == [("customer_id", "customer_id")]
    assert plan.ambiguous_source_columns == ("customerId",)
    assert plan.unmatched_target_columns == ()


def test_unmatched_columns_and_empty_tables_are_safe():
    plan = _plan(
        [_column("source_only", CanonicalType.INTEGER)],
        [_column("target_only", CanonicalType.BOOLEAN)],
    )
    assert plan.suggestions[0].decision is MappingDecision.UNMATCHED
    assert plan.unmatched_source_columns == ("source_only",)
    assert plan.unmatched_target_columns == ("target_only",)
    empty = _plan([], [])
    assert empty.suggestions == ()
    assert empty.unmatched_source_columns == ()
    assert empty.unmatched_target_columns == ()


def test_unknown_type_does_not_auto_suggest():
    suggestion = _plan(
        [_column("payload", CanonicalType.UNKNOWN)],
        [_column("payload", CanonicalType.UNKNOWN)],
    ).suggestions[0]
    assert suggestion.decision is MappingDecision.AMBIGUOUS
    assert suggestion.compatibility is ColumnCompatibility.UNKNOWN
    assert "UNKNOWN_TYPE" in [item.code for item in suggestion.evidence]


def test_output_is_deterministic_under_shuffled_input_order_and_evidence_order_is_fixed():
    source = [_column("second", ordinal=2), _column("first", ordinal=1)]
    target = [_column("second", ordinal=2), _column("first", ordinal=1)]
    first = _plan(source, target)
    second = _plan(list(reversed(source)), list(reversed(target)))
    assert first.to_dict() == second.to_dict()
    codes = [item.code for item in first.suggestions[0].evidence]
    assert codes == ["EXACT_NATIVE_NAME", "EXACT_CANONICAL_TYPE", "NULLABILITY_COMPATIBLE", "ORDINAL_PROXIMITY"]


def test_models_validate_confidence_assignment_uniqueness_immutability_and_safe_serialization():
    evidence = MappingEvidence(code="EXACT_NATIVE_NAME", explanation="Static explanation.", contribution=0.5)
    with pytest.raises(ValueError, match="confidence"):
        ColumnMappingSuggestion(
            source_column="a", target_column="b", confidence=1.1,
            compatibility=ColumnCompatibility.EXACT, decision=MappingDecision.SUGGESTED, evidence=(evidence,),
        )
    suggestion = ColumnMappingSuggestion(
        source_column="a", target_column="b", confidence=0.9,
        compatibility=ColumnCompatibility.EXACT, decision=MappingDecision.SUGGESTED, evidence=(evidence,),
    )
    identity = TableMappingIdentity(catalog_name="catalog", schema_name="schema", table_name="table", system="test")
    with pytest.raises(ValueError, match="one-to-one"):
        TableMappingPlan(
            source_table=identity, target_table=identity, suggestions=(suggestion, suggestion),
            unmatched_source_columns=(), unmatched_target_columns=(), ambiguous_source_columns=(), warnings=(),
        )
    with pytest.raises(ValueError, match="one-to-one"):
        TableMappingPlan(
            source_table=identity,
            target_table=identity,
            suggestions=(
                suggestion,
                ColumnMappingSuggestion(
                    source_column="other", target_column="b", confidence=0.9,
                    compatibility=ColumnCompatibility.EXACT, decision=MappingDecision.SUGGESTED, evidence=(evidence,),
                ),
            ),
            unmatched_source_columns=(), unmatched_target_columns=(), ambiguous_source_columns=(), warnings=(),
        )
    with pytest.raises(FrozenInstanceError):
        suggestion.confidence = 0.1  # type: ignore[misc]
    plan = _plan([_column("safe")], [_column("safe")])
    serialized = plan.to_dict()
    assert "password" not in str(serialized).casefold()
    assert "credential" not in str(serialized).casefold()
