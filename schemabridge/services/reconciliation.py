"""Reconcile paired source and target aggregate validation results.

This pure service aligns metrics by generated aliases, normalizes count-like
driver values, and classifies each check as matching, mismatching, or
unavailable.  It compares aggregate evidence only; it does not read or compare
individual business rows.
"""

from decimal import Decimal

from schemabridge.models.validation import *


def _count(value):
    """Normalize a non-negative integral driver value, or return ``None``."""

    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 0:
        return value
    if (
        isinstance(value, Decimal)
        and value.is_finite()
        and value >= 0
        and value == value.to_integral_value()
    ):
        return int(value)
    if isinstance(value, str) and __import__("re").fullmatch(r"[+-]?\d+", value):
        return int(value) if int(value) >= 0 else None
    return None


def reconcile_validation_results(
    source_sql,
    target_sql,
    *,
    source_metrics,
    target_metrics,
):
    """Build a validation report by comparing each generated metric pair.

    Missing or malformed values remain ``UNAVAILABLE`` rather than becoming a
    false match.  Any mismatch fails the report; otherwise unavailable evidence
    makes it incomplete, and only a complete match set passes.
    """

    results = []
    # Check definitions, not mapping order or raw cursor positions, establish
    # which source and target values belong together.
    for check in source_sql.checks:
        source_value = _count(source_metrics.get(check.source_metric_alias))
        target_value = _count(target_metrics.get(check.target_metric_alias))
        status = (
            ValidationStatus.UNAVAILABLE
            if source_value is None or target_value is None
            else (
                ValidationStatus.MATCH
                if source_value == target_value
                else ValidationStatus.MISMATCH
            )
        )
        results.append(
            ValidationCheckResult(
                check_id=check.check_id,
                check_type=check.check_type,
                source_value=source_value,
                target_value=target_value,
                status=status,
                difference=(
                    None
                    if status is ValidationStatus.UNAVAILABLE
                    else target_value - source_value
                ),
                source_column=check.source_column,
                target_column=check.target_column,
            )
        )

    matched_count = sum(
        item.status is ValidationStatus.MATCH for item in results
    )
    mismatched_count = sum(
        item.status is ValidationStatus.MISMATCH for item in results
    )
    unavailable_count = len(results) - matched_count - mismatched_count
    return MigrationValidationReport(
        source_table=source_sql.relation,
        target_table=target_sql.relation,
        check_results=tuple(results),
        status=(
            MigrationValidationStatus.FAILED
            if mismatched_count
            else (
                MigrationValidationStatus.INCOMPLETE
                if unavailable_count
                else MigrationValidationStatus.PASSED
            )
        ),
        matched_count=matched_count,
        mismatched_count=mismatched_count,
        unavailable_count=unavailable_count,
        warnings=(),
        approved_plan_version=1,
    )
