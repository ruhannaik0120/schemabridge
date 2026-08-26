"""Determine whether exact primary-key reconciliation is safe to run.

Aggregate validation is available for every approved mapping.  Exact key
reconciliation has stronger prerequisites: both discovered tables must expose
complete primary-key metadata and each source key must arrive at the matching
target key without a value-changing transformation.  This module makes that
decision explicit before a future executor reads any key values.
"""

from __future__ import annotations

from dataclasses import dataclass

from schemabridge.models.discovery import CoverageStatus, TableMetadata
from schemabridge.models.mapping import (
    ApprovedTableMappingPlan,
    MappingApprovalStatus,
    TransformationExpressionType,
)


@dataclass(frozen=True, slots=True)
class PrimaryKeyReconciliationEligibility:
    """Describe whether a mapping can safely use exact key reconciliation."""

    eligible: bool
    source_key_columns: tuple[str, ...] = ()
    target_key_columns: tuple[str, ...] = ()
    reason: str | None = None


def assess_primary_key_reconciliation(
    plan: ApprovedTableMappingPlan,
    *,
    source_table: TableMetadata,
    target_table: TableMetadata,
) -> PrimaryKeyReconciliationEligibility:
    """Return an exact-key eligibility decision from durable discovery data.

    A key mapping is accepted only when every source primary-key column has an
    approved direct copy to one target column and the resulting ordered target
    columns are precisely that table's discovered primary key.  Casts and
    computed expressions are deliberately excluded because equal business
    values could have different serialized key representations.
    """

    if not isinstance(plan, ApprovedTableMappingPlan) or not isinstance(
        source_table, TableMetadata
    ) or not isinstance(target_table, TableMetadata):
        raise TypeError("Primary-key reconciliation requires mapping and table metadata.")
    if (
        source_table.coverage.primary_key is not CoverageStatus.COMPLETE
        or target_table.coverage.primary_key is not CoverageStatus.COMPLETE
    ):
        return PrimaryKeyReconciliationEligibility(
            eligible=False, reason="PRIMARY_KEY_METADATA_INCOMPLETE"
        )
    source_key = source_table.primary_key
    target_key = target_table.primary_key
    if (
        source_key is None
        or target_key is None
        or not source_key.columns
        or not target_key.columns
    ):
        return PrimaryKeyReconciliationEligibility(
            eligible=False, reason="PRIMARY_KEY_UNAVAILABLE"
        )

    mapped: dict[str, str] = {}
    for approval in plan.approved_mappings:
        if (
            approval.status not in {
                MappingApprovalStatus.APPROVED,
                MappingApprovalStatus.OVERRIDDEN,
            }
            or approval.target_column is None
            or approval.transformation is None
        ):
            continue
        transformation = approval.transformation
        if (
            transformation.expression_type
            not in {
                TransformationExpressionType.DIRECT_COPY,
                TransformationExpressionType.SOURCE_COLUMN,
            }
            or transformation.source_columns != (approval.source_column,)
        ):
            continue
        mapped[approval.source_column] = approval.target_column

    try:
        mapped_target_key = tuple(mapped[column] for column in source_key.columns)
    except KeyError:
        return PrimaryKeyReconciliationEligibility(
            eligible=False, reason="PRIMARY_KEY_MAPPING_INCOMPLETE"
        )
    if mapped_target_key != target_key.columns:
        return PrimaryKeyReconciliationEligibility(
            eligible=False, reason="PRIMARY_KEY_MAPPING_MISMATCH"
        )
    return PrimaryKeyReconciliationEligibility(
        eligible=True,
        source_key_columns=source_key.columns,
        target_key_columns=target_key.columns,
    )


__all__ = [
    "PrimaryKeyReconciliationEligibility",
    "assess_primary_key_reconciliation",
]
