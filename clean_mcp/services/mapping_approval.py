"""Human-in-the-loop approval of deterministic mapping suggestions."""

from __future__ import annotations

from collections.abc import Iterable

from models.discovery import TableMetadata
from models.mapping import (
    ApprovedTableMappingPlan,
    ColumnCompatibility,
    ColumnMappingApproval,
    MappingApprovalStatus,
    MappingReviewDecision,
    TableMappingIdentity,
    TableMappingPlan,
    TransformationExpression,
)
from models.metadata import ColumnMetadata
from services.schema_mapping import _type_compatibility


def _identity(table: TableMetadata) -> TableMappingIdentity:
    return TableMappingIdentity(
        catalog_name=table.catalog_name,
        schema_name=table.schema_name,
        table_name=table.object_name,
        system=table.system,
    )


def _ordered_columns(columns: Iterable[ColumnMetadata]) -> tuple[ColumnMetadata, ...]:
    return tuple(
        sorted(
            columns,
            key=lambda item: (
                item.ordinal_position is None,
                item.ordinal_position if item.ordinal_position is not None else 0,
                item.column_name,
            ),
        )
    )


class MappingApprovalService:
    """Turn a Stage 4A plan and explicit reviewer choices into an immutable plan."""

    def create_review_plan(self, plan: TableMappingPlan) -> ApprovedTableMappingPlan:
        """Convert every suggestion to PENDING; this helper never auto-approves."""
        if not isinstance(plan, TableMappingPlan):
            raise TypeError("plan must be a TableMappingPlan.")
        approvals = tuple(
            ColumnMappingApproval(
                source_column=item.source_column,
                target_column=item.target_column,
                status=MappingApprovalStatus.PENDING,
                original_confidence=item.confidence,
                original_compatibility=item.compatibility,
                original_evidence=item.evidence,
                compatibility=item.compatibility,
            )
            for item in plan.suggestions
        )
        return ApprovedTableMappingPlan(
            source_table=plan.source_table,
            target_table=plan.target_table,
            approvals=approvals,
            approved_mappings=(),
            rejected_source_columns=(),
            unmatched_source_columns=plan.unmatched_source_columns,
            unmatched_target_columns=plan.unmatched_target_columns,
            warnings=plan.warnings,
        )

    def apply(
        self,
        plan: TableMappingPlan,
        *,
        source: TableMetadata,
        target: TableMetadata,
        decisions: tuple[MappingReviewDecision, ...],
    ) -> ApprovedTableMappingPlan:
        """Apply explicit reviewer decisions without changing the Stage 4A plan."""
        if not isinstance(plan, TableMappingPlan):
            raise TypeError("plan must be a TableMappingPlan.")
        if not isinstance(source, TableMetadata) or not isinstance(target, TableMetadata):
            raise TypeError("source and target must be TableMetadata values.")
        if plan.source_table != _identity(source) or plan.target_table != _identity(target):
            raise ValueError("mapping plan table identities must match the supplied tables.")
        if not isinstance(decisions, tuple) or not all(
            isinstance(item, MappingReviewDecision) for item in decisions
        ):
            raise TypeError("decisions must be a tuple of MappingReviewDecision values.")
        decision_by_source = {item.source_column: item for item in decisions}
        if len(decision_by_source) != len(decisions):
            raise ValueError("reviewer decisions must not duplicate a source column.")

        source_columns = {item.column_name: item for item in source.columns}
        target_columns = {item.column_name: item for item in target.columns}
        unknown_sources = set(decision_by_source).difference(source_columns)
        if unknown_sources:
            raise ValueError("reviewer decision references an unknown source column.")
        base_by_source = {item.source_column: item for item in plan.suggestions}
        if set(source_columns) != set(base_by_source):
            raise ValueError("mapping plan suggestions must match source table columns.")

        approvals: list[ColumnMappingApproval] = []
        for source_column in _ordered_columns(source.columns):
            base = base_by_source[source_column.column_name]
            decision = decision_by_source.get(source_column.column_name)
            if decision is None:
                approvals.append(ColumnMappingApproval(
                    source_column=base.source_column,
                    target_column=base.target_column,
                    status=MappingApprovalStatus.PENDING,
                    original_confidence=base.confidence,
                    original_compatibility=base.compatibility,
                    original_evidence=base.evidence,
                    compatibility=base.compatibility,
                ))
                continue

            if decision.target_column is not None and decision.target_column not in target_columns:
                raise ValueError("reviewer decision references an unknown target column.")
            status = decision.status
            selected_target = decision.target_column if decision.target_column is not None else base.target_column
            if status is MappingApprovalStatus.REJECTED:
                selected_target = None
            if status in {MappingApprovalStatus.APPROVED, MappingApprovalStatus.OVERRIDDEN}:
                if selected_target is None or selected_target not in target_columns:
                    raise ValueError("approved reviewer decision references an unknown target column.")
                compatibility, _ = _type_compatibility(
                    source_column,
                    target_columns[selected_target],
                )
                if (
                    status is MappingApprovalStatus.APPROVED
                    and compatibility is ColumnCompatibility.INCOMPATIBLE
                ):
                    raise ValueError("incompatible mappings require OVERRIDDEN status.")
                if (
                    status is MappingApprovalStatus.APPROVED
                    and selected_target == base.target_column
                    and base.confidence < 0.72
                ):
                    raise ValueError("low-confidence mappings require OVERRIDDEN status.")
            elif selected_target is not None and selected_target not in target_columns:
                raise ValueError("reviewer decision references an unknown target column.")
            if decision.transformation is not None:
                self._validate_transformation_sources(decision.transformation, source_columns)
            approvals.append(ColumnMappingApproval(
                source_column=source_column.column_name,
                target_column=selected_target,
                status=status,
                original_confidence=base.confidence,
                original_compatibility=base.compatibility,
                original_evidence=base.evidence,
                compatibility=(
                    _type_compatibility(source_column, target_columns[selected_target])[0]
                    if selected_target is not None and selected_target in target_columns
                    else base.compatibility
                ),
                reviewer_note=decision.reviewer_note,
                override_reason=decision.override_reason,
                transformation=decision.transformation,
            ))

        approved = tuple(
            item
            for item in approvals
            if item.status in {MappingApprovalStatus.APPROVED, MappingApprovalStatus.OVERRIDDEN}
        )
        targets = tuple(item.target_column for item in approved)
        if len(set(targets)) != len(targets):
            raise ValueError("approved mappings must not reuse a target column.")
        rejected = tuple(
            item.source_column for item in approvals if item.status is MappingApprovalStatus.REJECTED
        )
        used_targets = set(targets)
        return ApprovedTableMappingPlan(
            source_table=plan.source_table,
            target_table=plan.target_table,
            approvals=tuple(approvals),
            approved_mappings=approved,
            rejected_source_columns=rejected,
            unmatched_source_columns=plan.unmatched_source_columns,
            unmatched_target_columns=tuple(
                item.column_name for item in _ordered_columns(target.columns) if item.column_name not in used_targets
            ),
            warnings=plan.warnings,
        )

    @staticmethod
    def _validate_transformation_sources(
        expression: TransformationExpression,
        source_columns: dict[str, ColumnMetadata],
    ) -> None:
        if any(column not in source_columns for column in expression.source_columns):
            raise ValueError("transformation references an unknown source column.")
        for argument in expression.arguments:
            MappingApprovalService._validate_transformation_sources(argument, source_columns)


__all__ = ["MappingApprovalService"]
