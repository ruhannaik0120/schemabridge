"""Compile approved mappings through database-specific SQL rendering rules."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from schemabridge.models.mapping import (
    ApprovedTableMappingPlan,
    GeneratedTransformationSql,
    MappingApprovalStatus,
    SqlDialect,
    TransformationExpression,
    TransformationExpressionType,
    TransformationStatementType,
)
from schemabridge.models.metadata import CanonicalType
from schemabridge.models.transport import TransportRelation
from schemabridge.services.transformation_sql import (
    InvalidTransformationPlanError,
    UnsupportedTransformationError,
)


_MAX_EXPRESSION_DEPTH = 16


class DialectTransformationCompiler:
    """Share approval and parameter safety while letting each dialect render SQL."""

    def __init__(
        self,
        *,
        dialect: SqlDialect,
        canonical_types: Mapping[CanonicalType, str],
        quote_identifier: Callable[[Any], str],
        render_relation: Callable[[Any, Any, Any], str],
        require_matching_catalog: bool = False,
    ) -> None:
        self.dialect = dialect
        self.canonical_types = canonical_types
        self.quote_identifier = quote_identifier
        self.render_relation = render_relation
        self.require_matching_catalog = require_matching_catalog

    def compile_select(
        self,
        plan: ApprovedTableMappingPlan,
        *,
        staging_relation: TransportRelation,
        source_alias: str = "src",
    ) -> GeneratedTransformationSql:
        return self._compile(plan, staging_relation, source_alias, insert=False)

    def compile_insert_select(
        self,
        plan: ApprovedTableMappingPlan,
        *,
        staging_relation: TransportRelation,
        source_alias: str = "src",
    ) -> GeneratedTransformationSql:
        return self._compile(plan, staging_relation, source_alias, insert=True)

    def _expression(
        self,
        expression: TransformationExpression,
        alias: str,
        allowed_columns: frozenset[str],
        parameters: list[object],
        depth: int = 0,
        active: frozenset[int] = frozenset(),
    ) -> str:
        if depth >= _MAX_EXPRESSION_DEPTH or id(expression) in active:
            raise UnsupportedTransformationError("Unsupported transformation expression.")
        if any(column not in allowed_columns for column in expression.source_columns):
            raise InvalidTransformationPlanError("Invalid transformation plan.")

        column = lambda name: f"{self.quote_identifier(alias)}.{self.quote_identifier(name)}"
        kind = expression.expression_type
        if kind in {
            TransformationExpressionType.DIRECT_COPY,
            TransformationExpressionType.SOURCE_COLUMN,
        }:
            return column(expression.source_columns[0])
        if kind is TransformationExpressionType.CAST:
            target_type = self.canonical_types.get(expression.target_canonical_type)
            if target_type is None:
                raise UnsupportedTransformationError("Unsupported transformation expression.")
            return f"CAST({column(expression.source_columns[0])} AS {target_type})"
        if kind is TransformationExpressionType.CONCAT:
            arguments = ", ".join(column(item) for item in expression.source_columns)
            if expression.separator is None:
                return f"CONCAT({arguments})"
            parameters.append(expression.separator)
            return f"CONCAT_WS(%s, {arguments})"
        if kind is TransformationExpressionType.LITERAL:
            parameters.append(expression.literal_value)
            return "%s"
        if kind is TransformationExpressionType.COALESCE:
            arguments = [column(item) for item in expression.source_columns]
            arguments.extend(
                self._expression(
                    item,
                    alias,
                    allowed_columns,
                    parameters,
                    depth + 1,
                    active | {id(expression)},
                )
                for item in expression.arguments
            )
            if len(arguments) < 2:
                raise UnsupportedTransformationError("Unsupported transformation expression.")
            return "COALESCE(" + ", ".join(arguments) + ")"
        raise UnsupportedTransformationError("Unsupported transformation expression.")

    def _compile(
        self,
        plan: ApprovedTableMappingPlan,
        staging_relation: TransportRelation,
        source_alias: str,
        *,
        insert: bool,
    ) -> GeneratedTransformationSql:
        if (
            not isinstance(plan, ApprovedTableMappingPlan)
            or not isinstance(staging_relation, TransportRelation)
            or staging_relation.catalog_name is None
            or plan.target_table.catalog_name is None
            or (
                self.require_matching_catalog
                and staging_relation.catalog_name != plan.target_table.catalog_name
            )
        ):
            raise InvalidTransformationPlanError("Invalid transformation plan.")
        approved = [
            item
            for item in plan.approved_mappings
            if item.status in {MappingApprovalStatus.APPROVED, MappingApprovalStatus.OVERRIDDEN}
        ]
        if not approved or len({item.target_column for item in approved}) != len(approved):
            raise InvalidTransformationPlanError("Invalid transformation plan.")
        approved.sort(
            key=lambda item: (
                item.target_ordinal_position is None,
                item.target_ordinal_position or 0,
                item.target_column or "",
            )
        )
        allowed_columns = frozenset(item.source_column for item in plan.approvals)
        parameters: list[object] = []
        projections: list[str] = []
        targets: list[str] = []
        sources: list[str] = []
        for item in approved:
            if item.target_column is None or item.transformation is None:
                raise InvalidTransformationPlanError("Invalid transformation plan.")
            projections.append(
                self._expression(item.transformation, source_alias, allowed_columns, parameters)
            )
            targets.append(item.target_column)
            for source_column in item.transformation.source_columns:
                if source_column not in sources:
                    sources.append(source_column)

        source_relation = self.render_relation(
            staging_relation.catalog_name,
            staging_relation.schema_name,
            staging_relation.object_name,
        )
        target_relation = self.render_relation(
            plan.target_table.catalog_name,
            plan.target_table.schema_name,
            plan.target_table.table_name,
        )
        if insert:
            sql = (
                "INSERT INTO " + target_relation + " (\n    "
                + ",\n    ".join(self.quote_identifier(item) for item in targets)
                + "\n)\nSELECT\n    " + ",\n    ".join(projections)
                + "\nFROM " + source_relation + " AS " + self.quote_identifier(source_alias)
            )
            statement_type = TransformationStatementType.INSERT_SELECT
        else:
            sql = (
                "SELECT\n    "
                + ",\n    ".join(
                    f"{projection} AS {self.quote_identifier(target)}"
                    for projection, target in zip(projections, targets)
                )
                + "\nFROM " + source_relation + " AS " + self.quote_identifier(source_alias)
            )
            statement_type = TransformationStatementType.SELECT
        return GeneratedTransformationSql(
            dialect=self.dialect,
            statement_type=statement_type,
            sql=sql,
            parameters=tuple(parameters),
            source_relation=(staging_relation.catalog_name, staging_relation.schema_name, staging_relation.object_name),
            target_relation=(plan.target_table.catalog_name, plan.target_table.schema_name, plan.target_table.table_name),
            source_columns=tuple(sources),
            target_columns=tuple(targets),
            approved_plan_version=plan.version,
            warnings=(),
        )


__all__ = ["DialectTransformationCompiler"]
