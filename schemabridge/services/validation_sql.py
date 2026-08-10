"""Generate paired aggregate validation SQL from an approved mapping plan.

The source PostgreSQL and target Snowflake statements expose the same ordered
metric aliases so reconciliation can compare their single result rows without
understanding either driver's native shape.  Queries are generated internally
and remain read-only; clients do not supply validation SQL.
"""

from __future__ import annotations

from schemabridge.models.mapping import (
    ApprovedTableMappingPlan,
    MappingApprovalStatus,
    SqlDialect,
    TransformationExpressionType,
)
from schemabridge.models.validation import *
from schemabridge.services.transformation_sql import (
    InvalidTransformationPlanError,
    _expr,
    _q,
)


def compile_validation_sql(
    plan,
    *,
    source_schema,
    source_table,
    target_database,
    target_schema,
    target_table,
    source_alias="src",
    target_alias="tgt",
):
    """Compile matching PostgreSQL and Snowflake aggregate queries.

    The returned pair contains row count plus per-mapping null counts and,
    unless compatibility is unknown, distinct counts.  Expression rendering is
    shared with transformation compilation so source-side checks measure the
    same transformed values that the migration statement would select.

    Raises:
        InvalidTransformationPlanError: If no approved mappings exist or a
            mapping lacks a target column or transformation expression.
    """

    if not isinstance(plan, ApprovedTableMappingPlan):
        raise InvalidTransformationPlanError("Invalid validation plan.")
    approved = sorted(
        plan.approved_mappings,
        key=lambda item: (
            item.target_ordinal_position is None,
            item.target_ordinal_position or 0,
            item.target_column or "",
        ),
    )
    if not approved:
        raise InvalidTransformationPlanError("Invalid validation plan.")

    allowed = frozenset(item.source_column for item in plan.approvals)
    checks = [
        ValidationCheckDefinition(
            check_id="row_count",
            check_type=ValidationCheckType.ROW_COUNT,
            source_column=None,
            target_column=None,
            source_metric_alias="row_count",
            target_metric_alias="row_count",
        )
    ]
    source_parameters = []
    target_parameters = []
    source_projections = ['COUNT(*) AS "row_count"']
    target_projections = ['COUNT(*) AS "row_count"']
    warnings = []

    # Identical aliases are the comparison contract between two independently
    # executed queries; reconciliation never relies on database column order.
    for index, item in enumerate(approved):
        if item.target_column is None or item.transformation is None:
            raise InvalidTransformationPlanError("Invalid validation plan.")
        expression = _expr(
            item.transformation,
            source_alias,
            allowed,
            source_parameters,
        )
        target = f"{_q(target_alias)}.{_q(item.target_column)}"
        base = f"m{index:03d}"
        for check_type, body in (
            (
                ValidationCheckType.NULL_COUNT,
                "SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END)",
            ),
        ):
            alias = base + "_null_count"
            checks.append(
                ValidationCheckDefinition(
                    check_id=alias,
                    check_type=check_type,
                    source_column=item.source_column,
                    target_column=item.target_column,
                    source_metric_alias=alias,
                    target_metric_alias=alias,
                )
            )
            source_projections.append(body.format(expression) + f" AS {_q(alias)}")
            target_projections.append(body.format(target) + f" AS {_q(alias)}")
        if item.compatibility.name not in {"UNKNOWN"}:
            alias = base + "_distinct_count"
            checks.append(
                ValidationCheckDefinition(
                    check_id=alias,
                    check_type=ValidationCheckType.DISTINCT_COUNT,
                    source_column=item.source_column,
                    target_column=item.target_column,
                    source_metric_alias=alias,
                    target_metric_alias=alias,
                )
            )
            source_projections.append(
                f"COUNT(DISTINCT {expression}) AS {_q(alias)}"
            )
            target_projections.append(f"COUNT(DISTINCT {target}) AS {_q(alias)}")

    source_relation = ".".join((_q(source_schema), _q(source_table)))
    target_relation = ".".join(
        (_q(target_database), _q(target_schema), _q(target_table))
    )
    aliases = tuple(item.check_id for item in checks)
    return (
        GeneratedValidationSql(
            dialect=SqlDialect.POSTGRESQL,
            sql=(
                "SELECT\n    "
                + ",\n    ".join(source_projections)
                + "\nFROM "
                + source_relation
                + " AS "
                + _q(source_alias)
            ),
            parameters=tuple(source_parameters),
            relation=(source_schema, source_table),
            metric_aliases=aliases,
            checks=tuple(checks),
            warnings=tuple(sorted(set(warnings))),
        ),
        GeneratedValidationSql(
            dialect=SqlDialect.SNOWFLAKE,
            sql=(
                "SELECT\n    "
                + ",\n    ".join(target_projections)
                + "\nFROM "
                + target_relation
                + " AS "
                + _q(target_alias)
            ),
            parameters=tuple(target_parameters),
            relation=(target_database, target_schema, target_table),
            metric_aliases=aliases,
            checks=tuple(checks),
            warnings=tuple(sorted(set(warnings))),
        ),
    )
