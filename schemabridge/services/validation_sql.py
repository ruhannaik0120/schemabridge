"""Generate paired aggregate validation SQL from an approved mapping plan.

The source and target statements expose the same ordered
metric aliases so reconciliation can compare their single result rows without
understanding either driver's native shape.  Queries are generated internally
and remain read-only; clients do not supply validation SQL.
"""

from __future__ import annotations

from schemabridge.models.mapping import (
    ApprovedTableMappingPlan,
    MappingApprovalStatus,
    SqlDialect,
    TransformationExpression,
    TransformationExpressionType,
)
from schemabridge.models.metadata import CanonicalType
from schemabridge.models.validation import *
from schemabridge.services.transformation_sql import (
    InvalidTransformationPlanError,
    UnsupportedTransformationError,
)


_VALIDATION_TYPES = {
    SqlDialect.POSTGRESQL: {
        CanonicalType.BOOLEAN: "BOOLEAN",
        CanonicalType.INTEGER: "BIGINT",
        CanonicalType.DECIMAL: "DECIMAL",
        CanonicalType.FLOAT: "DOUBLE PRECISION",
        CanonicalType.STRING: "TEXT",
        CanonicalType.DATE: "DATE",
        CanonicalType.TIME: "TIME",
        CanonicalType.TIMESTAMP: "TIMESTAMP",
        CanonicalType.TIMESTAMP_TZ: "TIMESTAMPTZ",
        CanonicalType.BINARY: "BYTEA",
        CanonicalType.SEMI_STRUCTURED: "JSONB",
    },
    SqlDialect.MYSQL: {
        CanonicalType.BOOLEAN: "UNSIGNED",
        CanonicalType.INTEGER: "SIGNED",
        CanonicalType.DECIMAL: "DECIMAL(65,30)",
        CanonicalType.FLOAT: "DOUBLE",
        CanonicalType.STRING: "CHAR",
        CanonicalType.DATE: "DATE",
        CanonicalType.TIME: "TIME",
        CanonicalType.TIMESTAMP: "DATETIME",
        CanonicalType.BINARY: "BINARY",
        CanonicalType.SEMI_STRUCTURED: "JSON",
    },
    SqlDialect.SNOWFLAKE: {
        CanonicalType.BOOLEAN: "BOOLEAN",
        CanonicalType.INTEGER: "NUMBER(38,0)",
        CanonicalType.DECIMAL: "NUMBER",
        CanonicalType.FLOAT: "FLOAT",
        CanonicalType.STRING: "VARCHAR",
        CanonicalType.DATE: "DATE",
        CanonicalType.TIME: "TIME",
        CanonicalType.TIMESTAMP: "TIMESTAMP_NTZ",
        CanonicalType.TIMESTAMP_TZ: "TIMESTAMP_TZ",
        CanonicalType.BINARY: "BINARY",
        CanonicalType.SEMI_STRUCTURED: "VARIANT",
    },
}
_MAX_DEPTH = 16


def _quote(dialect: SqlDialect, value: str) -> str:
    """Quote one identifier using the selected database dialect."""

    if not isinstance(value, str) or not value or "\x00" in value:
        raise InvalidTransformationPlanError("Invalid validation plan.")
    if dialect is SqlDialect.MYSQL:
        return "`" + value.replace("`", "``") + "`"
    return '"' + value.replace('"', '""') + '"'


def _validation_expression(
    expression: TransformationExpression,
    *,
    dialect: SqlDialect,
    alias: str,
    allowed: frozenset[str],
    parameters: list[object],
    depth: int = 0,
    active: frozenset[int] = frozenset(),
) -> str:
    """Render an approved expression using only modeled dialect operations."""

    if depth >= _MAX_DEPTH or id(expression) in active:
        raise UnsupportedTransformationError("Unsupported validation expression.")
    if any(column not in allowed for column in expression.source_columns):
        raise InvalidTransformationPlanError("Invalid validation plan.")
    column = lambda name: f"{_quote(dialect, alias)}.{_quote(dialect, name)}"
    kind = expression.expression_type
    if kind in {
        TransformationExpressionType.DIRECT_COPY,
        TransformationExpressionType.SOURCE_COLUMN,
    }:
        return column(expression.source_columns[0])
    if kind is TransformationExpressionType.CAST:
        rendered = _VALIDATION_TYPES[dialect].get(expression.target_canonical_type)
        if rendered is None:
            raise UnsupportedTransformationError("Unsupported validation expression.")
        return f"CAST({column(expression.source_columns[0])} AS {rendered})"
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
            _validation_expression(
                item,
                dialect=dialect,
                alias=alias,
                allowed=allowed,
                parameters=parameters,
                depth=depth + 1,
                active=active | {id(expression)},
            )
            for item in expression.arguments
        )
        if len(arguments) < 2:
            raise UnsupportedTransformationError("Unsupported validation expression.")
        return "COALESCE(" + ", ".join(arguments) + ")"
    raise UnsupportedTransformationError("Unsupported validation expression.")


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
    source_dialect=SqlDialect.POSTGRESQL,
    target_dialect=SqlDialect.SNOWFLAKE,
):
    """Compile matching source and target aggregate queries.

    The returned pair contains row count plus per-mapping null counts and,
    unless compatibility is unknown, distinct counts.  Expression rendering is
    shared with transformation compilation so source-side checks measure the
    same transformed values that the migration statement would select.

    Raises:
        InvalidTransformationPlanError: If no approved mappings exist or a
            mapping lacks a target column or transformation expression.
    """

    if (
        not isinstance(plan, ApprovedTableMappingPlan)
        or source_dialect not in _VALIDATION_TYPES
        or target_dialect not in _VALIDATION_TYPES
    ):
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
        target = f"{_quote(target_dialect, target_alias)}.{_quote(target_dialect, item.target_column)}"
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
            source_expression = _validation_expression(
                item.transformation,
                dialect=source_dialect,
                alias=source_alias,
                allowed=allowed,
                parameters=source_parameters,
            )
            source_projections.append(
                body.format(source_expression) + f" AS {_quote(source_dialect, alias)}"
            )
            target_projections.append(
                body.format(target) + f" AS {_quote(target_dialect, alias)}"
            )
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
            source_expression = _validation_expression(
                item.transformation,
                dialect=source_dialect,
                alias=source_alias,
                allowed=allowed,
                parameters=source_parameters,
            )
            source_projections.append(
                f"COUNT(DISTINCT {source_expression}) AS {_quote(source_dialect, alias)}"
            )
            target_projections.append(
                f"COUNT(DISTINCT {target}) AS {_quote(target_dialect, alias)}"
            )

    source_relation = ".".join(
        (_quote(source_dialect, source_schema), _quote(source_dialect, source_table))
    )
    if target_dialect is SqlDialect.MYSQL:
        # MySQL uses one database name where the canonical model has both a
        # catalog and schema.  Rendering both would create invalid three-part
        # MySQL SQL and could point validation at the wrong relation.
        if target_database != target_schema:
            raise InvalidTransformationPlanError("Invalid validation plan.")
        target_relation = ".".join(
            (_quote(target_dialect, target_database), _quote(target_dialect, target_table))
        )
    else:
        target_relation = ".".join(
            (
                _quote(target_dialect, target_database),
                _quote(target_dialect, target_schema),
                _quote(target_dialect, target_table),
            )
        )
    aliases = tuple(item.check_id for item in checks)
    return (
        GeneratedValidationSql(
            dialect=source_dialect,
            sql=(
                "SELECT\n    "
                + ",\n    ".join(source_projections)
                + "\nFROM "
                + source_relation
                + " AS "
                + _quote(source_dialect, source_alias)
            ),
            parameters=tuple(source_parameters),
            relation=(source_schema, source_table),
            metric_aliases=aliases,
            checks=tuple(checks),
            warnings=tuple(sorted(set(warnings))),
        ),
        GeneratedValidationSql(
            dialect=target_dialect,
            sql=(
                "SELECT\n    "
                + ",\n    ".join(target_projections)
                + "\nFROM "
                + target_relation
                + " AS "
                + _quote(target_dialect, target_alias)
            ),
            parameters=tuple(target_parameters),
            relation=(target_database, target_schema, target_table),
            metric_aliases=aliases,
            checks=tuple(checks),
            warnings=tuple(sorted(set(warnings))),
        ),
    )
