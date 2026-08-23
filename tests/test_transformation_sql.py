from __future__ import annotations
from datetime import date
import pytest
from schemabridge.models.mapping import (
    MappingApprovalStatus,
    TransformationExpression,
    TransformationExpressionType,
)
from schemabridge.mapping.sql import (
    compile_snowflake_select,
    compile_snowflake_insert_select,
    InvalidTransformationPlanError,
)
from tests.support.builders import approved_mapping_plan as _approved


def test_insert_select_and_parameters_are_safe_and_aligned():
    result = compile_snowflake_insert_select(
        _approved(),
        staging_database="stage db",
        staging_schema="schema.with.dot",
        staging_table='x"; DROP;--',
    )
    assert result.parameters == (" ",)
    assert result.target_columns == ("full_name", "age")
    assert 'CONCAT_WS(%s, "src"."first_name", "src"."last_name")' in result.sql
    assert 'INSERT INTO "catalog"."schema"."people"' in result.sql
    assert "DROP" in result.sql and '"x""; DROP;--"' in result.sql
    assert ";" not in result.sql.rsplit('"', 1)[-1]


@pytest.mark.parametrize(
    "kind",
    [
        TransformationExpressionType.SOURCE_COLUMN,
        TransformationExpressionType.CAST,
        TransformationExpressionType.LITERAL,
        TransformationExpressionType.COALESCE,
    ],
)
def test_expression_forms(kind):
    plan = _approved()
    if kind is TransformationExpressionType.SOURCE_COLUMN:
        expr = TransformationExpression(expression_type=kind, source_columns=("age",))
    elif kind is TransformationExpressionType.CAST:
        from schemabridge.models.metadata import CanonicalType

        expr = TransformationExpression(
            expression_type=kind,
            source_columns=("age",),
            target_canonical_type=CanonicalType.STRING,
        )
    elif kind is TransformationExpressionType.LITERAL:
        expr = TransformationExpression(
            expression_type=kind, literal_value=date(2020, 1, 1)
        )
    else:
        expr = TransformationExpression(
            expression_type=kind, source_columns=("age", "first_name")
        )
    approval = plan.approved_mappings[0]
    from dataclasses import replace

    replacement = replace(approval, transformation=expr)
    plan = replace(
        plan,
        approvals=(replacement,) + plan.approvals[1:],
        approved_mappings=(replacement,) + plan.approved_mappings[1:],
    )
    result = compile_snowflake_select(
        plan, staging_database="db", staging_schema="s", staging_table="t"
    )
    assert result.sql.startswith("SELECT")


def test_identifiers_invalid_and_no_approved_rejected():
    with pytest.raises(InvalidTransformationPlanError):
        compile_snowflake_select(
            _approved(), staging_database="", staging_schema="s", staging_table="t"
        )
    from dataclasses import replace

    plan = _approved()
    empty = replace(
        plan,
        approved_mappings=(),
        approvals=tuple(
            replace(item, status=MappingApprovalStatus.PENDING, target_column=None)
            for item in plan.approvals
        ),
    )
    with pytest.raises(InvalidTransformationPlanError):
        compile_snowflake_select(
            empty, staging_database="d", staging_schema="s", staging_table="t"
        )
