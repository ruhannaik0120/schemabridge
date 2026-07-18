from __future__ import annotations
from datetime import date
import pytest
from models.mapping import MappingApprovalStatus, MappingReviewDecision, TransformationExpression, TransformationExpressionType
from services.schema_mapping import SchemaMappingService
from services.mapping_approval import MappingApprovalService
from services.transformation_sql import compile_snowflake_select, compile_snowflake_insert_select, InvalidTransformationPlanError
from tests.test_mapping_approval import _column, _table

def _approved():
    source=_table('source',_column('first_name',ordinal=1),_column('last_name',ordinal=2),_column('age',canonical_type=__import__('models.metadata',fromlist=['CanonicalType']).CanonicalType.INTEGER,ordinal=3))
    target=_table('people',_column('full_name',ordinal=1),_column('age',canonical_type=__import__('models.metadata',fromlist=['CanonicalType']).CanonicalType.INTEGER,ordinal=2))
    plan=SchemaMappingService().suggest(source,target)
    concat=TransformationExpression(expression_type=TransformationExpressionType.CONCAT,source_columns=('first_name','last_name'),separator=' ')
    direct=TransformationExpression(expression_type=TransformationExpressionType.DIRECT_COPY,source_columns=('age',))
    return MappingApprovalService().apply(plan,source=source,target=target,decisions=(MappingReviewDecision(source_column='first_name',target_column='full_name',status=MappingApprovalStatus.APPROVED,transformation=concat),MappingReviewDecision(source_column='age',status=MappingApprovalStatus.APPROVED,transformation=direct)))

def test_insert_select_and_parameters_are_safe_and_aligned():
    result=compile_snowflake_insert_select(_approved(),staging_database='stage db',staging_schema='schema.with.dot',staging_table='x"; DROP;--')
    assert result.parameters==(' ',)
    assert result.target_columns==('full_name','age')
    assert 'CONCAT_WS(%s, "src"."first_name", "src"."last_name")' in result.sql
    assert 'INSERT INTO "catalog"."schema"."people"' in result.sql
    assert 'DROP' in result.sql and '"x""; DROP;--"' in result.sql
    assert ';' not in result.sql.rsplit('"',1)[-1]

@pytest.mark.parametrize('kind', [TransformationExpressionType.SOURCE_COLUMN,TransformationExpressionType.CAST,TransformationExpressionType.LITERAL,TransformationExpressionType.COALESCE])
def test_expression_forms(kind):
    plan=_approved()
    if kind is TransformationExpressionType.SOURCE_COLUMN: expr=TransformationExpression(expression_type=kind,source_columns=('age',))
    elif kind is TransformationExpressionType.CAST:
        from models.metadata import CanonicalType
        expr=TransformationExpression(expression_type=kind,source_columns=('age',),target_canonical_type=CanonicalType.STRING)
    elif kind is TransformationExpressionType.LITERAL: expr=TransformationExpression(expression_type=kind,literal_value=date(2020,1,1))
    else: expr=TransformationExpression(expression_type=kind,source_columns=('age','first_name'))
    approval=plan.approved_mappings[0]
    from dataclasses import replace
    replacement=replace(approval,transformation=expr)
    plan=replace(plan,approvals=(replacement,)+plan.approvals[1:],approved_mappings=(replacement,)+plan.approved_mappings[1:])
    result=compile_snowflake_select(plan,staging_database='db',staging_schema='s',staging_table='t')
    assert result.sql.startswith('SELECT')

def test_identifiers_invalid_and_no_approved_rejected():
    with pytest.raises(InvalidTransformationPlanError): compile_snowflake_select(_approved(),staging_database='',staging_schema='s',staging_table='t')
    from dataclasses import replace
    plan=_approved(); empty=replace(plan,approved_mappings=(),approvals=tuple(replace(item,status=MappingApprovalStatus.PENDING,target_column=None) for item in plan.approvals))
    with pytest.raises(InvalidTransformationPlanError): compile_snowflake_select(empty,staging_database='d',staging_schema='s',staging_table='t')
