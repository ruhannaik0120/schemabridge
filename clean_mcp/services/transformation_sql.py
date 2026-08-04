"""Pure deterministic Snowflake SQL rendering for approved mappings."""
from __future__ import annotations
from typing import Any
from models.mapping import (ApprovedTableMappingPlan, GeneratedTransformationSql, MappingApprovalStatus, SqlDialect, TransformationExpression, TransformationExpressionType, TransformationStatementType)
from models.metadata import CanonicalType

class TransformationCompilationError(ValueError): pass
class UnsupportedTransformationError(TransformationCompilationError): pass
class InvalidTransformationPlanError(TransformationCompilationError): pass

_TYPES = {CanonicalType.BOOLEAN:"BOOLEAN", CanonicalType.INTEGER:"NUMBER(38,0)", CanonicalType.DECIMAL:"NUMBER", CanonicalType.FLOAT:"FLOAT", CanonicalType.STRING:"VARCHAR", CanonicalType.DATE:"DATE", CanonicalType.TIME:"TIME", CanonicalType.TIMESTAMP:"TIMESTAMP_NTZ", CanonicalType.TIMESTAMP_TZ:"TIMESTAMP_TZ", CanonicalType.BINARY:"BINARY", CanonicalType.SEMI_STRUCTURED:"VARIANT"}
_MAX_DEPTH = 16
def _q(value: Any) -> str:
    if isinstance(value,bool) or not isinstance(value,str) or not value or '\x00' in value: raise InvalidTransformationPlanError("Invalid transformation plan.")
    return '"'+value.replace('"','""')+'"'
def _relation(database:Any,schema:Any,table:Any)->str: return '.'.join((_q(database),_q(schema),_q(table)))
def _expr(expression:TransformationExpression, alias:str, allowed:frozenset[str], params:list[object], depth:int=0, active:frozenset[int]=frozenset())->str:
    if depth>=_MAX_DEPTH or id(expression) in active: raise UnsupportedTransformationError("Unsupported transformation expression.")
    if any(column not in allowed for column in expression.source_columns): raise InvalidTransformationPlanError("Invalid transformation plan.")
    col=lambda name: f'{_q(alias)}.{_q(name)}'
    kind=expression.expression_type
    if kind in {TransformationExpressionType.DIRECT_COPY,TransformationExpressionType.SOURCE_COLUMN}: return col(expression.source_columns[0])
    if kind is TransformationExpressionType.CAST:
        rendered=_TYPES.get(expression.target_canonical_type)
        if rendered is None: raise UnsupportedTransformationError("Unsupported transformation expression.")
        return f'CAST({col(expression.source_columns[0])} AS {rendered})'
    if kind is TransformationExpressionType.CONCAT:
        args=', '.join(col(item) for item in expression.source_columns)
        if expression.separator is None: return f'CONCAT({args})'
        params.append(expression.separator); return f'CONCAT_WS(%s, {args})'
    if kind is TransformationExpressionType.LITERAL:
        params.append(expression.literal_value); return '%s'
    if kind is TransformationExpressionType.COALESCE:
        args=[col(item) for item in expression.source_columns]
        args.extend(_expr(item,alias,allowed,params,depth+1,active|{id(expression)}) for item in expression.arguments)
        if len(args)<2: raise UnsupportedTransformationError("Unsupported transformation expression.")
        return 'COALESCE('+', '.join(args)+')'
    raise UnsupportedTransformationError("Unsupported transformation expression.")

class SnowflakeTransformationSqlCompiler:
    def compile_select(self, plan:ApprovedTableMappingPlan, *, staging_database:str, staging_schema:str, staging_table:str, source_alias:str='src')->GeneratedTransformationSql:
        return self._compile(plan,staging_database,staging_schema,staging_table,source_alias,False)
    def compile_insert_select(self, plan:ApprovedTableMappingPlan, *, staging_database:str, staging_schema:str, staging_table:str, source_alias:str='src')->GeneratedTransformationSql:
        return self._compile(plan,staging_database,staging_schema,staging_table,source_alias,True)
    def _compile(self,plan,db,schema,table,alias,insert):
        if not isinstance(plan,ApprovedTableMappingPlan): raise InvalidTransformationPlanError("Invalid transformation plan.")
        approved=[item for item in plan.approved_mappings if item.status in {MappingApprovalStatus.APPROVED,MappingApprovalStatus.OVERRIDDEN}]
        if not approved or len({item.target_column for item in approved})!=len(approved): raise InvalidTransformationPlanError("Invalid transformation plan.")
        approved.sort(key=lambda item:(item.target_ordinal_position is None,item.target_ordinal_position or 0,item.target_column or ''))
        allowed=frozenset(item.source_column for item in plan.approvals)
        params:list[object]=[]; projections=[]; targets=[]; sources=[]
        for item in approved:
            if item.target_column is None or item.transformation is None: raise InvalidTransformationPlanError("Invalid transformation plan.")
            rendered=_expr(item.transformation,alias,allowed,params)
            projections.append(rendered); targets.append(item.target_column)
            for column in item.transformation.source_columns:
                if column not in sources: sources.append(column)
        source_relation=_relation(db,schema,table); target_relation=_relation(plan.target_table.catalog_name,plan.target_table.schema_name,plan.target_table.table_name)
        if insert:
            sql='INSERT INTO '+target_relation+' (\n    '+',\n    '.join(_q(item) for item in targets)+'\n)\nSELECT\n    '+',\n    '.join(projections)+'\nFROM '+source_relation+' AS '+_q(alias)
            statement=TransformationStatementType.INSERT_SELECT
        else:
            sql='SELECT\n    '+',\n    '.join(f'{item} AS {_q(target)}' for item,target in zip(projections,targets))+'\nFROM '+source_relation+' AS '+_q(alias)
            statement=TransformationStatementType.SELECT
        return GeneratedTransformationSql(dialect=SqlDialect.SNOWFLAKE,statement_type=statement,sql=sql,parameters=tuple(params),source_relation=(db,schema,table),target_relation=(plan.target_table.catalog_name,plan.target_table.schema_name,plan.target_table.table_name),source_columns=tuple(sources),target_columns=tuple(targets),approved_plan_version=plan.version,warnings=())
def compile_snowflake_select(plan:ApprovedTableMappingPlan, **kwargs): return SnowflakeTransformationSqlCompiler().compile_select(plan,**kwargs)
def compile_snowflake_insert_select(plan:ApprovedTableMappingPlan, **kwargs): return SnowflakeTransformationSqlCompiler().compile_insert_select(plan,**kwargs)
