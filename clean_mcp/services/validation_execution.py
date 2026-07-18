from services.query_service import get_query_service
from services.validation_sql import compile_validation_sql
from services.reconciliation import reconcile_validation_results
from models.validation import *
class ValidationApprovalRequiredError(ValueError): pass
class ValidationExecutionError(ValueError): pass
class MalformedValidationExecutionResultError(ValueError): pass
class MigrationValidationExecutionService:
 def run(self,request):
  if not isinstance(request,MigrationValidationExecutionRequest) or request.explicitly_approved is not True: raise ValidationApprovalRequiredError('Validation approval is required.')
  source_sql,target_sql=compile_validation_sql(request.approved_mapping_plan,source_schema=request.source_schema,source_table=request.source_table,target_database=request.target_database,target_schema=request.target_schema,target_table=request.target_table)
  source=get_query_service(request.source_profile_id)
  try: sr=source.execute_query(sql=source_sql.sql,parameters=source_sql.parameters,timeout_seconds=request.timeout_seconds)
  except Exception: raise ValidationExecutionError('Validation source execution failed.') from None
  target=get_query_service(request.target_profile_id)
  try: tr=target.execute_query(sql=target_sql.sql,parameters=target_sql.parameters,timeout_seconds=request.timeout_seconds)
  except Exception: raise ValidationExecutionError('Validation target execution failed.') from None
  def row(response,expected):
   if not getattr(response,'success',False): raise ValidationExecutionError('Validation execution failed.')
   data=response.data; rows=data.get('rows',[]); cols=data.get('columns',[])
   if len(rows)!=1: raise MalformedValidationExecutionResultError('Malformed validation execution result.')
   value=rows[0]
   if isinstance(value,dict): return {str(k).casefold():v for k,v in value.items()}
   if isinstance(value,(tuple,list)) and len(value)==len(cols): return {str(k).casefold():v for k,v in zip(cols,value)}
   raise MalformedValidationExecutionResultError('Malformed validation execution result.')
  report=reconcile_validation_results(source_sql,target_sql,source_metrics=row(sr,source_sql),target_metrics=row(tr,target_sql))
  return MigrationValidationExecutionReport(source_profile_id=request.source_profile_id,target_profile_id=request.target_profile_id,source_sql_summary=source_sql,target_sql_summary=target_sql,validation_report=report,source_execution_status=ValidationExecutionStatus.SUCCEEDED,target_execution_status=ValidationExecutionStatus.SUCCEEDED)
