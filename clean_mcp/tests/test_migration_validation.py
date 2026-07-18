from tests.test_transformation_sql import _approved
from services.validation_sql import compile_validation_sql
from services.reconciliation import reconcile_validation_results
from models.validation import MigrationValidationStatus
def test_paired_sql_and_reconciliation():
 s,t=compile_validation_sql(_approved(),source_schema='public',source_table='people',target_database='db',target_schema='s',target_table='people')
 assert s.dialect.value=='POSTGRESQL' and t.dialect.value=='SNOWFLAKE'
 assert 'COUNT(*)' in s.sql and 'CONCAT_WS(%s' in s.sql and s.parameters==(' ',)
 r=reconcile_validation_results(s,t,source_metrics={x.check_id:1 for x in s.checks},target_metrics={x.check_id:1 for x in t.checks})
 assert r.status is MigrationValidationStatus.PASSED
 r=reconcile_validation_results(s,t,source_metrics={x.check_id:1 for x in s.checks},target_metrics={x.check_id:2 for x in t.checks})
 assert r.status is MigrationValidationStatus.FAILED and r.check_results[0].difference==1
