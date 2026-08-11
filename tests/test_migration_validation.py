from tests.test_transformation_sql import _approved
import pytest
from schemabridge.services.validation_sql import compile_validation_sql
from schemabridge.services.reconciliation import reconcile_validation_results
from schemabridge.models.validation import MigrationValidationStatus
def test_paired_sql_and_reconciliation():
 s,t=compile_validation_sql(_approved(),source_schema='public',source_table='people',target_database='db',target_schema='s',target_table='people')
 assert s.dialect.value=='POSTGRESQL' and t.dialect.value=='SNOWFLAKE'
 assert 'COUNT(*)' in s.sql and 'CONCAT_WS(%s' in s.sql and s.parameters==(' ',)
 r=reconcile_validation_results(s,t,approved_plan_version=1,source_metrics={x.check_id:1 for x in s.checks},target_metrics={x.check_id:1 for x in t.checks})
 assert r.status is MigrationValidationStatus.PASSED and r.approved_plan_version==1
 r=reconcile_validation_results(s,t,approved_plan_version=1,source_metrics={x.check_id:1 for x in s.checks},target_metrics={x.check_id:2 for x in t.checks})
 assert r.status is MigrationValidationStatus.FAILED and r.check_results[0].difference==1

def test_reconciliation_requires_an_explicit_approved_plan_version():
 s,t=compile_validation_sql(_approved(),source_schema='public',source_table='people',target_database='db',target_schema='s',target_table='people')
 metrics={x.check_id:1 for x in s.checks}
 with pytest.raises(TypeError):reconcile_validation_results(s,t,source_metrics=metrics,target_metrics=metrics)
