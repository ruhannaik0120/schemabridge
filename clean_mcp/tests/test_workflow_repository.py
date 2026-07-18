from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID
import hashlib
import importlib
import os
import pytest
from models.workflow import AuditActorType,MigrationWorkflowStatus,WorkflowArtifactType,WorkflowRelation
from persistence.config import ControlPlaneConfig
from persistence.errors import WorkflowConflictError,WorkflowPersistenceError
from persistence.migrations import ControlPlaneMigrationRunner
from persistence.postgresql import PostgreSQLWorkflowRepository
from services.workflow_persistence import WorkflowPersistenceService
from tests.test_mapping_approval import _column,_table

NOW=datetime(2026,1,1,tzinfo=timezone.utc)
ROW=(UUID(int=1),'name','source','target',{'catalog_name':None,'schema_name':'s','object_name':'t','system':'postgresql'},{'catalog_name':'d','schema_name':'s','object_name':'t','system':'snowflake'},'DRAFT',1,NOW,NOW,0,None,[])
class Cursor:
 def __init__(self,row=None,error=None):self.row=row;self.error=error;self.calls=[]
 def __enter__(self):return self
 def __exit__(self,*_):pass
 def execute(self,sql,params=None):
  self.calls.append((sql,params))
  if self.error:raise RuntimeError('postgresql://user:password@host/db SELECT secret')
 def fetchone(self):return self.row
class Connection:
 def __init__(self,cursor):self.value=cursor;self.closed=False
 def cursor(self):return self.value
 def close(self):self.closed=True

class CloseErrorConnection(Connection):
 def close(self):
  self.closed=True
  raise RuntimeError('close detail must not escape')

def test_postgresql_read_is_parameterized_returns_domain_and_closes():
 cursor=Cursor(ROW);connection=Connection(cursor);repo=PostgreSQLWorkflowRepository(ControlPlaneConfig(dsn='secret-dsn'),connect=lambda _dsn:connection)
 result=repo.get_workflow(UUID(int=1));assert result.status is MigrationWorkflowStatus.DRAFT and connection.closed
 assert cursor.calls[0][1]==(UUID(int=1),) and '%s' in cursor.calls[0][0]

def test_postgresql_driver_errors_are_fixed_redacted_and_cleanup():
 cursor=Cursor(error=True);connection=Connection(cursor);repo=PostgreSQLWorkflowRepository(ControlPlaneConfig(dsn='postgresql://user:password@host/db'),connect=lambda _dsn:connection)
 with pytest.raises(WorkflowPersistenceError) as caught:repo.get_workflow(UUID(int=1))
 assert str(caught.value)=='The workflow persistence operation failed.' and 'password' not in str(caught.value) and connection.closed

def test_postgresql_close_error_does_not_mask_a_successful_read():
 cursor=Cursor(ROW);connection=CloseErrorConnection(cursor);repo=PostgreSQLWorkflowRepository(ControlPlaneConfig(dsn='secret'),connect=lambda _dsn:connection)
 assert repo.get_workflow(UUID(int=1)).workflow_id==UUID(int=1) and connection.closed

def test_postgresql_artifact_read_restores_canonical_bytes_and_checks_hash():
 canonical=b'{"a":1,"b":2}';digest=hashlib.sha256(canonical).hexdigest()
 row=(UUID(int=2),UUID(int=1),WorkflowArtifactType.SOURCE_DISCOVERY.value,1,1,'{"b": 2, "a": 1}',digest,NOW)
 artifact=PostgreSQLWorkflowRepository._artifact(row)
 assert artifact.payload==canonical and artifact.payload_sha256==digest

def test_repository_construction_and_import_are_lazy():
 calls=[];repo=PostgreSQLWorkflowRepository(ControlPlaneConfig(dsn='secret'),connect=lambda dsn:calls.append(dsn))
 assert calls==[] and 'secret' not in repr(repo._config)

def test_optional_real_postgresql_contract_in_an_isolated_schema():
 if os.getenv('SCHEMABRIDGE_RUN_CONTROL_PLANE_INTEGRATION')!='1' or not os.getenv('SCHEMABRIDGE_CONTROL_PLANE_TEST_DSN'):
  pytest.skip('Set SCHEMABRIDGE_RUN_CONTROL_PLANE_INTEGRATION=1 and SCHEMABRIDGE_CONTROL_PLANE_TEST_DSN to run the isolated integration contract.')
 psycopg=importlib.import_module('psycopg');sql=importlib.import_module('psycopg.sql')
 dsn=os.environ['SCHEMABRIDGE_CONTROL_PLANE_TEST_DSN'];admin=psycopg.connect(dsn,autocommit=True)
 if 'test' not in admin.info.dbname.casefold():
  admin.close();pytest.skip('Safety guard requires a database name containing "test".')
 schema=f'schemabridge_test_{UUID(bytes=os.urandom(16)).hex}'
 admin.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
 def connect():return psycopg.connect(dsn,autocommit=False,options=f'-c search_path={schema}')
 try:
  assert ControlPlaneMigrationRunner(connect).run()==(1,)
  repo=PostgreSQLWorkflowRepository(ControlPlaneConfig(dsn='[integration-test-dsn]'),connect=lambda _:connect())
  source=_table('source',_column('id'));target=_table('target',_column('id'))
  source_relation=WorkflowRelation(catalog_name=source.catalog_name,schema_name=source.schema_name,object_name=source.object_name,system=source.system)
  target_relation=WorkflowRelation(catalog_name=target.catalog_name,schema_name=target.schema_name,object_name=target.object_name,system=target.system)
  def create():return WorkflowPersistenceService(repo).create_workflow(display_name='Integration contract',source_profile_id='source',target_profile_id='target',source_relation=source_relation,target_relation=target_relation,idempotency_key='concurrent-create')
  with ThreadPoolExecutor(max_workers=2) as pool:created=list(pool.map(lambda _:create(),range(2)))
  assert created[0]==created[1]
  service=WorkflowPersistenceService(repo);workflow,artifact=service.append_source_discovery(created[0].workflow_id,1,source,idempotency_key='append',actor_type=AuditActorType.SERVICE,actor_reference=None,request_id=None)
  discovered=service.transition_status(workflow.workflow_id,expected_version=2,new_status=MigrationWorkflowStatus.DISCOVERED,idempotency_key='discover')
  with pytest.raises(WorkflowConflictError):service.transition_status(workflow.workflow_id,expected_version=2,new_status=MigrationWorkflowStatus.DISCOVERED,idempotency_key='stale')
  assert discovered.version==3 and repo.list_artifacts(workflow.workflow_id)==(artifact,)
  assert [event.sequence_number for event in repo.list_audit_events(workflow.workflow_id)]==[1,2,3]
 finally:
  admin.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)));admin.close()
