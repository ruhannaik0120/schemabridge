from pathlib import Path
import hashlib
import pytest
from schemabridge.persistence.migrations import ControlPlaneMigrationRunner,_MIGRATIONS
from schemabridge.persistence.errors import WorkflowMigrationError

class Database:
 def __init__(self):self.applied={};self.executed=[];self.rollbacks=0;self.closes=0
 def connection(self):return Connection(self)
class Transaction:
 def __init__(self,db):self.db=db
 def __enter__(self):return self
 def __exit__(self,kind,*_):
  if kind:self.db.rollbacks+=1
class Cursor:
 def __init__(self,db):self.db=db;self.rows=[]
 def __enter__(self):return self
 def __exit__(self,*_):pass
 def execute(self,sql,params=None):
  self.db.executed.append((sql,params))
  if sql.startswith('SELECT version'):self.rows=sorted(self.db.applied.items())
  elif sql.startswith('INSERT INTO schemabridge_control_plane_migrations'):self.db.applied[params[0]]=params[2]
 def fetchall(self):return list(self.rows)
class Connection:
 def __init__(self,db):self.db=db
 def transaction(self):return Transaction(self.db)
 def cursor(self):return Cursor(self.db)
 def close(self):self.db.closes+=1

def test_migration_order_checksum_lock_repeat_and_cleanup(tmp_path):
 (tmp_path/'0002_second.sql').write_text('CREATE TABLE second(id INTEGER);',encoding='utf-8');(tmp_path/'0001_first.sql').write_text('CREATE TABLE first(id INTEGER);',encoding='utf-8')
 db=Database();runner=ControlPlaneMigrationRunner(db.connection,tmp_path)
 assert [x[0] for x in runner.discover()]==[1,2] and runner.run()==(1,2)
 count=len([x for x in db.executed if x[0].startswith('INSERT INTO schemabridge')]);runner.run();assert len([x for x in db.executed if x[0].startswith('INSERT INTO schemabridge')])==count
 (tmp_path/'0001_first.sql').write_text('CREATE TABLE changed(id INTEGER);',encoding='utf-8')
 with pytest.raises(WorkflowMigrationError):runner.run()
 assert db.rollbacks==1 and db.closes==3 and any('pg_advisory_xact_lock' in x[0] and x[1]==(748392615,) for x in db.executed)

def test_schema_migration_has_integrity_indexes_and_no_destructive_or_credentials():
 text=(_MIGRATIONS/'0001_workflow_audit.sql').read_text(encoding='utf-8');upper=text.upper()
 for table in ('migration_workflows','migration_workflow_artifacts','migration_audit_events','migration_idempotency'):assert f'CREATE TABLE IF NOT EXISTS {table}' in text
 assert 'PRIMARY KEY' in upper and 'REFERENCES migration_workflows' in text and 'UNIQUE(workflow_id, artifact_version)' in text
 assert 'FOREIGN KEY(workflow_id, artifact_id) REFERENCES migration_workflow_artifacts(workflow_id, artifact_id)' in text
 assert upper.count('CREATE INDEX')>=5 and 'CHECK (VERSION > 0)' in upper
 assert all(word not in upper for word in ('DROP ','TRUNCATE ','EXECUTE FORMAT'))
 assert all(word not in text.casefold() for word in ('password','private_key','access_token','postgresql://'))

def test_execution_migration_extends_states_artifacts_and_attempt_integrity():
 text=(_MIGRATIONS/'0002_workflow_execution.sql').read_text(encoding='utf-8');upper=text.upper()
 assert 'CREATE TABLE IF NOT EXISTS MIGRATION_EXECUTION_ATTEMPTS' in upper
 for value in ('EXECUTION_READY','EXECUTING','EXECUTED','EXECUTION_RECOVERY_REQUIRED','EXECUTION_EVIDENCE'):
  assert value in upper
 assert 'UX_MIGRATION_EXECUTION_ACTIVE_FINGERPRINT' in upper
 assert 'REFERENCES MIGRATION_WORKFLOW_ARTIFACTS(WORKFLOW_ID, ARTIFACT_ID)' in upper
 assert all(word not in text.casefold() for word in ('password','private_key','access_token','postgresql://'))

def test_migration_filenames_and_checksum_are_deterministic():
 runner=ControlPlaneMigrationRunner(lambda:None);items=runner.discover();assert [x[0] for x in items]==[1,2,3,4,5,6,7,8,9]
 assert items[0][3]==hashlib.sha256(items[0][2]).hexdigest()

def test_cleanup_migration_extends_artifact_constraints():
 text=(_MIGRATIONS/'0005_staging_cleanup.sql').read_text(encoding='utf-8').upper()
 assert 'STAGING_CLEANUP_EVIDENCE' in text

def test_migration_checksum_is_stable_across_windows_line_endings(tmp_path):
 sql=b"SELECT 1;\nSELECT 2;\n"
 (tmp_path/'0001_test.sql').write_bytes(sql.replace(b'\n',b'\r\n'))
 item=ControlPlaneMigrationRunner(lambda:None,tmp_path).discover()[0]
 assert item[2]==sql
 assert item[3]==hashlib.sha256(sql).hexdigest()

def test_validation_migration_adds_durable_claims_and_review_states():
 text=(_MIGRATIONS/'0003_workflow_validation.sql').read_text(encoding='utf-8');upper=text.upper()
 assert 'CREATE TABLE IF NOT EXISTS MIGRATION_VALIDATION_RUNS' in upper
 assert 'VALIDATION_REVIEW_REQUIRED' in upper and 'VALIDATION_RECOVERY_REQUIRED' in upper
 assert 'VALIDATE_WORKFLOW' in upper and 'DURATION_MS' in upper
 assert all(word not in text.casefold() for word in ('password','private_key','access_token','postgresql://'))

def test_transport_migration_adds_durable_claims_states_and_evidence():
 text=(_MIGRATIONS/'0004_workflow_transport.sql').read_text(encoding='utf-8');upper=text.upper()
 assert 'CREATE TABLE IF NOT EXISTS MIGRATION_TRANSPORT_ATTEMPTS' in upper
 for value in ('STAGING','STAGED','STAGING_RECOVERY_REQUIRED','STAGING_LOAD_EVIDENCE','LOAD_STAGING'):
  assert value in upper
 assert 'UX_MIGRATION_TRANSPORT_ACTIVE_FINGERPRINT' in upper
 assert 'REFERENCES MIGRATION_WORKFLOW_ARTIFACTS(WORKFLOW_ID, EVIDENCE_ARTIFACT_ID)' not in upper
 assert 'REFERENCES MIGRATION_WORKFLOW_ARTIFACTS(WORKFLOW_ID, ARTIFACT_ID)' in upper
 assert all(word not in text.casefold() for word in ('password','private_key','access_token','postgresql://'))

def test_background_job_migration_adds_durable_jobs_and_integrity_rules():
 text=(_MIGRATIONS/'0006_background_migration_jobs.sql').read_text(encoding='utf-8');upper=text.upper()
 assert 'CREATE TABLE IF NOT EXISTS MIGRATION_JOBS' in upper
 assert 'REFERENCES MIGRATION_WORKFLOWS(WORKFLOW_ID)' in upper
 assert 'CREATE_MIGRATION_JOB' in upper
 for value in ('QUEUED','RUNNING','SUCCEEDED','FAILED','RECOVERY_REQUIRED'):
  assert value in upper
 for value in ('PREPARING','STAGING','TRANSFORMING','EXECUTING','CLEANING_UP','VALIDATING','COMPLETED'):
  assert value in upper
 assert 'UX_MIGRATION_JOBS_ACTIVE_FINGERPRINT' in upper
 assert 'UNIQUE(WORKFLOW_ID, IDEMPOTENCY_KEY)' in upper
 assert all(word not in text.casefold() for word in ('password','private_key','access_token','postgresql://'))

def test_single_active_job_migration_protects_each_workflow():
 text=(_MIGRATIONS/'0007_single_active_migration_job.sql').read_text(encoding='utf-8');upper=text.upper()
 assert 'UX_MIGRATION_JOBS_ONE_ACTIVE_WORKFLOW' in upper
 assert 'ON MIGRATION_JOBS(WORKFLOW_ID)' in upper
 for value in ('QUEUED','RUNNING','SUCCEEDED','RECOVERY_REQUIRED'):
  assert value in upper

def test_job_review_status_migration_preserves_manual_review_boundary():
 text=(_MIGRATIONS/'0008_migration_job_review_status.sql').read_text(encoding='utf-8');upper=text.upper()
 assert 'REVIEW_REQUIRED' in upper
 assert 'MIGRATION_JOBS_STATUS_CHECK' in upper
 assert 'MIGRATION_JOBS_CHECK2' in upper
 assert 'UX_MIGRATION_JOBS_ACTIVE_FINGERPRINT' in upper
 assert 'UX_MIGRATION_JOBS_ONE_ACTIVE_WORKFLOW' in upper
 assert all(word not in text.casefold() for word in ('password','private_key','access_token','postgresql://'))

def test_job_progress_migration_adds_database_neutral_cumulative_counts():
 text=(_MIGRATIONS/'0009_migration_job_batch_progress.sql').read_text(encoding='utf-8');upper=text.upper()
 for value in ('BATCHES_COMPLETED','ROWS_READ','ROWS_WRITTEN','TOTAL_ROWS_ESTIMATE','PROGRESS_UPDATED_AT'):
  assert value in upper
 assert 'MIGRATION_JOBS_BATCH_PROGRESS_CHECK' in upper
 assert 'ROWS_READ = ROWS_WRITTEN' in upper
 assert all(vendor not in text.casefold() for vendor in ('postgresql','mysql','snowflake','sqlserver','databricks'))
 assert all(secret not in text.casefold() for secret in ('password','private_key','access_token','postgresql://'))
