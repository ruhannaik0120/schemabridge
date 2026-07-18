from pathlib import Path
import hashlib
import pytest
from persistence.migrations import ControlPlaneMigrationRunner,_MIGRATIONS
from persistence.errors import WorkflowMigrationError

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

def test_migration_filenames_and_checksum_are_deterministic():
 runner=ControlPlaneMigrationRunner(lambda:None);items=runner.discover();assert [x[0] for x in items]==[1]
 assert items[0][3]==hashlib.sha256(items[0][2]).hexdigest()
