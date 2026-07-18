"""Transactional PostgreSQL implementation of the workflow repository."""
from __future__ import annotations
import json
from datetime import timezone
from uuid import UUID
from models.workflow import *
from persistence.config import ControlPlaneConfig
from persistence.errors import *
from persistence.serialization import canonical_json_bytes

_WF_COLUMNS="workflow_id,display_name,source_profile_id,target_profile_id,source_relation,target_relation,status,version,created_at,updated_at,latest_artifact_version,last_error_code,warnings"
_ART_COLUMNS="artifact_id,workflow_id,artifact_type,artifact_version,schema_version,payload::text,payload_sha256,created_at"
_EVENT_COLUMNS="sequence_number,event_id,workflow_id,event_type,previous_status,new_status,workflow_version,artifact_id,artifact_type,actor_type,actor_reference,request_id,idempotency_key,occurred_at,metadata"

class PostgreSQLWorkflowRepository:
 def __init__(self,config:ControlPlaneConfig,*,connect=None):
  if not isinstance(config,ControlPlaneConfig) or not config.enabled:raise ValueError("Control-plane persistence is not configured.")
  self._config=config;self._connect=connect
 def _open(self):
  if self._connect is not None:return self._connect(self._config.dsn)
  try:
   import psycopg
   return psycopg.connect(self._config.dsn,autocommit=False)
  except Exception:raise WorkflowPersistenceError() from None
 @staticmethod
 def _relation(value):return json.dumps({"catalog_name":value.catalog_name,"schema_name":value.schema_name,"object_name":value.object_name,"system":value.system},ensure_ascii=False,sort_keys=True,separators=(",",":"))
 @staticmethod
 def _workflow(row):
  def relation(value):
   if isinstance(value,str):value=json.loads(value)
   return WorkflowRelation(**value)
  warnings=json.loads(row[12]) if isinstance(row[12],str) else row[12]
  return MigrationWorkflow(workflow_id=row[0],display_name=row[1],source_profile_id=row[2],target_profile_id=row[3],source_relation=relation(row[4]),target_relation=relation(row[5]),status=MigrationWorkflowStatus(row[6]),version=row[7],created_at=row[8].astimezone(timezone.utc),updated_at=row[9].astimezone(timezone.utc),latest_artifact_version=row[10],last_error_code=row[11],warnings=tuple(warnings))
 @staticmethod
 def _artifact(row):
  value=json.loads(row[5]) if isinstance(row[5],str) else row[5]
  payload=canonical_json_bytes(value)
  return WorkflowArtifact(artifact_id=row[0],workflow_id=row[1],artifact_type=WorkflowArtifactType(row[2]),artifact_version=row[3],schema_version=row[4],payload=payload,payload_sha256=row[6],created_at=row[7].astimezone(timezone.utc))
 @staticmethod
 def _event_model(row):
  metadata=json.loads(row[14]) if isinstance(row[14],str) else row[14]
  return MigrationAuditEvent(sequence_number=row[0],event_id=row[1],workflow_id=row[2],event_type=MigrationAuditEventType(row[3]),previous_status=MigrationWorkflowStatus(row[4]) if row[4] else None,new_status=MigrationWorkflowStatus(row[5]) if row[5] else None,workflow_version=row[6],artifact_id=row[7],artifact_type=WorkflowArtifactType(row[8]) if row[8] else None,actor_type=AuditActorType(row[9]),actor_reference=row[10],request_id=row[11],idempotency_key=row[12],occurred_at=row[13].astimezone(timezone.utc),metadata=AuditMetadata(**metadata))
 def _idem(self,cursor,scope,key,digest):
  cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",(f"{scope}:{key}",))
  cursor.execute("SELECT request_sha256, workflow_id, result_reference FROM migration_idempotency WHERE command_scope=%s AND idempotency_key=%s FOR UPDATE",(scope,key));row=cursor.fetchone()
  if row is None:return None
  if row[0]!=digest:raise WorkflowIdempotencyConflictError()
  return row[1],row[2]
 def _insert_idem(self,cursor,scope,key,kind,digest,workflow_id,result,at):
  cursor.execute("INSERT INTO migration_idempotency(command_scope,idempotency_key,command_type,request_sha256,workflow_id,result_reference,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",(scope,key,kind,digest,workflow_id,result,at))
 def _insert_event(self,cursor,event,version):
  cursor.execute("SELECT COALESCE(MAX(sequence_number),0)+1 FROM migration_audit_events WHERE workflow_id=%s",(event.workflow_id,));sequence=cursor.fetchone()[0]
  cursor.execute("INSERT INTO migration_audit_events(workflow_id,sequence_number,event_id,event_type,previous_status,new_status,workflow_version,artifact_id,artifact_type,actor_type,actor_reference,request_id,idempotency_key,occurred_at,metadata) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",(event.workflow_id,sequence,event.event_id,event.event_type.value,event.previous_status.value if event.previous_status else None,event.new_status.value if event.new_status else None,version,event.artifact_id,event.artifact_type.value if event.artifact_type else None,event.actor_type.value,event.actor_reference,event.request_id,event.idempotency_key,event.occurred_at,json.dumps({"reason_code":event.metadata.reason_code},sort_keys=True,separators=(",",":"))))
 @staticmethod
 def _close(connection):
  try:connection.close()
  except Exception:return None
 def create_workflow(self,workflow,event,*,idempotency_key,request_hash):
  if event.workflow_id!=workflow.workflow_id:raise WorkflowPersistenceError()
  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     replay=self._idem(cursor,"CREATE",idempotency_key,request_hash)
     if replay:
      cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s",(replay[0],));return self._workflow(cursor.fetchone())
     cursor.execute("INSERT INTO migration_workflows(workflow_id,display_name,source_profile_id,target_profile_id,source_relation,target_relation,status,version,created_at,updated_at,latest_artifact_version,last_error_code,warnings) VALUES (%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s,%s::jsonb)",(workflow.workflow_id,workflow.display_name,workflow.source_profile_id,workflow.target_profile_id,self._relation(workflow.source_relation),self._relation(workflow.target_relation),workflow.status.value,workflow.version,workflow.created_at,workflow.updated_at,workflow.latest_artifact_version,workflow.last_error_code,json.dumps(workflow.warnings)))
     self._insert_event(cursor,event,workflow.version);self._insert_idem(cursor,"CREATE",idempotency_key,"CREATE_WORKFLOW",request_hash,workflow.workflow_id,workflow.workflow_id,workflow.created_at);return workflow
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def get_workflow(self,workflow_id):
  connection=self._open()
  try:
   with connection.cursor() as cursor:
    cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s",(workflow_id,));row=cursor.fetchone()
    if row is None:raise WorkflowNotFoundError()
    return self._workflow(row)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def transition_status(self,workflow_id,expected_version,new_status,event,*,last_error_code,idempotency_key,request_hash):
  if event.workflow_id!=workflow_id:raise WorkflowPersistenceError()
  connection=self._open();scope=f"{workflow_id}:STATUS"
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     replay=self._idem(cursor,scope,idempotency_key,request_hash)
     if replay:
      cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s",(workflow_id,));return self._workflow(cursor.fetchone())
     cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s FOR UPDATE",(workflow_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     old=self._workflow(row)
     if old.version!=expected_version:raise WorkflowConflictError()
     if old.status is new_status:raise InvalidWorkflowTransitionError()
     version=old.version+1;cursor.execute("UPDATE migration_workflows SET status=%s,version=%s,updated_at=%s,last_error_code=%s WHERE workflow_id=%s AND version=%s",(new_status.value,version,event.occurred_at,last_error_code,workflow_id,expected_version))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     self._insert_event(cursor,event,version);self._insert_idem(cursor,scope,idempotency_key,"TRANSITION_STATUS",request_hash,workflow_id,workflow_id,event.occurred_at)
     return MigrationWorkflow(workflow_id=old.workflow_id,display_name=old.display_name,source_profile_id=old.source_profile_id,target_profile_id=old.target_profile_id,source_relation=old.source_relation,target_relation=old.target_relation,status=new_status,version=version,created_at=old.created_at,updated_at=event.occurred_at,latest_artifact_version=old.latest_artifact_version,last_error_code=last_error_code,warnings=old.warnings)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def mark_failed(self,workflow_id,expected_version,event,**kwargs):return self.transition_status(workflow_id,expected_version,MigrationWorkflowStatus.FAILED,event,**kwargs)
 def cancel_workflow(self,workflow_id,expected_version,event,**kwargs):return self.transition_status(workflow_id,expected_version,MigrationWorkflowStatus.CANCELLED,event,**kwargs)
 def append_artifact(self,workflow_id,expected_version,artifact,event,*,idempotency_key,request_hash):
  if artifact.workflow_id!=workflow_id:raise WorkflowArtifactValidationError()
  if event.workflow_id!=workflow_id or event.artifact_id!=artifact.artifact_id or event.artifact_type is not artifact.artifact_type:raise WorkflowPersistenceError()
  connection=self._open();scope=f"{workflow_id}:ARTIFACT"
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     replay=self._idem(cursor,scope,idempotency_key,request_hash)
     if replay:
      cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s",(workflow_id,));workflow=self._workflow(cursor.fetchone());cursor.execute(f"SELECT {_ART_COLUMNS} FROM migration_workflow_artifacts WHERE artifact_id=%s",(replay[1],));return workflow,self._artifact(cursor.fetchone())
     cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s FOR UPDATE",(workflow_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     old=self._workflow(row)
     if old.version!=expected_version or artifact.artifact_version!=old.latest_artifact_version+1:raise WorkflowConflictError()
     cursor.execute("INSERT INTO migration_workflow_artifacts(artifact_id,workflow_id,artifact_type,artifact_version,schema_version,payload,payload_sha256,created_at) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s)",(artifact.artifact_id,workflow_id,artifact.artifact_type.value,artifact.artifact_version,artifact.schema_version,artifact.payload.decode("utf-8"),artifact.payload_sha256,artifact.created_at))
     version=old.version+1;cursor.execute("UPDATE migration_workflows SET version=%s,updated_at=%s,latest_artifact_version=%s WHERE workflow_id=%s AND version=%s",(version,artifact.created_at,artifact.artifact_version,workflow_id,expected_version))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     self._insert_event(cursor,event,version);self._insert_idem(cursor,scope,idempotency_key,"APPEND_ARTIFACT",request_hash,workflow_id,artifact.artifact_id,artifact.created_at)
     return self._workflow((old.workflow_id,old.display_name,old.source_profile_id,old.target_profile_id,self._relation(old.source_relation),self._relation(old.target_relation),old.status.value,version,old.created_at,artifact.created_at,artifact.artifact_version,old.last_error_code,json.dumps(old.warnings))),artifact
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def _list(self,workflow_id,offset,limit,columns,table,order,mapper):
  if isinstance(offset,bool) or not isinstance(offset,int) or offset<0 or isinstance(limit,bool) or not isinstance(limit,int) or not 1<=limit<=500:raise ValueError("Pagination is invalid.")
  connection=self._open()
  try:
   with connection.cursor() as cursor:
    cursor.execute("SELECT 1 FROM migration_workflows WHERE workflow_id=%s",(workflow_id,))
    if cursor.fetchone() is None:raise WorkflowNotFoundError()
    cursor.execute(f"SELECT {columns} FROM {table} WHERE workflow_id=%s ORDER BY {order} OFFSET %s LIMIT %s",(workflow_id,offset,limit));return tuple(mapper(row) for row in cursor.fetchall())
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def list_artifacts(self,workflow_id,*,offset=0,limit=100):return self._list(workflow_id,offset,limit,_ART_COLUMNS,"migration_workflow_artifacts","artifact_version",self._artifact)
 def list_audit_events(self,workflow_id,*,offset=0,limit=100):return self._list(workflow_id,offset,limit,_EVENT_COLUMNS,"migration_audit_events","sequence_number",self._event_model)
 def close(self):return None
