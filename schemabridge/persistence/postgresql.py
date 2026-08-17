"""Implement the durable workflow repository with PostgreSQL transactions.

Each command opens a short-lived connection and groups idempotency lookup,
optimistic version checks, row locking, workflow mutation, immutable evidence,
and audit insertion in one transaction.  Database and driver failures are
collapsed to persistence-domain errors before leaving this boundary.
"""
from __future__ import annotations
import json
from dataclasses import replace
from datetime import datetime,timezone
from uuid import UUID
from schemabridge.models.migration_job import ALLOWED_JOB_STAGE_TRANSITIONS,MigrationJob,MigrationJobStage,MigrationJobStatus
from schemabridge.models.execution import MigrationExecutionAttempt,MigrationExecutionAttemptStatus
from schemabridge.models.workflow_validation import WorkflowValidationRun,WorkflowValidationRunStatus
from schemabridge.models.transport import BatchTransportProgress,TransportRelation
from schemabridge.models.workflow_transport import WorkflowTransportAttempt,WorkflowTransportAttemptStatus,WorkflowTransportEvidence
from schemabridge.models.workflow import *
from schemabridge.persistence.config import ControlPlaneConfig
from schemabridge.persistence.errors import *
from schemabridge.persistence.serialization import canonical_json_bytes

_WF_COLUMNS="workflow_id,display_name,source_profile_id,target_profile_id,source_relation,target_relation,status,version,created_at,updated_at,latest_artifact_version,last_error_code,warnings"
_ART_COLUMNS="artifact_id,workflow_id,artifact_type,artifact_version,schema_version,payload::text,payload_sha256,created_at"
_EVENT_COLUMNS="sequence_number,event_id,workflow_id,event_type,previous_status,new_status,workflow_version,artifact_id,artifact_type,actor_type,actor_reference,request_id,idempotency_key,occurred_at,metadata"
_ATT_COLUMNS="attempt_id,workflow_id,approved_mapping_artifact_version,transformation_preview_artifact_version,target_profile_id,execution_fingerprint,status,timeout_seconds,claimed_at,actor_type,idempotency_key,actor_reference,running_at,completed_at,evidence_artifact_id,failure_category"
_VAL_COLUMNS="run_id,workflow_id,execution_attempt_id,execution_evidence_artifact_version,approved_mapping_artifact_version,validation_preview_artifact_version,source_profile_id,target_profile_id,validation_fingerprint,status,timeout_seconds,claimed_at,actor_type,idempotency_key,actor_reference,running_at,completed_at,duration_ms,evidence_artifact_id,failure_category"
_TRANS_COLUMNS="attempt_id,workflow_id,source_discovery_artifact_version,approved_mapping_artifact_version,source_profile_id,target_profile_id,staging_relation,batch_size,timeout_seconds,transport_fingerprint,status,claimed_at,actor_type,idempotency_key,actor_reference,running_at,completed_at,evidence_artifact_id,failure_category"
_JOB_COLUMNS="job_id,workflow_id,expected_workflow_version,source_discovery_artifact_version,approved_mapping_artifact_version,source_profile_id,target_profile_id,batch_size,timeout_seconds,job_fingerprint,status,stage,queued_at,actor_type,idempotency_key,actor_reference,started_at,completed_at,duration_ms,failure_category,batches_completed,rows_read,rows_written,total_rows_estimate,progress_updated_at"

class PostgreSQLWorkflowRepository:
 """Persist workflow control-plane records with transactional guarantees."""

 def __init__(self,config:ControlPlaneConfig,*,connect=None):
  """Bind validated control-plane configuration and an optional test connector."""

  if not isinstance(config,ControlPlaneConfig) or not config.enabled:raise ValueError("Control-plane persistence is not configured.")
  self._config=config;self._connect=connect
 def _open(self):
  """Open a transaction-capable connection without exposing DSN failures."""

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
 @staticmethod
 def _attempt(row):
  return MigrationExecutionAttempt(attempt_id=row[0],workflow_id=row[1],approved_mapping_artifact_version=row[2],transformation_preview_artifact_version=row[3],target_profile_id=row[4],execution_fingerprint=row[5],status=MigrationExecutionAttemptStatus(row[6]),timeout_seconds=row[7],claimed_at=row[8].astimezone(timezone.utc),actor_type=AuditActorType(row[9]),idempotency_key=row[10],actor_reference=row[11],running_at=row[12].astimezone(timezone.utc) if row[12] else None,completed_at=row[13].astimezone(timezone.utc) if row[13] else None,evidence_artifact_id=row[14],failure_category=row[15])
 @staticmethod
 def _validation_run(row):
  return WorkflowValidationRun(run_id=row[0],workflow_id=row[1],execution_attempt_id=row[2],execution_evidence_artifact_version=row[3],approved_mapping_artifact_version=row[4],validation_preview_artifact_version=row[5],source_profile_id=row[6],target_profile_id=row[7],validation_fingerprint=row[8],status=WorkflowValidationRunStatus(row[9]),timeout_seconds=row[10],claimed_at=row[11].astimezone(timezone.utc),actor_type=AuditActorType(row[12]),idempotency_key=row[13],actor_reference=row[14],running_at=row[15].astimezone(timezone.utc) if row[15] else None,completed_at=row[16].astimezone(timezone.utc) if row[16] else None,duration_ms=row[17],evidence_artifact_id=row[18],failure_category=row[19])
 @staticmethod
 def _transport_attempt(row):
  relation=json.loads(row[6]) if isinstance(row[6],str) else row[6]
  return WorkflowTransportAttempt(attempt_id=row[0],workflow_id=row[1],source_discovery_artifact_version=row[2],approved_mapping_artifact_version=row[3],source_profile_id=row[4],target_profile_id=row[5],staging_relation=TransportRelation(**relation),batch_size=row[7],timeout_seconds=row[8],transport_fingerprint=row[9],status=WorkflowTransportAttemptStatus(row[10]),claimed_at=row[11].astimezone(timezone.utc),actor_type=AuditActorType(row[12]),idempotency_key=row[13],actor_reference=row[14],running_at=row[15].astimezone(timezone.utc) if row[15] else None,completed_at=row[16].astimezone(timezone.utc) if row[16] else None,evidence_artifact_id=row[17],failure_category=row[18])
 @staticmethod
 def _migration_job(row):
  progress=BatchTransportProgress(batches_completed=row[20],rows_read=row[21],rows_written=row[22],total_rows_estimate=row[23]) if row[24] else None
  return MigrationJob(job_id=row[0],workflow_id=row[1],expected_workflow_version=row[2],source_discovery_artifact_version=row[3],approved_mapping_artifact_version=row[4],source_profile_id=row[5],target_profile_id=row[6],batch_size=row[7],timeout_seconds=row[8],job_fingerprint=row[9],status=MigrationJobStatus(row[10]),stage=MigrationJobStage(row[11]),queued_at=row[12].astimezone(timezone.utc),actor_type=AuditActorType(row[13]),idempotency_key=row[14],actor_reference=row[15],started_at=row[16].astimezone(timezone.utc) if row[16] else None,completed_at=row[17].astimezone(timezone.utc) if row[17] else None,duration_ms=row[18],failure_category=row[19],batch_progress=progress,progress_updated_at=row[24].astimezone(timezone.utc) if row[24] else None)
 def _idem(self,cursor,scope,key,digest):
  """Serialize one idempotency scope and return its recorded result if present."""

  # The transaction-level advisory lock closes the race between checking a key
  # and inserting its first result, including when no row exists to lock yet.
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
  """Atomically create the workflow, audit root, and idempotency result."""

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
 def create_migration_job(self,job):
  if not isinstance(job,MigrationJob) or job.status is not MigrationJobStatus.QUEUED or job.stage is not MigrationJobStage.QUEUED:raise WorkflowPersistenceError()
  connection=self._open();scope=f"{job.workflow_id}:MIGRATION_JOB"
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     replay=self._idem(cursor,scope,job.idempotency_key,job.job_fingerprint)
     if replay:
      cursor.execute(f"SELECT {_JOB_COLUMNS} FROM migration_jobs WHERE job_id=%s",(replay[1],));row=cursor.fetchone()
      if row is None:raise WorkflowPersistenceError()
      return self._migration_job(row),False
     cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s FOR UPDATE",(job.workflow_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     workflow=self._workflow(row)
     if workflow.version!=job.expected_workflow_version:raise WorkflowConflictError()
     if workflow.status is not MigrationWorkflowStatus.MAPPING_APPROVED:raise WorkflowOperationUnavailableError()
     cursor.execute("SELECT 1 FROM migration_jobs WHERE workflow_id=%s AND status IN ('QUEUED','RUNNING','SUCCEEDED','REVIEW_REQUIRED','RECOVERY_REQUIRED') LIMIT 1",(job.workflow_id,))
     if cursor.fetchone() is not None:raise MigrationJobAlreadyActiveError()
     cursor.execute(f"INSERT INTO migration_jobs({_JOB_COLUMNS}) VALUES ({','.join(['%s']*25)})",(job.job_id,job.workflow_id,job.expected_workflow_version,job.source_discovery_artifact_version,job.approved_mapping_artifact_version,job.source_profile_id,job.target_profile_id,job.batch_size,job.timeout_seconds,job.job_fingerprint,job.status.value,job.stage.value,job.queued_at,job.actor_type.value,job.idempotency_key,job.actor_reference,job.started_at,job.completed_at,job.duration_ms,job.failure_category,0,0,0,None,None))
     self._insert_idem(cursor,scope,job.idempotency_key,"CREATE_MIGRATION_JOB",job.job_fingerprint,job.workflow_id,job.job_id,job.queued_at)
     return job,True
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def get_migration_job(self,job_id):
  connection=self._open()
  try:
   with connection.cursor() as cursor:
    cursor.execute(f"SELECT {_JOB_COLUMNS} FROM migration_jobs WHERE job_id=%s",(job_id,));row=cursor.fetchone()
    if row is None:raise MigrationJobNotFoundError()
    return self._migration_job(row)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def claim_next_migration_job(self,started_at):
  """Lock and claim the oldest queued job without racing another worker."""

  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     cursor.execute(f"SELECT {_JOB_COLUMNS} FROM migration_jobs WHERE status='QUEUED' ORDER BY queued_at,job_id FOR UPDATE SKIP LOCKED LIMIT 1")
     row=cursor.fetchone()
     if row is None:return None
     job=self._migration_job(row)
     cursor.execute("UPDATE migration_jobs SET status='RUNNING',stage='PREPARING',started_at=%s WHERE job_id=%s AND status='QUEUED'",(started_at,job.job_id))
     if cursor.rowcount!=1:raise WorkflowPersistenceError()
     return MigrationJob(job_id=job.job_id,workflow_id=job.workflow_id,expected_workflow_version=job.expected_workflow_version,source_discovery_artifact_version=job.source_discovery_artifact_version,approved_mapping_artifact_version=job.approved_mapping_artifact_version,source_profile_id=job.source_profile_id,target_profile_id=job.target_profile_id,batch_size=job.batch_size,timeout_seconds=job.timeout_seconds,job_fingerprint=job.job_fingerprint,status=MigrationJobStatus.RUNNING,stage=MigrationJobStage.PREPARING,queued_at=job.queued_at,actor_type=job.actor_type,idempotency_key=job.idempotency_key,actor_reference=job.actor_reference,started_at=started_at)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def update_migration_job_stage(self,job_id,expected_stage,new_stage):
  """Advance one locked running job only when the caller's stage is current."""

  if not isinstance(expected_stage,MigrationJobStage) or not isinstance(new_stage,MigrationJobStage):raise MigrationJobTransitionError()
  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     cursor.execute(f"SELECT {_JOB_COLUMNS} FROM migration_jobs WHERE job_id=%s FOR UPDATE",(job_id,));row=cursor.fetchone()
     if row is None:raise MigrationJobNotFoundError()
     job=self._migration_job(row)
     valid=job.status is MigrationJobStatus.RUNNING and job.stage is expected_stage and new_stage is not MigrationJobStage.COMPLETED and new_stage in ALLOWED_JOB_STAGE_TRANSITIONS[job.stage]
     if not valid:raise MigrationJobTransitionError()
     cursor.execute("UPDATE migration_jobs SET stage=%s WHERE job_id=%s AND status='RUNNING' AND stage=%s",(new_stage.value,job_id,expected_stage.value))
     if cursor.rowcount!=1:raise MigrationJobTransitionError()
     return replace(job,stage=new_stage)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def update_migration_job_progress(self,job_id,progress,updated_at):
  """Persist a strictly newer cumulative snapshot while staging is running."""

  if not isinstance(progress,BatchTransportProgress) or not isinstance(updated_at,datetime) or updated_at.tzinfo is None or updated_at.utcoffset()!=timezone.utc.utcoffset(updated_at):raise MigrationJobTransitionError()
  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     cursor.execute(f"SELECT {_JOB_COLUMNS} FROM migration_jobs WHERE job_id=%s FOR UPDATE",(job_id,));row=cursor.fetchone()
     if row is None:raise MigrationJobNotFoundError()
     job=self._migration_job(row);previous=job.batch_progress
     valid=job.status is MigrationJobStatus.RUNNING and job.stage is MigrationJobStage.STAGING and job.started_at is not None and updated_at>=job.started_at and progress.batches_completed>0 and progress.rows_read>0
     if previous is not None:
      valid=valid and job.progress_updated_at is not None and updated_at>job.progress_updated_at and progress.batches_completed>previous.batches_completed and progress.rows_read>previous.rows_read and progress.rows_written>previous.rows_written and progress.total_rows_estimate==previous.total_rows_estimate
     if not valid:raise MigrationJobTransitionError()
     try:updated=replace(job,batch_progress=progress,progress_updated_at=updated_at)
     except (TypeError,ValueError):raise MigrationJobTransitionError() from None
     cursor.execute("UPDATE migration_jobs SET batches_completed=%s,rows_read=%s,rows_written=%s,total_rows_estimate=%s,progress_updated_at=%s WHERE job_id=%s AND status='RUNNING' AND stage='STAGING'",(progress.batches_completed,progress.rows_read,progress.rows_written,progress.total_rows_estimate,updated_at,job_id))
     if cursor.rowcount!=1:raise MigrationJobTransitionError()
     return updated
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def finish_migration_job(self,job_id,expected_stage,outcome,completed_at,failure_category):
  """Store a terminal outcome once while holding the durable job lock."""

  if not isinstance(expected_stage,MigrationJobStage) or not isinstance(outcome,MigrationJobStatus):raise MigrationJobTransitionError()
  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     cursor.execute(f"SELECT {_JOB_COLUMNS} FROM migration_jobs WHERE job_id=%s FOR UPDATE",(job_id,));row=cursor.fetchone()
     if row is None:raise MigrationJobNotFoundError()
     job=self._migration_job(row)
     exact_success=outcome is MigrationJobStatus.SUCCEEDED and job.status is outcome and job.failure_category is None
     exact_failure=outcome in {MigrationJobStatus.FAILED,MigrationJobStatus.REVIEW_REQUIRED,MigrationJobStatus.RECOVERY_REQUIRED} and job.status is outcome and job.stage is expected_stage and job.failure_category==failure_category
     if exact_success or exact_failure:return job
     valid_outcome=outcome in {MigrationJobStatus.SUCCEEDED,MigrationJobStatus.FAILED,MigrationJobStatus.REVIEW_REQUIRED,MigrationJobStatus.RECOVERY_REQUIRED}
     valid_success=outcome is MigrationJobStatus.SUCCEEDED and job.stage is MigrationJobStage.VALIDATING and expected_stage is MigrationJobStage.VALIDATING and failure_category is None
     valid_failure=outcome in {MigrationJobStatus.FAILED,MigrationJobStatus.REVIEW_REQUIRED,MigrationJobStatus.RECOVERY_REQUIRED} and job.stage is expected_stage and failure_category is not None
     if job.status is not MigrationJobStatus.RUNNING or not valid_outcome or not (valid_success or valid_failure) or job.started_at is None or completed_at<job.started_at:raise MigrationJobTransitionError()
     duration=max(0,int((completed_at-job.started_at).total_seconds()*1000))
     try:updated=replace(job,status=outcome,stage=MigrationJobStage.COMPLETED if outcome is MigrationJobStatus.SUCCEEDED else job.stage,completed_at=completed_at,duration_ms=duration,failure_category=failure_category)
     except (TypeError,ValueError):raise MigrationJobTransitionError() from None
     cursor.execute("UPDATE migration_jobs SET status=%s,stage=%s,completed_at=%s,duration_ms=%s,failure_category=%s WHERE job_id=%s AND status='RUNNING' AND stage=%s",(updated.status.value,updated.stage.value,updated.completed_at,updated.duration_ms,updated.failure_category,job_id,expected_stage.value))
     if cursor.rowcount!=1:raise MigrationJobTransitionError()
     return updated
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def transition_status(self,workflow_id,expected_version,new_status,event,*,last_error_code,idempotency_key,request_hash):
  """Apply an optimistic status change while holding the workflow row lock."""

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
  """Atomically append immutable evidence and advance both version counters."""

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
 def append_artifact_operation(self,workflow_id,expected_version,artifact,artifact_event,*,new_status,transition_event,idempotency_key,request_hash):
  """Append evidence and an optional state transition in one transaction."""

  if artifact.workflow_id!=workflow_id:raise WorkflowArtifactValidationError()
  if artifact_event.workflow_id!=workflow_id or artifact_event.artifact_id!=artifact.artifact_id or artifact_event.artifact_type is not artifact.artifact_type:raise WorkflowPersistenceError()
  if (new_status is None)!=(transition_event is None):raise WorkflowPersistenceError()
  if transition_event is not None and transition_event.workflow_id!=workflow_id:raise WorkflowPersistenceError()
  connection=self._open();scope=f"{workflow_id}:ORCHESTRATION:{artifact.artifact_type.value}"
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     replay=self._idem(cursor,scope,idempotency_key,request_hash)
     if replay:
      cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s",(workflow_id,));workflow=self._workflow(cursor.fetchone())
      cursor.execute(f"SELECT {_ART_COLUMNS} FROM migration_workflow_artifacts WHERE artifact_id=%s",(replay[1],));return workflow,self._artifact(cursor.fetchone())
     cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s FOR UPDATE",(workflow_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     old=self._workflow(row)
     if old.version!=expected_version or artifact.artifact_version!=old.latest_artifact_version+1:raise WorkflowConflictError()
     cursor.execute("INSERT INTO migration_workflow_artifacts(artifact_id,workflow_id,artifact_type,artifact_version,schema_version,payload,payload_sha256,created_at) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s)",(artifact.artifact_id,workflow_id,artifact.artifact_type.value,artifact.artifact_version,artifact.schema_version,artifact.payload.decode("utf-8"),artifact.payload_sha256,artifact.created_at))
     version=old.version+1;status=(new_status or old.status)
     cursor.execute("UPDATE migration_workflows SET status=%s,version=%s,updated_at=%s,latest_artifact_version=%s WHERE workflow_id=%s AND version=%s",(status.value,version,artifact.created_at,artifact.artifact_version,workflow_id,expected_version))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     self._insert_event(cursor,artifact_event,version)
     if transition_event is not None:self._insert_event(cursor,transition_event,version)
     self._insert_idem(cursor,scope,idempotency_key,"APPEND_ARTIFACT",request_hash,workflow_id,artifact.artifact_id,artifact.created_at)
     result=MigrationWorkflow(workflow_id=old.workflow_id,display_name=old.display_name,source_profile_id=old.source_profile_id,target_profile_id=old.target_profile_id,source_relation=old.source_relation,target_relation=old.target_relation,status=status,version=version,created_at=old.created_at,updated_at=artifact.created_at,latest_artifact_version=artifact.artifact_version,last_error_code=old.last_error_code,warnings=old.warnings)
     return result,artifact
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
 def _get_artifact(self,workflow_id,where,params):
  connection=self._open()
  try:
   with connection.cursor() as cursor:
    cursor.execute("SELECT 1 FROM migration_workflows WHERE workflow_id=%s",(workflow_id,))
    if cursor.fetchone() is None:raise WorkflowNotFoundError()
    cursor.execute(f"SELECT {_ART_COLUMNS} FROM migration_workflow_artifacts WHERE workflow_id=%s AND {where}",(workflow_id,*params));row=cursor.fetchone()
    return self._artifact(row) if row is not None else None
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def get_artifact(self,workflow_id,artifact_version):return self._get_artifact(workflow_id,"artifact_version=%s",(artifact_version,))
 def get_artifact_by_id(self,workflow_id,artifact_id):return self._get_artifact(workflow_id,"artifact_id=%s",(artifact_id,))
 def get_latest_artifact(self,workflow_id,artifact_type):return self._get_artifact(workflow_id,"artifact_type=%s ORDER BY artifact_version DESC LIMIT 1",(artifact_type.value,))
 def get_transport_attempt_by_command(self,workflow_id,idempotency_key,request_hash):
  connection=self._open();scope=f"{workflow_id}:TRANSPORT"
  try:
   with connection.cursor() as cursor:
    cursor.execute("SELECT request_sha256,result_reference FROM migration_idempotency WHERE command_scope=%s AND idempotency_key=%s",(scope,idempotency_key));row=cursor.fetchone()
    if row is None:return None
    if row[0]!=request_hash:raise WorkflowIdempotencyConflictError()
    cursor.execute(f"SELECT {_TRANS_COLUMNS} FROM migration_transport_attempts WHERE attempt_id=%s",(row[1],));stored=cursor.fetchone()
    if stored is None:raise WorkflowPersistenceError()
    return self._transport_attempt(stored)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def claim_transport_attempt(self,workflow_id,expected_version,attempt,event,*,idempotency_key,request_hash):
  """Claim one staging load before any remote table operation starts."""

  if attempt.workflow_id!=workflow_id or attempt.status is not WorkflowTransportAttemptStatus.CLAIMED:raise WorkflowPersistenceError()
  if event.workflow_id!=workflow_id or event.previous_status is not MigrationWorkflowStatus.MAPPING_APPROVED or event.new_status is not MigrationWorkflowStatus.STAGING:raise WorkflowPersistenceError()
  connection=self._open();scope=f"{workflow_id}:TRANSPORT"
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     replay=self._idem(cursor,scope,idempotency_key,request_hash)
     if replay:
      cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s",(workflow_id,));workflow=self._workflow(cursor.fetchone())
      cursor.execute(f"SELECT {_TRANS_COLUMNS} FROM migration_transport_attempts WHERE attempt_id=%s",(replay[1],));return workflow,self._transport_attempt(cursor.fetchone()),False
     cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s FOR UPDATE",(workflow_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     old=self._workflow(row)
     if old.version!=expected_version:raise WorkflowConflictError()
     if old.status is not MigrationWorkflowStatus.MAPPING_APPROVED:raise InvalidWorkflowTransitionError()
     cursor.execute("SELECT 1 FROM migration_transport_attempts WHERE workflow_id=%s AND status IN ('CLAIMED','RUNNING','SUCCEEDED','OUTCOME_UNCERTAIN') LIMIT 1",(workflow_id,))
     if cursor.fetchone() is not None:raise WorkflowTransportAlreadyInProgressError()
     relation=json.dumps({"catalog_name":attempt.staging_relation.catalog_name,"schema_name":attempt.staging_relation.schema_name,"object_name":attempt.staging_relation.object_name},ensure_ascii=False,sort_keys=True,separators=(",",":"))
     cursor.execute("INSERT INTO migration_transport_attempts(attempt_id,workflow_id,source_discovery_artifact_version,approved_mapping_artifact_version,source_profile_id,target_profile_id,staging_relation,batch_size,timeout_seconds,transport_fingerprint,status,claimed_at,actor_type,idempotency_key,actor_reference) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s)",(attempt.attempt_id,workflow_id,attempt.source_discovery_artifact_version,attempt.approved_mapping_artifact_version,attempt.source_profile_id,attempt.target_profile_id,relation,attempt.batch_size,attempt.timeout_seconds,attempt.transport_fingerprint,attempt.status.value,attempt.claimed_at,attempt.actor_type.value,attempt.idempotency_key,attempt.actor_reference))
     version=old.version+1;cursor.execute("UPDATE migration_workflows SET status=%s,version=%s,updated_at=%s WHERE workflow_id=%s AND version=%s",(MigrationWorkflowStatus.STAGING.value,version,attempt.claimed_at,workflow_id,expected_version))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     self._insert_event(cursor,event,version);self._insert_idem(cursor,scope,idempotency_key,"LOAD_STAGING",request_hash,workflow_id,attempt.attempt_id,attempt.claimed_at)
     result=MigrationWorkflow(workflow_id=old.workflow_id,display_name=old.display_name,source_profile_id=old.source_profile_id,target_profile_id=old.target_profile_id,source_relation=old.source_relation,target_relation=old.target_relation,status=MigrationWorkflowStatus.STAGING,version=version,created_at=old.created_at,updated_at=attempt.claimed_at,latest_artifact_version=old.latest_artifact_version,last_error_code=old.last_error_code,warnings=old.warnings)
     return result,attempt,True
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def mark_transport_running(self,attempt_id,running_at):
  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     cursor.execute(f"SELECT {_TRANS_COLUMNS} FROM migration_transport_attempts WHERE attempt_id=%s FOR UPDATE",(attempt_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     attempt=self._transport_attempt(row)
     if attempt.status is not WorkflowTransportAttemptStatus.CLAIMED:return attempt,False
     cursor.execute("UPDATE migration_transport_attempts SET status='RUNNING',running_at=%s WHERE attempt_id=%s AND status='CLAIMED'",(running_at,attempt_id))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     cursor.execute(f"SELECT {_TRANS_COLUMNS} FROM migration_transport_attempts WHERE attempt_id=%s",(attempt_id,));return self._transport_attempt(cursor.fetchone()),True
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def complete_transport_attempt(self,workflow_id,expected_version,attempt_id,evidence,artifact,artifact_event,new_status,transition_event,*,completed_at,failure_category):
  """Store the staging result and workflow transition in one transaction."""

  if (evidence is None)!=(artifact is None) or (artifact_event is None)!=(artifact is None):raise WorkflowPersistenceError()
  if new_status is MigrationWorkflowStatus.STAGED:
   if evidence is None or failure_category is not None or artifact.artifact_type is not WorkflowArtifactType.STAGING_LOAD_EVIDENCE:raise WorkflowPersistenceError()
   terminal=WorkflowTransportAttemptStatus.SUCCEEDED
  elif new_status is MigrationWorkflowStatus.MAPPING_APPROVED:
   if evidence is not None or failure_category is None:raise WorkflowPersistenceError()
   terminal=WorkflowTransportAttemptStatus.FAILED_CLEANED_UP
  elif new_status is MigrationWorkflowStatus.STAGING_RECOVERY_REQUIRED:
   if evidence is not None or failure_category is None:raise WorkflowPersistenceError()
   terminal=WorkflowTransportAttemptStatus.OUTCOME_UNCERTAIN
  else:raise WorkflowPersistenceError()
  if transition_event.workflow_id!=workflow_id or transition_event.previous_status is not MigrationWorkflowStatus.STAGING or transition_event.new_status is not new_status:raise WorkflowPersistenceError()
  if artifact is not None and (artifact.workflow_id!=workflow_id or artifact_event.artifact_id!=artifact.artifact_id):raise WorkflowPersistenceError()
  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s FOR UPDATE",(workflow_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     old=self._workflow(row)
     if old.version!=expected_version or old.status is not MigrationWorkflowStatus.STAGING:raise WorkflowConflictError()
     cursor.execute(f"SELECT {_TRANS_COLUMNS} FROM migration_transport_attempts WHERE attempt_id=%s FOR UPDATE",(attempt_id,));attempt_row=cursor.fetchone()
     if attempt_row is None:raise WorkflowNotFoundError()
     attempt=self._transport_attempt(attempt_row)
     if attempt.status is not WorkflowTransportAttemptStatus.RUNNING:raise WorkflowConflictError()
     if evidence is not None:
      if evidence.attempt_id!=attempt_id or evidence.workflow_id!=workflow_id or evidence.transport_fingerprint!=attempt.transport_fingerprint or evidence.source_discovery_artifact_version!=attempt.source_discovery_artifact_version or evidence.approved_mapping_artifact_version!=attempt.approved_mapping_artifact_version or evidence.staging_relation!=attempt.staging_relation:raise WorkflowPersistenceError()
      if artifact.artifact_version!=old.latest_artifact_version+1:raise WorkflowConflictError()
      cursor.execute("INSERT INTO migration_workflow_artifacts(artifact_id,workflow_id,artifact_type,artifact_version,schema_version,payload,payload_sha256,created_at) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s)",(artifact.artifact_id,workflow_id,artifact.artifact_type.value,artifact.artifact_version,artifact.schema_version,artifact.payload.decode("utf-8"),artifact.payload_sha256,artifact.created_at))
     evidence_id=artifact.artifact_id if artifact is not None else None
     cursor.execute("UPDATE migration_transport_attempts SET status=%s,completed_at=%s,evidence_artifact_id=%s,failure_category=%s WHERE attempt_id=%s AND status='RUNNING'",(terminal.value,completed_at,evidence_id,failure_category,attempt_id))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     latest=artifact.artifact_version if artifact is not None else old.latest_artifact_version
     version=old.version+1;cursor.execute("UPDATE migration_workflows SET status=%s,version=%s,updated_at=%s,latest_artifact_version=%s WHERE workflow_id=%s AND version=%s",(new_status.value,version,completed_at,latest,workflow_id,expected_version))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     if artifact_event is not None:self._insert_event(cursor,artifact_event,version)
     self._insert_event(cursor,transition_event,version)
     updated_attempt=WorkflowTransportAttempt(attempt_id=attempt.attempt_id,workflow_id=attempt.workflow_id,source_discovery_artifact_version=attempt.source_discovery_artifact_version,approved_mapping_artifact_version=attempt.approved_mapping_artifact_version,source_profile_id=attempt.source_profile_id,target_profile_id=attempt.target_profile_id,staging_relation=attempt.staging_relation,batch_size=attempt.batch_size,timeout_seconds=attempt.timeout_seconds,transport_fingerprint=attempt.transport_fingerprint,status=terminal,claimed_at=attempt.claimed_at,actor_type=attempt.actor_type,idempotency_key=attempt.idempotency_key,actor_reference=attempt.actor_reference,running_at=attempt.running_at,completed_at=completed_at,evidence_artifact_id=evidence_id,failure_category=failure_category)
     result=MigrationWorkflow(workflow_id=old.workflow_id,display_name=old.display_name,source_profile_id=old.source_profile_id,target_profile_id=old.target_profile_id,source_relation=old.source_relation,target_relation=old.target_relation,status=new_status,version=version,created_at=old.created_at,updated_at=completed_at,latest_artifact_version=latest,last_error_code=old.last_error_code,warnings=old.warnings)
     return result,updated_attempt,artifact
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def get_transport_attempt(self,attempt_id):
  connection=self._open()
  try:
   with connection.cursor() as cursor:
    cursor.execute(f"SELECT {_TRANS_COLUMNS} FROM migration_transport_attempts WHERE attempt_id=%s",(attempt_id,));row=cursor.fetchone()
    if row is None:raise WorkflowNotFoundError()
    return self._transport_attempt(row)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def get_execution_attempt_by_command(self,workflow_id,idempotency_key,request_hash):
  connection=self._open();scope=f"{workflow_id}:EXECUTION"
  try:
   with connection.cursor() as cursor:
    cursor.execute("SELECT request_sha256,result_reference FROM migration_idempotency WHERE command_scope=%s AND idempotency_key=%s",(scope,idempotency_key));row=cursor.fetchone()
    if row is None:return None
    if row[0]!=request_hash:raise WorkflowIdempotencyConflictError()
    cursor.execute(f"SELECT {_ATT_COLUMNS} FROM migration_execution_attempts WHERE attempt_id=%s",(row[1],));attempt=cursor.fetchone()
    if attempt is None:raise WorkflowPersistenceError()
    return self._attempt(attempt)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def claim_execution_attempt(self,workflow_id,expected_version,attempt,event,*,idempotency_key,request_hash):
  """Durably claim execution and move the workflow to ``EXECUTING``."""

  if attempt.workflow_id!=workflow_id or attempt.status is not MigrationExecutionAttemptStatus.CLAIMED:raise WorkflowPersistenceError()
  if event.workflow_id!=workflow_id or event.previous_status is not MigrationWorkflowStatus.EXECUTION_READY or event.new_status is not MigrationWorkflowStatus.EXECUTING:raise WorkflowPersistenceError()
  connection=self._open();scope=f"{workflow_id}:EXECUTION"
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     replay=self._idem(cursor,scope,idempotency_key,request_hash)
     if replay:
      cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s",(workflow_id,));workflow=self._workflow(cursor.fetchone())
      cursor.execute(f"SELECT {_ATT_COLUMNS} FROM migration_execution_attempts WHERE attempt_id=%s",(replay[1],));return workflow,self._attempt(cursor.fetchone()),False
     cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s FOR UPDATE",(workflow_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     old=self._workflow(row)
     if old.version!=expected_version:raise WorkflowConflictError()
     if old.status is not MigrationWorkflowStatus.EXECUTION_READY:raise InvalidWorkflowTransitionError()
     cursor.execute("SELECT 1 FROM migration_execution_attempts WHERE workflow_id=%s AND status IN ('CLAIMED','RUNNING','SUCCEEDED','OUTCOME_UNCERTAIN') LIMIT 1",(workflow_id,))
     if cursor.fetchone() is not None:raise WorkflowExecutionAlreadyInProgressError()
     cursor.execute("INSERT INTO migration_execution_attempts(attempt_id,workflow_id,approved_mapping_artifact_version,transformation_preview_artifact_version,target_profile_id,execution_fingerprint,status,timeout_seconds,claimed_at,actor_type,idempotency_key,actor_reference) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(attempt.attempt_id,workflow_id,attempt.approved_mapping_artifact_version,attempt.transformation_preview_artifact_version,attempt.target_profile_id,attempt.execution_fingerprint,attempt.status.value,attempt.timeout_seconds,attempt.claimed_at,attempt.actor_type.value,attempt.idempotency_key,attempt.actor_reference))
     version=old.version+1;cursor.execute("UPDATE migration_workflows SET status=%s,version=%s,updated_at=%s WHERE workflow_id=%s AND version=%s",(MigrationWorkflowStatus.EXECUTING.value,version,attempt.claimed_at,workflow_id,expected_version))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     self._insert_event(cursor,event,version);self._insert_idem(cursor,scope,idempotency_key,"EXECUTE_WORKFLOW",request_hash,workflow_id,attempt.attempt_id,attempt.claimed_at)
     result=MigrationWorkflow(workflow_id=old.workflow_id,display_name=old.display_name,source_profile_id=old.source_profile_id,target_profile_id=old.target_profile_id,source_relation=old.source_relation,target_relation=old.target_relation,status=MigrationWorkflowStatus.EXECUTING,version=version,created_at=old.created_at,updated_at=attempt.claimed_at,latest_artifact_version=old.latest_artifact_version,last_error_code=old.last_error_code,warnings=old.warnings)
     return result,attempt,True
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def mark_execution_running(self,attempt_id,running_at):
  """Let exactly one claimant advance an execution attempt to running."""

  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     cursor.execute(f"SELECT {_ATT_COLUMNS} FROM migration_execution_attempts WHERE attempt_id=%s FOR UPDATE",(attempt_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     attempt=self._attempt(row)
     if attempt.status is not MigrationExecutionAttemptStatus.CLAIMED:return attempt,False
     cursor.execute("UPDATE migration_execution_attempts SET status='RUNNING',running_at=%s WHERE attempt_id=%s AND status='CLAIMED'",(running_at,attempt_id))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     cursor.execute(f"SELECT {_ATT_COLUMNS} FROM migration_execution_attempts WHERE attempt_id=%s",(attempt_id,));return self._attempt(cursor.fetchone()),True
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def complete_execution_attempt(self,workflow_id,expected_version,attempt_id,evidence,artifact,artifact_event,new_status,transition_event):
  """Commit terminal attempt, evidence, workflow, and audit updates together."""

  if evidence.attempt_id!=attempt_id or evidence.workflow_id!=workflow_id or artifact.workflow_id!=workflow_id or artifact.artifact_type is not WorkflowArtifactType.EXECUTION_EVIDENCE:raise WorkflowPersistenceError()
  if artifact_event.artifact_id!=artifact.artifact_id or transition_event.workflow_id!=workflow_id:raise WorkflowPersistenceError()
  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s FOR UPDATE",(workflow_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     old=self._workflow(row)
     if old.version!=expected_version or old.status is not MigrationWorkflowStatus.EXECUTING or artifact.artifact_version!=old.latest_artifact_version+1:raise WorkflowConflictError()
     cursor.execute(f"SELECT {_ATT_COLUMNS} FROM migration_execution_attempts WHERE attempt_id=%s FOR UPDATE",(attempt_id,));attempt_row=cursor.fetchone()
     if attempt_row is None:raise WorkflowNotFoundError()
     attempt=self._attempt(attempt_row)
     if attempt.status is not MigrationExecutionAttemptStatus.RUNNING:raise WorkflowConflictError()
     cursor.execute("INSERT INTO migration_workflow_artifacts(artifact_id,workflow_id,artifact_type,artifact_version,schema_version,payload,payload_sha256,created_at) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s)",(artifact.artifact_id,workflow_id,artifact.artifact_type.value,artifact.artifact_version,artifact.schema_version,artifact.payload.decode("utf-8"),artifact.payload_sha256,artifact.created_at))
     cursor.execute("UPDATE migration_execution_attempts SET status=%s,completed_at=%s,evidence_artifact_id=%s,failure_category=%s WHERE attempt_id=%s AND status='RUNNING'",(evidence.status.value,evidence.completed_at,artifact.artifact_id,evidence.failure_category,attempt_id))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     version=old.version+1;cursor.execute("UPDATE migration_workflows SET status=%s,version=%s,updated_at=%s,latest_artifact_version=%s WHERE workflow_id=%s AND version=%s",(new_status.value,version,evidence.completed_at,artifact.artifact_version,workflow_id,expected_version))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     self._insert_event(cursor,artifact_event,version);self._insert_event(cursor,transition_event,version)
     updated_attempt=MigrationExecutionAttempt(attempt_id=attempt.attempt_id,workflow_id=attempt.workflow_id,approved_mapping_artifact_version=attempt.approved_mapping_artifact_version,transformation_preview_artifact_version=attempt.transformation_preview_artifact_version,target_profile_id=attempt.target_profile_id,execution_fingerprint=attempt.execution_fingerprint,status=evidence.status,timeout_seconds=attempt.timeout_seconds,claimed_at=attempt.claimed_at,actor_type=attempt.actor_type,idempotency_key=attempt.idempotency_key,actor_reference=attempt.actor_reference,running_at=attempt.running_at,completed_at=evidence.completed_at,evidence_artifact_id=artifact.artifact_id,failure_category=evidence.failure_category)
     result=MigrationWorkflow(workflow_id=old.workflow_id,display_name=old.display_name,source_profile_id=old.source_profile_id,target_profile_id=old.target_profile_id,source_relation=old.source_relation,target_relation=old.target_relation,status=new_status,version=version,created_at=old.created_at,updated_at=evidence.completed_at,latest_artifact_version=artifact.artifact_version,last_error_code=old.last_error_code,warnings=old.warnings)
     return result,updated_attempt,artifact
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def get_execution_attempt(self,attempt_id):
  connection=self._open()
  try:
   with connection.cursor() as cursor:
    cursor.execute(f"SELECT {_ATT_COLUMNS} FROM migration_execution_attempts WHERE attempt_id=%s",(attempt_id,));row=cursor.fetchone()
    if row is None:raise WorkflowNotFoundError()
    return self._attempt(row)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def get_validation_run_by_command(self,workflow_id,idempotency_key,request_hash):
  connection=self._open();scope=f"{workflow_id}:VALIDATION"
  try:
   with connection.cursor() as cursor:
    cursor.execute("SELECT request_sha256,result_reference FROM migration_idempotency WHERE command_scope=%s AND idempotency_key=%s",(scope,idempotency_key));row=cursor.fetchone()
    if row is None:return None
    if row[0]!=request_hash:raise WorkflowIdempotencyConflictError()
    cursor.execute(f"SELECT {_VAL_COLUMNS} FROM migration_validation_runs WHERE run_id=%s",(row[1],));run=cursor.fetchone()
    if run is None:raise WorkflowPersistenceError()
    return self._validation_run(run)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def claim_validation_run(self,workflow_id,expected_version,run,artifact,artifact_event,transition_event,*,idempotency_key,request_hash):
  """Persist the validation plan and claim its run in one transaction."""

  if run.workflow_id!=workflow_id or run.status is not WorkflowValidationRunStatus.CLAIMED or artifact.workflow_id!=workflow_id or artifact.artifact_type is not WorkflowArtifactType.VALIDATION_PREVIEW:raise WorkflowPersistenceError()
  if artifact_event.artifact_id!=artifact.artifact_id or transition_event.previous_status is not MigrationWorkflowStatus.EXECUTED or transition_event.new_status is not MigrationWorkflowStatus.VALIDATING:raise WorkflowPersistenceError()
  connection=self._open();scope=f"{workflow_id}:VALIDATION"
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     replay=self._idem(cursor,scope,idempotency_key,request_hash)
     if replay:
      cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s",(workflow_id,));workflow=self._workflow(cursor.fetchone())
      cursor.execute(f"SELECT {_VAL_COLUMNS} FROM migration_validation_runs WHERE run_id=%s",(replay[1],));stored=self._validation_run(cursor.fetchone())
      cursor.execute(f"SELECT {_ART_COLUMNS} FROM migration_workflow_artifacts WHERE workflow_id=%s AND artifact_version=%s",(workflow_id,stored.validation_preview_artifact_version));return workflow,stored,self._artifact(cursor.fetchone()),False
     cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s FOR UPDATE",(workflow_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     old=self._workflow(row)
     if old.version!=expected_version:raise WorkflowConflictError()
     if old.status is not MigrationWorkflowStatus.EXECUTED:raise InvalidWorkflowTransitionError()
     if artifact.artifact_version!=old.latest_artifact_version+1 or run.validation_preview_artifact_version!=artifact.artifact_version:raise WorkflowConflictError()
     cursor.execute("SELECT 1 FROM migration_validation_runs WHERE workflow_id=%s AND status IN ('CLAIMED','RUNNING','SUCCEEDED','REVIEW_REQUIRED','OUTCOME_UNCERTAIN') LIMIT 1",(workflow_id,))
     if cursor.fetchone() is not None:raise WorkflowValidationAlreadyInProgressError()
     cursor.execute("INSERT INTO migration_workflow_artifacts(artifact_id,workflow_id,artifact_type,artifact_version,schema_version,payload,payload_sha256,created_at) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s)",(artifact.artifact_id,workflow_id,artifact.artifact_type.value,artifact.artifact_version,artifact.schema_version,artifact.payload.decode("utf-8"),artifact.payload_sha256,artifact.created_at))
     cursor.execute("INSERT INTO migration_validation_runs(run_id,workflow_id,execution_attempt_id,execution_evidence_artifact_version,approved_mapping_artifact_version,validation_preview_artifact_version,source_profile_id,target_profile_id,validation_fingerprint,status,timeout_seconds,claimed_at,actor_type,idempotency_key,actor_reference) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(run.run_id,workflow_id,run.execution_attempt_id,run.execution_evidence_artifact_version,run.approved_mapping_artifact_version,run.validation_preview_artifact_version,run.source_profile_id,run.target_profile_id,run.validation_fingerprint,run.status.value,run.timeout_seconds,run.claimed_at,run.actor_type.value,run.idempotency_key,run.actor_reference))
     version=old.version+1;cursor.execute("UPDATE migration_workflows SET status=%s,version=%s,updated_at=%s,latest_artifact_version=%s WHERE workflow_id=%s AND version=%s",(MigrationWorkflowStatus.VALIDATING.value,version,run.claimed_at,artifact.artifact_version,workflow_id,expected_version))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     self._insert_event(cursor,artifact_event,version);self._insert_event(cursor,transition_event,version);self._insert_idem(cursor,scope,idempotency_key,"VALIDATE_WORKFLOW",request_hash,workflow_id,run.run_id,run.claimed_at)
     result=MigrationWorkflow(workflow_id=old.workflow_id,display_name=old.display_name,source_profile_id=old.source_profile_id,target_profile_id=old.target_profile_id,source_relation=old.source_relation,target_relation=old.target_relation,status=MigrationWorkflowStatus.VALIDATING,version=version,created_at=old.created_at,updated_at=run.claimed_at,latest_artifact_version=artifact.artifact_version,last_error_code=old.last_error_code,warnings=old.warnings)
     return result,run,artifact,True
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def mark_validation_running(self,run_id,running_at):
  """Let exactly one claimant advance a validation run to running."""

  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     cursor.execute(f"SELECT {_VAL_COLUMNS} FROM migration_validation_runs WHERE run_id=%s FOR UPDATE",(run_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     run=self._validation_run(row)
     if run.status is not WorkflowValidationRunStatus.CLAIMED:return run,False
     cursor.execute("UPDATE migration_validation_runs SET status='RUNNING',running_at=%s WHERE run_id=%s AND status='CLAIMED'",(running_at,run_id))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     cursor.execute(f"SELECT {_VAL_COLUMNS} FROM migration_validation_runs WHERE run_id=%s",(run_id,));return self._validation_run(cursor.fetchone()),True
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def complete_validation_run(self,workflow_id,expected_version,run_id,report,artifact,artifact_event,new_status,transition_event,*,completed_at,failure_category):
  """Commit validation outcome, optional evidence, state, and audit atomically."""

  if (artifact is None)!=(report is None) or (artifact_event is None)!=(artifact is None):raise WorkflowPersistenceError()
  if artifact is not None and (artifact.workflow_id!=workflow_id or artifact.artifact_type is not WorkflowArtifactType.VALIDATION_EXECUTION_REPORT or artifact_event.artifact_id!=artifact.artifact_id):raise WorkflowPersistenceError()
  connection=self._open()
  try:
   with connection.transaction():
    with connection.cursor() as cursor:
     cursor.execute(f"SELECT {_WF_COLUMNS} FROM migration_workflows WHERE workflow_id=%s FOR UPDATE",(workflow_id,));row=cursor.fetchone()
     if row is None:raise WorkflowNotFoundError()
     old=self._workflow(row)
     if old.version!=expected_version or old.status is not MigrationWorkflowStatus.VALIDATING:raise WorkflowConflictError()
     cursor.execute(f"SELECT {_VAL_COLUMNS} FROM migration_validation_runs WHERE run_id=%s FOR UPDATE",(run_id,));run_row=cursor.fetchone()
     if run_row is None:raise WorkflowNotFoundError()
     run=self._validation_run(run_row)
     if run.status is not WorkflowValidationRunStatus.RUNNING:raise WorkflowConflictError()
     if artifact is not None:
      if artifact.artifact_version!=old.latest_artifact_version+1:raise WorkflowConflictError()
      cursor.execute("INSERT INTO migration_workflow_artifacts(artifact_id,workflow_id,artifact_type,artifact_version,schema_version,payload,payload_sha256,created_at) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s)",(artifact.artifact_id,workflow_id,artifact.artifact_type.value,artifact.artifact_version,artifact.schema_version,artifact.payload.decode("utf-8"),artifact.payload_sha256,artifact.created_at))
     terminal=WorkflowValidationRunStatus.OUTCOME_UNCERTAIN if report is None else WorkflowValidationRunStatus.SUCCEEDED if new_status is MigrationWorkflowStatus.VALIDATED else WorkflowValidationRunStatus.REVIEW_REQUIRED
     duration_ms=max(0,int((completed_at-run.running_at).total_seconds()*1000));cursor.execute("UPDATE migration_validation_runs SET status=%s,completed_at=%s,duration_ms=%s,evidence_artifact_id=%s,failure_category=%s WHERE run_id=%s AND status='RUNNING'",(terminal.value,completed_at,duration_ms,artifact.artifact_id if artifact else None,failure_category,run_id))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     version=old.version+1;latest=artifact.artifact_version if artifact else old.latest_artifact_version
     cursor.execute("UPDATE migration_workflows SET status=%s,version=%s,updated_at=%s,latest_artifact_version=%s WHERE workflow_id=%s AND version=%s",(new_status.value,version,completed_at,latest,workflow_id,expected_version))
     if cursor.rowcount!=1:raise WorkflowConflictError()
     if artifact_event is not None:self._insert_event(cursor,artifact_event,version)
     self._insert_event(cursor,transition_event,version)
     updated=WorkflowValidationRun(run_id=run.run_id,workflow_id=run.workflow_id,execution_attempt_id=run.execution_attempt_id,execution_evidence_artifact_version=run.execution_evidence_artifact_version,approved_mapping_artifact_version=run.approved_mapping_artifact_version,validation_preview_artifact_version=run.validation_preview_artifact_version,source_profile_id=run.source_profile_id,target_profile_id=run.target_profile_id,validation_fingerprint=run.validation_fingerprint,status=terminal,timeout_seconds=run.timeout_seconds,claimed_at=run.claimed_at,actor_type=run.actor_type,idempotency_key=run.idempotency_key,actor_reference=run.actor_reference,running_at=run.running_at,completed_at=completed_at,duration_ms=duration_ms,evidence_artifact_id=artifact.artifact_id if artifact else None,failure_category=failure_category)
     result=MigrationWorkflow(workflow_id=old.workflow_id,display_name=old.display_name,source_profile_id=old.source_profile_id,target_profile_id=old.target_profile_id,source_relation=old.source_relation,target_relation=old.target_relation,status=new_status,version=version,created_at=old.created_at,updated_at=completed_at,latest_artifact_version=latest,last_error_code=old.last_error_code,warnings=old.warnings)
     return result,updated,artifact
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def get_validation_run(self,run_id):
  connection=self._open()
  try:
   with connection.cursor() as cursor:
    cursor.execute(f"SELECT {_VAL_COLUMNS} FROM migration_validation_runs WHERE run_id=%s",(run_id,));row=cursor.fetchone()
    if row is None:raise WorkflowNotFoundError()
    return self._validation_run(row)
  except WorkflowError:raise
  except Exception:raise WorkflowPersistenceError() from None
  finally:self._close(connection)
 def list_audit_events(self,workflow_id,*,offset=0,limit=100):return self._list(workflow_id,offset,limit,_EVENT_COLUMNS,"migration_audit_events","sequence_number",self._event_model)
 def close(self):return None
