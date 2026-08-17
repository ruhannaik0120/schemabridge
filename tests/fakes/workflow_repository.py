"""Deterministic test fake for the workflow repository contract."""
from dataclasses import replace
from threading import RLock

from schemabridge.models.workflow import MigrationWorkflowStatus
from schemabridge.models.migration_job import ALLOWED_JOB_STAGE_TRANSITIONS,MigrationJobStage,MigrationJobStatus
from schemabridge.models.execution import MigrationExecutionAttemptStatus
from schemabridge.models.workflow_transport import WorkflowTransportAttemptStatus
from schemabridge.models.workflow_validation import WorkflowValidationRunStatus
from schemabridge.persistence.errors import InvalidWorkflowTransitionError,MigrationJobAlreadyActiveError,MigrationJobNotFoundError,MigrationJobTransitionError,WorkflowArtifactValidationError,WorkflowConflictError,WorkflowExecutionAlreadyInProgressError,WorkflowIdempotencyConflictError,WorkflowNotFoundError,WorkflowOperationUnavailableError,WorkflowPersistenceError,WorkflowTransportAlreadyInProgressError,WorkflowValidationAlreadyInProgressError


class InMemoryWorkflowRepository:
 def __init__(self):
  self._workflows={};self._artifacts={};self._events={};self._commands={};self._jobs={};self._transport_attempts={};self._attempts={};self._validation_runs={};self._lock=RLock();self.fail_audit=False;self.fail_idempotency=False
 def _replay(self,scope,key,digest):
  stored=self._commands.get((scope,key))
  if stored is None:return None
  if stored[0]!=digest:raise WorkflowIdempotencyConflictError()
  return stored[1]
 def _store(self,scope,key,digest,outcome):
  if self.fail_idempotency:raise WorkflowPersistenceError()
  self._commands[(scope,key)]=(digest,outcome)
 def _event(self,event):
  if self.fail_audit:raise WorkflowPersistenceError()
  values=self._events.setdefault(event.workflow_id,[])
  event=replace(event,sequence_number=len(values)+1);values.append(event);return event
 def create_workflow(self,workflow,event,*,idempotency_key,request_hash):
  if event.workflow_id!=workflow.workflow_id:raise WorkflowPersistenceError()
  with self._lock:
   replay=self._replay("CREATE",idempotency_key,request_hash)
   if replay is not None:return replay
   snapshot=(dict(self._workflows),{k:list(v) for k,v in self._events.items()},dict(self._commands))
   try:
    if workflow.workflow_id in self._workflows:raise WorkflowConflictError()
    self._workflows[workflow.workflow_id]=workflow;self._event(event);self._store("CREATE",idempotency_key,request_hash,workflow);return workflow
   except Exception:
    self._workflows,self._events,self._commands=snapshot;raise
 def get_workflow(self,workflow_id):
  with self._lock:
   try:return self._workflows[workflow_id]
   except KeyError:raise WorkflowNotFoundError() from None
 def create_migration_job(self,job):
  with self._lock:
   scope=f"{job.workflow_id}:MIGRATION_JOB";replay=self._replay(scope,job.idempotency_key,job.job_fingerprint)
   if replay is not None:return self._jobs[replay],False
   workflow=self.get_workflow(job.workflow_id)
   if workflow.version!=job.expected_workflow_version:raise WorkflowConflictError()
   if workflow.status is not MigrationWorkflowStatus.MAPPING_APPROVED:raise WorkflowOperationUnavailableError()
   active={MigrationJobStatus.QUEUED,MigrationJobStatus.RUNNING,MigrationJobStatus.SUCCEEDED,MigrationJobStatus.REVIEW_REQUIRED,MigrationJobStatus.RECOVERY_REQUIRED}
   if any(item.workflow_id==job.workflow_id and item.status in active for item in self._jobs.values()):raise MigrationJobAlreadyActiveError()
   snapshot=(dict(self._jobs),dict(self._commands))
   try:
    if job.job_id in self._jobs:raise WorkflowConflictError()
    self._jobs[job.job_id]=job;self._store(scope,job.idempotency_key,job.job_fingerprint,job.job_id);return job,True
   except Exception:
    self._jobs,self._commands=snapshot;raise
 def get_migration_job(self,job_id):
  with self._lock:
   try:return self._jobs[job_id]
   except KeyError:raise MigrationJobNotFoundError() from None
 def claim_next_migration_job(self,started_at):
  with self._lock:
   queued=[job for job in self._jobs.values() if job.status is MigrationJobStatus.QUEUED]
   if not queued:return None
   job=min(queued,key=lambda item:(item.queued_at,item.job_id.int))
   claimed=replace(job,status=MigrationJobStatus.RUNNING,stage=MigrationJobStage.PREPARING,started_at=started_at)
   self._jobs[job.job_id]=claimed;return claimed
 def update_migration_job_stage(self,job_id,expected_stage,new_stage):
  with self._lock:
   job=self.get_migration_job(job_id)
   valid=job.status is MigrationJobStatus.RUNNING and job.stage is expected_stage and new_stage is not MigrationJobStage.COMPLETED and new_stage in ALLOWED_JOB_STAGE_TRANSITIONS[job.stage]
   if not valid:raise MigrationJobTransitionError()
   updated=replace(job,stage=new_stage);self._jobs[job_id]=updated;return updated
 def update_migration_job_progress(self,job_id,progress,updated_at):
  with self._lock:
   job=self.get_migration_job(job_id);previous=job.batch_progress
   valid=job.status is MigrationJobStatus.RUNNING and job.stage is MigrationJobStage.STAGING and job.started_at is not None and updated_at>=job.started_at and progress.batches_completed>0 and progress.rows_read>0
   if previous is not None:
    valid=valid and job.progress_updated_at is not None and updated_at>job.progress_updated_at and progress.batches_completed>previous.batches_completed and progress.rows_read>previous.rows_read and progress.rows_written>previous.rows_written and progress.total_rows_estimate==previous.total_rows_estimate
   if not valid:raise MigrationJobTransitionError()
   try:updated=replace(job,batch_progress=progress,progress_updated_at=updated_at)
   except (TypeError,ValueError,AttributeError):raise MigrationJobTransitionError() from None
   self._jobs[job_id]=updated;return updated
 def finish_migration_job(self,job_id,expected_stage,outcome,completed_at,failure_category):
  with self._lock:
   job=self.get_migration_job(job_id)
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
   self._jobs[job_id]=updated;return updated
 def transition_status(self,workflow_id,expected_version,new_status,event,*,last_error_code,idempotency_key,request_hash):
  if event.workflow_id!=workflow_id:raise WorkflowPersistenceError()
  with self._lock:
   scope=f"{workflow_id}:STATUS";replay=self._replay(scope,idempotency_key,request_hash)
   if replay is not None:return replay
   old=self.get_workflow(workflow_id)
   if old.version!=expected_version:raise WorkflowConflictError()
   if old.status is new_status:raise InvalidWorkflowTransitionError()
   snapshot=(old,list(self._events.get(workflow_id,[])),dict(self._commands))
   try:
    result=replace(old,status=new_status,version=old.version+1,updated_at=event.occurred_at,last_error_code=last_error_code)
    self._workflows[workflow_id]=result;self._event(replace(event,workflow_version=result.version));self._store(scope,idempotency_key,request_hash,result);return result
   except Exception:
    self._workflows[workflow_id]=snapshot[0];self._events[workflow_id]=snapshot[1];self._commands=snapshot[2];raise
 def append_artifact(self,workflow_id,expected_version,artifact,event,*,idempotency_key,request_hash):
  if artifact.workflow_id!=workflow_id:raise WorkflowArtifactValidationError()
  if event.workflow_id!=workflow_id or event.artifact_id!=artifact.artifact_id or event.artifact_type is not artifact.artifact_type:raise WorkflowPersistenceError()
  with self._lock:
   scope=f"{workflow_id}:ARTIFACT";replay=self._replay(scope,idempotency_key,request_hash)
   if replay is not None:return replay
   old=self.get_workflow(workflow_id)
   if old.version!=expected_version or artifact.artifact_version!=old.latest_artifact_version+1:raise WorkflowConflictError()
   snapshot=(old,list(self._artifacts.get(workflow_id,[])),list(self._events.get(workflow_id,[])),dict(self._commands))
   try:
    if any(x.artifact_version==artifact.artifact_version for x in self._artifacts.get(workflow_id,[])):raise WorkflowConflictError()
    result=replace(old,version=old.version+1,updated_at=artifact.created_at,latest_artifact_version=artifact.artifact_version)
    self._workflows[workflow_id]=result;self._artifacts.setdefault(workflow_id,[]).append(artifact);self._event(replace(event,workflow_version=result.version));out=(result,artifact);self._store(scope,idempotency_key,request_hash,out);return out
   except Exception:
    self._workflows[workflow_id]=snapshot[0];self._artifacts[workflow_id]=snapshot[1];self._events[workflow_id]=snapshot[2];self._commands=snapshot[3];raise
 def append_artifact_operation(self,workflow_id,expected_version,artifact,artifact_event,*,new_status,transition_event,idempotency_key,request_hash):
  if artifact.workflow_id!=workflow_id:raise WorkflowArtifactValidationError()
  if artifact_event.workflow_id!=workflow_id or artifact_event.artifact_id!=artifact.artifact_id:raise WorkflowPersistenceError()
  if (new_status is None)!=(transition_event is None):raise WorkflowPersistenceError()
  if transition_event is not None and transition_event.workflow_id!=workflow_id:raise WorkflowPersistenceError()
  with self._lock:
   scope=f"{workflow_id}:ORCHESTRATION:{artifact.artifact_type.value}";replay=self._replay(scope,idempotency_key,request_hash)
   if replay is not None:return replay
   old=self.get_workflow(workflow_id)
   if old.version!=expected_version or artifact.artifact_version!=old.latest_artifact_version+1:raise WorkflowConflictError()
   snapshot=(old,list(self._artifacts.get(workflow_id,[])),list(self._events.get(workflow_id,[])),dict(self._commands))
   try:
    result=replace(old,status=new_status or old.status,version=old.version+1,updated_at=artifact.created_at,latest_artifact_version=artifact.artifact_version)
    self._workflows[workflow_id]=result;self._artifacts.setdefault(workflow_id,[]).append(artifact)
    self._event(replace(artifact_event,workflow_version=result.version))
    if transition_event is not None:self._event(replace(transition_event,workflow_version=result.version))
    out=(result,artifact);self._store(scope,idempotency_key,request_hash,out);return out
   except Exception:
    self._workflows[workflow_id]=snapshot[0];self._artifacts[workflow_id]=snapshot[1];self._events[workflow_id]=snapshot[2];self._commands=snapshot[3];raise
 def mark_failed(self,workflow_id,expected_version,event,**kwargs):return self.transition_status(workflow_id,expected_version,MigrationWorkflowStatus.FAILED,event,**kwargs)
 def cancel_workflow(self,workflow_id,expected_version,event,**kwargs):return self.transition_status(workflow_id,expected_version,MigrationWorkflowStatus.CANCELLED,event,**kwargs)
 def list_artifacts(self,workflow_id,*,offset=0,limit=100):
  self._page(offset,limit);self.get_workflow(workflow_id);return tuple(self._artifacts.get(workflow_id,[])[offset:offset+limit])
 def get_artifact(self,workflow_id,artifact_version):
  self.get_workflow(workflow_id)
  return next((item for item in self._artifacts.get(workflow_id,[]) if item.artifact_version==artifact_version),None)
 def get_artifact_by_id(self,workflow_id,artifact_id):
  self.get_workflow(workflow_id)
  return next((item for item in self._artifacts.get(workflow_id,[]) if item.artifact_id==artifact_id),None)
 def get_latest_artifact(self,workflow_id,artifact_type):
  self.get_workflow(workflow_id);matches=[item for item in self._artifacts.get(workflow_id,[]) if item.artifact_type is artifact_type]
  return matches[-1] if matches else None
 def get_transport_attempt_by_command(self,workflow_id,idempotency_key,request_hash):
  with self._lock:
   attempt_id=self._replay(f"{workflow_id}:TRANSPORT",idempotency_key,request_hash)
   return self._transport_attempts[attempt_id] if attempt_id is not None else None
 def claim_transport_attempt(self,workflow_id,expected_version,attempt,event,*,idempotency_key,request_hash):
  with self._lock:
   scope=f"{workflow_id}:TRANSPORT";replay=self._replay(scope,idempotency_key,request_hash)
   if replay is not None:return self.get_workflow(workflow_id),self._transport_attempts[replay],False
   old=self.get_workflow(workflow_id)
   if old.version!=expected_version:raise WorkflowConflictError()
   if old.status is not MigrationWorkflowStatus.MAPPING_APPROVED:raise InvalidWorkflowTransitionError()
   if any(item.workflow_id==workflow_id and item.status in {WorkflowTransportAttemptStatus.CLAIMED,WorkflowTransportAttemptStatus.RUNNING,WorkflowTransportAttemptStatus.SUCCEEDED,WorkflowTransportAttemptStatus.OUTCOME_UNCERTAIN} for item in self._transport_attempts.values()):raise WorkflowTransportAlreadyInProgressError()
   snapshot=(old,dict(self._transport_attempts),list(self._events.get(workflow_id,[])),dict(self._commands))
   try:
    result=replace(old,status=MigrationWorkflowStatus.STAGING,version=old.version+1,updated_at=attempt.claimed_at)
    self._workflows[workflow_id]=result;self._transport_attempts[attempt.attempt_id]=attempt;self._event(replace(event,workflow_version=result.version));self._store(scope,idempotency_key,request_hash,attempt.attempt_id)
    return result,attempt,True
   except Exception:
    self._workflows[workflow_id]=snapshot[0];self._transport_attempts=snapshot[1];self._events[workflow_id]=snapshot[2];self._commands=snapshot[3];raise
 def mark_transport_running(self,attempt_id,running_at):
  with self._lock:
   attempt=self._transport_attempts[attempt_id]
   if attempt.status is not WorkflowTransportAttemptStatus.CLAIMED:return attempt,False
   result=replace(attempt,status=WorkflowTransportAttemptStatus.RUNNING,running_at=running_at);self._transport_attempts[attempt_id]=result;return result,True
 def complete_transport_attempt(self,workflow_id,expected_version,attempt_id,evidence,artifact,artifact_event,new_status,transition_event,*,completed_at,failure_category):
  with self._lock:
   old=self.get_workflow(workflow_id);attempt=self._transport_attempts[attempt_id]
   if old.version!=expected_version or old.status is not MigrationWorkflowStatus.STAGING or attempt.status is not WorkflowTransportAttemptStatus.RUNNING:raise WorkflowConflictError()
   if (evidence is None)!=(artifact is None) or (artifact_event is None)!=(artifact is None):raise WorkflowPersistenceError()
   if artifact is not None and artifact.artifact_version!=old.latest_artifact_version+1:raise WorkflowConflictError()
   snapshot=(old,dict(self._transport_attempts),list(self._artifacts.get(workflow_id,[])),list(self._events.get(workflow_id,[])))
   try:
    terminal=WorkflowTransportAttemptStatus.SUCCEEDED if evidence is not None else WorkflowTransportAttemptStatus.FAILED_CLEANED_UP if new_status is MigrationWorkflowStatus.MAPPING_APPROVED else WorkflowTransportAttemptStatus.OUTCOME_UNCERTAIN
    updated_attempt=replace(attempt,status=terminal,completed_at=completed_at,evidence_artifact_id=artifact.artifact_id if artifact else None,failure_category=failure_category)
    latest=artifact.artifact_version if artifact else old.latest_artifact_version
    result=replace(old,status=new_status,version=old.version+1,updated_at=completed_at,latest_artifact_version=latest)
    self._transport_attempts[attempt_id]=updated_attempt;self._workflows[workflow_id]=result
    if artifact is not None:self._artifacts.setdefault(workflow_id,[]).append(artifact);self._event(replace(artifact_event,workflow_version=result.version))
    self._event(replace(transition_event,workflow_version=result.version));return result,updated_attempt,artifact
   except Exception:
    self._workflows[workflow_id]=snapshot[0];self._transport_attempts=snapshot[1];self._artifacts[workflow_id]=snapshot[2];self._events[workflow_id]=snapshot[3];raise
 def get_transport_attempt(self,attempt_id):
  try:return self._transport_attempts[attempt_id]
  except KeyError:raise WorkflowNotFoundError() from None
 def get_execution_attempt_by_command(self,workflow_id,idempotency_key,request_hash):
  with self._lock:
   attempt_id=self._replay(f"{workflow_id}:EXECUTION",idempotency_key,request_hash)
   return self._attempts[attempt_id] if attempt_id is not None else None
 def claim_execution_attempt(self,workflow_id,expected_version,attempt,event,*,idempotency_key,request_hash):
  with self._lock:
   scope=f"{workflow_id}:EXECUTION";replay=self._replay(scope,idempotency_key,request_hash)
   if replay is not None:return self.get_workflow(workflow_id),self._attempts[replay],False
   old=self.get_workflow(workflow_id)
   if old.version!=expected_version:raise WorkflowConflictError()
   if any(item.workflow_id==workflow_id and item.status in {MigrationExecutionAttemptStatus.CLAIMED,MigrationExecutionAttemptStatus.RUNNING,MigrationExecutionAttemptStatus.SUCCEEDED,MigrationExecutionAttemptStatus.OUTCOME_UNCERTAIN} for item in self._attempts.values()):raise WorkflowExecutionAlreadyInProgressError()
   snapshot=(old,dict(self._attempts),list(self._events.get(workflow_id,[])),dict(self._commands))
   try:
    result=replace(old,status=MigrationWorkflowStatus.EXECUTING,version=old.version+1,updated_at=attempt.claimed_at)
    self._workflows[workflow_id]=result;self._attempts[attempt.attempt_id]=attempt;self._event(replace(event,workflow_version=result.version));self._store(scope,idempotency_key,request_hash,attempt.attempt_id)
    return result,attempt,True
   except Exception:
    self._workflows[workflow_id]=snapshot[0];self._attempts=snapshot[1];self._events[workflow_id]=snapshot[2];self._commands=snapshot[3];raise
 def mark_execution_running(self,attempt_id,running_at):
  with self._lock:
   attempt=self._attempts[attempt_id]
   if attempt.status is not MigrationExecutionAttemptStatus.CLAIMED:return attempt,False
   result=replace(attempt,status=MigrationExecutionAttemptStatus.RUNNING,running_at=running_at)
   self._attempts[attempt_id]=result;return result,True
 def complete_execution_attempt(self,workflow_id,expected_version,attempt_id,evidence,artifact,artifact_event,new_status,transition_event):
  with self._lock:
   old=self.get_workflow(workflow_id);attempt=self._attempts[attempt_id]
   if old.version!=expected_version or attempt.status is not MigrationExecutionAttemptStatus.RUNNING or artifact.artifact_version!=old.latest_artifact_version+1:raise WorkflowConflictError()
   snapshot=(old,dict(self._attempts),list(self._artifacts.get(workflow_id,[])),list(self._events.get(workflow_id,[])))
   try:
    updated_attempt=replace(attempt,status=evidence.status,completed_at=evidence.completed_at,evidence_artifact_id=artifact.artifact_id,failure_category=evidence.failure_category)
    result=replace(old,status=new_status,version=old.version+1,updated_at=evidence.completed_at,latest_artifact_version=artifact.artifact_version)
    self._attempts[attempt_id]=updated_attempt;self._workflows[workflow_id]=result;self._artifacts.setdefault(workflow_id,[]).append(artifact)
    self._event(replace(artifact_event,workflow_version=result.version));self._event(replace(transition_event,workflow_version=result.version))
    return result,updated_attempt,artifact
   except Exception:
    self._workflows[workflow_id]=snapshot[0];self._attempts=snapshot[1];self._artifacts[workflow_id]=snapshot[2];self._events[workflow_id]=snapshot[3];raise
 def get_execution_attempt(self,attempt_id):
  try:return self._attempts[attempt_id]
  except KeyError:raise WorkflowNotFoundError() from None
 def get_validation_run_by_command(self,workflow_id,idempotency_key,request_hash):
  with self._lock:
   run_id=self._replay(f"{workflow_id}:VALIDATION",idempotency_key,request_hash)
   return self._validation_runs[run_id] if run_id is not None else None
 def claim_validation_run(self,workflow_id,expected_version,run,artifact,artifact_event,transition_event,*,idempotency_key,request_hash):
  with self._lock:
   scope=f"{workflow_id}:VALIDATION";replay=self._replay(scope,idempotency_key,request_hash)
   if replay is not None:return self.get_workflow(workflow_id),self._validation_runs[replay],self.get_artifact(workflow_id,self._validation_runs[replay].validation_preview_artifact_version),False
   old=self.get_workflow(workflow_id)
   if old.version!=expected_version:raise WorkflowConflictError()
   if old.status is not MigrationWorkflowStatus.EXECUTED:raise InvalidWorkflowTransitionError()
   if artifact.artifact_version!=old.latest_artifact_version+1 or run.validation_preview_artifact_version!=artifact.artifact_version:raise WorkflowConflictError()
   if any(item.workflow_id==workflow_id and item.status in {WorkflowValidationRunStatus.CLAIMED,WorkflowValidationRunStatus.RUNNING,WorkflowValidationRunStatus.SUCCEEDED,WorkflowValidationRunStatus.REVIEW_REQUIRED,WorkflowValidationRunStatus.OUTCOME_UNCERTAIN} for item in self._validation_runs.values()):raise WorkflowValidationAlreadyInProgressError()
   snapshot=(old,dict(self._validation_runs),list(self._artifacts.get(workflow_id,[])),list(self._events.get(workflow_id,[])),dict(self._commands))
   try:
    result=replace(old,status=MigrationWorkflowStatus.VALIDATING,version=old.version+1,updated_at=run.claimed_at,latest_artifact_version=artifact.artifact_version)
    self._workflows[workflow_id]=result;self._validation_runs[run.run_id]=run;self._artifacts.setdefault(workflow_id,[]).append(artifact)
    self._event(replace(artifact_event,workflow_version=result.version));self._event(replace(transition_event,workflow_version=result.version));self._store(scope,idempotency_key,request_hash,run.run_id)
    return result,run,artifact,True
   except Exception:
    self._workflows[workflow_id]=snapshot[0];self._validation_runs=snapshot[1];self._artifacts[workflow_id]=snapshot[2];self._events[workflow_id]=snapshot[3];self._commands=snapshot[4];raise
 def mark_validation_running(self,run_id,running_at):
  with self._lock:
   run=self._validation_runs[run_id]
   if run.status is not WorkflowValidationRunStatus.CLAIMED:return run,False
   result=replace(run,status=WorkflowValidationRunStatus.RUNNING,running_at=running_at);self._validation_runs[run_id]=result;return result,True
 def complete_validation_run(self,workflow_id,expected_version,run_id,report,artifact,artifact_event,new_status,transition_event,*,completed_at,failure_category):
  with self._lock:
   old=self.get_workflow(workflow_id);run=self._validation_runs[run_id]
   if old.version!=expected_version or old.status is not MigrationWorkflowStatus.VALIDATING or run.status is not WorkflowValidationRunStatus.RUNNING:raise WorkflowConflictError()
   if (artifact is None)!=(report is None) or (artifact_event is None)!=(artifact is None):raise WorkflowPersistenceError()
   if artifact is not None and artifact.artifact_version!=old.latest_artifact_version+1:raise WorkflowConflictError()
   snapshot=(old,dict(self._validation_runs),list(self._artifacts.get(workflow_id,[])),list(self._events.get(workflow_id,[])))
   try:
    status=WorkflowValidationRunStatus.OUTCOME_UNCERTAIN if report is None else WorkflowValidationRunStatus.SUCCEEDED if new_status is MigrationWorkflowStatus.VALIDATED else WorkflowValidationRunStatus.REVIEW_REQUIRED
    duration_ms=max(0,int((completed_at-run.running_at).total_seconds()*1000));updated_run=replace(run,status=status,completed_at=completed_at,duration_ms=duration_ms,evidence_artifact_id=artifact.artifact_id if artifact else None,failure_category=failure_category)
    result=replace(old,status=new_status,version=old.version+1,updated_at=completed_at,latest_artifact_version=artifact.artifact_version if artifact else old.latest_artifact_version)
    self._validation_runs[run_id]=updated_run;self._workflows[workflow_id]=result
    if artifact is not None:self._artifacts.setdefault(workflow_id,[]).append(artifact);self._event(replace(artifact_event,workflow_version=result.version))
    self._event(replace(transition_event,workflow_version=result.version));return result,updated_run,artifact
   except Exception:
    self._workflows[workflow_id]=snapshot[0];self._validation_runs=snapshot[1];self._artifacts[workflow_id]=snapshot[2];self._events[workflow_id]=snapshot[3];raise
 def get_validation_run(self,run_id):
  try:return self._validation_runs[run_id]
  except KeyError:raise WorkflowNotFoundError() from None
 def list_audit_events(self,workflow_id,*,offset=0,limit=100):
  self._page(offset,limit);self.get_workflow(workflow_id);return tuple(self._events.get(workflow_id,[])[offset:offset+limit])
 @staticmethod
 def _page(offset,limit):
  if isinstance(offset,bool) or not isinstance(offset,int) or offset<0 or isinstance(limit,bool) or not isinstance(limit,int) or not 1<=limit<=500:raise ValueError("Pagination is invalid.")
 def close(self):return None
