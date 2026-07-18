"""Deterministic test fake for the workflow repository contract."""
from dataclasses import replace
from threading import RLock

from models.workflow import MigrationWorkflowStatus
from persistence.errors import InvalidWorkflowTransitionError,WorkflowArtifactValidationError,WorkflowConflictError,WorkflowIdempotencyConflictError,WorkflowNotFoundError,WorkflowPersistenceError


class InMemoryWorkflowRepository:
 def __init__(self):
  self._workflows={};self._artifacts={};self._events={};self._commands={};self._lock=RLock();self.fail_audit=False;self.fail_idempotency=False
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
 def mark_failed(self,workflow_id,expected_version,event,**kwargs):return self.transition_status(workflow_id,expected_version,MigrationWorkflowStatus.FAILED,event,**kwargs)
 def cancel_workflow(self,workflow_id,expected_version,event,**kwargs):return self.transition_status(workflow_id,expected_version,MigrationWorkflowStatus.CANCELLED,event,**kwargs)
 def list_artifacts(self,workflow_id,*,offset=0,limit=100):
  self._page(offset,limit);self.get_workflow(workflow_id);return tuple(self._artifacts.get(workflow_id,[])[offset:offset+limit])
 def list_audit_events(self,workflow_id,*,offset=0,limit=100):
  self._page(offset,limit);self.get_workflow(workflow_id);return tuple(self._events.get(workflow_id,[])[offset:offset+limit])
 @staticmethod
 def _page(offset,limit):
  if isinstance(offset,bool) or not isinstance(offset,int) or offset<0 or isinstance(limit,bool) or not isinstance(limit,int) or not 1<=limit<=500:raise ValueError("Pagination is invalid.")
 def close(self):return None
