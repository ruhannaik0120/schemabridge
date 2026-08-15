"""Apply workflow policy before invoking the durable repository contract.

This service validates transitions and artifact ownership, creates canonical
payload hashes and audit events, and supplies repository operations with
optimistic versions and idempotency hashes.  It owns domain policy; the concrete
repository owns transaction and SQL mechanics.
"""
from __future__ import annotations
from datetime import datetime,timezone
import re
from typing import Any,Callable
from uuid import UUID,uuid4

from schemabridge.models.discovery import TableMetadata
from schemabridge.models.execution import MigrationExecutionAttempt,MigrationExecutionEvidence
from schemabridge.models.mapping import ApprovedTableMappingPlan,GeneratedTransformationSql,TableMappingPlan
from schemabridge.models.validation import GeneratedValidationSql,MigrationValidationExecutionReport
from schemabridge.models.workflow_validation import WorkflowValidationRun
from schemabridge.models.workflow_transport import WorkflowStagingCleanupEvidence,WorkflowTransportAttempt,WorkflowTransportEvidence
from schemabridge.models.workflow import *
from schemabridge.persistence.errors import InvalidWorkflowTransitionError,WorkflowArtifactValidationError,WorkflowOperationUnavailableError
from schemabridge.persistence.repository import WorkflowRepository
from schemabridge.persistence.serialization import request_hash,serialize_artifact

def _now():return datetime.now(timezone.utc)
class WorkflowPersistenceService:
 """Turn typed workflow commands into atomic repository operations."""

 def __init__(self,repository:WorkflowRepository,*,clock:Callable[[],datetime]=_now,uuid_factory:Callable[[],UUID]=uuid4):
  """Bind the repository and injectable identity/time sources."""

  self.repository=repository;self.clock=clock;self.uuid_factory=uuid_factory
 def _context(self,actor_type,actor_reference,request_id,idempotency_key):
  if not isinstance(actor_type,AuditActorType):raise TypeError("actor_type is invalid.")
  if not isinstance(idempotency_key,str) or not idempotency_key.strip() or len(idempotency_key)>128:raise ValueError("idempotency_key is invalid.")
  return actor_type,actor_reference,request_id,idempotency_key
 @staticmethod
 def _expected(value):
  if isinstance(value,bool) or not isinstance(value,int) or value<1:raise ValueError("expected_version must be a positive integer.")
 def create_workflow(self,*,display_name,source_profile_id,target_profile_id,source_relation,target_relation,idempotency_key,actor_type=AuditActorType.SYSTEM,actor_reference=None,request_id=None):
  """Create a version-one draft and its immutable creation audit event."""

  self._context(actor_type,actor_reference,request_id,idempotency_key);now=self.clock();workflow=MigrationWorkflow(workflow_id=self.uuid_factory(),display_name=display_name,source_profile_id=source_profile_id,target_profile_id=target_profile_id,source_relation=source_relation,target_relation=target_relation,status=MigrationWorkflowStatus.DRAFT,version=1,created_at=now,updated_at=now)
  digest=request_hash("CREATE_WORKFLOW",{"display_name":display_name,"source_profile_id":source_profile_id,"target_profile_id":target_profile_id,"source_relation":source_relation,"target_relation":target_relation})
  event=self._event(workflow,MigrationAuditEventType.WORKFLOW_CREATED,None,MigrationWorkflowStatus.DRAFT,actor_type,actor_reference,request_id,idempotency_key,now)
  return self.repository.create_workflow(workflow,event,idempotency_key=idempotency_key,request_hash=digest)
 def get_workflow(self,workflow_id):return self.repository.get_workflow(workflow_id)
 def list_artifacts(self,workflow_id,*,offset=0,limit=100):return self.repository.list_artifacts(workflow_id,offset=offset,limit=limit)
 def get_artifact(self,workflow_id,artifact_version):return self.repository.get_artifact(workflow_id,artifact_version)
 def get_artifact_by_id(self,workflow_id,artifact_id):return self.repository.get_artifact_by_id(workflow_id,artifact_id)
 def get_latest_artifact(self,workflow_id,artifact_type):return self.repository.get_latest_artifact(workflow_id,artifact_type)
 def get_transport_attempt_by_command(self,workflow_id,idempotency_key,command_hash):return self.repository.get_transport_attempt_by_command(workflow_id,idempotency_key,command_hash)
 def get_transport_attempt(self,attempt_id):return self.repository.get_transport_attempt(attempt_id)
 def get_execution_attempt_by_command(self,workflow_id,idempotency_key,command_hash):return self.repository.get_execution_attempt_by_command(workflow_id,idempotency_key,command_hash)
 def get_execution_attempt(self,attempt_id):return self.repository.get_execution_attempt(attempt_id)
 def get_validation_run_by_command(self,workflow_id,idempotency_key,command_hash):return self.repository.get_validation_run_by_command(workflow_id,idempotency_key,command_hash)
 def get_validation_run(self,run_id):return self.repository.get_validation_run(run_id)
 def list_audit_events(self,workflow_id,*,offset=0,limit=100):return self.repository.list_audit_events(workflow_id,offset=offset,limit=limit)
 def transition_status(self,workflow_id,*,expected_version,new_status,idempotency_key,reason_code=None,actor_type=AuditActorType.SYSTEM,actor_reference=None,request_id=None):
  """Validate and persist a caller-allowed administrative transition."""

  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version);current=self.repository.get_workflow(workflow_id)
  digest=request_hash("TRANSITION_STATUS",{"workflow_id":workflow_id,"expected_version":expected_version,"new_status":new_status,"reason_code":reason_code})
  # A stale expected version is resolved by the repository: it may be an exact
  # idempotent replay, which must remain valid after later workflow mutations.
  managed={MigrationWorkflowStatus.STAGING,MigrationWorkflowStatus.STAGED,MigrationWorkflowStatus.STAGING_RECOVERY_REQUIRED,MigrationWorkflowStatus.EXECUTION_READY,MigrationWorkflowStatus.EXECUTING,MigrationWorkflowStatus.EXECUTED,MigrationWorkflowStatus.EXECUTION_RECOVERY_REQUIRED,MigrationWorkflowStatus.VALIDATION_READY,MigrationWorkflowStatus.VALIDATING,MigrationWorkflowStatus.VALIDATED,MigrationWorkflowStatus.VALIDATION_REVIEW_REQUIRED,MigrationWorkflowStatus.VALIDATION_RECOVERY_REQUIRED}
  if current.version==expected_version and (new_status in managed or current.status in {MigrationWorkflowStatus.STAGING,MigrationWorkflowStatus.EXECUTING,MigrationWorkflowStatus.VALIDATING}):raise InvalidWorkflowTransitionError()
  if current.version==expected_version and new_status is not current.status and new_status not in ALLOWED_TRANSITIONS[current.status]:raise InvalidWorkflowTransitionError()
  event_type=MigrationAuditEventType.WORKFLOW_FAILED if new_status is MigrationWorkflowStatus.FAILED else MigrationAuditEventType.WORKFLOW_CANCELLED if new_status is MigrationWorkflowStatus.CANCELLED else MigrationAuditEventType.STATUS_CHANGED
  now=self.clock();event=self._event(current,event_type,current.status,new_status,actor_type,actor_reference,request_id,idempotency_key,now,AuditMetadata(reason_code=reason_code))
  kwargs=dict(last_error_code=reason_code if new_status is MigrationWorkflowStatus.FAILED else None,idempotency_key=idempotency_key,request_hash=digest)
  if new_status is MigrationWorkflowStatus.FAILED:return self.repository.mark_failed(workflow_id,expected_version,event,**kwargs)
  if new_status is MigrationWorkflowStatus.CANCELLED:return self.repository.cancel_workflow(workflow_id,expected_version,event,**kwargs)
  return self.repository.transition_status(workflow_id,expected_version,new_status,event,**kwargs)
 def mark_failed(self,workflow_id,**kwargs):return self.transition_status(workflow_id,new_status=MigrationWorkflowStatus.FAILED,**kwargs)
 def cancel_workflow(self,workflow_id,**kwargs):return self.transition_status(workflow_id,new_status=MigrationWorkflowStatus.CANCELLED,**kwargs)
 def _append(self,workflow_id,expected_version,kind,payload,idempotency_key,actor_type,actor_reference,request_id):
  """Validate, serialize, hash, and append one typed immutable artifact."""

  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version);workflow=self.repository.get_workflow(workflow_id);self._validate_identity(workflow,kind,payload)
  if workflow.version==expected_version and workflow.status in {MigrationWorkflowStatus.EXECUTING,MigrationWorkflowStatus.VALIDATING}:raise WorkflowOperationUnavailableError()
  try:data,digest=serialize_artifact(kind,payload)
  except (TypeError,ValueError):raise WorkflowArtifactValidationError() from None
  command_hash=request_hash("APPEND_ARTIFACT",{"workflow_id":workflow_id,"expected_version":expected_version,"artifact_type":kind,"payload_sha256":digest})
  now=self.clock();artifact=WorkflowArtifact(artifact_id=self.uuid_factory(),workflow_id=workflow_id,artifact_type=kind,artifact_version=workflow.latest_artifact_version+1,schema_version=1,payload=data,payload_sha256=digest,created_at=now)
  event=self._event(workflow,MigrationAuditEventType.ARTIFACT_APPENDED,workflow.status,workflow.status,actor_type,actor_reference,request_id,idempotency_key,now,artifact=artifact)
  return self.repository.append_artifact(workflow_id,expected_version,artifact,event,idempotency_key=idempotency_key,request_hash=command_hash)
 def append_source_discovery(self,workflow_id,expected_version,payload:TableMetadata,**context):return self._append(workflow_id,expected_version,WorkflowArtifactType.SOURCE_DISCOVERY,payload,**context)
 def append_target_discovery(self,workflow_id,expected_version,payload:TableMetadata,**context):return self._append(workflow_id,expected_version,WorkflowArtifactType.TARGET_DISCOVERY,payload,**context)
 def append_mapping_plan(self,workflow_id,expected_version,payload:TableMappingPlan,**context):return self._append(workflow_id,expected_version,WorkflowArtifactType.MAPPING_PLAN,payload,**context)
 def append_approved_mapping_plan(self,workflow_id,expected_version,payload:ApprovedTableMappingPlan,**context):return self._append(workflow_id,expected_version,WorkflowArtifactType.APPROVED_MAPPING_PLAN,payload,**context)
 def append_transformation_preview(self,workflow_id,expected_version,payload:GeneratedTransformationSql,**context):return self._append(workflow_id,expected_version,WorkflowArtifactType.TRANSFORMATION_PREVIEW,payload,**context)
 def append_validation_preview(self,workflow_id,expected_version,payload:tuple[GeneratedValidationSql,GeneratedValidationSql],**context):return self._append(workflow_id,expected_version,WorkflowArtifactType.VALIDATION_PREVIEW,payload,**context)
 def append_validation_execution_report(self,workflow_id,expected_version,payload:MigrationValidationExecutionReport,**context):return self._append(workflow_id,expected_version,WorkflowArtifactType.VALIDATION_EXECUTION_REPORT,payload,**context)
 def _record_operation(self,workflow_id,expected_version,kind,payload,new_status,command_hash,idempotency_key,actor_type,actor_reference,request_id):
  """Build evidence and an optional transition for one orchestrated command."""

  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version)
  if not isinstance(command_hash,str) or re.fullmatch(r"[0-9a-f]{64}",command_hash) is None:raise ValueError("command_hash is invalid.")
  workflow=self.repository.get_workflow(workflow_id);self._validate_identity(workflow,kind,payload)
  if workflow.version==expected_version and new_status is not None and new_status not in ALLOWED_TRANSITIONS[workflow.status]:raise InvalidWorkflowTransitionError()
  try:data,digest=serialize_artifact(kind,payload)
  except (TypeError,ValueError):raise WorkflowArtifactValidationError() from None
  now=self.clock();artifact=WorkflowArtifact(artifact_id=self.uuid_factory(),workflow_id=workflow_id,artifact_type=kind,artifact_version=workflow.latest_artifact_version+1,schema_version=1,payload=data,payload_sha256=digest,created_at=now)
  artifact_event=self._event(workflow,MigrationAuditEventType.ARTIFACT_APPENDED,workflow.status,workflow.status,actor_type,actor_reference,request_id,idempotency_key,now,artifact=artifact)
  transition_event=None
  if new_status is not None:transition_event=self._event(workflow,MigrationAuditEventType.STATUS_CHANGED,workflow.status,new_status,actor_type,actor_reference,request_id,idempotency_key,now)
  return self.repository.append_artifact_operation(workflow_id,expected_version,artifact,artifact_event,new_status=new_status,transition_event=transition_event,idempotency_key=idempotency_key,request_hash=command_hash)
 def record_source_discovery(self,workflow_id,expected_version,payload:TableMetadata,*,advance:bool,command_hash:str,**context):return self._record_operation(workflow_id,expected_version,WorkflowArtifactType.SOURCE_DISCOVERY,payload,MigrationWorkflowStatus.DISCOVERED if advance else None,command_hash,**context)
 def record_target_discovery(self,workflow_id,expected_version,payload:TableMetadata,*,advance:bool,command_hash:str,**context):return self._record_operation(workflow_id,expected_version,WorkflowArtifactType.TARGET_DISCOVERY,payload,MigrationWorkflowStatus.DISCOVERED if advance else None,command_hash,**context)
 def record_mapping_proposal(self,workflow_id,expected_version,payload:TableMappingPlan,*,command_hash:str,**context):return self._record_operation(workflow_id,expected_version,WorkflowArtifactType.MAPPING_PLAN,payload,MigrationWorkflowStatus.MAPPING_PROPOSED,command_hash,**context)
 def record_approved_mapping(self,workflow_id,expected_version,payload:ApprovedTableMappingPlan,*,command_hash:str,**context):return self._record_operation(workflow_id,expected_version,WorkflowArtifactType.APPROVED_MAPPING_PLAN,payload,MigrationWorkflowStatus.MAPPING_APPROVED,command_hash,**context)
 def record_transformation_preview(self,workflow_id,expected_version,payload:GeneratedTransformationSql,*,command_hash:str,**context):return self._record_operation(workflow_id,expected_version,WorkflowArtifactType.TRANSFORMATION_PREVIEW,payload,MigrationWorkflowStatus.EXECUTION_READY,command_hash,**context)
 def record_staging_cleanup(self,workflow_id,expected_version,payload:WorkflowStagingCleanupEvidence,*,command_hash:str,**context):return self._record_operation(workflow_id,expected_version,WorkflowArtifactType.STAGING_CLEANUP_EVIDENCE,payload,None,command_hash,**context)
 def claim_transport_attempt(self,workflow_id,expected_version,attempt:WorkflowTransportAttempt,*,command_hash,idempotency_key,actor_type,actor_reference,request_id):
  """Validate staging eligibility and delegate the durable pre-remote claim."""

  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version)
  if not isinstance(command_hash,str) or re.fullmatch(r"[0-9a-f]{64}",command_hash) is None:raise ValueError("command_hash is invalid.")
  workflow=self.repository.get_workflow(workflow_id)
  if workflow.version==expected_version and workflow.status is not MigrationWorkflowStatus.MAPPING_APPROVED:raise InvalidWorkflowTransitionError()
  event=self._event(workflow,MigrationAuditEventType.STATUS_CHANGED,MigrationWorkflowStatus.MAPPING_APPROVED,MigrationWorkflowStatus.STAGING,actor_type,actor_reference,request_id,idempotency_key,attempt.claimed_at)
  return self.repository.claim_transport_attempt(workflow_id,expected_version,attempt,event,idempotency_key=idempotency_key,request_hash=command_hash)
 def mark_transport_running(self,attempt_id,running_at):return self.repository.mark_transport_running(attempt_id,running_at)
 def complete_transport_attempt(self,workflow_id,expected_version,attempt_id,evidence:WorkflowTransportEvidence|None,new_status,*,completed_at,failure_category,idempotency_key,actor_type,actor_reference,request_id):
  """Prepare optional staging evidence and atomically record the outcome."""

  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version);workflow=self.repository.get_workflow(workflow_id)
  if workflow.version==expected_version and new_status not in ALLOWED_TRANSITIONS[workflow.status]:raise InvalidWorkflowTransitionError()
  artifact=None;artifact_event=None
  if evidence is not None:
   self._validate_identity(workflow,WorkflowArtifactType.STAGING_LOAD_EVIDENCE,evidence)
   try:data,digest=serialize_artifact(WorkflowArtifactType.STAGING_LOAD_EVIDENCE,evidence)
   except (TypeError,ValueError):raise WorkflowArtifactValidationError() from None
   artifact=WorkflowArtifact(artifact_id=self.uuid_factory(),workflow_id=workflow_id,artifact_type=WorkflowArtifactType.STAGING_LOAD_EVIDENCE,artifact_version=workflow.latest_artifact_version+1,schema_version=1,payload=data,payload_sha256=digest,created_at=completed_at)
   artifact_event=self._event(workflow,MigrationAuditEventType.ARTIFACT_APPENDED,workflow.status,workflow.status,actor_type,actor_reference,request_id,idempotency_key,completed_at,artifact=artifact)
  transition_event=self._event(workflow,MigrationAuditEventType.STATUS_CHANGED,workflow.status,new_status,actor_type,actor_reference,request_id,idempotency_key,completed_at)
  return self.repository.complete_transport_attempt(workflow_id,expected_version,attempt_id,evidence,artifact,artifact_event,new_status,transition_event,completed_at=completed_at,failure_category=failure_category)
 def claim_execution_attempt(self,workflow_id,expected_version,attempt:MigrationExecutionAttempt,*,command_hash,idempotency_key,actor_type,actor_reference,request_id):
  """Validate execution eligibility and delegate the durable claim."""

  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version)
  if not isinstance(command_hash,str) or re.fullmatch(r"[0-9a-f]{64}",command_hash) is None:raise ValueError("command_hash is invalid.")
  workflow=self.repository.get_workflow(workflow_id)
  if workflow.version==expected_version and workflow.status is not MigrationWorkflowStatus.EXECUTION_READY:raise InvalidWorkflowTransitionError()
  event=self._event(workflow,MigrationAuditEventType.STATUS_CHANGED,MigrationWorkflowStatus.EXECUTION_READY,MigrationWorkflowStatus.EXECUTING,actor_type,actor_reference,request_id,idempotency_key,attempt.claimed_at)
  return self.repository.claim_execution_attempt(workflow_id,expected_version,attempt,event,idempotency_key=idempotency_key,request_hash=command_hash)
 def mark_execution_running(self,attempt_id,running_at):return self.repository.mark_execution_running(attempt_id,running_at)
 def complete_execution_attempt(self,workflow_id,expected_version,attempt_id,evidence:MigrationExecutionEvidence,new_status,*,idempotency_key,actor_type,actor_reference,request_id):
  """Build immutable execution evidence for atomic repository completion."""

  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version)
  workflow=self.repository.get_workflow(workflow_id)
  if workflow.version==expected_version and new_status not in ALLOWED_TRANSITIONS[workflow.status]:raise InvalidWorkflowTransitionError()
  self._validate_identity(workflow,WorkflowArtifactType.EXECUTION_EVIDENCE,evidence)
  try:data,digest=serialize_artifact(WorkflowArtifactType.EXECUTION_EVIDENCE,evidence)
  except (TypeError,ValueError):raise WorkflowArtifactValidationError() from None
  artifact=WorkflowArtifact(artifact_id=self.uuid_factory(),workflow_id=workflow_id,artifact_type=WorkflowArtifactType.EXECUTION_EVIDENCE,artifact_version=workflow.latest_artifact_version+1,schema_version=1,payload=data,payload_sha256=digest,created_at=evidence.completed_at)
  artifact_event=self._event(workflow,MigrationAuditEventType.ARTIFACT_APPENDED,workflow.status,workflow.status,actor_type,actor_reference,request_id,idempotency_key,evidence.completed_at,artifact=artifact)
  transition_event=self._event(workflow,MigrationAuditEventType.STATUS_CHANGED,workflow.status,new_status,actor_type,actor_reference,request_id,idempotency_key,evidence.completed_at)
  return self.repository.complete_execution_attempt(workflow_id,expected_version,attempt_id,evidence,artifact,artifact_event,new_status,transition_event)
 def claim_validation_run(self,workflow_id,expected_version,run:WorkflowValidationRun,preview:tuple[GeneratedValidationSql,GeneratedValidationSql],*,command_hash,idempotency_key,actor_type,actor_reference,request_id):
  """Validate, persist, and claim the generated validation plan."""

  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version)
  if not isinstance(command_hash,str) or re.fullmatch(r"[0-9a-f]{64}",command_hash) is None:raise ValueError("command_hash is invalid.")
  workflow=self.repository.get_workflow(workflow_id);self._validate_identity(workflow,WorkflowArtifactType.VALIDATION_PREVIEW,preview)
  if workflow.version==expected_version and workflow.status is not MigrationWorkflowStatus.EXECUTED:raise InvalidWorkflowTransitionError()
  try:data,digest=serialize_artifact(WorkflowArtifactType.VALIDATION_PREVIEW,preview)
  except (TypeError,ValueError):raise WorkflowArtifactValidationError() from None
  if run.validation_preview_artifact_version!=workflow.latest_artifact_version+1:raise WorkflowArtifactValidationError()
  artifact=WorkflowArtifact(artifact_id=self.uuid_factory(),workflow_id=workflow_id,artifact_type=WorkflowArtifactType.VALIDATION_PREVIEW,artifact_version=run.validation_preview_artifact_version,schema_version=1,payload=data,payload_sha256=digest,created_at=run.claimed_at)
  artifact_event=self._event(workflow,MigrationAuditEventType.ARTIFACT_APPENDED,workflow.status,workflow.status,actor_type,actor_reference,request_id,idempotency_key,run.claimed_at,artifact=artifact)
  transition_event=self._event(workflow,MigrationAuditEventType.STATUS_CHANGED,MigrationWorkflowStatus.EXECUTED,MigrationWorkflowStatus.VALIDATING,actor_type,actor_reference,request_id,idempotency_key,run.claimed_at)
  return self.repository.claim_validation_run(workflow_id,expected_version,run,artifact,artifact_event,transition_event,idempotency_key=idempotency_key,request_hash=command_hash)
 def mark_validation_running(self,run_id,running_at):return self.repository.mark_validation_running(run_id,running_at)
 def complete_validation_run(self,workflow_id,expected_version,run_id,report:MigrationValidationExecutionReport|None,new_status,*,completed_at,failure_category,idempotency_key,actor_type,actor_reference,request_id):
  """Prepare optional report evidence and delegate terminal completion."""

  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version);workflow=self.repository.get_workflow(workflow_id)
  if workflow.version==expected_version and new_status not in ALLOWED_TRANSITIONS[workflow.status]:raise InvalidWorkflowTransitionError()
  artifact=None;artifact_event=None
  if report is not None:
   self._validate_identity(workflow,WorkflowArtifactType.VALIDATION_EXECUTION_REPORT,report)
   try:data,digest=serialize_artifact(WorkflowArtifactType.VALIDATION_EXECUTION_REPORT,report)
   except (TypeError,ValueError):raise WorkflowArtifactValidationError() from None
   artifact=WorkflowArtifact(artifact_id=self.uuid_factory(),workflow_id=workflow_id,artifact_type=WorkflowArtifactType.VALIDATION_EXECUTION_REPORT,artifact_version=workflow.latest_artifact_version+1,schema_version=1,payload=data,payload_sha256=digest,created_at=completed_at)
   artifact_event=self._event(workflow,MigrationAuditEventType.ARTIFACT_APPENDED,workflow.status,workflow.status,actor_type,actor_reference,request_id,idempotency_key,completed_at,artifact=artifact)
  transition_event=self._event(workflow,MigrationAuditEventType.STATUS_CHANGED,workflow.status,new_status,actor_type,actor_reference,request_id,idempotency_key,completed_at)
  return self.repository.complete_validation_run(workflow_id,expected_version,run_id,report,artifact,artifact_event,new_status,transition_event,completed_at=completed_at,failure_category=failure_category)
 @staticmethod
 def _validate_identity(workflow,kind,payload):
  """Prove that typed artifact relations/profiles belong to the workflow."""

  def relation(table):return WorkflowRelation(catalog_name=table.catalog_name,schema_name=table.schema_name,object_name=table.object_name,system=table.system)
  valid=True
  if kind is WorkflowArtifactType.SOURCE_DISCOVERY:valid=relation(payload)==workflow.source_relation
  elif kind is WorkflowArtifactType.TARGET_DISCOVERY:valid=relation(payload)==workflow.target_relation
  elif kind in {WorkflowArtifactType.MAPPING_PLAN,WorkflowArtifactType.APPROVED_MAPPING_PLAN}:
   source=WorkflowRelation(catalog_name=payload.source_table.catalog_name,schema_name=payload.source_table.schema_name,object_name=payload.source_table.table_name,system=payload.source_table.system);target=WorkflowRelation(catalog_name=payload.target_table.catalog_name,schema_name=payload.target_table.schema_name,object_name=payload.target_table.table_name,system=payload.target_table.system);valid=source==workflow.source_relation and target==workflow.target_relation
  elif kind is WorkflowArtifactType.TRANSFORMATION_PREVIEW:valid=tuple(x for x in (workflow.target_relation.catalog_name,workflow.target_relation.schema_name,workflow.target_relation.object_name))==payload.target_relation
  elif kind is WorkflowArtifactType.STAGING_LOAD_EVIDENCE:
   valid=(payload.workflow_id==workflow.workflow_id and payload.source_profile_id==workflow.source_profile_id and payload.target_profile_id==workflow.target_profile_id and (payload.source_relation.catalog_name,payload.source_relation.schema_name,payload.source_relation.object_name)==(workflow.source_relation.catalog_name,workflow.source_relation.schema_name,workflow.source_relation.object_name) and payload.staging_relation.catalog_name==workflow.target_relation.catalog_name and payload.staging_relation.schema_name==workflow.target_relation.schema_name and payload.staging_relation.object_name.startswith("SB_STAGE_"))
  elif kind is WorkflowArtifactType.STAGING_CLEANUP_EVIDENCE:
   valid=(payload.workflow_id==workflow.workflow_id and payload.target_profile_id==workflow.target_profile_id and payload.staging_relation.catalog_name==workflow.target_relation.catalog_name and payload.staging_relation.schema_name==workflow.target_relation.schema_name and payload.staging_relation.object_name.startswith("SB_STAGE_"))
  elif kind is WorkflowArtifactType.EXECUTION_EVIDENCE:valid=payload.workflow_id==workflow.workflow_id and payload.target_profile_id==workflow.target_profile_id and payload.target_relation==tuple(x for x in (workflow.target_relation.catalog_name,workflow.target_relation.schema_name,workflow.target_relation.object_name))
  elif kind is WorkflowArtifactType.VALIDATION_PREVIEW:valid=payload[0].relation[-2:]==(workflow.source_relation.schema_name,workflow.source_relation.object_name) and payload[1].relation[-3:]==tuple(x for x in (workflow.target_relation.catalog_name,workflow.target_relation.schema_name,workflow.target_relation.object_name))
  elif kind is WorkflowArtifactType.VALIDATION_EXECUTION_REPORT:
   valid=(payload.source_profile_id==workflow.source_profile_id and payload.target_profile_id==workflow.target_profile_id and payload.source_sql_summary.relation[-2:]==(workflow.source_relation.schema_name,workflow.source_relation.object_name) and payload.target_sql_summary.relation[-3:]==tuple(x for x in (workflow.target_relation.catalog_name,workflow.target_relation.schema_name,workflow.target_relation.object_name)))
  if not valid:raise WorkflowArtifactValidationError()
 @staticmethod
 def _event(workflow,event_type,previous,new,actor_type,actor_reference,request_id,key,now,metadata=AuditMetadata(),artifact=None):
  return MigrationAuditEvent(sequence_number=1,event_id=uuid4(),workflow_id=workflow.workflow_id,event_type=event_type,previous_status=previous,new_status=new,workflow_version=workflow.version,artifact_id=artifact.artifact_id if artifact else None,artifact_type=artifact.artifact_type if artifact else None,actor_type=actor_type,actor_reference=actor_reference,request_id=request_id,idempotency_key=key,occurred_at=now,metadata=metadata)
