"""Durable workflow policy, typed artifacts, hashing, and audit orchestration."""
from __future__ import annotations
from datetime import datetime,timezone
from typing import Any,Callable
from uuid import UUID,uuid4

from models.discovery import TableMetadata
from models.mapping import ApprovedTableMappingPlan,GeneratedTransformationSql,TableMappingPlan
from models.validation import GeneratedValidationSql,MigrationValidationExecutionReport
from models.workflow import *
from persistence.errors import InvalidWorkflowTransitionError,WorkflowArtifactValidationError
from persistence.repository import WorkflowRepository
from persistence.serialization import request_hash,serialize_artifact

def _now():return datetime.now(timezone.utc)
class WorkflowPersistenceService:
 def __init__(self,repository:WorkflowRepository,*,clock:Callable[[],datetime]=_now,uuid_factory:Callable[[],UUID]=uuid4):
  self.repository=repository;self.clock=clock;self.uuid_factory=uuid_factory
 def _context(self,actor_type,actor_reference,request_id,idempotency_key):
  if not isinstance(actor_type,AuditActorType):raise TypeError("actor_type is invalid.")
  if not isinstance(idempotency_key,str) or not idempotency_key.strip() or len(idempotency_key)>128:raise ValueError("idempotency_key is invalid.")
  return actor_type,actor_reference,request_id,idempotency_key
 @staticmethod
 def _expected(value):
  if isinstance(value,bool) or not isinstance(value,int) or value<1:raise ValueError("expected_version must be a positive integer.")
 def create_workflow(self,*,display_name,source_profile_id,target_profile_id,source_relation,target_relation,idempotency_key,actor_type=AuditActorType.SYSTEM,actor_reference=None,request_id=None):
  self._context(actor_type,actor_reference,request_id,idempotency_key);now=self.clock();workflow=MigrationWorkflow(workflow_id=self.uuid_factory(),display_name=display_name,source_profile_id=source_profile_id,target_profile_id=target_profile_id,source_relation=source_relation,target_relation=target_relation,status=MigrationWorkflowStatus.DRAFT,version=1,created_at=now,updated_at=now)
  digest=request_hash("CREATE_WORKFLOW",{"display_name":display_name,"source_profile_id":source_profile_id,"target_profile_id":target_profile_id,"source_relation":source_relation,"target_relation":target_relation})
  event=self._event(workflow,MigrationAuditEventType.WORKFLOW_CREATED,None,MigrationWorkflowStatus.DRAFT,actor_type,actor_reference,request_id,idempotency_key,now)
  return self.repository.create_workflow(workflow,event,idempotency_key=idempotency_key,request_hash=digest)
 def transition_status(self,workflow_id,*,expected_version,new_status,idempotency_key,reason_code=None,actor_type=AuditActorType.SYSTEM,actor_reference=None,request_id=None):
  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version);current=self.repository.get_workflow(workflow_id)
  digest=request_hash("TRANSITION_STATUS",{"workflow_id":workflow_id,"expected_version":expected_version,"new_status":new_status,"reason_code":reason_code})
  # A stale expected version is resolved by the repository: it may be an exact
  # idempotent replay, which must remain valid after later workflow mutations.
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
  self._context(actor_type,actor_reference,request_id,idempotency_key);self._expected(expected_version);workflow=self.repository.get_workflow(workflow_id);self._validate_identity(workflow,kind,payload)
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
 @staticmethod
 def _validate_identity(workflow,kind,payload):
  def relation(table):return WorkflowRelation(catalog_name=table.catalog_name,schema_name=table.schema_name,object_name=table.object_name,system=table.system)
  valid=True
  if kind is WorkflowArtifactType.SOURCE_DISCOVERY:valid=relation(payload)==workflow.source_relation
  elif kind is WorkflowArtifactType.TARGET_DISCOVERY:valid=relation(payload)==workflow.target_relation
  elif kind in {WorkflowArtifactType.MAPPING_PLAN,WorkflowArtifactType.APPROVED_MAPPING_PLAN}:
   source=WorkflowRelation(catalog_name=payload.source_table.catalog_name,schema_name=payload.source_table.schema_name,object_name=payload.source_table.table_name,system=payload.source_table.system);target=WorkflowRelation(catalog_name=payload.target_table.catalog_name,schema_name=payload.target_table.schema_name,object_name=payload.target_table.table_name,system=payload.target_table.system);valid=source==workflow.source_relation and target==workflow.target_relation
  elif kind is WorkflowArtifactType.TRANSFORMATION_PREVIEW:valid=tuple(x for x in (workflow.target_relation.catalog_name,workflow.target_relation.schema_name,workflow.target_relation.object_name))==payload.target_relation
  elif kind is WorkflowArtifactType.VALIDATION_PREVIEW:valid=payload[0].relation[-2:]==(workflow.source_relation.schema_name,workflow.source_relation.object_name) and payload[1].relation[-3:]==tuple(x for x in (workflow.target_relation.catalog_name,workflow.target_relation.schema_name,workflow.target_relation.object_name))
  elif kind is WorkflowArtifactType.VALIDATION_EXECUTION_REPORT:
   valid=(payload.source_profile_id==workflow.source_profile_id and payload.target_profile_id==workflow.target_profile_id and payload.source_sql_summary.relation[-2:]==(workflow.source_relation.schema_name,workflow.source_relation.object_name) and payload.target_sql_summary.relation[-3:]==tuple(x for x in (workflow.target_relation.catalog_name,workflow.target_relation.schema_name,workflow.target_relation.object_name)))
  if not valid:raise WorkflowArtifactValidationError()
 @staticmethod
 def _event(workflow,event_type,previous,new,actor_type,actor_reference,request_id,key,now,metadata=AuditMetadata(),artifact=None):
  return MigrationAuditEvent(sequence_number=1,event_id=uuid4(),workflow_id=workflow.workflow_id,event_type=event_type,previous_status=previous,new_status=new,workflow_version=workflow.version,artifact_id=artifact.artifact_id if artifact else None,artifact_type=artifact.artifact_type if artifact else None,actor_type=actor_type,actor_reference=actor_reference,request_id=request_id,idempotency_key=key,occurred_at=now,metadata=metadata)
