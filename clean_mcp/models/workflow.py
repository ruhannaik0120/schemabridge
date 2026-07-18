"""Immutable durable migration workflow and audit domain models."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID


class MigrationWorkflowStatus(str, Enum):
    DRAFT="DRAFT"; DISCOVERED="DISCOVERED"; MAPPING_PROPOSED="MAPPING_PROPOSED"; MAPPING_APPROVED="MAPPING_APPROVED"
    VALIDATION_READY="VALIDATION_READY"; VALIDATING="VALIDATING"; VALIDATED="VALIDATED"; FAILED="FAILED"; CANCELLED="CANCELLED"

class WorkflowArtifactType(str, Enum):
    SOURCE_DISCOVERY="SOURCE_DISCOVERY"; TARGET_DISCOVERY="TARGET_DISCOVERY"; MAPPING_PLAN="MAPPING_PLAN"; APPROVED_MAPPING_PLAN="APPROVED_MAPPING_PLAN"
    TRANSFORMATION_PREVIEW="TRANSFORMATION_PREVIEW"; VALIDATION_PREVIEW="VALIDATION_PREVIEW"; VALIDATION_EXECUTION_REPORT="VALIDATION_EXECUTION_REPORT"

class MigrationAuditEventType(str, Enum):
    WORKFLOW_CREATED="WORKFLOW_CREATED"; STATUS_CHANGED="STATUS_CHANGED"; ARTIFACT_APPENDED="ARTIFACT_APPENDED"; WORKFLOW_FAILED="WORKFLOW_FAILED"; WORKFLOW_CANCELLED="WORKFLOW_CANCELLED"

class AuditActorType(str, Enum): SYSTEM="SYSTEM"; USER="USER"; SERVICE="SERVICE"

_CODE=re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
def _text(value: str, name: str, limit: int=512) -> None:
    if not isinstance(value,str) or not value.strip() or len(value)>limit or "\x00" in value: raise ValueError(f"{name} is invalid.")
def _utc(value: datetime, name: str) -> None:
    if not isinstance(value,datetime) or value.tzinfo is None or value.utcoffset()!=timezone.utc.utcoffset(value): raise ValueError(f"{name} must be UTC.")
def _uuid(value: UUID, name: str) -> None:
    if not isinstance(value,UUID): raise TypeError(f"{name} must be a UUID.")
def _codes(values: tuple[str,...]) -> None:
    if not isinstance(values,tuple) or values!=tuple(sorted(set(values))) or any(not _CODE.fullmatch(x) for x in values): raise ValueError("warnings must be sorted fixed codes.")

@dataclass(frozen=True,slots=True,kw_only=True)
class WorkflowRelation:
    catalog_name: str|None; schema_name: str; object_name: str; system: str
    def __post_init__(self):
        if self.catalog_name is not None: _text(self.catalog_name,"catalog_name")
        _text(self.schema_name,"schema_name"); _text(self.object_name,"object_name"); _text(self.system,"system",64)

@dataclass(frozen=True,slots=True,kw_only=True,repr=False)
class MigrationWorkflow:
    workflow_id: UUID; display_name: str; source_profile_id: str; target_profile_id: str
    source_relation: WorkflowRelation; target_relation: WorkflowRelation; status: MigrationWorkflowStatus
    version: int; created_at: datetime; updated_at: datetime; latest_artifact_version: int=0
    last_error_code: str|None=None; warnings: tuple[str,...]=()
    def __post_init__(self):
        _uuid(self.workflow_id,"workflow_id"); _text(self.display_name,"display_name",200); _text(self.source_profile_id,"source_profile_id",256); _text(self.target_profile_id,"target_profile_id",256)
        if not isinstance(self.source_relation,WorkflowRelation) or not isinstance(self.target_relation,WorkflowRelation): raise TypeError("relations are invalid.")
        if not isinstance(self.status,MigrationWorkflowStatus): raise TypeError("status is invalid.")
        if isinstance(self.version,bool) or not isinstance(self.version,int) or self.version<1: raise ValueError("version must be positive.")
        if isinstance(self.latest_artifact_version,bool) or not isinstance(self.latest_artifact_version,int) or self.latest_artifact_version<0: raise ValueError("artifact version is invalid.")
        _utc(self.created_at,"created_at"); _utc(self.updated_at,"updated_at")
        if self.updated_at<self.created_at: raise ValueError("updated_at precedes created_at.")
        if self.last_error_code is not None and not _CODE.fullmatch(self.last_error_code): raise ValueError("last_error_code is invalid.")
        _codes(self.warnings)
    def __repr__(self): return f"MigrationWorkflow(workflow_id={self.workflow_id!r}, status={self.status.value!r}, version={self.version})"

@dataclass(frozen=True,slots=True,kw_only=True,repr=False)
class WorkflowArtifact:
    artifact_id: UUID; workflow_id: UUID; artifact_type: WorkflowArtifactType; artifact_version: int; schema_version: int
    payload: bytes=field(repr=False); payload_sha256: str; created_at: datetime
    def __post_init__(self):
        _uuid(self.artifact_id,"artifact_id"); _uuid(self.workflow_id,"workflow_id")
        if not isinstance(self.artifact_type,WorkflowArtifactType): raise TypeError("artifact_type is invalid.")
        for name in ("artifact_version","schema_version"):
            value=getattr(self,name)
            if isinstance(value,bool) or not isinstance(value,int) or value<1: raise ValueError(f"{name} must be positive.")
        if not isinstance(self.payload,bytes): raise TypeError("payload must be canonical bytes.")
        if not re.fullmatch(r"[0-9a-f]{64}",self.payload_sha256): raise ValueError("payload hash is invalid.")
        if hashlib.sha256(self.payload).hexdigest()!=self.payload_sha256: raise ValueError("payload hash does not match payload.")
        _utc(self.created_at,"created_at")
    def __repr__(self): return f"WorkflowArtifact(artifact_id={self.artifact_id!r}, artifact_type={self.artifact_type.value!r}, artifact_version={self.artifact_version})"

@dataclass(frozen=True,slots=True,kw_only=True)
class AuditMetadata:
    reason_code: str|None=None
    def __post_init__(self):
        if self.reason_code is not None and not _CODE.fullmatch(self.reason_code): raise ValueError("reason_code is invalid.")

@dataclass(frozen=True,slots=True,kw_only=True,repr=False)
class MigrationAuditEvent:
    sequence_number: int; event_id: UUID; workflow_id: UUID; event_type: MigrationAuditEventType
    previous_status: MigrationWorkflowStatus|None; new_status: MigrationWorkflowStatus|None; workflow_version: int
    artifact_id: UUID|None; artifact_type: WorkflowArtifactType|None; actor_type: AuditActorType
    actor_reference: str|None; request_id: str|None; idempotency_key: str; occurred_at: datetime; metadata: AuditMetadata=AuditMetadata()
    def __post_init__(self):
        if isinstance(self.sequence_number,bool) or not isinstance(self.sequence_number,int) or self.sequence_number<1: raise ValueError("sequence_number is invalid.")
        _uuid(self.event_id,"event_id"); _uuid(self.workflow_id,"workflow_id")
        if not isinstance(self.event_type,MigrationAuditEventType) or not isinstance(self.actor_type,AuditActorType): raise TypeError("event enum is invalid.")
        if self.previous_status is not None and not isinstance(self.previous_status,MigrationWorkflowStatus): raise TypeError("previous_status is invalid.")
        if self.new_status is not None and not isinstance(self.new_status,MigrationWorkflowStatus): raise TypeError("new_status is invalid.")
        if isinstance(self.workflow_version,bool) or not isinstance(self.workflow_version,int) or self.workflow_version<1: raise ValueError("workflow_version is invalid.")
        if self.artifact_id is not None: _uuid(self.artifact_id,"artifact_id")
        if self.artifact_type is not None and not isinstance(self.artifact_type,WorkflowArtifactType): raise TypeError("artifact_type is invalid.")
        for value,name,limit in ((self.actor_reference,"actor_reference",256),(self.request_id,"request_id",64)):
            if value is not None: _text(value,name,limit)
        _text(self.idempotency_key,"idempotency_key",128); _utc(self.occurred_at,"occurred_at")
        if not isinstance(self.metadata,AuditMetadata): raise TypeError("metadata is invalid.")
        has_artifact=self.artifact_id is not None and self.artifact_type is not None
        if self.event_type is MigrationAuditEventType.WORKFLOW_CREATED:
            valid=self.previous_status is None and self.new_status is MigrationWorkflowStatus.DRAFT and not has_artifact
        elif self.event_type is MigrationAuditEventType.ARTIFACT_APPENDED:
            valid=has_artifact and self.previous_status is self.new_status and self.new_status is not None
        else:
            valid=not has_artifact and self.previous_status is not None and self.new_status is not None
            if self.event_type is MigrationAuditEventType.STATUS_CHANGED: valid=valid and self.new_status not in {MigrationWorkflowStatus.FAILED,MigrationWorkflowStatus.CANCELLED}
            if self.event_type is MigrationAuditEventType.WORKFLOW_FAILED: valid=valid and self.new_status is MigrationWorkflowStatus.FAILED
            if self.event_type is MigrationAuditEventType.WORKFLOW_CANCELLED: valid=valid and self.new_status is MigrationWorkflowStatus.CANCELLED
        if not valid or (self.artifact_id is None)!=(self.artifact_type is None): raise ValueError("event fields are inconsistent.")
    def __repr__(self): return f"MigrationAuditEvent(sequence_number={self.sequence_number}, event_type={self.event_type.value!r}, workflow_version={self.workflow_version})"

ALLOWED_TRANSITIONS={
    MigrationWorkflowStatus.DRAFT:frozenset({MigrationWorkflowStatus.DISCOVERED,MigrationWorkflowStatus.FAILED,MigrationWorkflowStatus.CANCELLED}),
    MigrationWorkflowStatus.DISCOVERED:frozenset({MigrationWorkflowStatus.MAPPING_PROPOSED,MigrationWorkflowStatus.FAILED,MigrationWorkflowStatus.CANCELLED}),
    MigrationWorkflowStatus.MAPPING_PROPOSED:frozenset({MigrationWorkflowStatus.MAPPING_APPROVED,MigrationWorkflowStatus.FAILED,MigrationWorkflowStatus.CANCELLED}),
    MigrationWorkflowStatus.MAPPING_APPROVED:frozenset({MigrationWorkflowStatus.VALIDATION_READY,MigrationWorkflowStatus.FAILED,MigrationWorkflowStatus.CANCELLED}),
    MigrationWorkflowStatus.VALIDATION_READY:frozenset({MigrationWorkflowStatus.VALIDATING,MigrationWorkflowStatus.FAILED,MigrationWorkflowStatus.CANCELLED}),
    MigrationWorkflowStatus.VALIDATING:frozenset({MigrationWorkflowStatus.VALIDATED,MigrationWorkflowStatus.FAILED,MigrationWorkflowStatus.CANCELLED}),
    MigrationWorkflowStatus.VALIDATED:frozenset(),MigrationWorkflowStatus.FAILED:frozenset(),MigrationWorkflowStatus.CANCELLED:frozenset(),
}
