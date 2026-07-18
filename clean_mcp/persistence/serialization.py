"""Typed deterministic artifact serialization and request hashing."""
from __future__ import annotations
import hashlib,json,math
from dataclasses import fields,is_dataclass
from datetime import date,datetime,time
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any
from uuid import UUID

from models.discovery import TableMetadata
from models.mapping import ApprovedTableMappingPlan,GeneratedTransformationSql,TableMappingPlan
from models.validation import GeneratedValidationSql,MigrationValidationExecutionReport
from models.workflow import WorkflowArtifactType

def _canonical(value:Any)->Any:
    if value is None or isinstance(value,(str,bool,int)): return value
    if isinstance(value,float):
        if not math.isfinite(value): raise TypeError("Canonical values must be finite.")
        return value
    if isinstance(value,Decimal):
        if not value.is_finite(): raise TypeError("Canonical values must be finite.")
        return str(value)
    if isinstance(value,datetime):
        if value.tzinfo is None: raise TypeError("Canonical datetimes must be timezone-aware.")
        return value.isoformat().replace("+00:00","Z")
    if isinstance(value,(date,time)): return value.isoformat()
    if isinstance(value,(Enum,UUID)): return str(value.value if isinstance(value,Enum) else value)
    if isinstance(value,(tuple,list)): return [_canonical(item) for item in value]
    if isinstance(value,(set,frozenset)): return sorted((_canonical(item) for item in value),key=lambda x:json.dumps(x,sort_keys=True,ensure_ascii=False))
    if isinstance(value,(dict,MappingProxyType)):
        return {str(key):_canonical(item) for key,item in sorted(value.items(),key=lambda pair:str(pair[0])) if str(key)!="vendor_metadata"}
    if is_dataclass(value):
        return {item.name:_canonical(getattr(value,item.name)) for item in fields(value) if item.name!="vendor_metadata"}
    raise TypeError("Unsupported canonical value.")

_EXPECTED={
 WorkflowArtifactType.SOURCE_DISCOVERY:TableMetadata,WorkflowArtifactType.TARGET_DISCOVERY:TableMetadata,
 WorkflowArtifactType.MAPPING_PLAN:TableMappingPlan,WorkflowArtifactType.APPROVED_MAPPING_PLAN:ApprovedTableMappingPlan,
 WorkflowArtifactType.TRANSFORMATION_PREVIEW:GeneratedTransformationSql,
 WorkflowArtifactType.VALIDATION_EXECUTION_REPORT:MigrationValidationExecutionReport,
}
def canonical_json_bytes(value:Any)->bytes:
    return json.dumps(_canonical(value),ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False).encode("utf-8")
def serialize_artifact(kind:WorkflowArtifactType,payload:Any)->tuple[bytes,str]:
    if kind is WorkflowArtifactType.VALIDATION_PREVIEW:
        valid=isinstance(payload,tuple) and len(payload)==2 and all(isinstance(item,GeneratedValidationSql) for item in payload)
    else: valid=isinstance(payload,_EXPECTED.get(kind,()))
    if not valid: raise TypeError("Artifact payload type does not match artifact type.")
    data=canonical_json_bytes(payload)
    return data,hashlib.sha256(data).hexdigest()
def request_hash(command_type:str,value:Any)->str:
    data=canonical_json_bytes({"command_type":command_type,"request":value})
    return hashlib.sha256(data).hexdigest()
