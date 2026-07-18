from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from models.metadata import _MetadataModel,_json_value
from models.mapping import SqlDialect
class ValidationCheckType(str,Enum): ROW_COUNT='ROW_COUNT'; NULL_COUNT='NULL_COUNT'; DISTINCT_COUNT='DISTINCT_COUNT'
class ValidationStatus(str,Enum): MATCH='MATCH'; MISMATCH='MISMATCH'; UNAVAILABLE='UNAVAILABLE'
class MigrationValidationStatus(str,Enum): PASSED='PASSED'; FAILED='FAILED'; INCOMPLETE='INCOMPLETE'
@dataclass(frozen=True,slots=True,kw_only=True)
class ValidationCheckDefinition(_MetadataModel):
 check_id:str; check_type:ValidationCheckType; source_column:str|None; target_column:str|None; source_metric_alias:str; target_metric_alias:str
 def to_dict(self): return _json_value({n:getattr(self,n) for n in self.__dataclass_fields__})
@dataclass(frozen=True,slots=True,kw_only=True)
class GeneratedValidationSql(_MetadataModel):
 dialect:SqlDialect; sql:str; parameters:tuple[object,...]; relation:tuple[str,...]; metric_aliases:tuple[str,...]; checks:tuple[ValidationCheckDefinition,...]; warnings:tuple[str,...]=()
 def __post_init__(self):
  if not self.sql.strip() or len(set(self.metric_aliases))!=len(self.metric_aliases) or len({x.check_id for x in self.checks})!=len(self.checks): raise ValueError('Invalid validation SQL.')
 def to_dict(self): return _json_value({n:getattr(self,n) for n in self.__dataclass_fields__})
@dataclass(frozen=True,slots=True,kw_only=True)
class ValidationCheckResult(_MetadataModel):
 check_id:str; check_type:ValidationCheckType; source_value:int|None; target_value:int|None; status:ValidationStatus; difference:int|None; source_column:str|None; target_column:str|None
 def to_dict(self): return _json_value({n:getattr(self,n) for n in self.__dataclass_fields__})
@dataclass(frozen=True,slots=True,kw_only=True)
class MigrationValidationReport(_MetadataModel):
 source_table:tuple[str,...]; target_table:tuple[str,...]; check_results:tuple[ValidationCheckResult,...]; status:MigrationValidationStatus; matched_count:int; mismatched_count:int; unavailable_count:int; warnings:tuple[str,...]; approved_plan_version:int
 def to_dict(self): return _json_value({n:getattr(self,n) for n in self.__dataclass_fields__})
