from __future__ import annotations
from types import SimpleNamespace
import pytest
from models.validation import MigrationValidationExecutionRequest,MigrationValidationStatus
from services.validation_execution import MigrationValidationExecutionService,ValidationApprovalRequiredError,MalformedValidationExecutionResultError
from tests.test_transformation_sql import _approved

def _request(approved=True):
 return MigrationValidationExecutionRequest(source_profile_id='pg',target_profile_id='sf',approved_mapping_plan=_approved(),source_schema='public',source_table='people',target_database='db',target_schema='schema',target_table='people',timeout_seconds=9,explicitly_approved=approved)
class FakeService:
 def __init__(self,name,metrics,events):self.name=name;self.metrics=metrics;self.events=events;self.calls=[]
 def execute_query(self,**kwargs):
  self.calls.append(kwargs);self.events.append(self.name)
  return SimpleNamespace(success=True,data={'columns':list(self.metrics),'rows':[tuple(self.metrics.values())]})
def test_approval_gate_prevents_resolution(monkeypatch):
 monkeypatch.setattr('services.validation_execution.get_query_service',lambda _:(_ for _ in ()).throw(AssertionError()))
 with pytest.raises(ValidationApprovalRequiredError): MigrationValidationExecutionService().run(_request(False))
def test_profile_isolation_parameter_order_and_reconciliation(monkeypatch):
 events=[];metrics={'row_count':1,'m000_null_count':0,'m000_distinct_count':1,'m001_null_count':0,'m001_distinct_count':1};pg=FakeService('pg',metrics,events);sf=FakeService('sf',metrics,events)
 monkeypatch.setattr('services.validation_execution.get_query_service',lambda key:pg if key=='pg' else sf)
 report=MigrationValidationExecutionService().run(_request())
 assert events==['pg','sf'] and report.validation_report.status is MigrationValidationStatus.PASSED
 assert pg.calls[0]['parameters']==(' ',) and sf.calls[0].get('parameters') in (None,())
 assert pg.calls[0]['timeout_seconds']==sf.calls[0]['timeout_seconds']==9
def test_malformed_multiple_rows_is_not_a_validation_mismatch(monkeypatch):
 class Bad(FakeService):
  def execute_query(self,**kwargs):return SimpleNamespace(success=True,data={'columns':['row_count'],'rows':[(1,),(1,)]})
 monkeypatch.setattr('services.validation_execution.get_query_service',lambda _:Bad('x',{},[]))
 with pytest.raises(MalformedValidationExecutionResultError): MigrationValidationExecutionService().run(_request())
