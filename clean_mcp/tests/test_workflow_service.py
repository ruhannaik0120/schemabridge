from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID
import pytest
from models.workflow import *
from persistence.errors import *
from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from persistence.serialization import canonical_json_bytes,serialize_artifact
from services.workflow_persistence import WorkflowPersistenceService
from services.schema_mapping import SchemaMappingService
from tests.test_mapping_approval import _column,_table
from tests.test_transformation_sql import _approved
from services.transformation_sql import compile_snowflake_select
from services.validation_sql import compile_validation_sql
from services.reconciliation import reconcile_validation_results
from models.validation import MigrationValidationExecutionReport,ValidationExecutionStatus

NOW=datetime(2026,1,1,tzinfo=timezone.utc)
class IDs:
 def __init__(self):self.value=10
 def __call__(self):self.value+=1;return UUID(int=self.value)
def relation(table):return WorkflowRelation(catalog_name=table.catalog_name,schema_name=table.schema_name,object_name=table.object_name,system=table.system)
def setup():
 source=_table('source',_column('id'));target=_table('target',_column('id'));repo=InMemoryWorkflowRepository();service=WorkflowPersistenceService(repo,clock=lambda:NOW,uuid_factory=IDs());workflow=service.create_workflow(display_name='Durable migration',source_profile_id='pg',target_profile_id='sf',source_relation=relation(source),target_relation=relation(target),idempotency_key='create-1');return service,repo,workflow,source,target
CTX={'idempotency_key':'artifact-1','actor_type':AuditActorType.SERVICE,'actor_reference':'discovery','request_id':'request-1'}

def test_create_replay_and_idempotency_conflict():
 service,repo,first,source,target=setup()
 replay=service.create_workflow(display_name='Durable migration',source_profile_id='pg',target_profile_id='sf',source_relation=relation(source),target_relation=relation(target),idempotency_key='create-1')
 assert replay is first and len(repo.list_audit_events(first.workflow_id))==1
 with pytest.raises(WorkflowIdempotencyConflictError):service.create_workflow(display_name='changed',source_profile_id='pg',target_profile_id='sf',source_relation=relation(source),target_relation=relation(target),idempotency_key='create-1')

def test_transition_concurrency_replay_terminal_and_audit_order():
 service,repo,w,_,_=setup();changed=service.transition_status(w.workflow_id,expected_version=1,new_status=MigrationWorkflowStatus.DISCOVERED,idempotency_key='status-1')
 replay=service.transition_status(w.workflow_id,expected_version=1,new_status=MigrationWorkflowStatus.DISCOVERED,idempotency_key='status-1')
 assert replay==changed and changed.version==2 and [x.sequence_number for x in repo.list_audit_events(w.workflow_id)]==[1,2]
 with pytest.raises(WorkflowConflictError):service.transition_status(w.workflow_id,expected_version=1,new_status=MigrationWorkflowStatus.MAPPING_PROPOSED,idempotency_key='status-2')
 failed=service.mark_failed(w.workflow_id,expected_version=2,idempotency_key='fail-1',reason_code='DOWNSTREAM_FAILED')
 assert failed.status is MigrationWorkflowStatus.FAILED and failed.last_error_code=='DOWNSTREAM_FAILED'
 with pytest.raises(InvalidWorkflowTransitionError):service.transition_status(w.workflow_id,expected_version=3,new_status=MigrationWorkflowStatus.DISCOVERED,idempotency_key='status-3')
 with pytest.raises(ValueError):service.transition_status(w.workflow_id,expected_version=True,new_status=MigrationWorkflowStatus.DISCOVERED,idempotency_key='bad-version')

def test_transition_replay_remains_valid_after_a_later_transition():
 service,repo,w,_,_=setup()
 discovered=service.transition_status(w.workflow_id,expected_version=1,new_status=MigrationWorkflowStatus.DISCOVERED,idempotency_key='discover')
 service.transition_status(w.workflow_id,expected_version=2,new_status=MigrationWorkflowStatus.MAPPING_PROPOSED,idempotency_key='propose')
 replay=service.transition_status(w.workflow_id,expected_version=1,new_status=MigrationWorkflowStatus.DISCOVERED,idempotency_key='discover')
 assert replay==discovered and len(repo.list_audit_events(w.workflow_id))==3

def test_concurrent_duplicate_create_commits_once():
 service,repo,_,source,target=setup()
 def create():return service.create_workflow(display_name='Concurrent',source_profile_id='pg',target_profile_id='sf',source_relation=relation(source),target_relation=relation(target),idempotency_key='concurrent-create')
 with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:create(),range(2)))
 assert results[0] is results[1] and len([event for events in repo._events.values() for event in events if event.idempotency_key=='concurrent-create'])==1

def test_artifact_append_hash_version_replay_and_identity():
 service,repo,w,source,target=setup();updated,artifact=service.append_source_discovery(w.workflow_id,1,source,**CTX)
 assert updated.version==2 and updated.latest_artifact_version==1 and artifact.payload_sha256==serialize_artifact(WorkflowArtifactType.SOURCE_DISCOVERY,source)[1]
 replay=service.append_source_discovery(w.workflow_id,1,source,**CTX)
 assert replay==(updated,artifact) and len(repo.list_artifacts(w.workflow_id))==1 and len(repo.list_audit_events(w.workflow_id))==2
 with pytest.raises(WorkflowArtifactValidationError):service.append_target_discovery(w.workflow_id,2,source,**{**CTX,'idempotency_key':'bad-target'})

def test_canonical_json_and_hash_are_deterministic_and_typed():
 first=canonical_json_bytes({'b':2,'a':1});second=canonical_json_bytes({'a':1,'b':2});assert first==second
 source=_table('source',_column('id'));data,digest=serialize_artifact(WorkflowArtifactType.SOURCE_DISCOVERY,source);changed,_=serialize_artifact(WorkflowArtifactType.SOURCE_DISCOVERY,_table('source',_column('other')))
 assert len(digest)==64 and data!=changed and b'vendor_metadata' not in data
 with pytest.raises(TypeError):serialize_artifact(WorkflowArtifactType.SOURCE_DISCOVERY,{'arbitrary':'json'})

def test_every_artifact_type_has_a_typed_serializer():
 approved=_approved();source=_table('source',_column('first_name'),_column('last_name'),_column('age',ordinal=3));target=_table('people',_column('full_name'),_column('age',ordinal=2));plan=SchemaMappingService().suggest(source,target)
 transform=compile_snowflake_select(approved,staging_database='db',staging_schema='s',staging_table='t');validation=compile_validation_sql(approved,source_schema='schema',source_table='source',target_database='catalog',target_schema='schema',target_table='people')
 metrics={check.check_id:1 for check in validation[0].checks};reconciled=reconcile_validation_results(validation[0],validation[1],source_metrics=metrics,target_metrics=metrics)
 report=MigrationValidationExecutionReport(source_profile_id='pg',target_profile_id='sf',source_sql_summary=validation[0],target_sql_summary=validation[1],validation_report=reconciled,source_execution_status=ValidationExecutionStatus.SUCCEEDED,target_execution_status=ValidationExecutionStatus.SUCCEEDED)
 values=((WorkflowArtifactType.SOURCE_DISCOVERY,source),(WorkflowArtifactType.TARGET_DISCOVERY,target),(WorkflowArtifactType.MAPPING_PLAN,plan),(WorkflowArtifactType.APPROVED_MAPPING_PLAN,approved),(WorkflowArtifactType.TRANSFORMATION_PREVIEW,transform),(WorkflowArtifactType.VALIDATION_PREVIEW,validation),(WorkflowArtifactType.VALIDATION_EXECUTION_REPORT,report))
 assert all(len(serialize_artifact(kind,value)[1])==64 for kind,value in values)

def test_fake_rolls_back_audit_and_idempotency_failures():
 service,repo,w,source,_=setup();repo.fail_audit=True
 with pytest.raises(WorkflowPersistenceError):service.append_source_discovery(w.workflow_id,1,source,**CTX)
 assert repo.get_workflow(w.workflow_id).version==1 and repo.list_artifacts(w.workflow_id)==()
 repo.fail_audit=False;repo.fail_idempotency=True
 with pytest.raises(WorkflowPersistenceError):service.append_source_discovery(w.workflow_id,1,source,**CTX)
 assert repo.get_workflow(w.workflow_id).version==1 and repo.list_artifacts(w.workflow_id)==()

def test_pagination_is_stable_and_bounded():
 service,repo,w,source,_=setup();service.append_source_discovery(w.workflow_id,1,source,**CTX)
 assert len(repo.list_audit_events(w.workflow_id,offset=1,limit=1))==1
 with pytest.raises(ValueError):repo.list_audit_events(w.workflow_id,limit=501)
