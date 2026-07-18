from dataclasses import FrozenInstanceError
from datetime import datetime,timezone,timedelta
from uuid import UUID
import hashlib
import pytest
from models.workflow import *
from persistence.config import ControlPlaneConfig

NOW=datetime(2026,1,1,tzinfo=timezone.utc)
REL=WorkflowRelation(catalog_name='Data.B"ase',schema_name='München.Schema',object_name='T;--',system='postgresql')
def workflow(status=MigrationWorkflowStatus.DRAFT,warnings=()):return MigrationWorkflow(workflow_id=UUID(int=1),display_name='Migration — one',source_profile_id='source-private',target_profile_id='target-private',source_relation=REL,target_relation=REL,status=status,version=1,created_at=NOW,updated_at=NOW,warnings=warnings)

def test_workflow_is_immutable_utc_and_repr_redacted():
 w=workflow()
 with pytest.raises(FrozenInstanceError):w.version=2
 assert 'source-private' not in repr(w) and 'target-private' not in repr(w)
 with pytest.raises(ValueError):MigrationWorkflow(workflow_id=UUID(int=1),display_name='x',source_profile_id='s',target_profile_id='t',source_relation=REL,target_relation=REL,status=MigrationWorkflowStatus.DRAFT,version=1,created_at=datetime(2026,1,1),updated_at=NOW)

def test_warning_and_version_invariants():
 with pytest.raises(ValueError):workflow(warnings=('Z_WARNING','A_WARNING'))
 with pytest.raises(ValueError):MigrationWorkflow(workflow_id=UUID(int=1),display_name='x',source_profile_id='s',target_profile_id='t',source_relation=REL,target_relation=REL,status=MigrationWorkflowStatus.DRAFT,version=0,created_at=NOW,updated_at=NOW)

def test_artifact_payload_hash_is_an_integrity_invariant():
 payload=b'{"value":1}'
 artifact=WorkflowArtifact(artifact_id=UUID(int=2),workflow_id=UUID(int=1),artifact_type=WorkflowArtifactType.SOURCE_DISCOVERY,artifact_version=1,schema_version=1,payload=payload,payload_sha256=hashlib.sha256(payload).hexdigest(),created_at=NOW)
 assert artifact.payload==payload
 with pytest.raises(ValueError):WorkflowArtifact(artifact_id=UUID(int=2),workflow_id=UUID(int=1),artifact_type=WorkflowArtifactType.SOURCE_DISCOVERY,artifact_version=1,schema_version=1,payload=payload,payload_sha256='0'*64,created_at=NOW)

def test_transition_matrix_has_exact_ordinary_and_terminal_policy():
 ordinary=[(MigrationWorkflowStatus.DRAFT,MigrationWorkflowStatus.DISCOVERED),(MigrationWorkflowStatus.DISCOVERED,MigrationWorkflowStatus.MAPPING_PROPOSED),(MigrationWorkflowStatus.MAPPING_PROPOSED,MigrationWorkflowStatus.MAPPING_APPROVED),(MigrationWorkflowStatus.MAPPING_APPROVED,MigrationWorkflowStatus.VALIDATION_READY),(MigrationWorkflowStatus.VALIDATION_READY,MigrationWorkflowStatus.VALIDATING),(MigrationWorkflowStatus.VALIDATING,MigrationWorkflowStatus.VALIDATED)]
 assert all(right in ALLOWED_TRANSITIONS[left] for left,right in ordinary)
 assert all(not ALLOWED_TRANSITIONS[x] for x in (MigrationWorkflowStatus.VALIDATED,MigrationWorkflowStatus.FAILED,MigrationWorkflowStatus.CANCELLED))

def test_control_plane_config_is_explicit_and_dsn_redacted(monkeypatch):
 config=ControlPlaneConfig(dsn='postgresql://user:password@host/db')
 assert 'password' not in repr(config) and '[REDACTED]' in repr(config)
 monkeypatch.delenv('SCHEMABRIDGE_CONTROL_PLANE_DSN',raising=False)
 assert ControlPlaneConfig.from_environment().enabled is False
