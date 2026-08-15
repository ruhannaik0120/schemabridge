"""Verify durable transport lifecycle models and artifact round trips."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from schemabridge.models.transport import TransportRelation
from schemabridge.models.workflow import (
    ALLOWED_TRANSITIONS,
    AuditActorType,
    MigrationWorkflowStatus,
    WorkflowArtifact,
    WorkflowArtifactType,
)
from schemabridge.models.workflow_transport import (
    WorkflowTransportAttempt,
    WorkflowTransportAttemptStatus,
    WorkflowTransportEvidence,
)
from schemabridge.persistence.artifact_codec import (
    workflow_transport_evidence_from_artifact,
)
from schemabridge.persistence.errors import WorkflowArtifactValidationError
from schemabridge.persistence.serialization import serialize_artifact


NOW = datetime(2026, 8, 15, tzinfo=timezone.utc)
ATTEMPT_ID = UUID("12345678-1234-5678-1234-567812345678")
WORKFLOW_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
FINGERPRINT = "a" * 64
SOURCE = TransportRelation(
    catalog_name="source_db",
    schema_name="lab",
    object_name="customers",
)
STAGING = TransportRelation(
    catalog_name="SCHEMABRIDGE_LAB",
    schema_name="PUBLIC",
    object_name="SB_STAGE_12345678123456781234567812345678",
)


def _attempt(**overrides) -> WorkflowTransportAttempt:
    values = {
        "attempt_id": ATTEMPT_ID,
        "workflow_id": WORKFLOW_ID,
        "source_discovery_artifact_version": 1,
        "approved_mapping_artifact_version": 4,
        "source_profile_id": "postgres-source",
        "target_profile_id": "snowflake-target",
        "staging_relation": STAGING,
        "batch_size": 500,
        "timeout_seconds": 30,
        "transport_fingerprint": FINGERPRINT,
        "status": WorkflowTransportAttemptStatus.CLAIMED,
        "claimed_at": NOW,
        "actor_type": AuditActorType.USER,
        "idempotency_key": "load-staging-1",
    }
    values.update(overrides)
    return WorkflowTransportAttempt(**values)


def _evidence() -> WorkflowTransportEvidence:
    return WorkflowTransportEvidence(
        attempt_id=ATTEMPT_ID,
        workflow_id=WORKFLOW_ID,
        source_relation=SOURCE,
        staging_relation=STAGING,
        source_profile_id="postgres-source",
        target_profile_id="snowflake-target",
        batch_size=500,
        batch_count=3,
        column_count=6,
        rows_read=1005,
        rows_written=1005,
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=2),
        duration_ms=2000,
        source_discovery_artifact_version=1,
        approved_mapping_artifact_version=4,
        transport_fingerprint=FINGERPRINT,
    )


def test_transport_workflow_states_have_safe_paths() -> None:
    assert MigrationWorkflowStatus.STAGING in ALLOWED_TRANSITIONS[
        MigrationWorkflowStatus.MAPPING_APPROVED
    ]
    assert ALLOWED_TRANSITIONS[MigrationWorkflowStatus.STAGING] >= {
        MigrationWorkflowStatus.STAGED,
        MigrationWorkflowStatus.MAPPING_APPROVED,
        MigrationWorkflowStatus.STAGING_RECOVERY_REQUIRED,
    }
    assert MigrationWorkflowStatus.EXECUTION_READY in ALLOWED_TRANSITIONS[
        MigrationWorkflowStatus.STAGED
    ]
    assert MigrationWorkflowStatus.STAGING not in ALLOWED_TRANSITIONS[
        MigrationWorkflowStatus.STAGING_RECOVERY_REQUIRED
    ]


def test_attempt_lifecycle_requires_consistent_timestamps_and_evidence() -> None:
    running = _attempt(
        status=WorkflowTransportAttemptStatus.RUNNING,
        running_at=NOW + timedelta(seconds=1),
    )
    succeeded = replace(
        running,
        status=WorkflowTransportAttemptStatus.SUCCEEDED,
        completed_at=NOW + timedelta(seconds=2),
        evidence_artifact_id=UUID(int=9),
    )

    assert succeeded.status is WorkflowTransportAttemptStatus.SUCCEEDED
    with pytest.raises(ValueError, match="lifecycle"):
        replace(running, status=WorkflowTransportAttemptStatus.SUCCEEDED)
    with pytest.raises(ValueError, match="lifecycle"):
        replace(
            running,
            status=WorkflowTransportAttemptStatus.OUTCOME_UNCERTAIN,
            completed_at=NOW + timedelta(seconds=2),
        )


def test_success_evidence_contains_counts_but_no_business_rows() -> None:
    evidence = _evidence()
    payload, digest = serialize_artifact(
        WorkflowArtifactType.STAGING_LOAD_EVIDENCE,
        evidence,
    )

    assert b"1005" in payload
    assert b"customer records" not in payload
    artifact = WorkflowArtifact(
        artifact_id=UUID(int=10),
        workflow_id=WORKFLOW_ID,
        artifact_type=WorkflowArtifactType.STAGING_LOAD_EVIDENCE,
        artifact_version=5,
        schema_version=1,
        payload=payload,
        payload_sha256=digest,
        created_at=NOW + timedelta(seconds=2),
    )
    assert workflow_transport_evidence_from_artifact(artifact) == evidence


def test_transport_evidence_rejects_false_row_totals() -> None:
    with pytest.raises(ValueError, match="row counts"):
        replace(_evidence(), rows_written=1004)


def test_transport_codec_rejects_wrong_artifact_type() -> None:
    payload, digest = serialize_artifact(
        WorkflowArtifactType.STAGING_LOAD_EVIDENCE,
        _evidence(),
    )
    wrong = WorkflowArtifact(
        artifact_id=UUID(int=10),
        workflow_id=WORKFLOW_ID,
        artifact_type=WorkflowArtifactType.EXECUTION_EVIDENCE,
        artifact_version=5,
        schema_version=1,
        payload=payload,
        payload_sha256=digest,
        created_at=NOW,
    )

    with pytest.raises(WorkflowArtifactValidationError):
        workflow_transport_evidence_from_artifact(wrong)
