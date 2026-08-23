"""Stable identifiers and domain builders for migration-job tests."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from schemabridge.models.migration_job import (
    MigrationJob,
    MigrationJobStage,
    MigrationJobStatus,
)
from schemabridge.models.workflow import (
    AuditActorType,
    MigrationWorkflow,
    MigrationWorkflowStatus,
    WorkflowRelation,
)

NOW = datetime(2026, 8, 16, tzinfo=timezone.utc)
WORKFLOW_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
JOB_ID = UUID("11111111-2222-3333-4444-555555555555")
EXECUTION_ATTEMPT_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def migration_workflow(
    *,
    status: MigrationWorkflowStatus = MigrationWorkflowStatus.MAPPING_APPROVED,
    version: int = 5,
) -> MigrationWorkflow:
    return MigrationWorkflow(
        workflow_id=WORKFLOW_ID,
        display_name="Background migration",
        source_profile_id="mysql-source",
        target_profile_id="snowflake-target",
        source_relation=WorkflowRelation(
            catalog_name="source",
            schema_name="source",
            object_name="customers",
            system="mysql",
        ),
        target_relation=WorkflowRelation(
            catalog_name="target",
            schema_name="public",
            object_name="customers",
            system="snowflake",
        ),
        status=status,
        version=version,
        created_at=NOW,
        updated_at=NOW,
        latest_artifact_version=4,
    )


def migration_job(**overrides) -> MigrationJob:
    values = {
        "job_id": JOB_ID,
        "workflow_id": WORKFLOW_ID,
        "expected_workflow_version": 5,
        "source_discovery_artifact_version": 1,
        "approved_mapping_artifact_version": 4,
        "source_profile_id": "mysql-source",
        "target_profile_id": "snowflake-target",
        "batch_size": 500,
        "timeout_seconds": 30,
        "job_fingerprint": "a" * 64,
        "status": MigrationJobStatus.QUEUED,
        "stage": MigrationJobStage.QUEUED,
        "queued_at": NOW,
        "actor_type": AuditActorType.USER,
        "idempotency_key": "create-job-1",
    }
    values.update(overrides)
    return MigrationJob(**values)
