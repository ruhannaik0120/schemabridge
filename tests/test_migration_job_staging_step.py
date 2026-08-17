"""Verify background jobs reuse the durable connector-neutral staging path."""

from dataclasses import replace
from datetime import timedelta
from uuid import UUID

import pytest

from schemabridge.models.migration_job import MigrationJobStage, MigrationJobStatus
from schemabridge.models.workflow import AuditActorType, MigrationWorkflowStatus
from schemabridge.services.migration_job_pipeline import MigrationJobStagingStep
from schemabridge.services.migration_jobs import (
    MigrationJobClaimService,
    MigrationJobCompletionService,
    MigrationJobProgressService,
    MigrationJobSubmissionService,
)
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from tests.test_migration_job_repository import JOB_ID
from tests.test_workflow_transport import (
    FakeTransport,
    _approved,
    _orchestrator,
)
from schemabridge.services.batch_transport import BatchTransportDisposition
from schemabridge.transport.base import BatchTransportError


class PrepareFailureTransport(FakeTransport):
    def prepare(self, **kwargs):
        self.prepare_calls.append(kwargs)
        raise BatchTransportError("safe test failure")


def _context(disposition, *, mutate_workflow=False, claim=True):
    repository = InMemoryWorkflowRepository()
    created, approved = _approved(repository)
    workflow_id = UUID(created["workflow_id"])
    persistence = WorkflowPersistenceService(repository)
    workflow = repository.get_workflow(workflow_id)
    queued_at = workflow.updated_at + timedelta(seconds=1)
    submission = MigrationJobSubmissionService(
        persistence,
        clock=lambda: queued_at,
        uuid_factory=lambda: JOB_ID,
    )
    queued, was_created = submission.create(
        workflow_id,
        expected_version=approved["workflow"]["version"],
        source_discovery_artifact_version=1,
        approved_mapping_artifact_version=approved["artifact"]["artifact_version"],
        batch_size=100,
        timeout_seconds=20,
        idempotency_key="background-staging-job",
        actor_type=AuditActorType.SERVICE,
        actor_reference="test-worker",
    )
    assert was_created is True
    claimed = (
        MigrationJobClaimService(
            persistence,
            clock=lambda: queued_at + timedelta(seconds=1),
        ).claim_next()
        if claim
        else queued
    )
    if mutate_workflow:
        repository._workflows[workflow_id] = replace(
            repository._workflows[workflow_id],
            version=repository._workflows[workflow_id].version + 1,
        )
    transport = FakeTransport(disposition)
    completion = MigrationJobCompletionService(
        persistence,
        clock=lambda: queued_at + timedelta(seconds=20),
    )
    progress_times = iter(
        (
            queued_at + timedelta(seconds=2),
            queued_at + timedelta(seconds=3),
        )
    )
    step = MigrationJobStagingStep(
        persistence,
        _orchestrator(repository, transport),
        completion_service=completion,
        progress_service=MigrationJobProgressService(
            persistence,
            clock=lambda: next(progress_times),
        ),
    )
    return step, claimed, repository, transport, workflow_id


def test_success_moves_real_transport_result_into_transforming_stage() -> None:
    step, claimed, repository, transport, workflow_id = _context(
        BatchTransportDisposition.SUCCEEDED
    )

    result = step.run(claimed)

    assert result.job.status is MigrationJobStatus.RUNNING
    assert result.job.stage is MigrationJobStage.TRANSFORMING
    assert repository.get_migration_job(JOB_ID) == result.job
    assert result.transport is not None
    assert result.transport.evidence.rows_read == 3
    assert result.transport.evidence.rows_written == 3
    assert result.transport.evidence.batch_count == 2
    assert result.job.batch_progress is not None
    assert result.job.batch_progress.batches_completed == 2
    assert result.job.batch_progress.rows_read == 3
    assert result.job.batch_progress.rows_written == 3
    assert result.job.batch_progress.estimated_percent_complete == 100
    assert result.job.progress_updated_at is not None
    assert result.transport.evidence.staging_relation.object_name.startswith("SB_STAGE_")
    assert repository.get_workflow(workflow_id).status is MigrationWorkflowStatus.STAGED
    assert len(transport.prepare_calls) == len(transport.run_calls) == 1
    assert transport.prepare_calls[0]["batch_size"] == 100
    assert transport.prepare_calls[0]["source_profile_id"] == "pg-source"
    assert transport.prepare_calls[0]["target_profile_id"] == "sf-target"


def test_stale_workflow_fails_during_preparation_before_transport() -> None:
    step, claimed, repository, transport, _workflow_id = _context(
        BatchTransportDisposition.SUCCEEDED,
        mutate_workflow=True,
    )

    result = step.run(claimed)

    assert result.transport is None
    assert result.job.status is MigrationJobStatus.FAILED
    assert result.job.stage is MigrationJobStage.PREPARING
    assert result.job.failure_category == "JOB_PREPARATION_FAILED"
    assert repository.get_migration_job(JOB_ID) == result.job
    assert transport.prepare_calls == transport.run_calls == []


def test_connector_preparation_failure_is_confirmed_before_remote_loading() -> None:
    step, claimed, repository, original, _workflow_id = _context(
        BatchTransportDisposition.SUCCEEDED
    )
    failing = PrepareFailureTransport(BatchTransportDisposition.SUCCEEDED)
    step.transport_orchestrator.transport_service = failing

    result = step.run(claimed)

    assert result.job.status is MigrationJobStatus.FAILED
    assert result.job.stage is MigrationJobStage.STAGING
    assert result.job.failure_category == "STAGING_PREPARATION_FAILED"
    assert repository.get_migration_job(JOB_ID) == result.job
    assert len(failing.prepare_calls) == 1
    assert failing.run_calls == []
    assert original.prepare_calls == original.run_calls == []


@pytest.mark.parametrize(
    ("disposition", "status", "category"),
    [
        (
            BatchTransportDisposition.CONFIRMED_FAILED_CLEANED_UP,
            MigrationJobStatus.FAILED,
            "STAGING_LOAD_FAILED",
        ),
        (
            BatchTransportDisposition.OUTCOME_UNCERTAIN,
            MigrationJobStatus.RECOVERY_REQUIRED,
            "STAGING_OUTCOME_UNCERTAIN",
        ),
    ],
)
def test_transport_outcome_is_classified_on_the_background_job(
    disposition,
    status,
    category,
) -> None:
    step, claimed, repository, _transport, _workflow_id = _context(disposition)

    result = step.run(claimed)

    assert result.transport is None
    assert result.job.status is status
    assert result.job.stage is MigrationJobStage.STAGING
    assert result.job.failure_category == category
    assert repository.get_migration_job(JOB_ID) == result.job
