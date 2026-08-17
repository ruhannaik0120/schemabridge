"""Verify server-controlled construction of background migration jobs."""

import hashlib
from dataclasses import replace
from datetime import datetime, timezone
from uuid import UUID

import pytest

from schemabridge.models.migration_job import MigrationJobStage, MigrationJobStatus
from schemabridge.models.workflow import (
    AuditActorType,
    MigrationWorkflowStatus,
    WorkflowArtifact,
    WorkflowArtifactType,
)
from schemabridge.persistence.errors import (
    WorkflowOperationUnavailableError,
    WorkflowStaleArtifactReferenceError,
)
from schemabridge.services.migration_jobs import (
    MigrationJobClaimService,
    MigrationJobCompletionService,
    MigrationJobProgressService,
    MigrationJobSubmissionService,
)
from schemabridge.persistence.errors import MigrationJobTransitionError
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from tests.fakes.workflow_repository import InMemoryWorkflowRepository
from tests.test_migration_job_repository import JOB_ID, NOW, WORKFLOW_ID, _workflow


SECOND_JOB_ID = UUID("22222222-3333-4444-5555-666666666666")


class IDs:
    def __init__(self):
        self.values = iter((JOB_ID, SECOND_JOB_ID))

    def __call__(self):
        return next(self.values)


def _artifact(kind, version, marker):
    payload = (f'{{"marker":"{marker}"}}').encode("utf-8")
    return WorkflowArtifact(
        artifact_id=UUID(int=version),
        workflow_id=WORKFLOW_ID,
        artifact_type=kind,
        artifact_version=version,
        schema_version=1,
        payload=payload,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        created_at=NOW,
    )


def _service(*, workflow=None):
    repository = InMemoryWorkflowRepository()
    value = workflow or _workflow()
    repository._workflows[WORKFLOW_ID] = value
    repository._artifacts[WORKFLOW_ID] = [
        _artifact(WorkflowArtifactType.SOURCE_DISCOVERY, 1, "source"),
        _artifact(WorkflowArtifactType.APPROVED_MAPPING_PLAN, 4, "approved"),
    ]
    service = MigrationJobSubmissionService(
        WorkflowPersistenceService(repository),
        clock=lambda: NOW,
        uuid_factory=IDs(),
    )
    return service, repository


def _create(service, **overrides):
    values = {
        "expected_version": 5,
        "source_discovery_artifact_version": 1,
        "approved_mapping_artifact_version": 4,
        "batch_size": 500,
        "timeout_seconds": 30,
        "idempotency_key": "create-background-job-1",
        "actor_type": AuditActorType.USER,
        "actor_reference": "reviewer-1",
    }
    values.update(overrides)
    return service.create(WORKFLOW_ID, **values)


def test_service_controls_identity_profiles_hash_and_initial_state() -> None:
    service, _repository = _service()

    job, created = _create(service)

    assert created is True
    assert job.job_id == JOB_ID
    assert job.queued_at == NOW
    assert job.source_profile_id == "mysql-source"
    assert job.target_profile_id == "snowflake-target"
    assert job.status is MigrationJobStatus.QUEUED
    assert job.stage is MigrationJobStage.QUEUED
    assert len(job.job_fingerprint) == 64


def test_exact_replay_returns_original_job_after_workflow_progresses() -> None:
    service, repository = _service()
    original, _created = _create(service)
    repository._workflows[WORKFLOW_ID] = replace(
        repository._workflows[WORKFLOW_ID],
        status=MigrationWorkflowStatus.STAGING,
        version=6,
    )

    replay, created = _create(service)

    assert replay == original
    assert created is False


def test_current_unapproved_workflow_and_wrong_artifact_type_are_rejected() -> None:
    service, _repository = _service(
        workflow=_workflow(status=MigrationWorkflowStatus.MAPPING_PROPOSED)
    )
    with pytest.raises(WorkflowOperationUnavailableError):
        _create(service)

    service, repository = _service()
    repository._artifacts[WORKFLOW_ID][0] = _artifact(
        WorkflowArtifactType.TARGET_DISCOVERY, 1, "wrong"
    )
    with pytest.raises(WorkflowStaleArtifactReferenceError):
        _create(service)


def test_batch_limit_is_rejected_before_postgresql() -> None:
    service, _repository = _service()

    with pytest.raises(ValueError, match="batch_size exceeds"):
        _create(service, batch_size=10_001)


def test_claim_service_uses_server_time_and_returns_no_job_after_claim() -> None:
    submission, repository = _service()
    queued, _created = _create(submission)
    started_at = datetime(2026, 8, 16, 0, 0, 1, tzinfo=timezone.utc)
    service = MigrationJobClaimService(
        WorkflowPersistenceService(repository), clock=lambda: started_at
    )

    claimed = service.claim_next()

    assert claimed.job_id == queued.job_id
    assert claimed.status is MigrationJobStatus.RUNNING
    assert claimed.stage is MigrationJobStage.PREPARING
    assert claimed.started_at == started_at
    assert service.claim_next() is None


def test_progress_service_advances_only_the_expected_next_stage() -> None:
    submission, repository = _service()
    _create(submission)
    claim = MigrationJobClaimService(
        WorkflowPersistenceService(repository), clock=lambda: NOW
    )
    running = claim.claim_next()
    progress = MigrationJobProgressService(WorkflowPersistenceService(repository))

    staging = progress.advance(
        running.job_id,
        expected_stage=MigrationJobStage.PREPARING,
        new_stage=MigrationJobStage.STAGING,
    )

    assert staging.stage is MigrationJobStage.STAGING
    with pytest.raises(MigrationJobTransitionError):
        progress.advance(
            running.job_id,
            expected_stage=MigrationJobStage.PREPARING,
            new_stage=MigrationJobStage.STAGING,
        )


def test_completion_service_distinguishes_success_failure_and_uncertainty() -> None:
    completed_at = datetime(2026, 8, 16, 0, 0, 5, tzinfo=timezone.utc)

    submission, repository = _service()
    _create(submission)
    repository._jobs[JOB_ID] = replace(
        repository._jobs[JOB_ID],
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.VALIDATING,
        started_at=NOW,
    )
    completion = MigrationJobCompletionService(
        WorkflowPersistenceService(repository), clock=lambda: completed_at
    )
    succeeded = completion.succeed(JOB_ID)

    submission, repository = _service()
    _create(submission)
    repository._jobs[JOB_ID] = replace(
        repository._jobs[JOB_ID],
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.STAGING,
        started_at=NOW,
    )
    completion = MigrationJobCompletionService(
        WorkflowPersistenceService(repository), clock=lambda: completed_at
    )
    failed = completion.fail(
        JOB_ID,
        expected_stage=MigrationJobStage.STAGING,
        failure_category="STAGING_FAILED",
    )

    submission, repository = _service()
    _create(submission)
    repository._jobs[JOB_ID] = replace(
        repository._jobs[JOB_ID],
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.EXECUTING,
        started_at=NOW,
    )
    completion = MigrationJobCompletionService(
        WorkflowPersistenceService(repository), clock=lambda: completed_at
    )
    uncertain = completion.require_recovery(
        JOB_ID,
        expected_stage=MigrationJobStage.EXECUTING,
        failure_category="TARGET_OUTCOME_UNCERTAIN",
    )

    submission, repository = _service()
    _create(submission)
    repository._jobs[JOB_ID] = replace(
        repository._jobs[JOB_ID],
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.VALIDATING,
        started_at=NOW,
    )
    completion = MigrationJobCompletionService(
        WorkflowPersistenceService(repository), clock=lambda: completed_at
    )
    review = completion.require_review(
        JOB_ID,
        failure_category="VALIDATION_MISMATCH",
    )

    assert succeeded.status is MigrationJobStatus.SUCCEEDED
    assert succeeded.stage is MigrationJobStage.COMPLETED
    assert failed.status is MigrationJobStatus.FAILED
    assert failed.stage is MigrationJobStage.STAGING
    assert uncertain.status is MigrationJobStatus.RECOVERY_REQUIRED
    assert uncertain.stage is MigrationJobStage.EXECUTING
    assert review.status is MigrationJobStatus.REVIEW_REQUIRED
    assert review.stage is MigrationJobStage.VALIDATING
