"""Verify the small boundary between job claiming and pipeline processing."""

from datetime import timedelta

import pytest

from schemabridge.models.migration_job import MigrationJobStage, MigrationJobStatus
from schemabridge.services.migration_job_worker import (
    MigrationJobProcessorContractError,
    MigrationJobWorker,
)
from schemabridge.services.migration_jobs import (
    MigrationJobClaimService,
    MigrationJobCompletionService,
    MigrationJobProgressService,
)
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from tests.test_migration_job_repository import JOB_ID, NOW
from tests.test_migration_job_service import _create, _service


class RecordingProcessor:
    def __init__(self, persistence):
        self.calls = []
        self.progress = MigrationJobProgressService(persistence)
        self.completion = MigrationJobCompletionService(
            persistence,
            clock=lambda: NOW + timedelta(seconds=5),
        )

    def process(self, job):
        self.calls.append(job)
        current = MigrationJobStage.PREPARING
        for following in (
            MigrationJobStage.STAGING,
            MigrationJobStage.TRANSFORMING,
            MigrationJobStage.EXECUTING,
            MigrationJobStage.CLEANING_UP,
            MigrationJobStage.VALIDATING,
        ):
            self.progress.advance(
                job.job_id,
                expected_stage=current,
                new_stage=following,
            )
            current = following
        return self.completion.succeed(job.job_id)


class MustNotRunProcessor:
    def process(self, _job):
        raise AssertionError("processor must not run when the queue is empty")


class UnfinishedProcessor:
    def process(self, job):
        return job


def _worker(processor, *, queued=True):
    submission, repository = _service()
    if queued:
        _create(submission)
    persistence = WorkflowPersistenceService(repository)
    claim = MigrationJobClaimService(persistence, clock=lambda: NOW)
    return MigrationJobWorker(claim, processor(persistence)), repository


def test_worker_returns_none_without_invoking_processor_when_queue_is_empty() -> None:
    worker, _repository = _worker(
        lambda _persistence: MustNotRunProcessor(),
        queued=False,
    )

    assert worker.run_once() is None


def test_worker_claims_one_job_and_passes_it_to_processor() -> None:
    worker, repository = _worker(RecordingProcessor)

    result = worker.run_once()

    assert len(worker.processor.calls) == 1
    claimed = worker.processor.calls[0]
    assert claimed.job_id == JOB_ID
    assert claimed.status is MigrationJobStatus.RUNNING
    assert claimed.stage is MigrationJobStage.PREPARING
    assert result.status is MigrationJobStatus.SUCCEEDED
    assert result.stage is MigrationJobStage.COMPLETED
    assert repository.get_migration_job(JOB_ID) == result


def test_worker_rejects_a_processor_that_returns_an_unfinished_job() -> None:
    worker, repository = _worker(lambda _persistence: UnfinishedProcessor())

    with pytest.raises(MigrationJobProcessorContractError, match="terminal job"):
        worker.run_once()

    durable = repository.get_migration_job(JOB_ID)
    assert durable.status is MigrationJobStatus.RUNNING
    assert durable.stage is MigrationJobStage.PREPARING


def test_worker_requires_a_processor_contract() -> None:
    submission, repository = _service()
    persistence = WorkflowPersistenceService(repository)
    claim = MigrationJobClaimService(persistence, clock=lambda: NOW)

    with pytest.raises(TypeError, match="process"):
        MigrationJobWorker(claim, object())
