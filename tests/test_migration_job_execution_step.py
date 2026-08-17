"""Verify background jobs reuse approved target execution and cleanup."""

from datetime import timedelta
from uuid import UUID

import pytest

from schemabridge.models.migration_job import MigrationJobStage, MigrationJobStatus
from schemabridge.models.workflow import MigrationWorkflowStatus
from schemabridge.persistence.errors import WorkflowPreviewCompilationError
from schemabridge.services.migration_execution import (
    TargetExecutionDisposition,
    TargetExecutionResult,
)
from schemabridge.services.migration_job_pipeline import MigrationJobExecutionStep
from schemabridge.services.migration_jobs import MigrationJobCompletionService
from schemabridge.services.transformation_sql import SnowflakeTransformationSqlCompiler
from schemabridge.services.workflow_execution import WorkflowExecutionOrchestrator
from schemabridge.services.workflow_orchestration import WorkflowPlanningOrchestrator
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from tests.test_migration_job_repository import JOB_ID
from tests.test_migration_job_staging_step import _context
from tests.test_workflow_execution_api import FakeExecutor
from schemabridge.services.batch_transport import BatchTransportDisposition


EXECUTION_ATTEMPT_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


class CleanupFailureTransport:
    def __init__(self, delegate):
        self.delegate = delegate
        self.cleanup_calls = []

    def cleanup_staging(self, **kwargs):
        self.cleanup_calls.append(kwargs)
        raise RuntimeError("private cleanup failure")


class PreviewFailurePlanning:
    def preview_transformation(self, *_args, **_kwargs):
        raise WorkflowPreviewCompilationError()


def _execution_context(
    *,
    executor=None,
    planning=None,
    cleanup="normal",
):
    staging_step, claimed, repository, transport, workflow_id = _context(
        BatchTransportDisposition.SUCCEEDED
    )
    staged = staging_step.run(claimed)
    persistence = WorkflowPersistenceService(repository)
    compiler = SnowflakeTransformationSqlCompiler()
    planning = planning or WorkflowPlanningOrchestrator(
        persistence,
        discovery_resolver=lambda _profile_id: None,
        mapping_service=object(),
        approval_service=object(),
        transformation_compiler=compiler,
    )
    executor = executor or FakeExecutor()
    cleanup_service = (
        transport
        if cleanup == "normal"
        else CleanupFailureTransport(transport)
        if cleanup == "failure"
        else None
    )
    start = repository.get_workflow(workflow_id).updated_at + timedelta(seconds=1)
    times = iter(start + timedelta(seconds=index) for index in range(20))
    execution = WorkflowExecutionOrchestrator(
        persistence,
        transformation_compiler=compiler,
        execution_service=executor,
        staging_cleanup_service=cleanup_service,
        clock=lambda: next(times),
        uuid_factory=lambda: EXECUTION_ATTEMPT_ID,
    )
    completion = MigrationJobCompletionService(
        persistence,
        clock=lambda: staged.job.started_at + timedelta(seconds=60),
    )
    step = MigrationJobExecutionStep(
        persistence,
        planning,
        execution,
        completion_service=completion,
    )
    return step, staged, repository, transport, executor, cleanup_service, workflow_id


def test_success_compiles_executes_cleans_up_and_reaches_validation() -> None:
    step, staged, repository, transport, executor, _cleanup, workflow_id = (
        _execution_context()
    )

    result = step.run(staged)

    assert result.job.status is MigrationJobStatus.RUNNING
    assert result.job.stage is MigrationJobStage.VALIDATING
    assert repository.get_migration_job(JOB_ID) == result.job
    assert result.preview is not None
    assert result.preview.result.statement_type.value == "INSERT_SELECT"
    assert result.execution is not None
    assert result.execution.evidence.status.value == "SUCCEEDED"
    assert result.execution.cleanup_evidence is not None
    assert executor.invocations == 1
    assert len(transport.cleanup_calls) == 1
    assert repository.get_workflow(workflow_id).status is MigrationWorkflowStatus.EXECUTED


def test_transformation_failure_stops_before_target_execution() -> None:
    step, staged, repository, _transport, executor, _cleanup, _workflow_id = (
        _execution_context(planning=PreviewFailurePlanning())
    )

    result = step.run(staged)

    assert result.job.status is MigrationJobStatus.FAILED
    assert result.job.stage is MigrationJobStage.TRANSFORMING
    assert result.job.failure_category == "TRANSFORMATION_COMPILATION_FAILED"
    assert repository.get_migration_job(JOB_ID) == result.job
    assert executor.invocations == 0


@pytest.mark.parametrize(
    ("disposition", "status", "category"),
    [
        (
            TargetExecutionDisposition.CONFIRMED_FAILED_ROLLED_BACK,
            MigrationJobStatus.FAILED,
            "TARGET_EXECUTION_FAILED",
        ),
        (
            TargetExecutionDisposition.OUTCOME_UNCERTAIN,
            MigrationJobStatus.RECOVERY_REQUIRED,
            "TARGET_OUTCOME_UNCERTAIN",
        ),
    ],
)
def test_target_outcomes_are_classified_without_cleanup(
    disposition,
    status,
    category,
) -> None:
    executor = FakeExecutor(
        [TargetExecutionResult(disposition, failure_category=category)]
    )
    step, staged, repository, transport, _executor, _cleanup, _workflow_id = (
        _execution_context(executor=executor)
    )

    result = step.run(staged)

    assert result.job.status is status
    assert result.job.stage is MigrationJobStage.EXECUTING
    assert result.job.failure_category == category
    assert repository.get_migration_job(JOB_ID) == result.job
    assert transport.cleanup_calls == []


def test_committed_execution_with_failed_cleanup_requires_recovery() -> None:
    step, staged, repository, _transport, executor, cleanup, _workflow_id = (
        _execution_context(cleanup="failure")
    )

    result = step.run(staged)

    assert executor.invocations == 1
    assert len(cleanup.cleanup_calls) == 1
    assert result.job.status is MigrationJobStatus.RECOVERY_REQUIRED
    assert result.job.stage is MigrationJobStage.CLEANING_UP
    assert result.job.failure_category == "STAGING_CLEANUP_FAILED"
    assert repository.get_migration_job(JOB_ID) == result.job


def test_missing_cleanup_confirmation_blocks_validation() -> None:
    step, staged, repository, _transport, executor, _cleanup, _workflow_id = (
        _execution_context(cleanup="none")
    )

    result = step.run(staged)

    assert executor.invocations == 1
    assert result.execution is not None
    assert result.execution.cleanup_evidence is None
    assert result.job.status is MigrationJobStatus.RECOVERY_REQUIRED
    assert result.job.stage is MigrationJobStage.CLEANING_UP
    assert result.job.failure_category == "STAGING_CLEANUP_NOT_CONFIRMED"
    assert repository.get_migration_job(JOB_ID) == result.job
