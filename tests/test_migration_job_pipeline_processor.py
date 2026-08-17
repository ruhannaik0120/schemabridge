"""Verify the worker runs the assembled migration pipeline and stops safely."""

from datetime import timedelta
from uuid import UUID

from schemabridge.models.migration_job import MigrationJobStage, MigrationJobStatus
from schemabridge.services.batch_transport import BatchTransportDisposition
from schemabridge.services.migration_execution import (
    TargetExecutionDisposition,
    TargetExecutionResult,
)
from schemabridge.services.migration_job_pipeline import (
    MigrationJobExecutionStep,
    MigrationJobPipelineProcessor,
    MigrationJobValidationStep,
)
from schemabridge.services.migration_job_worker import MigrationJobWorker
from schemabridge.services.migration_jobs import (
    MigrationJobClaimService,
    MigrationJobCompletionService,
)
from schemabridge.services.transformation_sql import SnowflakeTransformationSqlCompiler
from schemabridge.services.validation_sql import compile_validation_sql
from schemabridge.services.workflow_execution import WorkflowExecutionOrchestrator
from schemabridge.services.workflow_orchestration import WorkflowPlanningOrchestrator
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from schemabridge.services.workflow_validation import WorkflowValidationOrchestrator
from tests.test_migration_job_execution_step import EXECUTION_ATTEMPT_ID
from tests.test_migration_job_staging_step import _context
from tests.test_workflow_execution_api import FakeExecutor
from tests.test_workflow_validation_api import FakeValidationExecutor


VALIDATION_RUN_ID = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")


def _worker_context(
    *,
    staging_disposition=BatchTransportDisposition.SUCCEEDED,
    execution_disposition=TargetExecutionDisposition.SUCCEEDED,
    validation_mismatch=False,
    validation_fail=False,
):
    staging_step, queued, repository, transport, workflow_id = _context(
        staging_disposition,
        claim=False,
    )
    persistence = WorkflowPersistenceService(repository)
    completion = MigrationJobCompletionService(
        persistence,
        clock=lambda: queued.queued_at + timedelta(seconds=120),
    )
    compiler = SnowflakeTransformationSqlCompiler()
    planning = WorkflowPlanningOrchestrator(
        persistence,
        discovery_resolver=lambda _profile_id: None,
        mapping_service=object(),
        approval_service=object(),
        transformation_compiler=compiler,
    )
    execution_result = TargetExecutionResult(
        execution_disposition,
        affected_rows=(3 if execution_disposition is TargetExecutionDisposition.SUCCEEDED else None),
        failure_category=(
            None
            if execution_disposition is TargetExecutionDisposition.SUCCEEDED
            else "TARGET_EXECUTION_FAILED"
            if execution_disposition
            is TargetExecutionDisposition.CONFIRMED_FAILED_ROLLED_BACK
            else "TARGET_OUTCOME_UNCERTAIN"
        ),
    )
    executor = FakeExecutor([execution_result])
    base = repository.get_workflow(workflow_id).updated_at
    execution_times = iter(
        base + timedelta(seconds=20 + index) for index in range(20)
    )
    execution = WorkflowExecutionOrchestrator(
        persistence,
        transformation_compiler=compiler,
        execution_service=executor,
        staging_cleanup_service=transport,
        clock=lambda: next(execution_times),
        uuid_factory=lambda: EXECUTION_ATTEMPT_ID,
    )
    execution_step = MigrationJobExecutionStep(
        persistence,
        planning,
        execution,
        completion_service=completion,
    )
    validation_executor = FakeValidationExecutor(
        mismatch=validation_mismatch,
        fail=validation_fail,
    )
    validation_times = iter(
        base + timedelta(seconds=60 + index) for index in range(20)
    )
    validation = WorkflowValidationOrchestrator(
        persistence,
        validation_compiler=compile_validation_sql,
        validation_execution_service=validation_executor,
        clock=lambda: next(validation_times),
        uuid_factory=lambda: VALIDATION_RUN_ID,
    )
    validation_step = MigrationJobValidationStep(
        persistence,
        validation,
        completion_service=completion,
    )
    processor = MigrationJobPipelineProcessor(
        staging_step,
        execution_step,
        validation_step,
    )
    claim = MigrationJobClaimService(
        persistence,
        clock=lambda: queued.queued_at + timedelta(seconds=1),
    )
    worker = MigrationJobWorker(claim, processor)
    return worker, repository, transport, executor, validation_executor


def test_worker_runs_the_complete_assembled_pipeline_once() -> None:
    worker, repository, transport, executor, validation = _worker_context()

    result = worker.run_once()

    assert result.status is MigrationJobStatus.SUCCEEDED
    assert result.stage is MigrationJobStage.COMPLETED
    assert repository.get_migration_job(result.job_id) == result
    assert len(transport.run_calls) == 1
    assert executor.invocations == 1
    assert len(transport.cleanup_calls) == 1
    assert validation.invocations == 1
    assert worker.run_once() is None


def test_staging_failure_stops_before_execution_and_validation() -> None:
    worker, _repository, transport, executor, validation = _worker_context(
        staging_disposition=BatchTransportDisposition.CONFIRMED_FAILED_CLEANED_UP
    )

    result = worker.run_once()

    assert result.status is MigrationJobStatus.FAILED
    assert result.stage is MigrationJobStage.STAGING
    assert len(transport.run_calls) == 1
    assert executor.invocations == 0
    assert validation.invocations == 0


def test_uncertain_execution_stops_before_validation() -> None:
    worker, _repository, transport, executor, validation = _worker_context(
        execution_disposition=TargetExecutionDisposition.OUTCOME_UNCERTAIN
    )

    result = worker.run_once()

    assert result.status is MigrationJobStatus.RECOVERY_REQUIRED
    assert result.stage is MigrationJobStage.EXECUTING
    assert result.failure_category == "TARGET_OUTCOME_UNCERTAIN"
    assert executor.invocations == 1
    assert transport.cleanup_calls == []
    assert validation.invocations == 0


def test_validation_mismatch_returns_review_required_terminal_job() -> None:
    worker, _repository, transport, executor, validation = _worker_context(
        validation_mismatch=True
    )

    result = worker.run_once()

    assert result.status is MigrationJobStatus.REVIEW_REQUIRED
    assert result.stage is MigrationJobStage.VALIDATING
    assert result.failure_category == "VALIDATION_MISMATCH"
    assert executor.invocations == 1
    assert len(transport.cleanup_calls) == 1
    assert validation.invocations == 1
