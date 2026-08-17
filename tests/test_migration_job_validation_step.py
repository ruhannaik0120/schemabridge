"""Verify background jobs finish from durable aggregate validation results."""

from dataclasses import replace
from datetime import timedelta
from uuid import UUID

from schemabridge.models.migration_job import MigrationJobStage, MigrationJobStatus
from schemabridge.models.validation import MigrationValidationExecutionReport
from schemabridge.models.workflow import MigrationWorkflowStatus
from schemabridge.services.migration_job_pipeline import MigrationJobValidationStep
from schemabridge.services.migration_jobs import MigrationJobCompletionService
from schemabridge.services.reconciliation import reconcile_validation_results
from schemabridge.services.validation_sql import compile_validation_sql
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from schemabridge.services.workflow_validation import WorkflowValidationOrchestrator
from tests.test_migration_job_execution_step import _execution_context
from tests.test_migration_job_repository import JOB_ID
from tests.test_workflow_validation_api import FakeValidationExecutor


VALIDATION_RUN_ID = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")


class IncompleteValidationExecutor(FakeValidationExecutor):
    def run(self, request):
        successful = super().run(request)
        source = {
            alias: 3 for alias in successful.source_sql_summary.metric_aliases
        }
        target = dict(source)
        target[successful.target_sql_summary.metric_aliases[0]] = None
        report = reconcile_validation_results(
            successful.source_sql_summary,
            successful.target_sql_summary,
            approved_plan_version=request.approved_mapping_plan.version,
            source_metrics=source,
            target_metrics=target,
        )
        return MigrationValidationExecutionReport(
            source_profile_id=successful.source_profile_id,
            target_profile_id=successful.target_profile_id,
            source_sql_summary=successful.source_sql_summary,
            target_sql_summary=successful.target_sql_summary,
            validation_report=report,
            source_execution_status=successful.source_execution_status,
            target_execution_status=successful.target_execution_status,
        )


def _validation_context(validation_executor):
    execution_step, staged, repository, _transport, _executor, _cleanup, workflow_id = (
        _execution_context()
    )
    executed = execution_step.run(staged)
    persistence = WorkflowPersistenceService(repository)
    start = repository.get_workflow(workflow_id).updated_at + timedelta(seconds=1)
    times = iter(start + timedelta(seconds=index) for index in range(20))
    orchestrator = WorkflowValidationOrchestrator(
        persistence,
        validation_compiler=compile_validation_sql,
        validation_execution_service=validation_executor,
        clock=lambda: next(times),
        uuid_factory=lambda: VALIDATION_RUN_ID,
    )
    completion = MigrationJobCompletionService(
        persistence,
        clock=lambda: executed.job.started_at + timedelta(seconds=120),
    )
    step = MigrationJobValidationStep(
        persistence,
        orchestrator,
        completion_service=completion,
    )
    return step, executed, repository, workflow_id


def test_matching_validation_finishes_the_job_successfully() -> None:
    validation_executor = FakeValidationExecutor()
    step, executed, repository, workflow_id = _validation_context(
        validation_executor
    )

    result = step.run(executed)

    assert result.validation is not None
    assert result.validation.report.validation_report.status.value == "PASSED"
    assert result.job.status is MigrationJobStatus.SUCCEEDED
    assert result.job.stage is MigrationJobStage.COMPLETED
    assert result.job.failure_category is None
    assert repository.get_migration_job(JOB_ID) == result.job
    assert repository.get_workflow(workflow_id).status is MigrationWorkflowStatus.VALIDATED
    assert validation_executor.invocations == 1


def test_validation_mismatch_requires_review_without_retrying_migration() -> None:
    validation_executor = FakeValidationExecutor(mismatch=True)
    step, executed, repository, workflow_id = _validation_context(
        validation_executor
    )

    result = step.run(executed)

    assert result.validation is not None
    assert result.validation.report.validation_report.status.value == "FAILED"
    assert result.job.status is MigrationJobStatus.REVIEW_REQUIRED
    assert result.job.stage is MigrationJobStage.VALIDATING
    assert result.job.failure_category == "VALIDATION_MISMATCH"
    assert repository.get_migration_job(JOB_ID) == result.job
    assert repository.get_workflow(workflow_id).status is (
        MigrationWorkflowStatus.VALIDATION_REVIEW_REQUIRED
    )


def test_incomplete_validation_requires_review() -> None:
    validation_executor = IncompleteValidationExecutor()
    step, executed, repository, workflow_id = _validation_context(
        validation_executor
    )

    result = step.run(executed)

    assert result.validation is not None
    assert result.validation.report.validation_report.status.value == "INCOMPLETE"
    assert result.job.status is MigrationJobStatus.REVIEW_REQUIRED
    assert result.job.failure_category == "VALIDATION_INCOMPLETE"
    assert repository.get_workflow(workflow_id).status is (
        MigrationWorkflowStatus.VALIDATION_REVIEW_REQUIRED
    )


def test_uncertain_validation_result_requires_recovery() -> None:
    validation_executor = FakeValidationExecutor(fail=True)
    step, executed, repository, workflow_id = _validation_context(
        validation_executor
    )

    result = step.run(executed)

    assert result.validation is None
    assert result.job.status is MigrationJobStatus.RECOVERY_REQUIRED
    assert result.job.stage is MigrationJobStage.VALIDATING
    assert result.job.failure_category == "VALIDATION_OUTCOME_UNCERTAIN"
    assert repository.get_migration_job(JOB_ID) == result.job
    assert repository.get_workflow(workflow_id).status is (
        MigrationWorkflowStatus.VALIDATION_RECOVERY_REQUIRED
    )
