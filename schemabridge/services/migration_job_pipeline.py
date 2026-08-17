"""Connect durable background jobs to existing migration workflow operations."""

from __future__ import annotations

from dataclasses import dataclass

from schemabridge.models.migration_job import (
    MigrationJob,
    MigrationJobStage,
    MigrationJobStatus,
)
from schemabridge.models.mapping import TransformationStatementType
from schemabridge.models.validation import MigrationValidationStatus
from schemabridge.models.workflow import (
    AuditActorType,
    MigrationWorkflowStatus,
    WorkflowArtifactType,
)
from schemabridge.persistence.errors import (
    MigrationJobTransitionError,
    WorkflowArtifactValidationError,
    WorkflowExecutionAlreadyInProgressError,
    WorkflowExecutionConfirmedFailureError,
    WorkflowExecutionOutcomeUncertainError,
    WorkflowMappingApprovalRequiredError,
    WorkflowOperationUnavailableError,
    WorkflowPreviewCompilationError,
    WorkflowRequiredArtifactError,
    WorkflowStaleArtifactReferenceError,
    WorkflowStagingCleanupError,
    WorkflowTargetProfileNotWriteCapableError,
    WorkflowTargetProfileUnavailableError,
    WorkflowTransportAlreadyInProgressError,
    WorkflowTransportConfirmedFailureError,
    WorkflowTransportOutcomeUncertainError,
    WorkflowUnsafeGeneratedStatementError,
    WorkflowUnsupportedExecutionConnectorError,
    WorkflowUnsafeValidationQueryError,
    WorkflowValidationAlreadyInProgressError,
    WorkflowValidationExecutionError,
    WorkflowValidationNotReadyError,
    WorkflowValidationOutcomeUncertainError,
)
from schemabridge.services.migration_jobs import (
    MigrationJobCompletionService,
    MigrationJobProgressService,
)
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from schemabridge.services.workflow_execution import (
    WorkflowExecutionOrchestrator,
    WorkflowExecutionResult,
)
from schemabridge.services.workflow_orchestration import (
    WorkflowPlanningOrchestrator,
    WorkflowPlanningResult,
)
from schemabridge.services.workflow_transport import (
    WorkflowTransportOrchestrator,
    WorkflowTransportResult,
)
from schemabridge.services.workflow_validation import (
    WorkflowValidationOrchestrator,
    WorkflowValidationResult,
)
from schemabridge.transport.base import BatchTransportError


@dataclass(frozen=True, slots=True)
class MigrationJobStagingResult:
    """Return either staged transport evidence or a classified terminal job."""

    job: MigrationJob
    transport: WorkflowTransportResult | None


class MigrationJobStagingStep:
    """Verify a claimed job and run its connector-neutral staging transport."""

    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        transport_orchestrator: WorkflowTransportOrchestrator,
        *,
        completion_service: MigrationJobCompletionService,
    ) -> None:
        self.persistence = persistence
        self.transport_orchestrator = transport_orchestrator
        self.progress = MigrationJobProgressService(persistence)
        self.completion = completion_service

    def _prepare(self, job: MigrationJob) -> None:
        durable = self.persistence.get_migration_job(job.job_id)
        if (
            durable != job
            or job.status is not MigrationJobStatus.RUNNING
            or job.stage is not MigrationJobStage.PREPARING
        ):
            raise MigrationJobTransitionError()

        workflow = self.persistence.get_workflow(job.workflow_id)
        if (
            workflow.version != job.expected_workflow_version
            or workflow.status is not MigrationWorkflowStatus.MAPPING_APPROVED
            or workflow.source_profile_id != job.source_profile_id
            or workflow.target_profile_id != job.target_profile_id
        ):
            raise WorkflowOperationUnavailableError()

        for version, artifact_type in (
            (
                job.source_discovery_artifact_version,
                WorkflowArtifactType.SOURCE_DISCOVERY,
            ),
            (
                job.approved_mapping_artifact_version,
                WorkflowArtifactType.APPROVED_MAPPING_PLAN,
            ),
        ):
            artifact = self.persistence.get_artifact(job.workflow_id, version)
            if artifact is None:
                raise WorkflowRequiredArtifactError()
            latest = self.persistence.get_latest_artifact(job.workflow_id, artifact_type)
            if (
                artifact.artifact_type is not artifact_type
                or latest is None
                or latest.artifact_version != version
            ):
                raise WorkflowStaleArtifactReferenceError()

    @staticmethod
    def _staging_key(job: MigrationJob) -> str:
        return f"job-{job.job_id.hex}-staging"

    def run(self, job: MigrationJob) -> MigrationJobStagingResult:
        """Load managed staging or durably classify why that step stopped."""

        try:
            self._prepare(job)
        except (
            WorkflowOperationUnavailableError,
            WorkflowRequiredArtifactError,
            WorkflowStaleArtifactReferenceError,
        ):
            failed = self.completion.fail(
                job.job_id,
                expected_stage=MigrationJobStage.PREPARING,
                failure_category="JOB_PREPARATION_FAILED",
            )
            return MigrationJobStagingResult(failed, None)

        staging = self.progress.advance(
            job.job_id,
            expected_stage=MigrationJobStage.PREPARING,
            new_stage=MigrationJobStage.STAGING,
        )
        try:
            transport = self.transport_orchestrator.run(
                job.workflow_id,
                expected_version=job.expected_workflow_version,
                source_discovery_artifact_version=(
                    job.source_discovery_artifact_version
                ),
                approved_mapping_artifact_version=(
                    job.approved_mapping_artifact_version
                ),
                source_profile_id=job.source_profile_id,
                target_profile_id=job.target_profile_id,
                batch_size=job.batch_size,
                timeout_seconds=job.timeout_seconds,
                idempotency_key=self._staging_key(job),
                actor_type=AuditActorType.SERVICE,
                actor_reference="migration-job-worker",
            )
        except WorkflowTransportConfirmedFailureError:
            failed = self.completion.fail(
                job.job_id,
                expected_stage=MigrationJobStage.STAGING,
                failure_category="STAGING_LOAD_FAILED",
            )
            return MigrationJobStagingResult(failed, None)
        except (
            WorkflowTransportAlreadyInProgressError,
            WorkflowTransportOutcomeUncertainError,
        ):
            uncertain = self.completion.require_recovery(
                job.job_id,
                expected_stage=MigrationJobStage.STAGING,
                failure_category="STAGING_OUTCOME_UNCERTAIN",
            )
            return MigrationJobStagingResult(uncertain, None)
        except (
            BatchTransportError,
            WorkflowOperationUnavailableError,
            WorkflowRequiredArtifactError,
            WorkflowStaleArtifactReferenceError,
        ):
            failed = self.completion.fail(
                job.job_id,
                expected_stage=MigrationJobStage.STAGING,
                failure_category="STAGING_PREPARATION_FAILED",
            )
            return MigrationJobStagingResult(failed, None)

        transforming = self.progress.advance(
            staging.job_id,
            expected_stage=MigrationJobStage.STAGING,
            new_stage=MigrationJobStage.TRANSFORMING,
        )
        return MigrationJobStagingResult(transforming, transport)


@dataclass(frozen=True, slots=True)
class MigrationJobExecutionStepResult:
    """Return successful execution evidence or a classified terminal job."""

    job: MigrationJob
    preview: WorkflowPlanningResult | None
    execution: WorkflowExecutionResult | None


class MigrationJobExecutionStep:
    """Compile, execute, and confirm cleanup for one successfully staged job."""

    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        planning_orchestrator: WorkflowPlanningOrchestrator,
        execution_orchestrator: WorkflowExecutionOrchestrator,
        *,
        completion_service: MigrationJobCompletionService,
    ) -> None:
        self.persistence = persistence
        self.planning_orchestrator = planning_orchestrator
        self.execution_orchestrator = execution_orchestrator
        self.progress = MigrationJobProgressService(persistence)
        self.completion = completion_service

    @staticmethod
    def _key(job: MigrationJob, operation: str) -> str:
        return f"job-{job.job_id.hex}-{operation}"

    def run(
        self,
        staged: MigrationJobStagingResult,
    ) -> MigrationJobExecutionStepResult:
        """Run approved staging-to-target execution and confirm cleanup."""

        job = staged.job
        durable = self.persistence.get_migration_job(job.job_id)
        if (
            staged.transport is None
            or durable != job
            or job.status is not MigrationJobStatus.RUNNING
            or job.stage is not MigrationJobStage.TRANSFORMING
        ):
            raise MigrationJobTransitionError()

        try:
            preview = self.planning_orchestrator.preview_transformation(
                job.workflow_id,
                expected_version=staged.transport.workflow.version,
                approved_mapping_artifact_version=(
                    job.approved_mapping_artifact_version
                ),
                staging_database=None,
                staging_schema=None,
                staging_table=None,
                statement_type=TransformationStatementType.INSERT_SELECT,
                idempotency_key=self._key(job, "transformation"),
                actor_type=AuditActorType.SERVICE,
                actor_reference="migration-job-worker",
                request_id=None,
            )
        except (
            WorkflowArtifactValidationError,
            WorkflowMappingApprovalRequiredError,
            WorkflowOperationUnavailableError,
            WorkflowPreviewCompilationError,
            WorkflowRequiredArtifactError,
            WorkflowStaleArtifactReferenceError,
        ):
            failed = self.completion.fail(
                job.job_id,
                expected_stage=MigrationJobStage.TRANSFORMING,
                failure_category="TRANSFORMATION_COMPILATION_FAILED",
            )
            return MigrationJobExecutionStepResult(failed, None, None)

        executing = self.progress.advance(
            job.job_id,
            expected_stage=MigrationJobStage.TRANSFORMING,
            new_stage=MigrationJobStage.EXECUTING,
        )
        try:
            execution = self.execution_orchestrator.execute(
                job.workflow_id,
                expected_version=preview.workflow.version,
                approved_mapping_artifact_version=(
                    job.approved_mapping_artifact_version
                ),
                transformation_preview_artifact_version=(
                    preview.artifact.artifact_version
                ),
                target_profile_id=job.target_profile_id,
                timeout_seconds=job.timeout_seconds,
                idempotency_key=self._key(job, "execution"),
                actor_type=AuditActorType.SERVICE,
                actor_reference="migration-job-worker",
                request_id=None,
            )
        except WorkflowExecutionConfirmedFailureError:
            failed = self.completion.fail(
                job.job_id,
                expected_stage=MigrationJobStage.EXECUTING,
                failure_category="TARGET_EXECUTION_FAILED",
            )
            return MigrationJobExecutionStepResult(failed, preview, None)
        except (
            WorkflowExecutionAlreadyInProgressError,
            WorkflowExecutionOutcomeUncertainError,
        ):
            uncertain = self.completion.require_recovery(
                job.job_id,
                expected_stage=MigrationJobStage.EXECUTING,
                failure_category="TARGET_OUTCOME_UNCERTAIN",
            )
            return MigrationJobExecutionStepResult(uncertain, preview, None)
        except WorkflowStagingCleanupError:
            cleanup = self.progress.advance(
                job.job_id,
                expected_stage=MigrationJobStage.EXECUTING,
                new_stage=MigrationJobStage.CLEANING_UP,
            )
            uncertain = self.completion.require_recovery(
                cleanup.job_id,
                expected_stage=MigrationJobStage.CLEANING_UP,
                failure_category="STAGING_CLEANUP_FAILED",
            )
            return MigrationJobExecutionStepResult(uncertain, preview, None)
        except (
            WorkflowOperationUnavailableError,
            WorkflowRequiredArtifactError,
            WorkflowStaleArtifactReferenceError,
            WorkflowTargetProfileNotWriteCapableError,
            WorkflowTargetProfileUnavailableError,
            WorkflowUnsafeGeneratedStatementError,
            WorkflowUnsupportedExecutionConnectorError,
        ):
            failed = self.completion.fail(
                job.job_id,
                expected_stage=MigrationJobStage.EXECUTING,
                failure_category="TARGET_EXECUTION_PREPARATION_FAILED",
            )
            return MigrationJobExecutionStepResult(failed, preview, None)

        cleanup = self.progress.advance(
            executing.job_id,
            expected_stage=MigrationJobStage.EXECUTING,
            new_stage=MigrationJobStage.CLEANING_UP,
        )
        if execution.cleanup_evidence is None:
            uncertain = self.completion.require_recovery(
                cleanup.job_id,
                expected_stage=MigrationJobStage.CLEANING_UP,
                failure_category="STAGING_CLEANUP_NOT_CONFIRMED",
            )
            return MigrationJobExecutionStepResult(uncertain, preview, execution)
        validating = self.progress.advance(
            cleanup.job_id,
            expected_stage=MigrationJobStage.CLEANING_UP,
            new_stage=MigrationJobStage.VALIDATING,
        )
        return MigrationJobExecutionStepResult(validating, preview, execution)


@dataclass(frozen=True, slots=True)
class MigrationJobValidationStepResult:
    """Return the terminal job and validation evidence when it is trusted."""

    job: MigrationJob
    validation: WorkflowValidationResult | None


class MigrationJobValidationStep:
    """Run paired aggregate validation and classify the final job outcome."""

    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        validation_orchestrator: WorkflowValidationOrchestrator,
        *,
        completion_service: MigrationJobCompletionService,
    ) -> None:
        self.persistence = persistence
        self.validation_orchestrator = validation_orchestrator
        self.completion = completion_service

    @staticmethod
    def _key(job: MigrationJob) -> str:
        return f"job-{job.job_id.hex}-validation"

    def run(
        self,
        executed: MigrationJobExecutionStepResult,
    ) -> MigrationJobValidationStepResult:
        """Validate one committed execution and finish its background job."""

        job = executed.job
        durable = self.persistence.get_migration_job(job.job_id)
        if (
            executed.execution is None
            or executed.execution.cleanup_evidence is None
            or durable != job
            or job.status is not MigrationJobStatus.RUNNING
            or job.stage is not MigrationJobStage.VALIDATING
        ):
            raise MigrationJobTransitionError()

        try:
            validation = self.validation_orchestrator.validate(
                job.workflow_id,
                expected_version=executed.execution.workflow.version,
                execution_evidence_artifact_version=(
                    executed.execution.artifact.artifact_version
                ),
                approved_mapping_artifact_version=(
                    job.approved_mapping_artifact_version
                ),
                source_profile_id=job.source_profile_id,
                target_profile_id=job.target_profile_id,
                timeout_seconds=job.timeout_seconds,
                idempotency_key=self._key(job),
                actor_type=AuditActorType.SERVICE,
                actor_reference="migration-job-worker",
                request_id=None,
            )
        except (
            WorkflowExecutionOutcomeUncertainError,
            WorkflowValidationAlreadyInProgressError,
            WorkflowValidationExecutionError,
            WorkflowValidationOutcomeUncertainError,
        ):
            uncertain = self.completion.require_recovery(
                job.job_id,
                expected_stage=MigrationJobStage.VALIDATING,
                failure_category="VALIDATION_OUTCOME_UNCERTAIN",
            )
            return MigrationJobValidationStepResult(uncertain, None)
        except (
            WorkflowOperationUnavailableError,
            WorkflowRequiredArtifactError,
            WorkflowStaleArtifactReferenceError,
            WorkflowUnsafeValidationQueryError,
            WorkflowValidationNotReadyError,
        ):
            review = self.completion.require_review(
                job.job_id,
                failure_category="VALIDATION_PREPARATION_FAILED",
            )
            return MigrationJobValidationStepResult(review, None)

        report_status = validation.report.validation_report.status
        if report_status is MigrationValidationStatus.PASSED:
            completed = self.completion.succeed(job.job_id)
        elif report_status is MigrationValidationStatus.FAILED:
            completed = self.completion.require_review(
                job.job_id,
                failure_category="VALIDATION_MISMATCH",
            )
        else:
            completed = self.completion.require_review(
                job.job_id,
                failure_category="VALIDATION_INCOMPLETE",
            )
        return MigrationJobValidationStepResult(completed, validation)


class MigrationJobPipelineProcessor:
    """Run the existing staging, execution, cleanup, and validation steps."""

    _TERMINAL = {
        MigrationJobStatus.SUCCEEDED,
        MigrationJobStatus.FAILED,
        MigrationJobStatus.REVIEW_REQUIRED,
        MigrationJobStatus.RECOVERY_REQUIRED,
    }

    def __init__(
        self,
        staging_step: MigrationJobStagingStep,
        execution_step: MigrationJobExecutionStep,
        validation_step: MigrationJobValidationStep,
    ) -> None:
        self.staging_step = staging_step
        self.execution_step = execution_step
        self.validation_step = validation_step

    def process(self, job: MigrationJob) -> MigrationJob:
        """Process one claimed job, stopping at the first terminal outcome."""

        staged = self.staging_step.run(job)
        if staged.job.status in self._TERMINAL:
            return staged.job

        executed = self.execution_step.run(staged)
        if executed.job.status in self._TERMINAL:
            return executed.job

        validated = self.validation_step.run(executed)
        if validated.job.status not in self._TERMINAL:
            raise MigrationJobTransitionError()
        return validated.job


__all__ = [
    "MigrationJobExecutionStep",
    "MigrationJobExecutionStepResult",
    "MigrationJobPipelineProcessor",
    "MigrationJobStagingResult",
    "MigrationJobStagingStep",
    "MigrationJobValidationStep",
    "MigrationJobValidationStepResult",
]
