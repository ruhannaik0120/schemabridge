"""Coordinate durable post-execution validation and reconciliation.

The orchestrator proves that migration execution committed, generates paired
read-only checks from approved mappings, persists a validation plan, claims the
run, and records its report.  Concurrent or indeterminate work is quarantined
so a missing response cannot be mistaken for successful validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from uuid import UUID, uuid4

from schemabridge.models.execution import MigrationExecutionAttemptStatus, MigrationTransactionOutcome
from schemabridge.models.mapping import MappingApprovalStatus, SqlDialect
from schemabridge.models.validation import (
    GeneratedValidationSql,
    MigrationValidationExecutionReport,
    MigrationValidationExecutionRequest,
    MigrationValidationStatus,
)
from schemabridge.models.workflow import (
    AuditActorType,
    MigrationWorkflow,
    MigrationWorkflowStatus,
    WorkflowArtifact,
    WorkflowArtifactType,
)
from schemabridge.models.workflow_validation import WorkflowValidationRun, WorkflowValidationRunStatus
from schemabridge.persistence.artifact_codec import (
    approved_mapping_plan_from_artifact,
    execution_evidence_from_artifact,
    validation_execution_report_from_artifact,
    validation_preview_from_artifact,
)
from schemabridge.persistence.errors import (
    WorkflowExecutionOutcomeUncertainError,
    WorkflowOperationUnavailableError,
    WorkflowRequiredArtifactError,
    WorkflowStaleArtifactReferenceError,
    WorkflowUnsafeValidationQueryError,
    WorkflowValidationAlreadyInProgressError,
    WorkflowValidationExecutionError,
    WorkflowValidationNotReadyError,
    WorkflowValidationOutcomeUncertainError,
)
from schemabridge.persistence.serialization import request_hash, serialize_artifact
from schemabridge.services.workflow_persistence import WorkflowPersistenceService


def _now() -> datetime:
    """Return a timezone-aware timestamp; tests may inject a fixed clock."""

    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class WorkflowValidationResult:
    """Bundle terminal workflow state, run, plan/evidence artifacts, and report."""

    workflow: MigrationWorkflow
    run: WorkflowValidationRun
    plan_artifact: WorkflowArtifact
    evidence_artifact: WorkflowArtifact
    report: MigrationValidationExecutionReport


class WorkflowValidationOrchestrator:
    """Enforce durable validation preconditions around read-only domain services."""

    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        *,
        validation_compiler: Callable,
        validation_execution_service: object,
        clock: Callable[[], datetime] = _now,
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        """Bind persistence, compilation, execution, and deterministic test hooks."""

        self.persistence = persistence
        self.validation_compiler = validation_compiler
        self.validation_execution_service = validation_execution_service
        self.clock = clock
        self.uuid_factory = uuid_factory

    def _artifact(
        self,
        workflow_id: UUID,
        artifact_version: int,
        artifact_type: WorkflowArtifactType,
        *,
        require_latest: bool,
    ) -> WorkflowArtifact:
        """Load a workflow-owned artifact and optionally require the latest version."""

        artifact = self.persistence.get_artifact(workflow_id, artifact_version)
        if artifact is None:
            raise WorkflowRequiredArtifactError()
        if artifact.artifact_type is not artifact_type:
            raise WorkflowStaleArtifactReferenceError()
        if require_latest:
            latest = self.persistence.get_latest_artifact(workflow_id, artifact_type)
            if latest is None or latest.artifact_version != artifact_version:
                raise WorkflowStaleArtifactReferenceError()
        return artifact

    @staticmethod
    def _safe_plan(plan: tuple[GeneratedValidationSql, GeneratedValidationSql]) -> None:
        """Require an ordered PostgreSQL/Snowflake pair of read-only statements."""

        if len(plan) != 2 or plan[0].dialect is not SqlDialect.POSTGRESQL or plan[1].dialect is not SqlDialect.SNOWFLAKE:
            raise WorkflowUnsafeValidationQueryError()
        forbidden = (" INSERT ", " UPDATE ", " DELETE ", " MERGE ", " CREATE ", " ALTER ", " DROP ", " BEGIN ", " COMMIT ", " ROLLBACK ")
        for generated in plan:
            normalized = " " + generated.sql.strip().upper() + " "
            if not normalized.startswith(" SELECT") or ";" in normalized or any(token in normalized for token in forbidden):
                raise WorkflowUnsafeValidationQueryError()

    @staticmethod
    def _command_hash(
        workflow_id: UUID,
        *,
        expected_version: int,
        execution_evidence_artifact_version: int,
        approved_mapping_artifact_version: int,
        source_profile_id: str,
        target_profile_id: str,
        timeout_seconds: int | None,
    ) -> str:
        """Hash all validation inputs used to recognize an exact replay."""

        return request_hash(
            "WORKFLOW_VALIDATE",
            {
                "workflow_id": workflow_id,
                "expected_version": expected_version,
                "execution_evidence_artifact_version": execution_evidence_artifact_version,
                "approved_mapping_artifact_version": approved_mapping_artifact_version,
                "source_profile_id": source_profile_id,
                "target_profile_id": target_profile_id,
                "timeout_seconds": timeout_seconds,
            },
        )

    def _terminal_result(self, workflow_id: UUID, run: WorkflowValidationRun) -> WorkflowValidationResult:
        """Replay a terminal run or surface its recorded recovery condition."""

        if run.status is WorkflowValidationRunStatus.OUTCOME_UNCERTAIN:
            raise WorkflowValidationOutcomeUncertainError()
        if run.status not in {WorkflowValidationRunStatus.SUCCEEDED, WorkflowValidationRunStatus.REVIEW_REQUIRED} or run.evidence_artifact_id is None:
            raise WorkflowValidationAlreadyInProgressError()
        plan_artifact = self._artifact(
            workflow_id,
            run.validation_preview_artifact_version,
            WorkflowArtifactType.VALIDATION_PREVIEW,
            require_latest=False,
        )
        evidence_artifact = self.persistence.get_artifact_by_id(workflow_id, run.evidence_artifact_id)
        if evidence_artifact is None:
            raise WorkflowRequiredArtifactError()
        return WorkflowValidationResult(
            workflow=self.persistence.get_workflow(workflow_id),
            run=run,
            plan_artifact=plan_artifact,
            evidence_artifact=evidence_artifact,
            report=validation_execution_report_from_artifact(evidence_artifact),
        )

    def _complete_uncertain(
        self,
        workflow: MigrationWorkflow,
        run: WorkflowValidationRun,
        *,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
        idempotency_key: str,
        failure_category: str,
    ) -> None:
        """Persist recovery-required state, then raise the public domain failure."""

        self.persistence.complete_validation_run(
            workflow.workflow_id,
            workflow.version,
            run.run_id,
            None,
            MigrationWorkflowStatus.VALIDATION_RECOVERY_REQUIRED,
            completed_at=self.clock(),
            failure_category=failure_category,
            idempotency_key=idempotency_key,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
        )
        raise WorkflowValidationOutcomeUncertainError()

    def validate(
        self,
        workflow_id: UUID,
        *,
        expected_version: int,
        execution_evidence_artifact_version: int,
        approved_mapping_artifact_version: int,
        source_profile_id: str,
        target_profile_id: str,
        timeout_seconds: int | None,
        idempotency_key: str,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
    ) -> WorkflowValidationResult:
        """Plan, claim, run, reconcile, and persist one workflow validation.

        Exact replays return durable evidence.  A new run must reference latest
        committed execution evidence and an approved plan for the workflow's
        configured PostgreSQL and Snowflake profiles.
        """

        command_hash = self._command_hash(
            workflow_id,
            expected_version=expected_version,
            execution_evidence_artifact_version=execution_evidence_artifact_version,
            approved_mapping_artifact_version=approved_mapping_artifact_version,
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
            timeout_seconds=timeout_seconds,
        )
        # A completed first call has already changed workflow state, so replay
        # resolution must happen before current-state eligibility checks.
        replay = self.persistence.get_validation_run_by_command(workflow_id, idempotency_key, command_hash)
        if replay is not None and replay.status in {
            WorkflowValidationRunStatus.SUCCEEDED,
            WorkflowValidationRunStatus.REVIEW_REQUIRED,
            WorkflowValidationRunStatus.OUTCOME_UNCERTAIN,
        }:
            return self._terminal_result(workflow_id, replay)

        workflow = self.persistence.get_workflow(workflow_id)
        if replay is None:
            current = workflow.version == expected_version
            if current and workflow.status is MigrationWorkflowStatus.EXECUTION_RECOVERY_REQUIRED:
                raise WorkflowExecutionOutcomeUncertainError()
            if current and workflow.status is MigrationWorkflowStatus.VALIDATING:
                raise WorkflowValidationAlreadyInProgressError()
            if current and workflow.status is not MigrationWorkflowStatus.EXECUTED:
                raise WorkflowValidationNotReadyError()
            if source_profile_id != workflow.source_profile_id or target_profile_id != workflow.target_profile_id:
                raise WorkflowValidationNotReadyError()
            execution_artifact = self._artifact(
                workflow_id,
                execution_evidence_artifact_version,
                WorkflowArtifactType.EXECUTION_EVIDENCE,
                require_latest=current,
            )
            approved_artifact = self._artifact(
                workflow_id,
                approved_mapping_artifact_version,
                WorkflowArtifactType.APPROVED_MAPPING_PLAN,
                require_latest=current,
            )
            execution = execution_evidence_from_artifact(execution_artifact)
            # Remote statement completion alone is insufficient; validation is
            # allowed only when the persisted evidence proves a committed write.
            if execution.status is not MigrationExecutionAttemptStatus.SUCCEEDED or execution.transaction_outcome is not MigrationTransactionOutcome.COMMITTED:
                raise WorkflowValidationNotReadyError()
            approved = approved_mapping_plan_from_artifact(approved_artifact)
            if not approved.approved_mappings or any(item.status is MappingApprovalStatus.PENDING for item in approved.approvals):
                raise WorkflowValidationNotReadyError()
            if workflow.source_relation.system.casefold() not in {"postgresql", "postgres"} or workflow.target_relation.system.casefold() != "snowflake" or workflow.target_relation.catalog_name is None:
                raise WorkflowValidationNotReadyError()
            try:
                plan = self.validation_compiler(
                    approved,
                    source_schema=workflow.source_relation.schema_name,
                    source_table=workflow.source_relation.object_name,
                    target_database=workflow.target_relation.catalog_name,
                    target_schema=workflow.target_relation.schema_name,
                    target_table=workflow.target_relation.object_name,
                )
            except Exception:
                raise WorkflowUnsafeValidationQueryError() from None
            self._safe_plan(plan)
            # The fingerprint binds the run to execution, approval, generated
            # checks, and profiles without persisting credentials or raw SQL in
            # the idempotency key.
            _, plan_hash = serialize_artifact(WorkflowArtifactType.VALIDATION_PREVIEW, plan)
            fingerprint = request_hash(
                "WORKFLOW_VALIDATION_FINGERPRINT",
                {
                    "workflow_id": workflow_id,
                    "execution_evidence_hash": execution_artifact.payload_sha256,
                    "approved_mapping_hash": approved_artifact.payload_sha256,
                    "validation_plan_hash": plan_hash,
                    "source_profile_id": source_profile_id,
                    "target_profile_id": target_profile_id,
                },
            )
            proposed = WorkflowValidationRun(
                run_id=self.uuid_factory(),
                workflow_id=workflow_id,
                execution_attempt_id=execution.attempt_id,
                execution_evidence_artifact_version=execution_evidence_artifact_version,
                approved_mapping_artifact_version=approved_mapping_artifact_version,
                validation_preview_artifact_version=workflow.latest_artifact_version + 1,
                source_profile_id=source_profile_id,
                target_profile_id=target_profile_id,
                validation_fingerprint=fingerprint,
                status=WorkflowValidationRunStatus.CLAIMED,
                timeout_seconds=timeout_seconds or 30,
                claimed_at=self.clock(),
                actor_type=actor_type,
                idempotency_key=idempotency_key,
                actor_reference=actor_reference,
            )
            # Persist the immutable plan and durable claim before either remote
            # validation query begins.
            workflow, run, plan_artifact, _ = self.persistence.claim_validation_run(
                workflow_id,
                expected_version,
                proposed,
                plan,
                command_hash=command_hash,
                idempotency_key=idempotency_key,
                actor_type=actor_type,
                actor_reference=actor_reference,
                request_id=request_id,
            )
        else:
            run = replay
            plan_artifact = self._artifact(workflow_id, run.validation_preview_artifact_version, WorkflowArtifactType.VALIDATION_PREVIEW, require_latest=False)
            approved_artifact = self._artifact(workflow_id, run.approved_mapping_artifact_version, WorkflowArtifactType.APPROVED_MAPPING_PLAN, require_latest=False)
            approved = approved_mapping_plan_from_artifact(approved_artifact)

        # Execute the persisted plan, not a freshly supplied client payload, so
        # the eventual evidence refers to exactly what the claim recorded.
        plan = validation_preview_from_artifact(plan_artifact)
        self._safe_plan(plan)
        running, acquired = self.persistence.mark_validation_running(run.run_id, self.clock())
        if not acquired:
            # A stale RUNNING claim is uncertain rather than failed: one or both
            # remote queries may have completed after contact was lost.
            if running.status is WorkflowValidationRunStatus.RUNNING and running.running_at is not None and (self.clock() - running.running_at).total_seconds() > running.timeout_seconds:
                self._complete_uncertain(
                    workflow,
                    running,
                    actor_type=actor_type,
                    actor_reference=actor_reference,
                    request_id=request_id,
                    idempotency_key=idempotency_key,
                    failure_category="VALIDATION_TIMEOUT_UNCERTAIN",
                )
            raise WorkflowValidationAlreadyInProgressError()
        request = MigrationValidationExecutionRequest(
            source_profile_id=run.source_profile_id,
            target_profile_id=run.target_profile_id,
            approved_mapping_plan=approved,
            source_schema=workflow.source_relation.schema_name,
            source_table=workflow.source_relation.object_name,
            target_database=workflow.target_relation.catalog_name or "",
            target_schema=workflow.target_relation.schema_name,
            target_table=workflow.target_relation.object_name,
            timeout_seconds=run.timeout_seconds,
            explicitly_approved=True,
        )
        try:
            report = self.validation_execution_service.run(request)
        except Exception:
            self._complete_uncertain(
                workflow,
                running,
                actor_type=actor_type,
                actor_reference=actor_reference,
                request_id=request_id,
                idempotency_key=idempotency_key,
                failure_category="VALIDATION_CONNECTOR_OUTCOME_UNCERTAIN",
            )
        # A report is trusted only when it echoes the claimed plan and profiles;
        # otherwise it cannot serve as evidence for this workflow run.
        if not isinstance(report, MigrationValidationExecutionReport) or (report.source_sql_summary, report.target_sql_summary) != plan or report.source_profile_id != run.source_profile_id or report.target_profile_id != run.target_profile_id:
            self._complete_uncertain(
                workflow,
                running,
                actor_type=actor_type,
                actor_reference=actor_reference,
                request_id=request_id,
                idempotency_key=idempotency_key,
                failure_category="VALIDATION_RESULT_UNTRUSTED",
            )
        final_status = MigrationWorkflowStatus.VALIDATED if report.validation_report.status is MigrationValidationStatus.PASSED else MigrationWorkflowStatus.VALIDATION_REVIEW_REQUIRED
        updated, completed_run, evidence_artifact = self.persistence.complete_validation_run(
            workflow_id,
            workflow.version,
            running.run_id,
            report,
            final_status,
            completed_at=self.clock(),
            failure_category=None,
            idempotency_key=idempotency_key,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
        )
        if evidence_artifact is None:
            raise WorkflowValidationExecutionError()
        return WorkflowValidationResult(updated, completed_run, plan_artifact, evidence_artifact, report)


__all__ = ["WorkflowValidationOrchestrator", "WorkflowValidationResult"]
