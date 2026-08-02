"""Approval-gated durable orchestration for one target migration execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from uuid import UUID, uuid4

from models.execution import (
    MigrationExecutionAttempt,
    MigrationExecutionAttemptStatus,
    MigrationExecutionEvidence,
    MigrationTransactionOutcome,
)
from models.mapping import MappingApprovalStatus, TransformationStatementType
from models.workflow import (
    AuditActorType,
    MigrationWorkflow,
    MigrationWorkflowStatus,
    WorkflowArtifact,
    WorkflowArtifactType,
)
from persistence.artifact_codec import (
    approved_mapping_plan_from_artifact,
    execution_evidence_from_artifact,
    transformation_sql_from_artifact,
)
from persistence.errors import (
    WorkflowExecutionAlreadyInProgressError,
    WorkflowExecutionConfirmedFailureError,
    WorkflowExecutionOutcomeUncertainError,
    WorkflowMappingApprovalRequiredError,
    WorkflowOperationUnavailableError,
    WorkflowRequiredArtifactError,
    WorkflowStaleArtifactReferenceError,
    WorkflowUnsafeGeneratedStatementError,
)
from persistence.serialization import request_hash
from services.migration_execution import (
    TargetExecutionDisposition,
    TargetExecutionResult,
)
from services.workflow_persistence import WorkflowPersistenceService


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class WorkflowExecutionResult:
    workflow: MigrationWorkflow
    attempt: MigrationExecutionAttempt
    artifact: WorkflowArtifact
    evidence: MigrationExecutionEvidence


class WorkflowExecutionOrchestrator:
    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        *,
        transformation_compiler: object,
        execution_service: object,
        clock: Callable[[], datetime] = _now,
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self.persistence = persistence
        self.transformation_compiler = transformation_compiler
        self.execution_service = execution_service
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

    def _terminal_result(
        self,
        workflow_id: UUID,
        attempt: MigrationExecutionAttempt,
    ) -> WorkflowExecutionResult:
        if attempt.status is MigrationExecutionAttemptStatus.FAILED_ROLLED_BACK:
            raise WorkflowExecutionConfirmedFailureError()
        if attempt.status is MigrationExecutionAttemptStatus.OUTCOME_UNCERTAIN:
            raise WorkflowExecutionOutcomeUncertainError()
        if (
            attempt.status is not MigrationExecutionAttemptStatus.SUCCEEDED
            or attempt.evidence_artifact_id is None
        ):
            raise WorkflowExecutionAlreadyInProgressError()
        artifact = self.persistence.get_artifact_by_id(
            workflow_id, attempt.evidence_artifact_id
        )
        if artifact is None:
            raise WorkflowRequiredArtifactError()
        evidence = execution_evidence_from_artifact(artifact)
        return WorkflowExecutionResult(
            self.persistence.get_workflow(workflow_id), attempt, artifact, evidence
        )

    @staticmethod
    def _command_hash(
        workflow_id: UUID,
        *,
        expected_version: int,
        approved_mapping_artifact_version: int,
        transformation_preview_artifact_version: int,
        target_profile_id: str,
        timeout_seconds: int | None,
    ) -> str:
        return request_hash(
            "WORKFLOW_EXECUTE",
            {
                "workflow_id": workflow_id,
                "expected_version": expected_version,
                "approved_mapping_artifact_version": approved_mapping_artifact_version,
                "transformation_preview_artifact_version": transformation_preview_artifact_version,
                "target_profile_id": target_profile_id,
                "timeout_seconds": timeout_seconds,
            },
        )

    def _complete(
        self,
        workflow: MigrationWorkflow,
        attempt: MigrationExecutionAttempt,
        *,
        result: TargetExecutionResult,
        connector_type: str,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
        idempotency_key: str,
    ) -> WorkflowExecutionResult:
        completed_at = self.clock()
        started_at = attempt.running_at
        if started_at is None:
            raise WorkflowExecutionOutcomeUncertainError()
        if result.disposition is TargetExecutionDisposition.SUCCEEDED:
            status = MigrationExecutionAttemptStatus.SUCCEEDED
            transaction = MigrationTransactionOutcome.COMMITTED
            new_status = MigrationWorkflowStatus.EXECUTED
            failure_category = None
        elif (
            result.disposition
            is TargetExecutionDisposition.CONFIRMED_FAILED_ROLLED_BACK
        ):
            status = MigrationExecutionAttemptStatus.FAILED_ROLLED_BACK
            transaction = MigrationTransactionOutcome.ROLLED_BACK
            new_status = MigrationWorkflowStatus.EXECUTION_READY
            failure_category = result.failure_category or "TARGET_EXECUTION_FAILED"
        else:
            status = MigrationExecutionAttemptStatus.OUTCOME_UNCERTAIN
            transaction = MigrationTransactionOutcome.UNKNOWN
            new_status = MigrationWorkflowStatus.EXECUTION_RECOVERY_REQUIRED
            failure_category = result.failure_category or "TARGET_OUTCOME_UNCERTAIN"
        evidence = MigrationExecutionEvidence(
            attempt_id=attempt.attempt_id,
            workflow_id=workflow.workflow_id,
            status=status,
            statement_count=1,
            affected_rows=result.affected_rows if status is MigrationExecutionAttemptStatus.SUCCEEDED else None,
            target_relation=(
                workflow.target_relation.catalog_name,
                workflow.target_relation.schema_name,
                workflow.target_relation.object_name,
            ),
            target_profile_id=workflow.target_profile_id,
            connector_type=connector_type,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=max(0, int((completed_at - started_at).total_seconds() * 1000)),
            transaction_outcome=transaction,
            approved_mapping_artifact_version=attempt.approved_mapping_artifact_version,
            transformation_preview_artifact_version=attempt.transformation_preview_artifact_version,
            execution_fingerprint=attempt.execution_fingerprint,
            failure_category=failure_category,
        )
        updated, updated_attempt, artifact = self.persistence.complete_execution_attempt(
            workflow.workflow_id,
            workflow.version,
            attempt.attempt_id,
            evidence,
            new_status,
            idempotency_key=idempotency_key,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
        )
        if status is MigrationExecutionAttemptStatus.FAILED_ROLLED_BACK:
            raise WorkflowExecutionConfirmedFailureError()
        if status is MigrationExecutionAttemptStatus.OUTCOME_UNCERTAIN:
            raise WorkflowExecutionOutcomeUncertainError()
        return WorkflowExecutionResult(updated, updated_attempt, artifact, evidence)

    def execute(
        self,
        workflow_id: UUID,
        *,
        expected_version: int,
        approved_mapping_artifact_version: int,
        transformation_preview_artifact_version: int,
        target_profile_id: str,
        timeout_seconds: int | None,
        idempotency_key: str,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
    ) -> WorkflowExecutionResult:
        command_hash = self._command_hash(
            workflow_id,
            expected_version=expected_version,
            approved_mapping_artifact_version=approved_mapping_artifact_version,
            transformation_preview_artifact_version=transformation_preview_artifact_version,
            target_profile_id=target_profile_id,
            timeout_seconds=timeout_seconds,
        )
        replay = self.persistence.get_execution_attempt_by_command(
            workflow_id, idempotency_key, command_hash
        )
        if replay is not None and replay.status in {
            MigrationExecutionAttemptStatus.SUCCEEDED,
            MigrationExecutionAttemptStatus.FAILED_ROLLED_BACK,
            MigrationExecutionAttemptStatus.OUTCOME_UNCERTAIN,
        }:
            return self._terminal_result(workflow_id, replay)

        workflow = self.persistence.get_workflow(workflow_id)
        current_command = workflow.version == expected_version
        if current_command and workflow.status in {
            MigrationWorkflowStatus.DRAFT,
            MigrationWorkflowStatus.DISCOVERED,
            MigrationWorkflowStatus.MAPPING_PROPOSED,
        }:
            raise WorkflowMappingApprovalRequiredError()
        if current_command and workflow.status is MigrationWorkflowStatus.EXECUTING:
            raise WorkflowExecutionAlreadyInProgressError()
        if (
            current_command
            and workflow.status is MigrationWorkflowStatus.EXECUTION_RECOVERY_REQUIRED
        ):
            raise WorkflowExecutionOutcomeUncertainError()
        approved_artifact = self._artifact(
            workflow_id,
            approved_mapping_artifact_version,
            WorkflowArtifactType.APPROVED_MAPPING_PLAN,
            require_latest=current_command,
        )
        preview_artifact = self._artifact(
            workflow_id,
            transformation_preview_artifact_version,
            WorkflowArtifactType.TRANSFORMATION_PREVIEW,
            require_latest=current_command,
        )
        if current_command and workflow.status is not MigrationWorkflowStatus.EXECUTION_READY:
            raise WorkflowOperationUnavailableError()
        if target_profile_id != workflow.target_profile_id:
            raise WorkflowUnsafeGeneratedStatementError()
        approved = approved_mapping_plan_from_artifact(approved_artifact)
        if not approved.approved_mappings or any(
            item.status is MappingApprovalStatus.PENDING for item in approved.approvals
        ):
            raise WorkflowMappingApprovalRequiredError()
        preview = transformation_sql_from_artifact(preview_artifact)
        if preview.statement_type is not TransformationStatementType.INSERT_SELECT:
            raise WorkflowUnsafeGeneratedStatementError()
        if preview.approved_plan_version != approved.version:
            raise WorkflowStaleArtifactReferenceError()
        try:
            expected_preview = self.transformation_compiler.compile_insert_select(
                approved,
                staging_database=preview.source_relation[0],
                staging_schema=preview.source_relation[1],
                staging_table=preview.source_relation[2],
            )
        except Exception:
            raise WorkflowUnsafeGeneratedStatementError() from None
        if expected_preview != preview:
            raise WorkflowUnsafeGeneratedStatementError()
        self.execution_service.validate_preview(preview)
        target = self.execution_service.prepare(
            target_profile_id,
            target_database=workflow.target_relation.catalog_name,
            target_system=workflow.target_relation.system,
            timeout_seconds=timeout_seconds,
        )
        fingerprint = request_hash(
            "WORKFLOW_EXECUTION_FINGERPRINT",
            {
                "workflow_id": workflow_id,
                "approved_mapping_hash": approved_artifact.payload_sha256,
                "preview_hash": preview_artifact.payload_sha256,
                "target_profile_id": target_profile_id,
            },
        )
        claimed_at = self.clock()
        proposed_attempt = MigrationExecutionAttempt(
            attempt_id=self.uuid_factory(),
            workflow_id=workflow_id,
            approved_mapping_artifact_version=approved_mapping_artifact_version,
            transformation_preview_artifact_version=transformation_preview_artifact_version,
            target_profile_id=target_profile_id,
            execution_fingerprint=fingerprint,
            status=MigrationExecutionAttemptStatus.CLAIMED,
            timeout_seconds=target.timeout_seconds,
            claimed_at=claimed_at,
            actor_type=actor_type,
            idempotency_key=idempotency_key,
            actor_reference=actor_reference,
        )
        claimed_workflow, attempt, _ = self.persistence.claim_execution_attempt(
            workflow_id,
            expected_version,
            proposed_attempt,
            command_hash=command_hash,
            idempotency_key=idempotency_key,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
        )
        if (
            attempt.approved_mapping_artifact_version
            != approved_mapping_artifact_version
            or attempt.transformation_preview_artifact_version
            != transformation_preview_artifact_version
            or attempt.target_profile_id != target_profile_id
            or attempt.execution_fingerprint != fingerprint
        ):
            raise WorkflowUnsafeGeneratedStatementError()
        if attempt.status in {
            MigrationExecutionAttemptStatus.SUCCEEDED,
            MigrationExecutionAttemptStatus.FAILED_ROLLED_BACK,
            MigrationExecutionAttemptStatus.OUTCOME_UNCERTAIN,
        }:
            return self._terminal_result(workflow_id, attempt)
        running, acquired = self.persistence.mark_execution_running(
            attempt.attempt_id, self.clock()
        )
        if not acquired:
            if (
                running.status is MigrationExecutionAttemptStatus.RUNNING
                and running.running_at is not None
                and (self.clock() - running.running_at).total_seconds()
                > running.timeout_seconds
            ):
                return self._complete(
                    claimed_workflow,
                    running,
                    result=TargetExecutionResult(
                        TargetExecutionDisposition.OUTCOME_UNCERTAIN,
                        failure_category="TARGET_EXECUTION_TIMEOUT_UNCERTAIN",
                    ),
                    connector_type=target.connector_type,
                    actor_type=actor_type,
                    actor_reference=actor_reference,
                    request_id=request_id,
                    idempotency_key=idempotency_key,
                )
            raise WorkflowExecutionAlreadyInProgressError()
        result = self.execution_service.execute(target, preview)
        return self._complete(
            claimed_workflow,
            running,
            result=result,
            connector_type=target.connector_type,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
            idempotency_key=idempotency_key,
        )


__all__ = ["WorkflowExecutionOrchestrator", "WorkflowExecutionResult"]
