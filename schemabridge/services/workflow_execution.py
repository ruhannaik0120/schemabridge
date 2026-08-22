"""Coordinate one approval-gated, durable target migration execution.

The orchestrator verifies workflow state and immutable artifact lineage,
recompiles the approved SQL, claims execution in the control plane, crosses the
target database boundary once, and records evidence. Confirmed rollback is retryable;
an outcome that cannot be proven moves the workflow into manual recovery.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from uuid import UUID, uuid4

from schemabridge.models.execution import (
    MigrationExecutionAttempt,
    MigrationExecutionAttemptStatus,
    MigrationExecutionEvidence,
    MigrationTransactionOutcome,
)
from schemabridge.models.mapping import MappingApprovalStatus, TransformationStatementType
from schemabridge.models.workflow import (
    AuditActorType,
    MigrationWorkflow,
    MigrationWorkflowStatus,
    WorkflowArtifact,
    WorkflowArtifactType,
)
from schemabridge.persistence.artifact_codec import (
    approved_mapping_plan_from_artifact,
    execution_evidence_from_artifact,
    transformation_sql_from_artifact,
    workflow_staging_cleanup_evidence_from_artifact,
    workflow_transport_evidence_from_artifact,
)
from schemabridge.persistence.errors import (
    WorkflowExecutionAlreadyInProgressError,
    WorkflowExecutionConfirmedFailureError,
    WorkflowExecutionOutcomeUncertainError,
    WorkflowMappingApprovalRequiredError,
    WorkflowOperationUnavailableError,
    WorkflowRequiredArtifactError,
    WorkflowStaleArtifactReferenceError,
    WorkflowUnsafeGeneratedStatementError,
    WorkflowStagingCleanupError,
    WorkflowUnsupportedExecutionConnectorError,
)
from schemabridge.persistence.serialization import request_hash
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from schemabridge.models.workflow_transport import WorkflowStagingCleanupEvidence
from schemabridge.models.transport import TransportRelation
from schemabridge.target_execution import (
    TargetExecutionDisposition,
    TargetExecutionRegistry,
    TargetExecutionResult,
    UnsupportedTargetSystemError,
)


def _now() -> datetime:
    """Return a timezone-aware timestamp; injectable clocks keep tests stable."""

    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class WorkflowExecutionResult:
    """Bundle the terminal workflow, attempt, artifact, and typed evidence."""

    workflow: MigrationWorkflow
    attempt: MigrationExecutionAttempt
    artifact: WorkflowArtifact
    evidence: MigrationExecutionEvidence
    cleanup_artifact: WorkflowArtifact | None = None
    cleanup_evidence: WorkflowStagingCleanupEvidence | None = None


class WorkflowExecutionOrchestrator:
    """Enforce the approval and concurrency boundary around target writes."""

    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        *,
        target_registry: TargetExecutionRegistry,
        execution_service: object,
        staging_cleanup_service: object | None = None,
        clock: Callable[[], datetime] = _now,
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        """Bind persistence, pure compilation, remote execution, and test clocks."""

        self.persistence = persistence
        self.target_registry = target_registry
        self.execution_service = execution_service
        self.staging_cleanup_service = staging_cleanup_service
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
        """Load a referenced artifact and optionally require its latest version."""

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
        """Replay the externally visible outcome of a terminal attempt."""

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
        """Hash every execution input used to identify an exact replay."""

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

    def _cleanup_after_commit(
        self,
        result: WorkflowExecutionResult,
        *,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
    ) -> WorkflowExecutionResult:
        """Drop managed staging once and persist immutable success evidence."""

        if self.staging_cleanup_service is None:
            return result
        workflow_id = result.workflow.workflow_id
        staging_artifact = self.persistence.get_latest_artifact(
            workflow_id, WorkflowArtifactType.STAGING_LOAD_EVIDENCE
        )
        if staging_artifact is None:
            # Backward-compatible workflows may use externally managed staging.
            return result
        staging = workflow_transport_evidence_from_artifact(staging_artifact)
        if (
            staging.approved_mapping_artifact_version
            != result.evidence.approved_mapping_artifact_version
            or staging.target_profile_id != result.evidence.target_profile_id
        ):
            raise WorkflowStagingCleanupError()
        cleanup_fingerprint = request_hash(
            "WORKFLOW_STAGING_CLEANUP_FINGERPRINT",
            {
                "workflow_id": workflow_id,
                "transport_artifact_hash": staging_artifact.payload_sha256,
                "execution_artifact_hash": result.artifact.payload_sha256,
                "staging_relation": staging.staging_relation,
                "target_profile_id": staging.target_profile_id,
            },
        )
        existing_artifact = self.persistence.get_latest_artifact(
            workflow_id, WorkflowArtifactType.STAGING_CLEANUP_EVIDENCE
        )
        if existing_artifact is not None:
            existing = workflow_staging_cleanup_evidence_from_artifact(
                existing_artifact
            )
            if (
                existing.execution_attempt_id == result.attempt.attempt_id
                and existing.transport_attempt_id == staging.attempt_id
                and existing.cleanup_fingerprint == cleanup_fingerprint
            ):
                return WorkflowExecutionResult(
                    workflow=self.persistence.get_workflow(workflow_id),
                    attempt=result.attempt,
                    artifact=result.artifact,
                    evidence=result.evidence,
                    cleanup_artifact=existing_artifact,
                    cleanup_evidence=existing,
                )
            raise WorkflowStagingCleanupError()
        started_at = self.clock()
        try:
            self.staging_cleanup_service.cleanup_staging(
                target_profile_id=staging.target_profile_id,
                target_database=staging.staging_relation.catalog_name,
                relation=staging.staging_relation,
                timeout_seconds=result.attempt.timeout_seconds,
            )
        except Exception:
            # The target commit is already durable. An exact execution replay
            # retries only this idempotent DROP TABLE IF EXISTS operation.
            raise WorkflowStagingCleanupError() from None
        completed_at = self.clock()
        evidence = WorkflowStagingCleanupEvidence(
            workflow_id=workflow_id,
            transport_attempt_id=staging.attempt_id,
            execution_attempt_id=result.attempt.attempt_id,
            staging_relation=staging.staging_relation,
            target_profile_id=staging.target_profile_id,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=max(
                0, int((completed_at - started_at).total_seconds() * 1000)
            ),
            cleanup_fingerprint=cleanup_fingerprint,
        )
        current = self.persistence.get_workflow(workflow_id)
        updated, artifact = self.persistence.record_staging_cleanup(
            workflow_id,
            current.version,
            evidence,
            command_hash=request_hash(
                "WORKFLOW_RECORD_STAGING_CLEANUP",
                {
                    "workflow_id": workflow_id,
                    "expected_version": current.version,
                    "cleanup_fingerprint": cleanup_fingerprint,
                },
            ),
            idempotency_key=f"cleanup-{result.attempt.attempt_id}",
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
        )
        return WorkflowExecutionResult(
            workflow=updated,
            attempt=result.attempt,
            artifact=result.artifact,
            evidence=result.evidence,
            cleanup_artifact=artifact,
            cleanup_evidence=evidence,
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
        """Classify a remote result and durably complete its claimed attempt.

        The persistence call records the terminal attempt, evidence artifact,
        workflow transition, audit event, and idempotency result together.
        """

        completed_at = self.clock()
        started_at = attempt.running_at
        if started_at is None:
            raise WorkflowExecutionOutcomeUncertainError()
        # Only a proven commit or rollback permits a definitive terminal
        # classification.  All other remote outcomes enter recovery quarantine.
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
        """Verify, claim, execute, and persist one approved transformation.

        Exact idempotent replays return the recorded terminal outcome.  New
        commands must reference the latest approved mapping and preview, match
        the workflow's target profile, and acquire the durable execution claim.
        """

        command_hash = self._command_hash(
            workflow_id,
            expected_version=expected_version,
            approved_mapping_artifact_version=approved_mapping_artifact_version,
            transformation_preview_artifact_version=transformation_preview_artifact_version,
            target_profile_id=target_profile_id,
            timeout_seconds=timeout_seconds,
        )
        # Replay lookup precedes current-state checks because a successful first
        # request has already advanced the workflow beyond EXECUTION_READY.
        replay = self.persistence.get_execution_attempt_by_command(
            workflow_id, idempotency_key, command_hash
        )
        if replay is not None and replay.status in {
            MigrationExecutionAttemptStatus.SUCCEEDED,
            MigrationExecutionAttemptStatus.FAILED_ROLLED_BACK,
            MigrationExecutionAttemptStatus.OUTCOME_UNCERTAIN,
        }:
            terminal = self._terminal_result(workflow_id, replay)
            return self._cleanup_after_commit(
                terminal,
                actor_type=actor_type,
                actor_reference=actor_reference,
                request_id=request_id,
            )

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
        # Recompile from the persisted approved mapping rather than trusting the
        # stored SQL preview directly.  Equality proves that neither the SQL nor
        # its bound parameters escaped the approval boundary.
        try:
            adapter = self.target_registry.resolve(workflow.target_relation.system)
        except UnsupportedTargetSystemError:
            raise WorkflowUnsupportedExecutionConnectorError() from None
        if not adapter.capabilities.supports_insert_select_execution:
            raise WorkflowUnsupportedExecutionConnectorError()
        try:
            expected_preview = adapter.compiler.compile_insert_select(
                approved,
                staging_relation=TransportRelation(
                    catalog_name=preview.source_relation[0],
                    schema_name=preview.source_relation[1],
                    object_name=preview.source_relation[2],
                ),
            )
        except Exception:
            raise WorkflowUnsafeGeneratedStatementError() from None
        if expected_preview != preview:
            raise WorkflowUnsafeGeneratedStatementError()
        adapter.validate_preview(preview)
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
        # Claim in the control plane before making the non-transactional remote
        # call so concurrent requests cannot both start the same write.
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
            terminal = self._terminal_result(workflow_id, attempt)
            return self._cleanup_after_commit(
                terminal,
                actor_type=actor_type,
                actor_reference=actor_reference,
                request_id=request_id,
            )
        running, acquired = self.persistence.mark_execution_running(
            attempt.attempt_id, self.clock()
        )
        if not acquired:
            # A timed-out RUNNING claim is not assumed failed: the target may
            # have committed after the caller lost contact, so retry is unsafe.
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
        result = adapter.execute(target, preview)
        completed = self._complete(
            claimed_workflow,
            running,
            result=result,
            connector_type=target.connector_type,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
            idempotency_key=idempotency_key,
        )
        return self._cleanup_after_commit(
            completed,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
        )


__all__ = ["WorkflowExecutionOrchestrator", "WorkflowExecutionResult"]
