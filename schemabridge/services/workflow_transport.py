"""Coordinate one durable, idempotent source-to-staging workflow operation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from uuid import UUID, uuid4

from schemabridge.models.transport import BatchTransportResult
from schemabridge.models.workflow import (
    AuditActorType,
    MigrationWorkflow,
    MigrationWorkflowStatus,
    WorkflowArtifact,
    WorkflowArtifactType,
)
from schemabridge.models.workflow_transport import (
    WorkflowTransportAttempt,
    WorkflowTransportAttemptStatus,
    WorkflowTransportEvidence,
)
from schemabridge.persistence.artifact_codec import (
    approved_mapping_plan_from_artifact,
    table_metadata_from_artifact,
    workflow_transport_evidence_from_artifact,
)
from schemabridge.persistence.errors import (
    WorkflowOperationUnavailableError,
    WorkflowRequiredArtifactError,
    WorkflowStaleArtifactReferenceError,
    WorkflowTransportAlreadyInProgressError,
    WorkflowTransportConfirmedFailureError,
    WorkflowTransportOutcomeUncertainError,
)
from schemabridge.persistence.serialization import request_hash
from schemabridge.services.batch_transport import (
    BatchTransportDisposition,
    BatchTransportService,
    ProfileBoundBatchTransportResult,
)
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from schemabridge.transport.base import BatchProgressReporter


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class WorkflowTransportResult:
    """Bundle the successful durable workflow, claim, artifact, and evidence."""

    workflow: MigrationWorkflow
    attempt: WorkflowTransportAttempt
    artifact: WorkflowArtifact
    evidence: WorkflowTransportEvidence


class WorkflowTransportOrchestrator:
    """Claim once, run source-to-staging transport, and persist its outcome."""

    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        *,
        transport_service: object,
        clock: Callable[[], datetime] = _now,
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self.persistence = persistence
        self.transport_service = transport_service
        self.clock = clock
        self.uuid_factory = uuid_factory

    def _artifact(
        self,
        workflow_id: UUID,
        artifact_version: int,
        artifact_type: WorkflowArtifactType,
    ) -> WorkflowArtifact:
        artifact = self.persistence.get_artifact(workflow_id, artifact_version)
        if artifact is None:
            raise WorkflowRequiredArtifactError()
        if artifact.artifact_type is not artifact_type:
            raise WorkflowStaleArtifactReferenceError()
        latest = self.persistence.get_latest_artifact(workflow_id, artifact_type)
        if latest is None or latest.artifact_version != artifact_version:
            raise WorkflowStaleArtifactReferenceError()
        return artifact

    @staticmethod
    def _command_hash(
        workflow_id: UUID,
        *,
        expected_version: int,
        source_discovery_artifact_version: int,
        approved_mapping_artifact_version: int,
        source_profile_id: str,
        target_profile_id: str,
        batch_size: int | None,
        timeout_seconds: int | None,
    ) -> str:
        return request_hash(
            "WORKFLOW_LOAD_STAGING",
            {
                "workflow_id": workflow_id,
                "expected_version": expected_version,
                "source_discovery_artifact_version": source_discovery_artifact_version,
                "approved_mapping_artifact_version": approved_mapping_artifact_version,
                "source_profile_id": source_profile_id,
                "target_profile_id": target_profile_id,
                "batch_size": batch_size,
                "timeout_seconds": timeout_seconds,
            },
        )

    def _terminal_result(
        self,
        workflow_id: UUID,
        attempt: WorkflowTransportAttempt,
    ) -> WorkflowTransportResult:
        if attempt.status is WorkflowTransportAttemptStatus.FAILED_CLEANED_UP:
            raise WorkflowTransportConfirmedFailureError()
        if attempt.status is WorkflowTransportAttemptStatus.OUTCOME_UNCERTAIN:
            raise WorkflowTransportOutcomeUncertainError()
        if (
            attempt.status is not WorkflowTransportAttemptStatus.SUCCEEDED
            or attempt.evidence_artifact_id is None
        ):
            raise WorkflowTransportAlreadyInProgressError()
        artifact = self.persistence.get_artifact_by_id(
            workflow_id,
            attempt.evidence_artifact_id,
        )
        if artifact is None:
            raise WorkflowRequiredArtifactError()
        evidence = workflow_transport_evidence_from_artifact(artifact)
        return WorkflowTransportResult(
            workflow=self.persistence.get_workflow(workflow_id),
            attempt=attempt,
            artifact=artifact,
            evidence=evidence,
        )

    def _complete(
        self,
        workflow: MigrationWorkflow,
        attempt: WorkflowTransportAttempt,
        result: ProfileBoundBatchTransportResult,
        *,
        actor_type: AuditActorType,
        actor_reference: str | None,
        request_id: str | None,
        idempotency_key: str,
    ) -> WorkflowTransportResult:
        completed_at = self.clock()
        if attempt.running_at is None:
            raise WorkflowTransportOutcomeUncertainError()
        evidence = None
        if result.disposition is BatchTransportDisposition.SUCCEEDED:
            if not isinstance(result.result, BatchTransportResult):
                raise WorkflowTransportOutcomeUncertainError()
            transfer = result.result
            evidence = WorkflowTransportEvidence(
                attempt_id=attempt.attempt_id,
                workflow_id=workflow.workflow_id,
                source_relation=transfer.source_relation,
                staging_relation=transfer.staging_relation,
                source_profile_id=attempt.source_profile_id,
                target_profile_id=attempt.target_profile_id,
                batch_size=transfer.batch_size,
                batch_count=transfer.batch_count,
                column_count=transfer.column_count,
                rows_read=transfer.rows_read,
                rows_written=transfer.rows_written,
                started_at=attempt.running_at,
                completed_at=completed_at,
                duration_ms=max(
                    0,
                    int((completed_at - attempt.running_at).total_seconds() * 1000),
                ),
                source_discovery_artifact_version=attempt.source_discovery_artifact_version,
                approved_mapping_artifact_version=attempt.approved_mapping_artifact_version,
                transport_fingerprint=attempt.transport_fingerprint,
            )
            new_status = MigrationWorkflowStatus.STAGED
            failure_category = None
        elif (
            result.disposition
            is BatchTransportDisposition.CONFIRMED_FAILED_CLEANED_UP
        ):
            new_status = MigrationWorkflowStatus.MAPPING_APPROVED
            failure_category = result.failure_category or "STAGING_LOAD_FAILED"
        else:
            new_status = MigrationWorkflowStatus.STAGING_RECOVERY_REQUIRED
            failure_category = (
                result.failure_category or "STAGING_OUTCOME_UNCERTAIN"
            )

        updated, updated_attempt, artifact = self.persistence.complete_transport_attempt(
            workflow.workflow_id,
            workflow.version,
            attempt.attempt_id,
            evidence,
            new_status,
            completed_at=completed_at,
            failure_category=failure_category,
            idempotency_key=idempotency_key,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
        )
        if new_status is MigrationWorkflowStatus.MAPPING_APPROVED:
            raise WorkflowTransportConfirmedFailureError()
        if new_status is MigrationWorkflowStatus.STAGING_RECOVERY_REQUIRED:
            raise WorkflowTransportOutcomeUncertainError()
        if artifact is None or evidence is None:
            raise WorkflowTransportOutcomeUncertainError()
        return WorkflowTransportResult(updated, updated_attempt, artifact, evidence)

    def run(
        self,
        workflow_id: UUID,
        *,
        expected_version: int,
        source_discovery_artifact_version: int,
        approved_mapping_artifact_version: int,
        source_profile_id: str,
        target_profile_id: str,
        batch_size: int | None,
        timeout_seconds: int | None,
        idempotency_key: str,
        actor_type: AuditActorType = AuditActorType.USER,
        actor_reference: str | None = None,
        request_id: str | None = None,
        progress_reporter: BatchProgressReporter | None = None,
    ) -> WorkflowTransportResult:
        """Execute one exact idempotent source-to-managed-staging command."""

        command_hash = self._command_hash(
            workflow_id,
            expected_version=expected_version,
            source_discovery_artifact_version=source_discovery_artifact_version,
            approved_mapping_artifact_version=approved_mapping_artifact_version,
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
            batch_size=batch_size,
            timeout_seconds=timeout_seconds,
        )
        replay = self.persistence.get_transport_attempt_by_command(
            workflow_id,
            idempotency_key,
            command_hash,
        )
        if replay is not None:
            return self._terminal_result(workflow_id, replay)

        workflow = self.persistence.get_workflow(workflow_id)
        if workflow.status is not MigrationWorkflowStatus.MAPPING_APPROVED:
            raise WorkflowOperationUnavailableError()
        if (
            workflow.source_profile_id != source_profile_id
            or workflow.target_profile_id != target_profile_id
            or workflow.target_relation.catalog_name is None
        ):
            raise WorkflowOperationUnavailableError()
        source_artifact = self._artifact(
            workflow_id,
            source_discovery_artifact_version,
            WorkflowArtifactType.SOURCE_DISCOVERY,
        )
        approved_artifact = self._artifact(
            workflow_id,
            approved_mapping_artifact_version,
            WorkflowArtifactType.APPROVED_MAPPING_PLAN,
        )
        source_table = table_metadata_from_artifact(source_artifact)
        approved_mapping_plan_from_artifact(approved_artifact)
        prepared = self.transport_service.prepare(
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
            target_database=workflow.target_relation.catalog_name,
            batch_size=batch_size,
            timeout_seconds=timeout_seconds,
        )
        attempt_id = self.uuid_factory()
        staging_relation = BatchTransportService.staging_relation(
            transport_id=attempt_id,
            target_database=workflow.target_relation.catalog_name,
            target_schema=workflow.target_relation.schema_name,
        )
        fingerprint = request_hash(
            "STAGING_TRANSPORT_FINGERPRINT",
            {
                "workflow_id": workflow_id,
                "source_payload_sha256": source_artifact.payload_sha256,
                "approved_payload_sha256": approved_artifact.payload_sha256,
                "source_profile_id": source_profile_id,
                "target_profile_id": target_profile_id,
                "staging_relation": staging_relation,
                "batch_size": prepared.batch_size,
                "timeout_seconds": prepared.timeout_seconds,
            },
        )
        attempt = WorkflowTransportAttempt(
            attempt_id=attempt_id,
            workflow_id=workflow_id,
            source_discovery_artifact_version=source_discovery_artifact_version,
            approved_mapping_artifact_version=approved_mapping_artifact_version,
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
            staging_relation=staging_relation,
            batch_size=prepared.batch_size,
            timeout_seconds=prepared.timeout_seconds,
            transport_fingerprint=fingerprint,
            status=WorkflowTransportAttemptStatus.CLAIMED,
            claimed_at=self.clock(),
            actor_type=actor_type,
            idempotency_key=idempotency_key,
            actor_reference=actor_reference,
        )
        claimed_workflow, claimed_attempt, acquired = self.persistence.claim_transport_attempt(
            workflow_id,
            expected_version,
            attempt,
            command_hash=command_hash,
            idempotency_key=idempotency_key,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
        )
        if not acquired:
            return self._terminal_result(workflow_id, claimed_attempt)
        running, acquired_runner = self.persistence.mark_transport_running(
            claimed_attempt.attempt_id,
            self.clock(),
        )
        if not acquired_runner:
            return self._terminal_result(workflow_id, running)
        try:
            remote_result = self.transport_service.run(
                prepared,
                transport_id=running.attempt_id,
                source_table=source_table,
                target_database=workflow.target_relation.catalog_name,
                target_schema=workflow.target_relation.schema_name,
                progress_reporter=progress_reporter,
            )
        except Exception:
            # Once RUNNING has been stored, an unexpected exception cannot prove
            # whether Snowflake accepted work. Persist the conservative outcome.
            remote_result = ProfileBoundBatchTransportResult(
                disposition=BatchTransportDisposition.OUTCOME_UNCERTAIN,
                failure_category="STAGING_OUTCOME_UNCERTAIN",
            )
        return self._complete(
            claimed_workflow,
            running,
            remote_result,
            actor_type=actor_type,
            actor_reference=actor_reference,
            request_id=request_id,
            idempotency_key=idempotency_key,
        )


__all__ = ["WorkflowTransportOrchestrator", "WorkflowTransportResult"]
