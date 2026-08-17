"""Create approval-bound background migration jobs without trusting API internals."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable
from uuid import UUID, uuid4

from schemabridge.models.migration_job import (
    MigrationJob,
    MigrationJobStage,
    MigrationJobStatus,
)
from schemabridge.models.workflow import (
    AuditActorType,
    MigrationWorkflowStatus,
    WorkflowArtifact,
    WorkflowArtifactType,
)
from schemabridge.persistence.errors import (
    WorkflowOperationUnavailableError,
    WorkflowRequiredArtifactError,
    WorkflowStaleArtifactReferenceError,
)
from schemabridge.persistence.serialization import request_hash
from schemabridge.services.workflow_persistence import WorkflowPersistenceService


def _now() -> datetime:
    return datetime.now(timezone.utc)


class MigrationJobSubmissionService:
    """Validate a job request and persist one server-controlled queued record."""

    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        *,
        clock: Callable[[], datetime] = _now,
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self.persistence = persistence
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

    def create(
        self,
        workflow_id: UUID,
        *,
        expected_version: int,
        source_discovery_artifact_version: int,
        approved_mapping_artifact_version: int,
        batch_size: int,
        timeout_seconds: int,
        idempotency_key: str,
        actor_type: AuditActorType = AuditActorType.USER,
        actor_reference: str | None = None,
    ) -> tuple[MigrationJob, bool]:
        if not isinstance(idempotency_key, str) or not idempotency_key.strip() or len(idempotency_key) > 128:
            raise ValueError("idempotency_key is invalid.")
        if not isinstance(actor_type, AuditActorType):
            raise TypeError("actor_type is invalid.")

        workflow = self.persistence.get_workflow(workflow_id)
        current_request = workflow.version == expected_version
        if current_request and workflow.status is not MigrationWorkflowStatus.MAPPING_APPROVED:
            raise WorkflowOperationUnavailableError()

        source_artifact = self._artifact(
            workflow_id,
            source_discovery_artifact_version,
            WorkflowArtifactType.SOURCE_DISCOVERY,
            require_latest=current_request,
        )
        approved_artifact = self._artifact(
            workflow_id,
            approved_mapping_artifact_version,
            WorkflowArtifactType.APPROVED_MAPPING_PLAN,
            require_latest=current_request,
        )
        fingerprint = request_hash(
            "CREATE_MIGRATION_JOB",
            {
                "workflow_id": workflow_id,
                "expected_version": expected_version,
                "source_discovery_artifact_version": source_discovery_artifact_version,
                "source_discovery_sha256": source_artifact.payload_sha256,
                "approved_mapping_artifact_version": approved_mapping_artifact_version,
                "approved_mapping_sha256": approved_artifact.payload_sha256,
                "source_profile_id": workflow.source_profile_id,
                "target_profile_id": workflow.target_profile_id,
                "batch_size": batch_size,
                "timeout_seconds": timeout_seconds,
            },
        )
        job = MigrationJob(
            job_id=self.uuid_factory(),
            workflow_id=workflow_id,
            expected_workflow_version=expected_version,
            source_discovery_artifact_version=source_discovery_artifact_version,
            approved_mapping_artifact_version=approved_mapping_artifact_version,
            source_profile_id=workflow.source_profile_id,
            target_profile_id=workflow.target_profile_id,
            batch_size=batch_size,
            timeout_seconds=timeout_seconds,
            job_fingerprint=fingerprint,
            status=MigrationJobStatus.QUEUED,
            stage=MigrationJobStage.QUEUED,
            queued_at=self.clock(),
            actor_type=actor_type,
            idempotency_key=idempotency_key,
            actor_reference=actor_reference,
        )
        return self.persistence.create_migration_job(job)

    def get(self, job_id: UUID) -> MigrationJob:
        return self.persistence.get_migration_job(job_id)


class MigrationJobClaimService:
    """Give one worker exclusive ownership of the oldest queued job."""

    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        *,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self.persistence = persistence
        self.clock = clock

    def claim_next(self) -> MigrationJob | None:
        return self.persistence.claim_next_migration_job(self.clock())


class MigrationJobProgressService:
    """Advance a claimed job through one safe non-terminal stage at a time."""

    def __init__(self, persistence: WorkflowPersistenceService) -> None:
        self.persistence = persistence

    def advance(
        self,
        job_id: UUID,
        *,
        expected_stage: MigrationJobStage,
        new_stage: MigrationJobStage,
    ) -> MigrationJob:
        return self.persistence.update_migration_job_stage(
            job_id,
            expected_stage,
            new_stage,
        )


class MigrationJobCompletionService:
    """Finish a running job with a trusted time and explicit outcome class."""

    def __init__(
        self,
        persistence: WorkflowPersistenceService,
        *,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self.persistence = persistence
        self.clock = clock

    def succeed(self, job_id: UUID) -> MigrationJob:
        return self.persistence.finish_migration_job(
            job_id,
            MigrationJobStage.VALIDATING,
            MigrationJobStatus.SUCCEEDED,
            self.clock(),
            None,
        )

    def fail(
        self,
        job_id: UUID,
        *,
        expected_stage: MigrationJobStage,
        failure_category: str,
    ) -> MigrationJob:
        return self.persistence.finish_migration_job(
            job_id,
            expected_stage,
            MigrationJobStatus.FAILED,
            self.clock(),
            failure_category,
        )

    def require_recovery(
        self,
        job_id: UUID,
        *,
        expected_stage: MigrationJobStage,
        failure_category: str,
    ) -> MigrationJob:
        return self.persistence.finish_migration_job(
            job_id,
            expected_stage,
            MigrationJobStatus.RECOVERY_REQUIRED,
            self.clock(),
            failure_category,
        )

    def require_review(
        self,
        job_id: UUID,
        *,
        failure_category: str,
    ) -> MigrationJob:
        return self.persistence.finish_migration_job(
            job_id,
            MigrationJobStage.VALIDATING,
            MigrationJobStatus.REVIEW_REQUIRED,
            self.clock(),
            failure_category,
        )


__all__ = [
    "MigrationJobClaimService",
    "MigrationJobCompletionService",
    "MigrationJobProgressService",
    "MigrationJobSubmissionService",
]
