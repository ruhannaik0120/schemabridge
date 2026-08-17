"""Submit and inspect durable background migration job records."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Response, status

from schemabridge.persistence.errors import (
    MigrationJobAlreadyActiveError,
    MigrationJobNotFoundError,
    WorkflowConflictError,
    WorkflowIdempotencyConflictError,
    WorkflowNotFoundError,
    WorkflowOperationUnavailableError,
    WorkflowPersistenceError,
    WorkflowRequiredArtifactError,
    WorkflowStaleArtifactReferenceError,
)

from ..adapters.jobs import migration_job_to_api
from ..dependencies import get_migration_job_submission_service
from ..errors import ApiError
from ..schemas.common import ErrorResponse
from ..schemas.jobs import (
    MigrationJobCreateRequest,
    MigrationJobCreateResponse,
    MigrationJobSchema,
)


router = APIRouter(prefix="/api/v1/migrations", tags=["migration-jobs"])
IdempotencyKey = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=128),
]
_ERRORS = {
    400: {"model": ErrorResponse, "description": "The migration job command is invalid."},
    404: {"model": ErrorResponse, "description": "The workflow or migration job is unavailable."},
    409: {"model": ErrorResponse, "description": "The migration job conflicts with durable state."},
    422: {"model": ErrorResponse, "description": "The request schema is invalid."},
    503: {"model": ErrorResponse, "description": "The workflow control plane is unavailable."},
}


def _raise_job_error(error: Exception) -> None:
    """Translate job-domain failures without exposing persistence details."""

    if isinstance(error, MigrationJobNotFoundError):
        raise ApiError(404, "MIGRATION_JOB_NOT_FOUND", "The requested migration job is unavailable.") from None
    if isinstance(error, WorkflowNotFoundError):
        raise ApiError(404, "WORKFLOW_NOT_FOUND", "The requested workflow is unavailable.") from None
    if isinstance(error, WorkflowIdempotencyConflictError):
        raise ApiError(409, "IDEMPOTENCY_KEY_CONFLICT", "The idempotency key conflicts with an earlier command.") from None
    if isinstance(error, WorkflowConflictError):
        raise ApiError(409, "WORKFLOW_VERSION_CONFLICT", "The workflow version is stale.") from None
    if isinstance(error, MigrationJobAlreadyActiveError):
        raise ApiError(409, "MIGRATION_JOB_ALREADY_ACTIVE", "The workflow already has an active migration job.") from None
    if isinstance(error, WorkflowOperationUnavailableError):
        raise ApiError(409, "MIGRATION_JOB_UNAVAILABLE", "A migration job cannot be created in the current workflow state.") from None
    if isinstance(error, WorkflowRequiredArtifactError):
        raise ApiError(409, "REQUIRED_WORKFLOW_ARTIFACT_MISSING", "A required workflow artifact is unavailable.") from None
    if isinstance(error, WorkflowStaleArtifactReferenceError):
        raise ApiError(409, "STALE_WORKFLOW_ARTIFACT", "The referenced workflow artifact is stale.") from None
    if isinstance(error, WorkflowPersistenceError):
        raise ApiError(503, "WORKFLOW_PERSISTENCE_FAILED", "Durable workflow persistence is unavailable.") from None
    raise error


@router.post(
    "/workflows/{workflow_id}/jobs",
    operation_id="migration_job_create",
    summary="Create a queued migration job",
    response_model=MigrationJobCreateResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_ERRORS,
)
async def create_migration_job(
    workflow_id: UUID,
    command: MigrationJobCreateRequest,
    response: Response,
    idempotency_key: IdempotencyKey,
    service=Depends(get_migration_job_submission_service),
) -> MigrationJobCreateResponse:
    """Persist a safe queued work order; this endpoint does not execute it."""

    try:
        job, created = service.create(
            workflow_id,
            expected_version=command.expected_version,
            source_discovery_artifact_version=command.source_discovery_artifact_version,
            approved_mapping_artifact_version=command.approved_mapping_artifact_version,
            batch_size=command.batch_size,
            timeout_seconds=command.timeout_seconds,
            idempotency_key=idempotency_key,
            actor_type=command.actor_type,
            actor_reference=command.actor_reference,
        )
        # A replay returns the old resource rather than claiming it was created again.
        response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
        return MigrationJobCreateResponse(job=migration_job_to_api(job), created=created)
    except (TypeError, ValueError):
        raise ApiError(400, "INVALID_MIGRATION_JOB_COMMAND", "The migration job command is invalid.") from None
    except Exception as error:
        _raise_job_error(error)
        raise


@router.get(
    "/jobs/{job_id}",
    operation_id="migration_job_get",
    summary="Get a migration job",
    response_model=MigrationJobSchema,
    responses=_ERRORS,
)
async def get_migration_job(
    job_id: UUID,
    service=Depends(get_migration_job_submission_service),
) -> MigrationJobSchema:
    """Return the current durable view of one migration work order."""

    try:
        return migration_job_to_api(service.get(job_id))
    except Exception as error:
        _raise_job_error(error)
        raise


__all__ = ["router"]
