"""Durable migration workflow command and query routes."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request, status

from schemabridge.persistence.errors import (
    InvalidWorkflowTransitionError,
    WorkflowArtifactValidationError,
    WorkflowConflictError,
    WorkflowIdempotencyConflictError,
    WorkflowNotFoundError,
    WorkflowPersistenceError,
    WorkflowConnectorOperationError,
    WorkflowMappingApprovalRequiredError,
    WorkflowOperationUnavailableError,
    WorkflowPreviewCompilationError,
    WorkflowRequiredArtifactError,
    WorkflowStaleArtifactReferenceError,
    WorkflowTargetProfileUnavailableError,
    WorkflowTargetProfileNotWriteCapableError,
    WorkflowUnsupportedExecutionConnectorError,
    WorkflowUnsafeGeneratedStatementError,
    WorkflowExecutionAlreadyInProgressError,
    WorkflowExecutionOutcomeUncertainError,
    WorkflowExecutionConfirmedFailureError,
    WorkflowUnsafeValidationQueryError,
    WorkflowValidationAlreadyInProgressError,
    WorkflowValidationExecutionError,
    WorkflowValidationNotReadyError,
    WorkflowValidationOutcomeUncertainError,
)

from ..adapters.migrations import (
    approved_plan_to_api,
    approved_plan_to_domain,
    decision_to_domain,
    plan_to_api,
    plan_to_domain,
    table_to_api,
    table_to_domain,
    transformation_sql_to_api,
    execution_report_to_api,
)
from ..adapters.workflows import (
    artifact_to_api,
    audit_event_to_api,
    execution_attempt_to_api,
    execution_evidence_to_api,
    transformation_sql_to_domain,
    workflow_relation_to_domain,
    workflow_to_api,
    validation_run_to_api,
)
from ..dependencies import (
    get_workflow_persistence_service,
    get_workflow_planning_orchestrator,
    get_workflow_execution_orchestrator,
    get_workflow_validation_orchestrator,
)
from ..errors import ApiError
from ..schemas.common import ErrorResponse
from ..schemas.workflows import (
    ApprovedMappingPlanArtifactRequest,
    MappingPlanArtifactRequest,
    MigrationAuditEventListResponse,
    MigrationWorkflowSchema,
    SourceDiscoveryArtifactRequest,
    TargetDiscoveryArtifactRequest,
    TransformationPreviewArtifactRequest,
    WorkflowArtifactAppendRequest,
    WorkflowArtifactAppendResponse,
    WorkflowArtifactListResponse,
    WorkflowCreateRequest,
    WorkflowApprovalOperationResponse,
    WorkflowDiscoveryOperationResponse,
    WorkflowMappingApprovalCommand,
    WorkflowMappingOperationResponse,
    WorkflowPlanningCommand,
    WorkflowStatusTransitionRequest,
    WorkflowTransformationPreviewCommand,
    WorkflowTransformationPreviewOperationResponse,
    WorkflowExecutionCommand,
    WorkflowExecutionOperationResponse,
    WorkflowValidationCommand,
    WorkflowValidationOperationResponse,
    ValidationExecutionArtifactPayload,
)


router = APIRouter(prefix="/api/v1/migrations/workflows", tags=["migrations"])
IdempotencyKey = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=128),
]
_ERRORS = {
    400: {"model": ErrorResponse, "description": "The workflow command is invalid."},
    404: {"model": ErrorResponse, "description": "The workflow is unavailable."},
    409: {"model": ErrorResponse, "description": "The workflow command conflicts with durable state."},
    422: {"model": ErrorResponse, "description": "The request schema is invalid."},
    502: {"model": ErrorResponse, "description": "A connector operation failed."},
    503: {"model": ErrorResponse, "description": "The workflow control plane is unavailable."},
}


def _raise_workflow_error(error: Exception) -> None:
    if isinstance(error, WorkflowNotFoundError):
        raise ApiError(404, "WORKFLOW_NOT_FOUND", "The requested workflow is unavailable.") from None
    if isinstance(error, WorkflowIdempotencyConflictError):
        raise ApiError(409, "IDEMPOTENCY_KEY_CONFLICT", "The idempotency key conflicts with an earlier command.") from None
    if isinstance(error, WorkflowConflictError):
        raise ApiError(409, "WORKFLOW_VERSION_CONFLICT", "The workflow version is stale.") from None
    if isinstance(error, InvalidWorkflowTransitionError):
        raise ApiError(409, "INVALID_WORKFLOW_TRANSITION", "The workflow status transition is invalid.") from None
    if isinstance(error, WorkflowRequiredArtifactError):
        raise ApiError(409, "REQUIRED_WORKFLOW_ARTIFACT_MISSING", "A required workflow artifact is unavailable.") from None
    if isinstance(error, WorkflowStaleArtifactReferenceError):
        raise ApiError(409, "STALE_WORKFLOW_ARTIFACT", "The referenced workflow artifact is stale.") from None
    if isinstance(error, WorkflowMappingApprovalRequiredError):
        raise ApiError(409, "MAPPING_APPROVAL_REQUIRED", "A complete approved mapping is required.") from None
    if isinstance(error, WorkflowOperationUnavailableError):
        raise ApiError(409, "WORKFLOW_OPERATION_UNAVAILABLE", "The operation is unavailable in the current workflow state.") from None
    if isinstance(error, WorkflowConnectorOperationError):
        raise ApiError(502, "WORKFLOW_DISCOVERY_FAILED", "Workflow schema discovery could not be completed.") from None
    if isinstance(error, WorkflowPreviewCompilationError):
        raise ApiError(400, "WORKFLOW_PREVIEW_COMPILATION_FAILED", "The transformation preview could not be compiled.") from None
    if isinstance(error, WorkflowTargetProfileUnavailableError):
        raise ApiError(409, "TARGET_PROFILE_UNAVAILABLE", "The target profile is unavailable for execution.") from None
    if isinstance(error, WorkflowTargetProfileNotWriteCapableError):
        raise ApiError(409, "TARGET_PROFILE_NOT_WRITE_CAPABLE", "The target profile is not approved for writes.") from None
    if isinstance(error, WorkflowUnsupportedExecutionConnectorError):
        raise ApiError(409, "UNSUPPORTED_EXECUTION_CONNECTOR", "The target connector does not support migration execution.") from None
    if isinstance(error, WorkflowUnsafeGeneratedStatementError):
        raise ApiError(409, "UNSAFE_GENERATED_STATEMENT", "The persisted transformation is not safe to execute.") from None
    if isinstance(error, WorkflowExecutionAlreadyInProgressError):
        raise ApiError(409, "EXECUTION_ALREADY_IN_PROGRESS", "A workflow execution is already in progress.") from None
    if isinstance(error, WorkflowExecutionOutcomeUncertainError):
        raise ApiError(409, "EXECUTION_OUTCOME_UNCERTAIN", "The target execution outcome requires manual investigation.") from None
    if isinstance(error, WorkflowExecutionConfirmedFailureError):
        raise ApiError(502, "EXECUTION_CONFIRMED_FAILED", "The target execution failed and was rolled back.") from None
    if isinstance(error, WorkflowValidationNotReadyError):
        raise ApiError(409, "WORKFLOW_NOT_READY_FOR_VALIDATION", "The workflow is not ready for validation.") from None
    if isinstance(error, WorkflowValidationAlreadyInProgressError):
        raise ApiError(409, "VALIDATION_ALREADY_IN_PROGRESS", "Workflow validation is already in progress.") from None
    if isinstance(error, WorkflowUnsafeValidationQueryError):
        raise ApiError(400, "UNSAFE_VALIDATION_QUERY", "The generated validation query is unsafe.") from None
    if isinstance(error, WorkflowValidationExecutionError):
        raise ApiError(502, "VALIDATION_EXECUTION_FAILED", "Workflow validation could not be completed.") from None
    if isinstance(error, WorkflowValidationOutcomeUncertainError):
        raise ApiError(502, "VALIDATION_OUTCOME_UNCERTAIN", "The validation outcome requires manual investigation.") from None
    if isinstance(error, WorkflowArtifactValidationError):
        raise ApiError(400, "INVALID_WORKFLOW_ARTIFACT", "The workflow artifact is invalid.") from None
    if isinstance(error, WorkflowPersistenceError):
        raise ApiError(503, "WORKFLOW_PERSISTENCE_FAILED", "Durable workflow persistence is unavailable.") from None
    raise error


def _request_id(request: Request) -> str | None:
    value = getattr(request.state, "request_id", None)
    return value if isinstance(value, str) else None


@router.post(
    "",
    operation_id="workflow_create",
    summary="Create a durable migration workflow",
    response_model=MigrationWorkflowSchema,
    status_code=status.HTTP_201_CREATED,
    responses=_ERRORS,
)
async def create_workflow(
    command: WorkflowCreateRequest,
    request: Request,
    idempotency_key: IdempotencyKey,
    service=Depends(get_workflow_persistence_service),
) -> MigrationWorkflowSchema:
    try:
        result = service.create_workflow(
            display_name=command.display_name,
            source_profile_id=command.source_profile_id,
            target_profile_id=command.target_profile_id,
            source_relation=workflow_relation_to_domain(command.source_relation),
            target_relation=workflow_relation_to_domain(command.target_relation),
            idempotency_key=idempotency_key,
            actor_type=command.actor_type,
            actor_reference=command.actor_reference,
            request_id=_request_id(request),
        )
        return workflow_to_api(result)
    except (TypeError, ValueError):
        raise ApiError(400, "INVALID_WORKFLOW_COMMAND", "The workflow command is invalid.") from None
    except Exception as error:
        _raise_workflow_error(error)
        raise


@router.get(
    "/{workflow_id}",
    operation_id="workflow_get",
    summary="Get a durable migration workflow",
    response_model=MigrationWorkflowSchema,
    responses=_ERRORS,
)
async def get_workflow(
    workflow_id: UUID,
    service=Depends(get_workflow_persistence_service),
) -> MigrationWorkflowSchema:
    try:
        return workflow_to_api(service.get_workflow(workflow_id))
    except Exception as error:
        _raise_workflow_error(error)
        raise


def _planning_context(command, request: Request, idempotency_key: str) -> dict:
    return {
        "expected_version": command.expected_version,
        "idempotency_key": idempotency_key,
        "actor_type": command.actor_type,
        "actor_reference": command.actor_reference,
        "request_id": _request_id(request),
    }


@router.post(
    "/{workflow_id}/discover-source",
    operation_id="workflow_discover_source",
    summary="Discover and persist the workflow source schema",
    response_model=WorkflowDiscoveryOperationResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_ERRORS,
)
async def discover_workflow_source(
    workflow_id: UUID,
    command: WorkflowPlanningCommand,
    request: Request,
    idempotency_key: IdempotencyKey,
    orchestrator=Depends(get_workflow_planning_orchestrator),
) -> WorkflowDiscoveryOperationResponse:
    try:
        result = orchestrator.discover_source(
            workflow_id, **_planning_context(command, request, idempotency_key)
        )
        return WorkflowDiscoveryOperationResponse(
            workflow=workflow_to_api(result.workflow),
            artifact=artifact_to_api(result.artifact),
            result=table_to_api(result.result),
        )
    except Exception as error:
        _raise_workflow_error(error)
        raise


@router.post(
    "/{workflow_id}/discover-target",
    operation_id="workflow_discover_target",
    summary="Discover and persist the workflow target schema",
    response_model=WorkflowDiscoveryOperationResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_ERRORS,
)
async def discover_workflow_target(
    workflow_id: UUID,
    command: WorkflowPlanningCommand,
    request: Request,
    idempotency_key: IdempotencyKey,
    orchestrator=Depends(get_workflow_planning_orchestrator),
) -> WorkflowDiscoveryOperationResponse:
    try:
        result = orchestrator.discover_target(
            workflow_id, **_planning_context(command, request, idempotency_key)
        )
        return WorkflowDiscoveryOperationResponse(
            workflow=workflow_to_api(result.workflow),
            artifact=artifact_to_api(result.artifact),
            result=table_to_api(result.result),
        )
    except Exception as error:
        _raise_workflow_error(error)
        raise


@router.post(
    "/{workflow_id}/mapping-proposals",
    operation_id="workflow_mapping_generate",
    summary="Generate and persist a workflow mapping proposal",
    response_model=WorkflowMappingOperationResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_ERRORS,
)
async def generate_workflow_mapping(
    workflow_id: UUID,
    command: WorkflowPlanningCommand,
    request: Request,
    idempotency_key: IdempotencyKey,
    orchestrator=Depends(get_workflow_planning_orchestrator),
) -> WorkflowMappingOperationResponse:
    try:
        result = orchestrator.generate_mapping(
            workflow_id, **_planning_context(command, request, idempotency_key)
        )
        return WorkflowMappingOperationResponse(
            workflow=workflow_to_api(result.workflow),
            artifact=artifact_to_api(result.artifact),
            result=plan_to_api(result.result),
        )
    except Exception as error:
        _raise_workflow_error(error)
        raise


@router.post(
    "/{workflow_id}/mapping-approvals",
    operation_id="workflow_mapping_approve",
    summary="Approve and persist a referenced workflow mapping proposal",
    response_model=WorkflowApprovalOperationResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_ERRORS,
)
async def approve_workflow_mapping(
    workflow_id: UUID,
    command: WorkflowMappingApprovalCommand,
    request: Request,
    idempotency_key: IdempotencyKey,
    orchestrator=Depends(get_workflow_planning_orchestrator),
) -> WorkflowApprovalOperationResponse:
    context = _planning_context(command, request, idempotency_key)
    try:
        result = orchestrator.approve_mapping(
            workflow_id,
            mapping_artifact_version=command.mapping_artifact_version,
            decisions=tuple(decision_to_domain(item) for item in command.decisions),
            **context,
        )
        return WorkflowApprovalOperationResponse(
            workflow=workflow_to_api(result.workflow),
            artifact=artifact_to_api(result.artifact),
            result=approved_plan_to_api(result.result),
        )
    except (TypeError, ValueError):
        raise ApiError(409, "MAPPING_APPROVAL_REQUIRED", "A complete approved mapping is required.") from None
    except Exception as error:
        _raise_workflow_error(error)
        raise


@router.post(
    "/{workflow_id}/transformation-previews",
    operation_id="workflow_transformation_preview",
    summary="Compile and persist an approval-gated transformation preview",
    response_model=WorkflowTransformationPreviewOperationResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_ERRORS,
)
async def preview_workflow_transformation(
    workflow_id: UUID,
    command: WorkflowTransformationPreviewCommand,
    request: Request,
    idempotency_key: IdempotencyKey,
    orchestrator=Depends(get_workflow_planning_orchestrator),
) -> WorkflowTransformationPreviewOperationResponse:
    try:
        result = orchestrator.preview_transformation(
            workflow_id,
            approved_mapping_artifact_version=command.approved_mapping_artifact_version,
            staging_database=command.staging_database,
            staging_schema=command.staging_schema,
            staging_table=command.staging_table,
            statement_type=command.statement_type,
            **_planning_context(command, request, idempotency_key),
        )
        return WorkflowTransformationPreviewOperationResponse(
            workflow=workflow_to_api(result.workflow),
            artifact=artifact_to_api(result.artifact),
            result=transformation_sql_to_api(result.result),
        )
    except Exception as error:
        _raise_workflow_error(error)
        raise


@router.post(
    "/{workflow_id}/execute",
    operation_id="workflow_execute",
    summary="Execute the latest approved persisted transformation",
    response_model=WorkflowExecutionOperationResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_ERRORS,
)
async def execute_workflow_transformation(
    workflow_id: UUID,
    command: WorkflowExecutionCommand,
    request: Request,
    idempotency_key: IdempotencyKey,
    orchestrator=Depends(get_workflow_execution_orchestrator),
) -> WorkflowExecutionOperationResponse:
    try:
        result = orchestrator.execute(
            workflow_id,
            approved_mapping_artifact_version=command.approved_mapping_artifact_version,
            transformation_preview_artifact_version=command.transformation_preview_artifact_version,
            target_profile_id=command.target_profile_id,
            timeout_seconds=command.timeout_seconds,
            **_planning_context(command, request, idempotency_key),
        )
        return WorkflowExecutionOperationResponse(
            workflow=workflow_to_api(result.workflow),
            artifact=artifact_to_api(result.artifact),
            attempt=execution_attempt_to_api(result.attempt),
            result=execution_evidence_to_api(result.evidence),
        )
    except Exception as error:
        _raise_workflow_error(error)
        raise


@router.post(
    "/{workflow_id}/validate",
    operation_id="workflow_validate",
    summary="Run persisted post-execution validation and reconciliation",
    response_model=WorkflowValidationOperationResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_ERRORS,
)
async def validate_workflow_migration(
    workflow_id: UUID,
    command: WorkflowValidationCommand,
    request: Request,
    idempotency_key: IdempotencyKey,
    orchestrator=Depends(get_workflow_validation_orchestrator),
) -> WorkflowValidationOperationResponse:
    try:
        result = orchestrator.validate(
            workflow_id,
            execution_evidence_artifact_version=command.execution_evidence_artifact_version,
            approved_mapping_artifact_version=command.approved_mapping_artifact_version,
            source_profile_id=command.source_profile_id,
            target_profile_id=command.target_profile_id,
            timeout_seconds=command.timeout_seconds,
            **_planning_context(command, request, idempotency_key),
        )
        report = execution_report_to_api(result.report)
        return WorkflowValidationOperationResponse(
            workflow=workflow_to_api(result.workflow),
            run=validation_run_to_api(result.run),
            plan_artifact=artifact_to_api(result.plan_artifact),
            evidence_artifact=artifact_to_api(result.evidence_artifact),
            result=ValidationExecutionArtifactPayload(
                source_profile_id=result.report.source_profile_id,
                target_profile_id=result.report.target_profile_id,
                **report.model_dump(mode="python"),
            ),
        )
    except Exception as error:
        _raise_workflow_error(error)
        raise


@router.post(
    "/{workflow_id}/transitions",
    operation_id="workflow_transition",
    summary="Transition a durable migration workflow",
    response_model=MigrationWorkflowSchema,
    responses=_ERRORS,
)
async def transition_workflow(
    workflow_id: UUID,
    command: WorkflowStatusTransitionRequest,
    request: Request,
    idempotency_key: IdempotencyKey,
    service=Depends(get_workflow_persistence_service),
) -> MigrationWorkflowSchema:
    try:
        result = service.transition_status(
            workflow_id,
            expected_version=command.expected_version,
            new_status=command.new_status,
            idempotency_key=idempotency_key,
            reason_code=command.reason_code,
            actor_type=command.actor_type,
            actor_reference=command.actor_reference,
            request_id=_request_id(request),
        )
        return workflow_to_api(result)
    except (TypeError, ValueError):
        raise ApiError(400, "INVALID_WORKFLOW_COMMAND", "The workflow command is invalid.") from None
    except Exception as error:
        _raise_workflow_error(error)
        raise


def _append_artifact(service, workflow_id: UUID, command, context: dict):
    if isinstance(command, SourceDiscoveryArtifactRequest):
        return service.append_source_discovery(workflow_id, command.expected_version, table_to_domain(command.payload), **context)
    if isinstance(command, TargetDiscoveryArtifactRequest):
        return service.append_target_discovery(workflow_id, command.expected_version, table_to_domain(command.payload), **context)
    if isinstance(command, MappingPlanArtifactRequest):
        return service.append_mapping_plan(workflow_id, command.expected_version, plan_to_domain(command.payload), **context)
    if isinstance(command, ApprovedMappingPlanArtifactRequest):
        return service.append_approved_mapping_plan(workflow_id, command.expected_version, approved_plan_to_domain(command.payload), **context)
    if isinstance(command, TransformationPreviewArtifactRequest):
        return service.append_transformation_preview(workflow_id, command.expected_version, transformation_sql_to_domain(command.payload), **context)
    raise TypeError("Unsupported workflow artifact command.")


@router.post(
    "/{workflow_id}/artifacts",
    operation_id="workflow_artifact_append",
    summary="Append a typed immutable workflow artifact",
    response_model=WorkflowArtifactAppendResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_ERRORS,
)
async def append_workflow_artifact(
    workflow_id: UUID,
    command: WorkflowArtifactAppendRequest,
    request: Request,
    idempotency_key: IdempotencyKey,
    service=Depends(get_workflow_persistence_service),
) -> WorkflowArtifactAppendResponse:
    context = {
        "idempotency_key": idempotency_key,
        "actor_type": command.actor_type,
        "actor_reference": command.actor_reference,
        "request_id": _request_id(request),
    }
    try:
        workflow, artifact = _append_artifact(service, workflow_id, command, context)
        return WorkflowArtifactAppendResponse(
            workflow=workflow_to_api(workflow),
            artifact=artifact_to_api(artifact),
        )
    except (TypeError, ValueError):
        raise ApiError(400, "INVALID_WORKFLOW_ARTIFACT", "The workflow artifact is invalid.") from None
    except Exception as error:
        _raise_workflow_error(error)
        raise


@router.get(
    "/{workflow_id}/artifacts",
    operation_id="workflow_artifact_list",
    summary="List immutable workflow artifacts",
    response_model=WorkflowArtifactListResponse,
    responses=_ERRORS,
)
async def list_workflow_artifacts(
    workflow_id: UUID,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    service=Depends(get_workflow_persistence_service),
) -> WorkflowArtifactListResponse:
    try:
        items = service.list_artifacts(workflow_id, offset=offset, limit=limit)
        return WorkflowArtifactListResponse(
            items=tuple(artifact_to_api(item) for item in items),
            offset=offset,
            limit=limit,
        )
    except Exception as error:
        _raise_workflow_error(error)
        raise


@router.get(
    "/{workflow_id}/audit-events",
    operation_id="workflow_audit_event_list",
    summary="List append-only workflow audit events",
    response_model=MigrationAuditEventListResponse,
    responses=_ERRORS,
)
async def list_workflow_audit_events(
    workflow_id: UUID,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    service=Depends(get_workflow_persistence_service),
) -> MigrationAuditEventListResponse:
    try:
        items = service.list_audit_events(workflow_id, offset=offset, limit=limit)
        return MigrationAuditEventListResponse(
            items=tuple(audit_event_to_api(item) for item in items),
            offset=offset,
            limit=limit,
        )
    except Exception as error:
        _raise_workflow_error(error)
        raise
