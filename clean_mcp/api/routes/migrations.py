"""Versioned production migration workflow routes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Annotated

from fastapi import APIRouter, Depends

from models.mapping import TransformationStatementType

from ..adapters.migrations import (
    approved_plan_to_api,
    approved_plan_to_domain,
    decision_to_domain,
    execution_report_to_api,
    execution_request_to_domain,
    plan_to_api,
    plan_to_domain,
    table_to_api,
    table_to_domain,
    transformation_sql_to_api,
    validation_sql_to_api,
)
from ..dependencies import (
    get_mapping_approval_service,
    get_schema_discovery_service,
    get_schema_mapping_service,
    get_transformation_compiler,
    get_validation_compiler,
    get_validation_execution_service_factory,
)
from ..errors import ApiError
from ..schemas.common import ErrorResponse
from ..schemas.migrations import (
    ApprovedTableMappingPlanSchema,
    DiscoveryRequest,
    GeneratedTransformationSqlSchema,
    MappingApprovalRequest,
    MappingSuggestionRequest,
    TableMappingPlanSchema,
    TableMetadataSchema,
    TransformationPreviewRequest,
    ValidationExecutionRequestSchema,
    ValidationExecutionResponse,
    ValidationPreviewRequest,
    ValidationPreviewResponse,
)

router = APIRouter(prefix="/api/v1/migrations", tags=["migrations"])
_CLIENT_ERRORS = {
    400: {"model": ErrorResponse, "description": "Invalid migration workflow request."},
    404: {"model": ErrorResponse, "description": "Requested migration resource unavailable."},
    409: {"model": ErrorResponse, "description": "Mapping or approval conflict."},
    422: {"model": ErrorResponse, "description": "Request schema validation failed."},
    502: {"model": ErrorResponse, "description": "Downstream database operation failed."},
}


def _error(status: int, code: str, message: str) -> ApiError:
    return ApiError(status, code, message)


@router.post(
    "/discover",
    name="migration_discover",
    operation_id="migration_discover",
    summary="Discover canonical table metadata",
    response_model=TableMetadataSchema,
    responses=_CLIENT_ERRORS,
)
async def discover(
    request: DiscoveryRequest,
    resolver: Annotated[Callable, Depends(get_schema_discovery_service)],
) -> TableMetadataSchema:
    try:
        connector = resolver(request.profile_id)
        metadata = connector.get_table_metadata(
            database=request.database_name,
            schema=request.schema_name,
            table=request.table_name,
        )
    except Exception as error:
        class_names = {item.__name__ for item in type(error).__mro__}
        if "UnknownProfileError" in class_names:
            raise _error(404, "PROFILE_NOT_FOUND", "The requested connection profile is unavailable.") from None
        if "SchemaDiscoveryError" in class_names:
            raise _error(502, "DISCOVERY_FAILED", "Table discovery could not be completed.") from None
        raise
    if metadata is None:
        raise _error(404, "TABLE_NOT_FOUND", "The requested table is unavailable.")
    try:
        return table_to_api(metadata)
    except (TypeError, ValueError):
        raise _error(502, "DISCOVERY_RESULT_INVALID", "Table discovery returned invalid canonical metadata.") from None


@router.post(
    "/mappings/suggest",
    name="migration_mapping_suggest",
    operation_id="migration_mapping_suggest",
    summary="Suggest deterministic column mappings",
    response_model=TableMappingPlanSchema,
    responses=_CLIENT_ERRORS,
)
async def suggest_mappings(
    request: MappingSuggestionRequest,
    service=Depends(get_schema_mapping_service),
) -> TableMappingPlanSchema:
    try:
        return plan_to_api(service.suggest(table_to_domain(request.source), table_to_domain(request.target)))
    except (TypeError, ValueError):
        raise _error(400, "INVALID_CANONICAL_METADATA", "Canonical table metadata is invalid.") from None


@router.post(
    "/mappings/approve",
    name="migration_mapping_approve",
    operation_id="migration_mapping_approve",
    summary="Apply explicit mapping decisions",
    response_model=ApprovedTableMappingPlanSchema,
    responses=_CLIENT_ERRORS,
)
async def approve_mappings(
    request: MappingApprovalRequest,
    service=Depends(get_mapping_approval_service),
) -> ApprovedTableMappingPlanSchema:
    try:
        result = service.apply(
            plan_to_domain(request.plan),
            source=table_to_domain(request.source),
            target=table_to_domain(request.target),
            decisions=tuple(decision_to_domain(item) for item in request.decisions),
        )
        return approved_plan_to_api(result)
    except (TypeError, ValueError):
        raise _error(409, "MAPPING_APPROVAL_CONFLICT", "The mapping decisions conflict with the mapping plan.") from None


@router.post(
    "/transformations/preview",
    name="migration_transformation_preview",
    operation_id="migration_transformation_preview",
    summary="Compile Snowflake transformation SQL",
    response_model=GeneratedTransformationSqlSchema,
    responses=_CLIENT_ERRORS,
)
async def preview_transformation(
    request: TransformationPreviewRequest,
    compiler=Depends(get_transformation_compiler),
) -> GeneratedTransformationSqlSchema:
    try:
        plan = approved_plan_to_domain(request.approved_plan)
    except (TypeError, ValueError):
        raise _error(400, "TRANSFORMATION_COMPILATION_FAILED", "The transformation preview could not be compiled.") from None
    try:
        kwargs = {
            "staging_database": request.staging_database,
            "staging_schema": request.staging_schema,
            "staging_table": request.staging_table,
        }
        if request.statement_type is TransformationStatementType.SELECT:
            generated = compiler.compile_select(plan, **kwargs)
        else:
            generated = compiler.compile_insert_select(plan, **kwargs)
        return transformation_sql_to_api(generated)
    except Exception as error:
        name = type(error).__name__
        if name not in {"UnsupportedTransformationError", "TransformationCompilationError", "InvalidTransformationPlanError"}:
            raise
        code = "UNSUPPORTED_TRANSFORMATION" if name == "UnsupportedTransformationError" else "TRANSFORMATION_COMPILATION_FAILED"
        message = "The transformation is unsupported." if code == "UNSUPPORTED_TRANSFORMATION" else "The transformation preview could not be compiled."
        raise _error(400, code, message) from None


@router.post(
    "/validations/preview",
    name="migration_validation_preview",
    operation_id="migration_validation_preview",
    summary="Compile paired validation SQL",
    response_model=ValidationPreviewResponse,
    responses=_CLIENT_ERRORS,
)
async def preview_validation(
    request: ValidationPreviewRequest,
    compiler: Annotated[Callable, Depends(get_validation_compiler)],
) -> ValidationPreviewResponse:
    try:
        plan = approved_plan_to_domain(request.approved_plan)
    except (TypeError, ValueError):
        raise _error(400, "VALIDATION_COMPILATION_FAILED", "The validation preview could not be compiled.") from None
    try:
        source, target = compiler(
            plan,
            source_schema=request.source_schema,
            source_table=request.source_table,
            target_database=request.target_database,
            target_schema=request.target_schema,
            target_table=request.target_table,
        )
        return ValidationPreviewResponse(source=validation_sql_to_api(source), target=validation_sql_to_api(target))
    except Exception as error:
        if type(error).__name__ not in {"TransformationCompilationError", "InvalidTransformationPlanError", "UnsupportedTransformationError"}:
            raise
        raise _error(400, "VALIDATION_COMPILATION_FAILED", "The validation preview could not be compiled.") from None


@router.post(
    "/validations/execute",
    name="migration_validation_execute",
    operation_id="migration_validation_execute",
    summary="Execute an approved validation",
    response_model=ValidationExecutionResponse,
    responses=_CLIENT_ERRORS,
)
async def execute_validation(
    request: ValidationExecutionRequestSchema,
    service_factory: Annotated[Callable, Depends(get_validation_execution_service_factory)],
) -> ValidationExecutionResponse:
    if request.explicitly_approved is not True:
        raise _error(400, "VALIDATION_APPROVAL_REQUIRED", "Explicit validation approval is required.")
    try:
        domain_request = execution_request_to_domain(request)
    except (TypeError, ValueError):
        raise _error(400, "INVALID_VALIDATION_REQUEST", "The validation execution request is invalid.") from None
    try:
        service = service_factory()
        return execution_report_to_api(service.run(domain_request))
    except Exception as error:
        name = type(error).__name__
        if name == "ValidationApprovalRequiredError":
            raise _error(400, "VALIDATION_APPROVAL_REQUIRED", "Explicit validation approval is required.") from None
        if name == "MalformedValidationExecutionResultError":
            raise _error(502, "VALIDATION_RESULT_INVALID", "Validation returned an invalid aggregate result.") from None
        if name == "ValidationExecutionError":
            raise _error(502, "VALIDATION_EXECUTION_FAILED", "Validation execution could not be completed.") from None
        raise
