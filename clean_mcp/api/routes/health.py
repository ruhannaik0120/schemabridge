"""Database-independent operational health routes."""

from fastapi import APIRouter, Request

from .. import __version__
from ..errors import ApiError
from ..schemas.common import ErrorResponse, HealthResponse, ReadinessResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get(
    "/live",
    operation_id="health_live",
    summary="Check process liveness",
    response_model=HealthResponse,
)
async def live() -> HealthResponse:
    return HealthResponse(status="ok", service="schemabridge-api", version=__version__)


@router.get(
    "/ready",
    operation_id="health_ready",
    summary="Check application readiness",
    response_model=ReadinessResponse,
    responses={503: {"model": ErrorResponse}},
)
async def ready(request: Request) -> ReadinessResponse:
    if not getattr(request.app.state, "ready", False):
        raise ApiError(503, "SERVICE_NOT_READY", "The application is not ready.")
    return ReadinessResponse(status="ok", service="schemabridge-api", version=__version__)
