"""Tool-layer routing between legacy and named QueryService instances."""

from __future__ import annotations

from collections.abc import Callable
from time import perf_counter
from uuid import uuid4

from config import ConfigError
from logger import logger
from models.errors import ErrorCode, StructuredError
from models.responses import ToolResponse
from services.profile_registry import ProfileRegistryError
from services.query_service import QueryService, get_query_service
from services.runtime_state import runtime_lock, runtime_metadata


def _profile_resolution_error(*, tool_name: str, start_time: float) -> dict:
    """Return a standard, credential-free response for named routing failures."""

    request_id = uuid4().hex[:12]
    response = ToolResponse(
        success=False,
        tool=tool_name,
        request_id=request_id,
        environment="DATABASE",
        execution_time_ms=int((perf_counter() - start_time) * 1000),
        data={},
        metadata={
            "profile": "unresolved",
            "db_type": "",
            **runtime_metadata(),
        },
        error=StructuredError(
            code=ErrorCode.CONFIG_INVALID,
            message="Unable to resolve the selected connection profile.",
            request_id=request_id,
            detail="The selected connection profile is missing, invalid, or unavailable.",
            hint="Choose a configured connection profile and try again.",
            retryable=False,
        ),
    )
    return response.to_dict()


def invoke_query_service(
    *,
    profile_id: str | None,
    tool_name: str,
    operation: Callable[[QueryService], ToolResponse],
) -> dict:
    """Invoke one operation through the legacy or named service path."""

    if profile_id is None:
        with runtime_lock:
            return operation(get_query_service()).to_dict()

    start_time = perf_counter()
    try:
        service = get_query_service(profile_id)
    except (ProfileRegistryError, ConfigError):
        logger.error("Named query service resolution failed")
        return _profile_resolution_error(tool_name=tool_name, start_time=start_time)
    return operation(service).to_dict()
