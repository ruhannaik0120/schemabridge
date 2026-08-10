"""Stable common Pydantic transport conventions."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints

BoundedText = Annotated[str, StringConstraints(min_length=1, max_length=512)]


class ApiSchema(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        ser_json_temporal="iso8601",
    )


class HealthResponse(ApiSchema):
    status: str
    service: str
    version: str


class ReadinessResponse(ApiSchema):
    status: str
    service: str
    version: str


class ErrorDetail(ApiSchema):
    code: str
    message: str
    request_id: str
    field: str | None = None


class ErrorResponse(ApiSchema):
    error: ErrorDetail
