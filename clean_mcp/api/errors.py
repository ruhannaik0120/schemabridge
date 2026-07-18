"""Stable, redacted public API errors."""

from __future__ import annotations

import re
from dataclasses import dataclass

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .schemas.common import ErrorResponse

_ERROR_CODE_PATTERN = re.compile(r"[A-Z][A-Z0-9_]{1,63}\Z")


@dataclass(frozen=True, slots=True)
class ApiError(Exception):
    status_code: int
    code: str
    message: str

    def __post_init__(self) -> None:
        if isinstance(self.status_code, bool) or not isinstance(self.status_code, int) or not 400 <= self.status_code <= 599:
            raise ValueError("API error status codes must be integers from 400 through 599.")
        if not isinstance(self.code, str) or _ERROR_CODE_PATTERN.fullmatch(self.code) is None:
            raise ValueError("API error codes must be stable uppercase identifiers.")
        if (
            not isinstance(self.message, str)
            or not 1 <= len(self.message) <= 512
            or any(ord(character) < 0x20 and character not in "\t" for character in self.message)
        ):
            raise ValueError("API error messages must be bounded safe text.")


def error_payload(
    request: Request,
    code: str,
    message: str,
    *,
    field: str | None = None,
) -> dict[str, object]:
    request.state.error_code = code
    detail = {
        "code": code,
        "message": message,
        "request_id": getattr(request.state, "request_id", "unavailable"),
    }
    if field is not None:
        detail["field"] = field
    return {"error": detail}


def _safe_validation_field(error: RequestValidationError) -> str | None:
    """Return only the first bounded schema location, never its input or context."""

    errors = error.errors()
    if not errors:
        return None
    location = errors[0].get("loc", ())
    parts: list[str] = []
    for part in location:
        if part in {"body", "query", "path", "header", "cookie"} and not parts:
            continue
        if isinstance(part, int) and not isinstance(part, bool) and part >= 0:
            parts.append(str(part))
        elif isinstance(part, str) and 1 <= len(part) <= 64 and all(
            character.isascii() and (character.isalnum() or character in "_-") for character in part
        ):
            parts.append(part)
        else:
            return None
    rendered = ".".join(parts)
    return rendered if 1 <= len(rendered) <= 256 else None


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error_handler(request: Request, error: ApiError) -> JSONResponse:
        return JSONResponse(error_payload(request, error.code, error.message), status_code=error.status_code)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, error: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            error_payload(
                request,
                "REQUEST_VALIDATION_FAILED",
                "The request did not match the required schema.",
                field=_safe_validation_field(error),
            ),
            status_code=422,
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, error: StarletteHTTPException) -> JSONResponse:
        if error.status_code == 404:
            return JSONResponse(
                error_payload(request, "RESOURCE_NOT_FOUND", "The requested resource was not found."),
                status_code=404,
            )
        return JSONResponse(
            error_payload(request, "HTTP_ERROR", "The request could not be completed."),
            status_code=error.status_code,
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, _error: Exception) -> JSONResponse:
        return JSONResponse(
            error_payload(request, "INTERNAL_ERROR", "An unexpected application error occurred."),
            status_code=500,
        )


__all__ = ["ApiError", "error_payload", "install_error_handlers"]
