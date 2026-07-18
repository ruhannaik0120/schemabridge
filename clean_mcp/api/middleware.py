"""Security, bounded-body, correlation, and safe request logging middleware."""

from __future__ import annotations

import logging
import secrets
import time
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .errors import error_payload

_LOGGER = logging.getLogger("schemabridge.api")
_MAX_REQUEST_ID_LENGTH = 64


class _PayloadTooLarge(Exception):
    pass


def safe_request_id(value: str | None) -> str:
    """Preserve only bounded visible ASCII IDs; otherwise create an opaque ID."""

    if (
        value
        and len(value) <= _MAX_REQUEST_ID_LENGTH
        and all(0x21 <= ord(character) <= 0x7E for character in value)
    ):
        return value
    return secrets.token_hex(16)


def _declared_length(scope: Scope) -> int | None:
    values = [value for name, value in scope.get("headers", ()) if name.lower() == b"content-length"]
    if not values:
        return None
    if len(values) != 1:
        raise _PayloadTooLarge
    try:
        value = int(values[0].decode("ascii"))
    except (UnicodeDecodeError, ValueError):
        raise _PayloadTooLarge from None
    if value < 0:
        raise _PayloadTooLarge
    return value


class PlatformMiddleware:
    """Apply platform policy at the ASGI boundary, including streamed bodies."""

    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = safe_request_id(Headers(scope=scope).get("X-Request-ID"))
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        started = time.perf_counter()
        status_code = 500

        async def send_with_policy(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                headers = MutableHeaders(scope=message)
                headers.setdefault("X-Content-Type-Options", "nosniff")
                headers.setdefault("X-Frame-Options", "DENY")
                headers.setdefault("Referrer-Policy", "no-referrer")
                cache_control = headers.get("Cache-Control")
                if scope.get("path", "").startswith("/api/v1") and (
                    cache_control is None or "no-store" not in cache_control.casefold()
                ):
                    headers["Cache-Control"] = "no-store"
                else:
                    headers.setdefault("Cache-Control", "no-store")
                headers["X-Request-ID"] = request_id
            await send(message)

        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_body_bytes:
                    raise _PayloadTooLarge
            return message

        request = Request(scope)
        try:
            declared = _declared_length(scope)
            if declared is not None and declared > self.max_body_bytes:
                raise _PayloadTooLarge
            await self.app(scope, limited_receive, send_with_policy)
        except _PayloadTooLarge:
            state["error_code"] = "PAYLOAD_TOO_LARGE"
            response = JSONResponse(
                error_payload(request, "PAYLOAD_TOO_LARGE", "Request body exceeds the allowed size."),
                status_code=413,
            )
            status_code = 413
            await response(scope, receive, send_with_policy)
        except Exception:
            state["error_code"] = "INTERNAL_ERROR"
            _LOGGER.error(
                "Unhandled API request failure.",
                extra={"request_id": request_id, "method": scope.get("method", "UNKNOWN")},
            )
            response = JSONResponse(
                error_payload(request, "INTERNAL_ERROR", "An unexpected application error occurred."),
                status_code=500,
            )
            status_code = 500
            await response(scope, receive, send_with_policy)
        finally:
            route = scope.get("route")
            _LOGGER.info(
                "API request completed.",
                extra={
                    "request_id": request_id,
                    "method": scope.get("method", "UNKNOWN"),
                    "route": getattr(route, "path", "unmatched"),
                    "status_code": status_code,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                    "error_code": state.get("error_code"),
                },
            )


def install_middleware(app: Any, *, max_body_bytes: int) -> None:
    app.add_middleware(PlatformMiddleware, max_body_bytes=max_body_bytes)


__all__ = ["PlatformMiddleware", "install_middleware", "safe_request_id"]
