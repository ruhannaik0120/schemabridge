"""Structured error primitives for the MCP execution framework.

This module owns the reusable error schema returned to MCP clients. It should
not contain transport logic, SQL logic, or client-specific behavior.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class StructuredError:
    """Machine-readable error payload used by all MCP tools.

    The class exists so every tool returns the same error shape without exposing
    raw exceptions or transport-specific stack traces.
    """

    code: str
    message: str
    request_id: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    detail: str | None = None
    hint: str | None = None
    retryable: bool = False
    # Context contains safe diagnostic metadata only; credentials must never be
    # attached to an error returned to an AI client.
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the error into the contract expected by MCP clients."""

        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "request_id": self.request_id,
            "timestamp": self.timestamp,
            "retryable": self.retryable,
            "context": self.context,
        }
        if self.detail is not None:
            payload["detail"] = self.detail
        if self.hint is not None:
            payload["hint"] = self.hint
        return payload


class ErrorCode:
    """Canonical error codes returned by the service layer."""

    CONFIG_INVALID = "CONFIG_INVALID"
    CONNECTION_FAILED = "CONNECTION_FAILED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    QUERY_BLOCKED = "QUERY_BLOCKED"
    DATABASE_ERROR = "DATABASE_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"
