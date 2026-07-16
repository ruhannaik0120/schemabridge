"""Structured response primitives for the MCP execution framework.

This module owns the standard response envelope returned by every tool. It must
not know how SQL is executed or how MCP registers tools.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from models.errors import StructuredError


@dataclass(slots=True)
class ToolResponse:
    """Shared response envelope for all MCP tool calls.

    This abstraction keeps the response contract consistent across tools and
    preserves compatibility for clients that expect some top-level data fields.
    """

    success: bool
    tool: str
    request_id: str
    environment: str
    execution_time_ms: int
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    error: StructuredError | None = None

    _reserved_keys = {
        "success",
        "tool",
        "request_id",
        "timestamp",
        "execution_time_ms",
        "environment",
        "metadata",
        "error",
        "data",
    }

    def to_dict(self) -> dict[str, Any]:
        """Serialize the response without allowing payload keys to overwrite the envelope."""

        response = {
            "success": self.success,
            "tool": self.tool,
            "request_id": self.request_id,
            "timestamp": self.timestamp,
            "execution_time_ms": self.execution_time_ms,
            "environment": self.environment,
            "data": self.data,
            "metadata": self.metadata,
        }

        # Preserve top-level compatibility for existing clients while protecting envelope keys.
        for key, value in self.data.items():
            if key not in self._reserved_keys:
                response[key] = value

        if self.error is not None:
            response["error"] = self.error.to_dict()
        return response
