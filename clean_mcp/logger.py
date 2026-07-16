"""Structured logging for the MCP execution framework.

This module owns request-scoped log context, JSON formatting, and handler
registration. It should not know anything about SQL semantics or MCP tools.
"""

from __future__ import annotations

import contextvars
import json
import logging

from config import Config

Config.load()

_request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")
_environment_var: contextvars.ContextVar[str] = contextvars.ContextVar("environment", default="-")


class _RequestContextFilter(logging.Filter):
    """Populate request-scoped context on every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Attach correlation fields that callers did not explicitly provide."""

        # Libraries may emit logs without our custom fields; defaults keep every
        # record valid JSON while request-scoped calls receive correlation data.
        if not hasattr(record, "request_id"):
            record.request_id = _request_id_var.get()
        if not hasattr(record, "environment"):
            record.environment = _environment_var.get()
        if not hasattr(record, "success"):
            record.success = None
        if not hasattr(record, "execution_time_ms"):
            record.execution_time_ms = None
        return True


class _JsonFormatter(logging.Formatter):
    """Emit machine-readable logs with the fields useful for audit trails."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize one standard LogRecord into the framework JSON schema."""

        # JSON logs can be searched, correlated, or ingested by a reporting
        # platform without parsing human-formatted text.
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "module": record.module,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", "-"),
            "environment": getattr(record, "environment", "-"),
            "success": getattr(record, "success", None),
            "execution_time_ms": getattr(record, "execution_time_ms", None),
        }

        for key in ("tool", "database", "status", "event", "error_code"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value

        return json.dumps(payload, default=str)


# A named, non-propagating logger prevents duplicate output through root handlers.
logger = logging.getLogger("mcp_execution_framework")
logger.setLevel(getattr(logging, Config.LOG_LEVEL, logging.INFO))
logger.propagate = False

if not logger.handlers:
    # Handler registration is idempotent because modules can be imported more
    # than once by test runners and MCP client startup discovery.
    formatter = _JsonFormatter()

    # MCP uses stderr for technical diagnostics so protocol output on stdout
    # remains clean for the MCP client.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.addFilter(_RequestContextFilter())

    logger.addHandler(console_handler)


def set_request_id(request_id: str) -> contextvars.Token[str]:
    """Bind a request ID to the current async/thread execution context."""

    return _request_id_var.set(request_id)


def reset_request_id(token: contextvars.Token[str]) -> None:
    """Restore the request context that existed before the current call."""

    _request_id_var.reset(token)


def set_environment(environment: str) -> contextvars.Token[str]:
    """Bind the active database/environment label to subsequent log records."""

    return _environment_var.set(environment)


def reset_environment(token: contextvars.Token[str]) -> None:
    """Restore the previous environment logging context."""

    _environment_var.reset(token)
