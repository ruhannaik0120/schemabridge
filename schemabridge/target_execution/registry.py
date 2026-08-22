"""Register and resolve target adapters without changing workflow code."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from schemabridge.models.mapping import SqlDialect

from .base import (
    TargetExecutionAdapter,
    TargetExecutionCapabilities,
    TargetTransformationCompiler,
)


class TargetExecutionRegistryError(ValueError):
    """Base error for invalid or unavailable target adapter registration."""


class InvalidTargetAdapterError(TargetExecutionRegistryError):
    """Raised when an adapter does not implement the shared target skeleton."""


class DuplicateTargetAdapterError(TargetExecutionRegistryError):
    """Raised when two adapters claim the same database type."""


class UnsupportedTargetSystemError(TargetExecutionRegistryError):
    """Raised when no target adapter is registered for a requested system."""


@dataclass(frozen=True, slots=True)
class TargetExecutionCapabilitySummary:
    """Describe one registered target without exposing its implementation."""

    database_type: str
    dialect: SqlDialect
    capabilities: TargetExecutionCapabilities


_DATABASE_TYPE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")


def _database_key(value: object) -> str:
    if not isinstance(value, str):
        raise InvalidTargetAdapterError("Target database type is invalid.")
    normalized = value.strip().casefold()
    if _DATABASE_TYPE.fullmatch(normalized) is None:
        raise InvalidTargetAdapterError("Target database type is invalid.")
    return normalized


class TargetExecutionRegistry:
    """Hold one explicit target adapter per normalized database type."""

    def __init__(self, adapters: Iterable[TargetExecutionAdapter] = ()) -> None:
        self._adapters: dict[str, TargetExecutionAdapter] = {}
        for adapter in adapters:
            self.register(adapter)

    def register(self, adapter: TargetExecutionAdapter) -> None:
        """Register one complete adapter and reject silent replacement."""

        try:
            key = _database_key(adapter.database_type)
            dialect = adapter.dialect
            capabilities = adapter.capabilities
            compiler = adapter.compiler
        except (AttributeError, TypeError, ValueError):
            raise InvalidTargetAdapterError("Target adapter is invalid.") from None
        if (
            not isinstance(dialect, SqlDialect)
            or not isinstance(capabilities, TargetExecutionCapabilities)
            or not isinstance(compiler, TargetTransformationCompiler)
            or not callable(getattr(adapter, "validate_preview", None))
            or not callable(getattr(adapter, "execute", None))
        ):
            raise InvalidTargetAdapterError("Target adapter is invalid.")
        if key in self._adapters:
            raise DuplicateTargetAdapterError(
                "A target adapter is already registered for this database type."
            )
        self._adapters[key] = adapter

    def resolve(self, database_type: str) -> TargetExecutionAdapter:
        """Return the exact registered adapter for one requested target system."""

        try:
            key = _database_key(database_type)
        except InvalidTargetAdapterError:
            raise UnsupportedTargetSystemError(
                "The requested target database system is unsupported."
            ) from None
        try:
            return self._adapters[key]
        except KeyError:
            raise UnsupportedTargetSystemError(
                "The requested target database system is unsupported."
            ) from None

    @property
    def supported_database_types(self) -> tuple[str, ...]:
        """Return stable, safe capability names for diagnostics and tests."""

        return tuple(sorted(self._adapters))

    @property
    def capability_summaries(self) -> tuple[TargetExecutionCapabilitySummary, ...]:
        """Return a stable read-only description of all registered targets."""

        return tuple(
            TargetExecutionCapabilitySummary(
                database_type=database_type,
                dialect=adapter.dialect,
                capabilities=adapter.capabilities,
            )
            for database_type, adapter in sorted(self._adapters.items())
        )


__all__ = [
    "DuplicateTargetAdapterError",
    "InvalidTargetAdapterError",
    "TargetExecutionRegistry",
    "TargetExecutionRegistryError",
    "TargetExecutionCapabilitySummary",
    "UnsupportedTargetSystemError",
]
