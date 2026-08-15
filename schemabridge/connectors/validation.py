"""Connector capability for generated read-only validation queries."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from schemabridge.models.mapping import SqlDialect


@runtime_checkable
class ValidationQueryDialectProvider(Protocol):
    """Declare which SQL dialect a connector can safely validate."""

    def validation_sql_dialect(self) -> SqlDialect: ...


__all__ = ["ValidationQueryDialectProvider"]
