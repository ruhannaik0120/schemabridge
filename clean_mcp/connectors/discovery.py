"""Vendor-neutral structural contract for canonical schema discovery."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from models.discovery import DatabaseObjectMetadata, DatabaseObjectType, SchemaMetadata, TableMetadata


class SchemaDiscoveryError(RuntimeError):
    """Raised when a schema-discovery operation cannot be completed safely."""


class SchemaDiscoveryConnectionError(SchemaDiscoveryError):
    """Raised when discovery loses or cannot establish its database connection."""


class SchemaDiscoveryTimeoutError(SchemaDiscoveryError):
    """Raised when discovery is cancelled or exceeds its operation timeout."""


class MalformedDiscoveryResultError(SchemaDiscoveryError):
    """Raised when required catalog identity data is malformed."""


@runtime_checkable
class SchemaDiscoveryConnector(Protocol):
    """Structural interface implemented by connectors with canonical discovery."""

    def list_schemas(
        self,
        *,
        database: str | None = None,
        timeout_seconds: int | None = None,
    ) -> tuple[SchemaMetadata, ...]: ...

    def list_objects(
        self,
        *,
        database: str | None = None,
        schema: str,
        object_types: tuple[DatabaseObjectType, ...] | None = None,
        timeout_seconds: int | None = None,
    ) -> tuple[DatabaseObjectMetadata, ...]: ...

    def get_table_metadata(
        self,
        *,
        database: str | None = None,
        schema: str,
        table: str,
        timeout_seconds: int | None = None,
    ) -> TableMetadata | None: ...


__all__ = [
    "MalformedDiscoveryResultError",
    "SchemaDiscoveryConnectionError",
    "SchemaDiscoveryConnector",
    "SchemaDiscoveryError",
    "SchemaDiscoveryTimeoutError",
]
