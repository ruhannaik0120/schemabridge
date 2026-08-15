"""Database-neutral structural contracts for batch extraction and staging."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from schemabridge.models.transport import (
    BatchWriteResult,
    DataBatch,
    StagingTableDefinition,
    TransportRelation,
)


class BatchTransportError(RuntimeError):
    """Base error for failures at the source-to-staging boundary."""


class BatchTransportTimeoutError(BatchTransportError):
    """Raised when bounded extraction or loading exceeds its timeout."""


class BatchTransportConnectionError(BatchTransportError):
    """Raised when a transport connector cannot establish or retain a connection."""


class UnsupportedStagingTypeError(BatchTransportError):
    """Raised when a source type cannot be represented in staging without guessing."""


@runtime_checkable
class BatchSourceReader(Protocol):
    """Read a relation incrementally without loading it all into memory."""

    def read_batches(
        self,
        *,
        relation: TransportRelation,
        column_names: tuple[str, ...],
        batch_size: int,
        timeout_seconds: int,
    ) -> Iterator[DataBatch]: ...


@runtime_checkable
class StagingTableWriter(Protocol):
    """Prepare, load, and remove one SchemaBridge-managed staging table."""

    def prepare_staging_table(
        self,
        *,
        definition: StagingTableDefinition,
        timeout_seconds: int,
    ) -> None: ...

    def write_batch(
        self,
        *,
        definition: StagingTableDefinition,
        batch: DataBatch,
        timeout_seconds: int,
    ) -> BatchWriteResult: ...

    def drop_staging_table(
        self,
        *,
        relation: TransportRelation,
        timeout_seconds: int,
    ) -> None: ...


__all__ = [
    "BatchSourceReader",
    "BatchTransportConnectionError",
    "BatchTransportError",
    "BatchTransportTimeoutError",
    "StagingTableWriter",
    "UnsupportedStagingTypeError",
]
