"""Pluggable implementations for source-to-managed-staging transport.

The workflow owns approval, cleanup, and durable evidence.  A strategy owns
only how approved source rows reach the already-approved staging boundary.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from schemabridge.models.discovery import TableMetadata
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.transport import BatchTransportResult
from schemabridge.transport.base import (
    BatchProgressReporter,
    BatchSourceReader,
    StagingTableWriter,
)


@runtime_checkable
class TransportExecutionStrategy(Protocol):
    """Move one source table to one managed staging relation safely."""

    strategy_name: str

    def transfer(
        self,
        *,
        source_reader: BatchSourceReader,
        staging_writer: StagingTableWriter,
        source_profile: ConnectionProfile | None,
        target_profile: ConnectionProfile | None,
        transport_id: UUID,
        source_table: TableMetadata,
        target_database: str,
        target_schema: str,
        batch_size: int,
        timeout_seconds: int,
        progress_reporter: BatchProgressReporter | None,
    ) -> BatchTransportResult: ...


class SequentialBatchTransportStrategy:
    """Preserve the existing bounded connector-batch implementation."""

    strategy_name = "SEQUENTIAL_CONNECTOR_BATCHES"

    def transfer(
        self,
        *,
        source_reader: BatchSourceReader,
        staging_writer: StagingTableWriter,
        source_profile: ConnectionProfile | None,
        target_profile: ConnectionProfile | None,
        transport_id: UUID,
        source_table: TableMetadata,
        target_database: str,
        target_schema: str,
        batch_size: int,
        timeout_seconds: int,
        progress_reporter: BatchProgressReporter | None,
    ) -> BatchTransportResult:
        """Delegate to the proven in-process batch transport path."""

        # Imported lazily to avoid a dependency cycle: BatchTransportService
        # will select a strategy, while this default strategy preserves it.
        from schemabridge.services.batch_transport import BatchTransportService

        del source_profile, target_profile

        return BatchTransportService(
            source_reader=source_reader,
            staging_writer=staging_writer,
            progress_reporter=progress_reporter,
        ).transfer(
            transport_id=transport_id,
            source_table=source_table,
            target_database=target_database,
            target_schema=target_schema,
            batch_size=batch_size,
            timeout_seconds=timeout_seconds,
        )
