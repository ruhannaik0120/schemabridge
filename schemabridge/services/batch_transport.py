"""Coordinate one bounded source-to-managed-staging data transfer.

This service joins vendor-neutral reader and writer contracts. It does not
approve mappings, execute the final target transformation, persist business
rows, or decide durable workflow recovery policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Callable
from uuid import UUID

from schemabridge.models.discovery import TableMetadata
from schemabridge.models.transport import (
    BatchTransportResult,
    BatchWriteResult,
    DataBatch,
    StagingColumn,
    StagingTableDefinition,
    TransportRelation,
)
from schemabridge.transport.base import (
    BatchSourceReader,
    BatchTransportError,
    StagingTableWriter,
)


class BatchTransportInvariantError(BatchTransportError):
    """Raised when a connector violates the agreed batch contract."""


class BatchTransportService:
    """Move rows from one reader into one prepared target staging table."""

    def __init__(
        self,
        *,
        source_reader: BatchSourceReader,
        staging_writer: StagingTableWriter,
    ) -> None:
        if not isinstance(source_reader, BatchSourceReader):
            raise TypeError("source_reader must implement BatchSourceReader.")
        if not isinstance(staging_writer, StagingTableWriter):
            raise TypeError("staging_writer must implement StagingTableWriter.")
        self.source_reader = source_reader
        self.staging_writer = staging_writer

    @staticmethod
    def _positive(value: int, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer.")

    @staticmethod
    def _identifier(value: str, name: str) -> None:
        if not isinstance(value, str) or not value.strip() or "\x00" in value:
            raise ValueError(f"{name} is invalid.")

    @classmethod
    def staging_relation(
        cls,
        *,
        transport_id: UUID,
        target_database: str,
        target_schema: str,
    ) -> TransportRelation:
        """Derive one deterministic, workflow-safe staging table identity."""

        if not isinstance(transport_id, UUID):
            raise TypeError("transport_id must be a UUID.")
        cls._identifier(target_database, "target_database")
        cls._identifier(target_schema, "target_schema")
        return TransportRelation(
            catalog_name=target_database,
            schema_name=target_schema,
            object_name=f"SB_STAGE_{transport_id.hex.upper()}",
        )

    @staticmethod
    def _definition(
        source_table: TableMetadata,
        staging_relation: TransportRelation,
    ) -> StagingTableDefinition:
        if not isinstance(source_table, TableMetadata):
            raise TypeError("source_table must be TableMetadata.")
        if not source_table.columns:
            raise ValueError("source_table must contain columns.")
        columns = tuple(
            StagingColumn(
                name=column.column_name,
                canonical_type=column.canonical_type,
                nullable=column.nullable,
                character_length=column.character_length,
                numeric_precision=column.numeric_precision,
                numeric_scale=column.numeric_scale,
                datetime_precision=column.datetime_precision,
            )
            for column in source_table.columns
        )
        return StagingTableDefinition(relation=staging_relation, columns=columns)

    def transfer(
        self,
        *,
        transport_id: UUID,
        source_table: TableMetadata,
        target_database: str,
        target_schema: str,
        batch_size: int,
        timeout_seconds: int,
    ) -> BatchTransportResult:
        """Prepare staging, copy every batch, and return sanitized totals."""

        self._positive(batch_size, "batch_size")
        self._positive(timeout_seconds, "timeout_seconds")
        staging_relation = self.staging_relation(
            transport_id=transport_id,
            target_database=target_database,
            target_schema=target_schema,
        )
        definition = self._definition(source_table, staging_relation)
        source_relation = TransportRelation(
            catalog_name=source_table.catalog_name,
            schema_name=source_table.schema_name,
            object_name=source_table.object_name,
        )
        column_names = tuple(column.name for column in definition.columns)

        self.staging_writer.prepare_staging_table(
            definition=definition,
            timeout_seconds=timeout_seconds,
        )
        batch_count = 0
        rows_read = 0
        rows_written = 0
        for batch in self.source_reader.read_batches(
            relation=source_relation,
            column_names=column_names,
            batch_size=batch_size,
            timeout_seconds=timeout_seconds,
        ):
            if not isinstance(batch, DataBatch):
                raise BatchTransportInvariantError(
                    "The source reader returned an invalid batch."
                )
            expected_number = batch_count + 1
            if (
                batch.batch_number != expected_number
                or batch.column_names != column_names
                or batch.row_count > batch_size
            ):
                raise BatchTransportInvariantError(
                    "The source reader returned an invalid batch."
                )
            written = self.staging_writer.write_batch(
                definition=definition,
                batch=batch,
                timeout_seconds=timeout_seconds,
            )
            if (
                not isinstance(written, BatchWriteResult)
                or written.batch_number != batch.batch_number
                or written.rows_received != batch.row_count
                or written.rows_written != batch.row_count
            ):
                raise BatchTransportInvariantError(
                    "The staging writer returned invalid batch evidence."
                )
            batch_count += 1
            rows_read += batch.row_count
            rows_written += written.rows_written

        return BatchTransportResult(
            transport_id=transport_id,
            source_relation=source_relation,
            staging_relation=staging_relation,
            batch_size=batch_size,
            batch_count=batch_count,
            column_count=len(column_names),
            rows_read=rows_read,
            rows_written=rows_written,
        )


class BatchTransportDisposition(str, Enum):
    """Describe what SchemaBridge can prove after the remote load boundary."""

    SUCCEEDED = "SUCCEEDED"
    CONFIRMED_FAILED_CLEANED_UP = "CONFIRMED_FAILED_CLEANED_UP"
    OUTCOME_UNCERTAIN = "OUTCOME_UNCERTAIN"


@dataclass(frozen=True, slots=True)
class PreparedBatchTransport:
    """Hold credential-free policy and the two already-resolved connectors."""

    source_profile_id: str
    target_profile_id: str
    batch_size: int
    timeout_seconds: int
    source_reader: BatchSourceReader
    staging_writer: StagingTableWriter


@dataclass(frozen=True, slots=True)
class ProfileBoundBatchTransportResult:
    """Return success evidence or a fixed safe failure classification."""

    disposition: BatchTransportDisposition
    result: BatchTransportResult | None = None
    failure_category: str | None = None


class ProfileBoundBatchTransportService:
    """Resolve named profiles and classify cleanup after a transfer failure."""

    def __init__(self, database_service_factory: Callable[[str], object]) -> None:
        self.database_service_factory = database_service_factory

    def prepare(
        self,
        *,
        source_profile_id: str,
        target_profile_id: str,
        target_database: str,
        batch_size: int | None,
        timeout_seconds: int | None,
    ) -> PreparedBatchTransport:
        """Resolve profile limits and require compatible reader/writer connectors."""

        try:
            source = self.database_service_factory(source_profile_id)
            target = self.database_service_factory(target_profile_id)
            source_profile = source.profile
            target_profile = target.profile
            if (
                source_profile.profile_id != source_profile_id
                or target_profile.profile_id != target_profile_id
                or target_profile.write_enabled is not True
                or target_profile.database != target_database
                or not isinstance(source.connector, BatchSourceReader)
                or not isinstance(target.connector, StagingTableWriter)
            ):
                raise ValueError
            requested_batch = batch_size or min(
                source_profile.max_rows,
                target_profile.max_rows,
            )
            requested_timeout = timeout_seconds or min(
                source_profile.timeout_seconds,
                target_profile.timeout_seconds,
            )
            BatchTransportService._positive(requested_batch, "batch_size")
            BatchTransportService._positive(requested_timeout, "timeout_seconds")
            effective_batch = min(
                requested_batch,
                source_profile.max_rows,
                target_profile.max_rows,
            )
            effective_timeout = min(
                requested_timeout,
                source_profile.timeout_seconds,
                target_profile.timeout_seconds,
            )
        except Exception:
            raise BatchTransportError(
                "The selected profiles cannot perform batch transport."
            ) from None
        return PreparedBatchTransport(
            source_profile_id=source_profile_id,
            target_profile_id=target_profile_id,
            batch_size=effective_batch,
            timeout_seconds=effective_timeout,
            source_reader=source.connector,
            staging_writer=target.connector,
        )

    def cleanup_staging(
        self,
        *,
        target_profile_id: str,
        target_database: str,
        relation: TransportRelation,
        timeout_seconds: int,
    ) -> None:
        """Idempotently remove one exact SchemaBridge-managed staging table."""

        try:
            if (
                not isinstance(relation, TransportRelation)
                or relation.catalog_name != target_database
                or re.fullmatch(r"SB_STAGE_[0-9A-F]{32}", relation.object_name) is None
            ):
                raise ValueError
            target = self.database_service_factory(target_profile_id)
            profile = target.profile
            if (
                profile.profile_id != target_profile_id
                or profile.database != target_database
                or profile.write_enabled is not True
                or not isinstance(target.connector, StagingTableWriter)
            ):
                raise ValueError
            BatchTransportService._positive(timeout_seconds, "timeout_seconds")
            effective_timeout = min(timeout_seconds, profile.timeout_seconds)
            target.connector.drop_staging_table(
                relation=relation,
                timeout_seconds=effective_timeout,
            )
        except Exception:
            raise BatchTransportError("Managed staging cleanup failed.") from None

    @staticmethod
    def run(
        prepared: PreparedBatchTransport,
        *,
        transport_id: UUID,
        source_table: TableMetadata,
        target_database: str,
        target_schema: str,
    ) -> ProfileBoundBatchTransportResult:
        """Run once and prove cleanup before classifying a failure as retryable."""

        staging_relation = BatchTransportService.staging_relation(
            transport_id=transport_id,
            target_database=target_database,
            target_schema=target_schema,
        )
        try:
            result = BatchTransportService(
                source_reader=prepared.source_reader,
                staging_writer=prepared.staging_writer,
            ).transfer(
                transport_id=transport_id,
                source_table=source_table,
                target_database=target_database,
                target_schema=target_schema,
                batch_size=prepared.batch_size,
                timeout_seconds=prepared.timeout_seconds,
            )
            return ProfileBoundBatchTransportResult(
                disposition=BatchTransportDisposition.SUCCEEDED,
                result=result,
            )
        except Exception:
            # A failed remote call does not prove whether the staging system
            # accepted a batch. A successful DROP proves the managed staging
            # table is gone, making a later deliberate retry safe.
            try:
                prepared.staging_writer.drop_staging_table(
                    relation=staging_relation,
                    timeout_seconds=prepared.timeout_seconds,
                )
            except Exception:
                return ProfileBoundBatchTransportResult(
                    disposition=BatchTransportDisposition.OUTCOME_UNCERTAIN,
                    failure_category="STAGING_OUTCOME_UNCERTAIN",
                )
            return ProfileBoundBatchTransportResult(
                disposition=BatchTransportDisposition.CONFIRMED_FAILED_CLEANED_UP,
                failure_category="STAGING_LOAD_FAILED",
            )


__all__ = [
    "BatchTransportDisposition",
    "BatchTransportInvariantError",
    "BatchTransportService",
    "PreparedBatchTransport",
    "ProfileBoundBatchTransportResult",
    "ProfileBoundBatchTransportService",
]
