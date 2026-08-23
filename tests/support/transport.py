"""Connector-neutral transport builders and spies for tests."""

from __future__ import annotations

from schemabridge.models.discovery import (
    CoverageStatus,
    DatabaseObjectType,
    DiscoveryCoverage,
    ObjectPersistence,
    TableMetadata,
)
from schemabridge.models.metadata import CanonicalType, ColumnMetadata
from schemabridge.models.transport import BatchWriteResult


def _transport_column(
    name: str,
    ordinal: int,
    canonical_type: CanonicalType,
    *,
    nullable: bool,
    precision: int | None = None,
    scale: int | None = None,
) -> ColumnMetadata:
    return ColumnMetadata(
        catalog_name="source_db",
        schema_name="lab",
        table_name="customers",
        column_name=name,
        ordinal_position=ordinal,
        native_type="source type",
        canonical_type=canonical_type,
        nullable=nullable,
        character_length=None,
        numeric_precision=precision,
        numeric_scale=scale,
        datetime_precision=None,
    )


def transport_table(*, estimated_row_count: int | None = None) -> TableMetadata:
    coverage = DiscoveryCoverage(
        columns=CoverageStatus.COMPLETE,
        primary_key=CoverageStatus.COMPLETE,
        unique_constraints=CoverageStatus.COMPLETE,
        foreign_keys=CoverageStatus.COMPLETE,
        check_constraints=CoverageStatus.COMPLETE,
        comments=CoverageStatus.COMPLETE,
        estimated_row_count=CoverageStatus.COMPLETE,
        view_definition=CoverageStatus.NOT_APPLICABLE,
        partitioning=CoverageStatus.NOT_APPLICABLE,
        clustering=CoverageStatus.NOT_APPLICABLE,
    )
    return TableMetadata(
        catalog_name="source_db",
        schema_name="lab",
        object_name="customers",
        system="postgresql",
        object_type=DatabaseObjectType.TABLE,
        persistence=ObjectPersistence.PERMANENT,
        columns=(
            _transport_column(
                "customer_id",
                1,
                CanonicalType.INTEGER,
                nullable=False,
                precision=19,
                scale=0,
            ),
            _transport_column(
                "full_name",
                2,
                CanonicalType.STRING,
                nullable=True,
            ),
        ),
        coverage=coverage,
        estimated_row_count=estimated_row_count,
        vendor_metadata={},
    )


class BatchReaderSpy:
    def __init__(self, batches):
        self.batches = tuple(batches)
        self.calls = []

    def read_batches(self, **kwargs):
        self.calls.append(kwargs)
        yield from self.batches


class StagingWriterSpy:
    def __init__(self, *, rows_written_offset: int = 0):
        self.rows_written_offset = rows_written_offset
        self.prepared = []
        self.writes = []
        self.drops = []

    def prepare_staging_table(self, **kwargs):
        self.prepared.append(kwargs)

    def write_batch(self, **kwargs):
        self.writes.append(kwargs)
        batch = kwargs["batch"]
        return BatchWriteResult(
            batch_number=batch.batch_number,
            rows_received=batch.row_count,
            rows_written=batch.row_count + self.rows_written_offset,
        )

    def drop_staging_table(self, **kwargs):
        self.drops.append(kwargs)
