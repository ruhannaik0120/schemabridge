"""Verify connector-neutral source-to-staging coordination."""

from __future__ import annotations

from uuid import UUID

import pytest

from schemabridge.models.discovery import (
    CoverageStatus,
    DatabaseObjectType,
    DiscoveryCoverage,
    ObjectPersistence,
    TableMetadata,
)
from schemabridge.models.metadata import CanonicalType, ColumnMetadata
from schemabridge.models.transport import BatchWriteResult, DataBatch
from schemabridge.services.batch_transport import (
    BatchTransportInvariantError,
    BatchTransportService,
)
from schemabridge.transport.base import BatchTransportError


TRANSPORT_ID = UUID("12345678-1234-5678-1234-567812345678")


def _column(
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


def _table() -> TableMetadata:
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
            _column(
                "customer_id",
                1,
                CanonicalType.INTEGER,
                nullable=False,
                precision=19,
                scale=0,
            ),
            _column("full_name", 2, CanonicalType.STRING, nullable=True),
        ),
        coverage=coverage,
        vendor_metadata={},
    )


class Reader:
    def __init__(self, batches):
        self.batches = tuple(batches)
        self.calls = []

    def read_batches(self, **kwargs):
        self.calls.append(kwargs)
        yield from self.batches


class Writer:
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


def _service(reader: Reader, writer: Writer) -> BatchTransportService:
    return BatchTransportService(source_reader=reader, staging_writer=writer)


def test_transfer_prepares_staging_moves_batches_and_returns_only_counts() -> None:
    batches = (
        DataBatch(
            batch_number=1,
            column_names=("customer_id", "full_name"),
            rows=((1, "Asha"), (2, "Rahul")),
        ),
        DataBatch(
            batch_number=2,
            column_names=("customer_id", "full_name"),
            rows=((3, "Neha"),),
        ),
    )
    reader = Reader(batches)
    writer = Writer()

    result = _service(reader, writer).transfer(
        transport_id=TRANSPORT_ID,
        source_table=_table(),
        target_database="SCHEMABRIDGE_LAB",
        target_schema="PUBLIC",
        batch_size=2,
        timeout_seconds=10,
    )

    expected_name = "SB_STAGE_12345678123456781234567812345678"
    definition = writer.prepared[0]["definition"]
    assert definition.relation.object_name == expected_name
    assert [column.name for column in definition.columns] == [
        "customer_id",
        "full_name",
    ]
    assert definition.columns[0].numeric_precision == 19
    assert reader.calls[0]["relation"].object_name == "customers"
    assert reader.calls[0]["batch_size"] == 2
    assert len(writer.writes) == 2
    assert writer.drops == []
    assert result.batch_count == 2
    assert result.rows_read == result.rows_written == 3
    assert result.column_count == 2
    assert result.staging_relation.object_name == expected_name
    assert "Asha" not in repr(result)


def test_empty_source_still_prepares_an_empty_staging_table() -> None:
    reader = Reader(())
    writer = Writer()

    result = _service(reader, writer).transfer(
        transport_id=TRANSPORT_ID,
        source_table=_table(),
        target_database="SCHEMABRIDGE_LAB",
        target_schema="PUBLIC",
        batch_size=100,
        timeout_seconds=10,
    )

    assert len(writer.prepared) == 1
    assert writer.writes == []
    assert result.batch_count == result.rows_read == result.rows_written == 0


@pytest.mark.parametrize(
    "batch",
    [
        DataBatch(
            batch_number=2,
            column_names=("customer_id", "full_name"),
            rows=((1, "A"),),
        ),
        DataBatch(
            batch_number=1,
            column_names=("full_name", "customer_id"),
            rows=(("A", 1),),
        ),
        DataBatch(
            batch_number=1,
            column_names=("customer_id", "full_name"),
            rows=((1, "A"), (2, "B"), (3, "C")),
        ),
    ],
)
def test_transfer_rejects_reader_contract_violations(batch: DataBatch) -> None:
    writer = Writer()

    with pytest.raises(BatchTransportInvariantError, match="source reader"):
        _service(Reader((batch,)), writer).transfer(
            transport_id=TRANSPORT_ID,
            source_table=_table(),
            target_database="SCHEMABRIDGE_LAB",
            target_schema="PUBLIC",
            batch_size=2,
            timeout_seconds=10,
        )

    assert writer.writes == []


def test_transfer_rejects_non_batch_reader_output() -> None:
    writer = Writer()

    with pytest.raises(BatchTransportInvariantError, match="source reader"):
        _service(Reader(("not-a-batch",)), writer).transfer(
            transport_id=TRANSPORT_ID,
            source_table=_table(),
            target_database="SCHEMABRIDGE_LAB",
            target_schema="PUBLIC",
            batch_size=2,
            timeout_seconds=10,
        )

    assert writer.writes == []


def test_transfer_rejects_false_writer_evidence() -> None:
    batch = DataBatch(
        batch_number=1,
        column_names=("customer_id", "full_name"),
        rows=((1, "A"),),
    )

    class LyingWriter(Writer):
        def write_batch(self, **kwargs):
            self.writes.append(kwargs)
            return BatchWriteResult(
                batch_number=2,
                rows_received=1,
                rows_written=1,
            )

    with pytest.raises(BatchTransportInvariantError, match="writer"):
        _service(Reader((batch,)), LyingWriter()).transfer(
            transport_id=TRANSPORT_ID,
            source_table=_table(),
            target_database="SCHEMABRIDGE_LAB",
            target_schema="PUBLIC",
            batch_size=2,
            timeout_seconds=10,
        )


def test_transfer_does_not_delete_staging_after_failure() -> None:
    class FailingWriter(Writer):
        def write_batch(self, **kwargs):
            raise BatchTransportError("safe failure")

    batch = DataBatch(
        batch_number=1,
        column_names=("customer_id", "full_name"),
        rows=((1, "A"),),
    )
    writer = FailingWriter()

    with pytest.raises(BatchTransportError):
        _service(Reader((batch,)), writer).transfer(
            transport_id=TRANSPORT_ID,
            source_table=_table(),
            target_database="SCHEMABRIDGE_LAB",
            target_schema="PUBLIC",
            batch_size=2,
            timeout_seconds=10,
        )

    # Later durable recovery must decide whether evidence should be inspected
    # or the partially loaded staging table should be removed.
    assert writer.drops == []


def test_staging_name_is_deterministic_for_safe_recovery() -> None:
    first = BatchTransportService.staging_relation(
        transport_id=TRANSPORT_ID,
        target_database="SCHEMABRIDGE_LAB",
        target_schema="PUBLIC",
    )
    second = BatchTransportService.staging_relation(
        transport_id=TRANSPORT_ID,
        target_database="SCHEMABRIDGE_LAB",
        target_schema="PUBLIC",
    )

    assert first == second
