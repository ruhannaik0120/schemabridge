"""Verify connector-neutral source-to-staging coordination."""

from __future__ import annotations

from uuid import UUID
from types import SimpleNamespace

import pytest

from schemabridge.models.discovery import (
    CoverageStatus,
    DatabaseObjectType,
    DiscoveryCoverage,
    ObjectPersistence,
    TableMetadata,
)
from schemabridge.models.metadata import CanonicalType, ColumnMetadata
from schemabridge.models.transport import (
    BatchTransportProgress,
    BatchWriteResult,
    DataBatch,
)
from schemabridge.models.transport import TransportRelation
from schemabridge.services.batch_transport import (
    BatchTransportDisposition,
    BatchTransportInvariantError,
    BatchTransportService,
    ProfileBoundBatchTransportService,
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


def _table(*, estimated_row_count: int | None = None) -> TableMetadata:
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
        estimated_row_count=estimated_row_count,
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


class ProgressRecorder:
    def __init__(self) -> None:
        self.snapshots: list[BatchTransportProgress] = []

    def report(self, progress: BatchTransportProgress) -> None:
        self.snapshots.append(progress)


def _service(
    reader: Reader,
    writer: Writer,
    reporter: ProgressRecorder | None = None,
) -> BatchTransportService:
    return BatchTransportService(
        source_reader=reader,
        staging_writer=writer,
        progress_reporter=reporter,
    )


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


def test_transfer_reports_cumulative_progress_after_each_completed_batch() -> None:
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
    reporter = ProgressRecorder()

    _service(Reader(batches), Writer(), reporter).transfer(
        transport_id=TRANSPORT_ID,
        source_table=_table(estimated_row_count=3),
        target_database="SCHEMABRIDGE_LAB",
        target_schema="PUBLIC",
        batch_size=2,
        timeout_seconds=10,
    )

    assert reporter.snapshots == [
        BatchTransportProgress(
            batches_completed=1,
            rows_read=2,
            rows_written=2,
            total_rows_estimate=3,
        ),
        BatchTransportProgress(
            batches_completed=2,
            rows_read=3,
            rows_written=3,
            total_rows_estimate=3,
        ),
    ]
    assert [
        item.estimated_percent_complete for item in reporter.snapshots
    ] == [66, 100]


def test_transfer_does_not_report_a_batch_before_writer_confirmation() -> None:
    class FailingWriter(Writer):
        def write_batch(self, **kwargs):
            raise BatchTransportError("safe failure")

    reporter = ProgressRecorder()
    batch = DataBatch(
        batch_number=1,
        column_names=("customer_id", "full_name"),
        rows=((1, "Asha"),),
    )

    with pytest.raises(BatchTransportError):
        _service(Reader((batch,)), FailingWriter(), reporter).transfer(
            transport_id=TRANSPORT_ID,
            source_table=_table(estimated_row_count=1),
            target_database="SCHEMABRIDGE_LAB",
            target_schema="PUBLIC",
            batch_size=2,
            timeout_seconds=10,
        )

    assert reporter.snapshots == []


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


def _profile_service(
    profile_id,
    connector,
    *,
    database,
    db_type,
    write_enabled,
    max_rows,
    timeout,
):
    return SimpleNamespace(
        profile=SimpleNamespace(
            profile_id=profile_id,
            database=database,
            db_type=db_type,
            write_enabled=write_enabled,
            max_rows=max_rows,
            timeout_seconds=timeout,
        ),
        connector=connector,
    )


def test_profile_bound_prepare_requires_permissions_and_clamps_limits() -> None:
    reader = Reader(())
    writer = Writer()
    services = {
        "source": _profile_service(
            "source",
            reader,
            database="source_db",
            db_type="future_database",
            write_enabled=False,
            max_rows=500,
            timeout=30,
        ),
        "target": _profile_service(
            "target",
            writer,
            database="SCHEMABRIDGE_LAB",
            db_type="future_database",
            write_enabled=True,
            max_rows=200,
            timeout=20,
        ),
    }

    prepared = ProfileBoundBatchTransportService(services.__getitem__).prepare(
        source_profile_id="source",
        target_profile_id="target",
        target_database="SCHEMABRIDGE_LAB",
        batch_size=1000,
        timeout_seconds=60,
    )

    assert prepared.batch_size == 200
    assert prepared.timeout_seconds == 20
    assert prepared.source_reader is reader
    assert prepared.staging_writer is writer


def test_profile_bound_prepare_assigns_roles_by_capability_not_vendor_name() -> None:
    """The workflow chooses roles; connector names and db_type do not."""

    reader = Reader(())
    writer = Writer()
    services = {
        "chosen-reader": _profile_service(
            "chosen-reader",
            reader,
            database="operational_data",
            db_type="snowflake",
            write_enabled=False,
            max_rows=500,
            timeout=30,
        ),
        "chosen-writer": _profile_service(
            "chosen-writer",
            writer,
            database="landing_data",
            db_type="postgresql",
            write_enabled=True,
            max_rows=500,
            timeout=30,
        ),
    }

    prepared = ProfileBoundBatchTransportService(services.__getitem__).prepare(
        source_profile_id="chosen-reader",
        target_profile_id="chosen-writer",
        target_database="landing_data",
        batch_size=100,
        timeout_seconds=10,
    )

    assert prepared.source_reader is reader
    assert prepared.staging_writer is writer


def test_profile_bound_prepare_rejects_read_only_target() -> None:
    services = {
        "source": _profile_service(
            "source",
            Reader(()),
            database="source_db",
            db_type="future_database",
            write_enabled=False,
            max_rows=500,
            timeout=30,
        ),
        "target": _profile_service(
            "target",
            Writer(),
            database="SCHEMABRIDGE_LAB",
            db_type="future_database",
            write_enabled=False,
            max_rows=500,
            timeout=30,
        ),
    }

    with pytest.raises(BatchTransportError, match="cannot perform"):
        ProfileBoundBatchTransportService(services.__getitem__).prepare(
            source_profile_id="source",
            target_profile_id="target",
            target_database="SCHEMABRIDGE_LAB",
            batch_size=100,
            timeout_seconds=10,
        )


def test_profile_bound_run_confirms_cleanup_before_allowing_retry() -> None:
    batch = DataBatch(
        batch_number=1,
        column_names=("customer_id", "full_name"),
        rows=((1, "A"),),
    )

    class FailingWriter(Writer):
        def write_batch(self, **kwargs):
            raise BatchTransportError("remote failure")

    writer = FailingWriter()
    prepared = SimpleNamespace(
        source_profile_id="source",
        target_profile_id="target",
        source_reader=Reader((batch,)),
        staging_writer=writer,
        batch_size=2,
        timeout_seconds=10,
    )

    result = ProfileBoundBatchTransportService.run(
        prepared,
        transport_id=TRANSPORT_ID,
        source_table=_table(),
        target_database="SCHEMABRIDGE_LAB",
        target_schema="PUBLIC",
    )

    assert result.disposition is BatchTransportDisposition.CONFIRMED_FAILED_CLEANED_UP
    assert len(writer.drops) == 1


def test_profile_bound_run_forwards_database_neutral_progress() -> None:
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
    writer = Writer()
    reporter = ProgressRecorder()
    prepared = SimpleNamespace(
        source_profile_id="source",
        target_profile_id="target",
        source_reader=Reader(batches),
        staging_writer=writer,
        batch_size=2,
        timeout_seconds=10,
    )

    result = ProfileBoundBatchTransportService.run(
        prepared,
        transport_id=TRANSPORT_ID,
        source_table=_table(estimated_row_count=3),
        target_database="SCHEMABRIDGE_LAB",
        target_schema="PUBLIC",
        progress_reporter=reporter,
    )

    assert result.disposition is BatchTransportDisposition.SUCCEEDED
    assert [item.rows_written for item in reporter.snapshots] == [2, 3]
    assert [
        item.estimated_percent_complete for item in reporter.snapshots
    ] == [66, 100]


def test_profile_bound_run_cleans_staging_when_progress_reporting_fails() -> None:
    class BrokenReporter:
        def report(self, progress):
            raise RuntimeError("control plane unavailable")

    batch = DataBatch(
        batch_number=1,
        column_names=("customer_id", "full_name"),
        rows=((1, "Asha"),),
    )
    writer = Writer()
    prepared = SimpleNamespace(
        source_profile_id="source",
        target_profile_id="target",
        source_reader=Reader((batch,)),
        staging_writer=writer,
        batch_size=2,
        timeout_seconds=10,
    )

    result = ProfileBoundBatchTransportService.run(
        prepared,
        transport_id=TRANSPORT_ID,
        source_table=_table(estimated_row_count=1),
        target_database="SCHEMABRIDGE_LAB",
        target_schema="PUBLIC",
        progress_reporter=BrokenReporter(),
    )

    assert result.disposition is BatchTransportDisposition.CONFIRMED_FAILED_CLEANED_UP
    assert result.failure_category == "PROGRESS_REPORTING_FAILED"
    assert len(writer.writes) == 1
    assert len(writer.drops) == 1


def test_profile_bound_run_marks_failure_uncertain_when_cleanup_fails() -> None:
    class BrokenWriter(Writer):
        def prepare_staging_table(self, **kwargs):
            raise BatchTransportError("create response lost")

        def drop_staging_table(self, **kwargs):
            raise BatchTransportError("cleanup response lost")

    prepared = SimpleNamespace(
        source_profile_id="source",
        target_profile_id="target",
        source_reader=Reader(()),
        staging_writer=BrokenWriter(),
        batch_size=2,
        timeout_seconds=10,
    )

    result = ProfileBoundBatchTransportService.run(
        prepared,
        transport_id=TRANSPORT_ID,
        source_table=_table(),
        target_database="SCHEMABRIDGE_LAB",
        target_schema="PUBLIC",
    )

    assert result.disposition is BatchTransportDisposition.OUTCOME_UNCERTAIN
    assert result.failure_category == "STAGING_OUTCOME_UNCERTAIN"


def test_cleanup_accepts_only_exact_managed_table_and_write_profile() -> None:
    writer = Writer()
    services = {
        "target": _profile_service(
            "target",
            writer,
            database="SCHEMABRIDGE_LAB",
            db_type="future_database",
            write_enabled=True,
            max_rows=200,
            timeout=20,
        )
    }
    service = ProfileBoundBatchTransportService(services.__getitem__)
    relation = TransportRelation(
        catalog_name="SCHEMABRIDGE_LAB",
        schema_name="PUBLIC",
        object_name="SB_STAGE_12345678123456781234567812345678",
    )

    service.cleanup_staging(
        target_profile_id="target",
        target_database="SCHEMABRIDGE_LAB",
        relation=relation,
        timeout_seconds=60,
    )

    assert writer.drops == [{"relation": relation, "timeout_seconds": 20}]
    with pytest.raises(BatchTransportError, match="cleanup failed"):
        service.cleanup_staging(
            target_profile_id="target",
            target_database="SCHEMABRIDGE_LAB",
            relation=TransportRelation(
                catalog_name="SCHEMABRIDGE_LAB",
                schema_name="PUBLIC",
                object_name="CUSTOMERS_TARGET",
            ),
            timeout_seconds=10,
        )
