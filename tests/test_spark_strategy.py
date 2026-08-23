"""Verify the Spark strategy keeps SchemaBridge's staging guarantees."""

from __future__ import annotations

from dataclasses import replace
from uuid import UUID

import pytest

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.discovery import ConstraintType, KeyConstraintMetadata
from schemabridge.transport.spark import SparkTransportSettings, SparkTransportStrategy
from schemabridge.services.batch_transport import (
    PreparedBatchTransport,
    ProfileBoundBatchTransportService,
)
from schemabridge.transport.base import BatchTransportError
from schemabridge.transport.strategy import SequentialBatchTransportStrategy
from tests.support.transport import BatchReaderSpy as Reader
from tests.support.transport import StagingWriterSpy as Writer
from tests.support.transport import transport_table as _table


class QueryReader(Reader):
    def __init__(self) -> None:
        super().__init__([])
        self.queries: list[str] = []

    def execute_query(self, query, **_kwargs):
        self.queries.append(query)
        return {"rows": [{"lower_bound": 1, "upper_bound": 3}]}


class QueryWriter(Writer):
    def __init__(self, row_count: int) -> None:
        super().__init__()
        self.row_count = row_count
        self.queries: list[str] = []

    def execute_query(self, query, **_kwargs):
        self.queries.append(query)
        return {"rows": [{"row_count": self.row_count}]}


class SessionFactory:
    def __init__(self) -> None:
        self.session = object()
        self.stopped = False

    def create(self, _settings):
        return self.session

    def stop(self, session):
        assert session is self.session
        self.stopped = True


class DataFrame:
    def __init__(self, count: int) -> None:
        self._count = count

    def count(self):
        return self._count


class DataFrameReader:
    def __init__(self, dataframe: DataFrame) -> None:
        self.dataframe = dataframe
        self.partition_plan = None

    def load(self, _session, partition_plan):
        self.partition_plan = partition_plan
        return self.dataframe


class DataFrameWriter:
    def __init__(self) -> None:
        self.dataframe = None
        self.write_plan = None

    def append(self, dataframe, write_plan):
        self.dataframe = dataframe
        self.write_plan = write_plan


def _table_with_primary_key():
    return replace(
        _table(),
        primary_key=KeyConstraintMetadata(
            name="customers_pkey",
            constraint_type=ConstraintType.PRIMARY_KEY,
            columns=("customer_id",),
            vendor_metadata={},
        ),
    )


def _profile(profile_id: str, *, write_enabled: bool) -> ConnectionProfile:
    return ConnectionProfile(
        profile_id=profile_id,
        db_type="postgresql",
        host="database.internal",
        database="source_db" if not write_enabled else "warehouse",
        username="operator",
        password="private-value",
        write_enabled=write_enabled,
    )


def _snowflake_profile(profile_id: str, *, write_enabled: bool) -> ConnectionProfile:
    return ConnectionProfile(
        profile_id=profile_id,
        db_type="snowflake",
        host="organization-account",
        database="ANALYTICS",
        username="operator",
        password="private-value",
        connection_options={"warehouse": "COMPUTE_WH", "role": "ANALYST"},
        write_enabled=write_enabled,
    )


def _strategy(*, written_rows: int):
    session_factory = SessionFactory()
    dataframe = DataFrame(3)
    reader = DataFrameReader(dataframe)
    writer = DataFrameWriter()
    return (
        SparkTransportStrategy(
            SparkTransportSettings(num_partitions=2),
            session_factory=session_factory,
            dataframe_reader=reader,
            dataframe_writer=writer,
        ),
        session_factory,
        reader,
        writer,
        QueryReader(),
        QueryWriter(written_rows),
    )


def test_strategy_proves_partitioned_source_rows_match_managed_staging() -> None:
    strategy, session_factory, dataframe_reader, dataframe_writer, source, target = _strategy(written_rows=3)

    result = strategy.transfer(
        source_reader=source,
        staging_writer=target,
        source_profile=_profile("source", write_enabled=False),
        target_profile=_profile("target", write_enabled=True),
        transport_id=UUID(int=9),
        source_table=_table_with_primary_key(),
        target_database="warehouse",
        target_schema="landing",
        batch_size=500,
        timeout_seconds=30,
        progress_reporter=None,
    )

    assert result.rows_read == result.rows_written == 3
    assert result.batch_count == 1
    assert result.staging_relation.object_name == "SB_STAGE_00000000000000000000000000000009"
    assert target.prepared[0]["definition"].relation == result.staging_relation
    assert dataframe_reader.partition_plan.num_partitions == 2
    assert dataframe_writer.write_plan.staging_table == '"landing"."SB_STAGE_00000000000000000000000000000009"'
    assert session_factory.stopped is True
    assert source.queries[0].startswith('SELECT MIN("customer_id")')
    assert target.queries[0].startswith("SELECT COUNT(*) AS row_count")


def test_strategy_rejects_mismatched_counts_after_stopping_spark() -> None:
    strategy, session_factory, _reader, _writer, source, target = _strategy(written_rows=2)

    with pytest.raises(BatchTransportError, match="did not match"):
        strategy.transfer(
            source_reader=source,
            staging_writer=target,
            source_profile=_profile("source", write_enabled=False),
            target_profile=_profile("target", write_enabled=True),
            transport_id=UUID(int=9),
            source_table=_table_with_primary_key(),
            target_database="warehouse",
            target_schema="landing",
            batch_size=500,
            timeout_seconds=30,
            progress_reporter=None,
        )

    assert session_factory.stopped is True


def test_strategy_supports_snowflake_source_and_target_without_integer_partitioning() -> None:
    session_factory = SessionFactory()
    dataframe = DataFrame(3)
    snowflake_reader = DataFrameReader(dataframe)
    snowflake_writer = DataFrameWriter()
    target = QueryWriter(3)
    strategy = SparkTransportStrategy(
        SparkTransportSettings(num_partitions=2),
        session_factory=session_factory,
        snowflake_dataframe_reader=snowflake_reader,
        snowflake_dataframe_writer=snowflake_writer,
    )
    source_table = replace(_table(), catalog_name="ANALYTICS", schema_name="REPORTING", system="snowflake")

    result = strategy.transfer(
        source_reader=Reader(()),
        staging_writer=target,
        source_profile=_snowflake_profile("source", write_enabled=False),
        target_profile=_snowflake_profile("target", write_enabled=True),
        transport_id=UUID(int=9),
        source_table=source_table,
        target_database="ANALYTICS",
        target_schema="LANDING",
        batch_size=500,
        timeout_seconds=30,
        progress_reporter=None,
    )

    assert result.rows_read == result.rows_written == 3
    assert snowflake_reader.partition_plan.query.startswith('SELECT "customer_id"')
    assert snowflake_writer.write_plan.staging_table == '"ANALYTICS"."LANDING"."SB_STAGE_00000000000000000000000000000009"'
    assert session_factory.stopped is True


def test_profile_bound_service_selects_spark_only_for_eligible_large_tables() -> None:
    source = QueryReader()
    target = QueryWriter(3)
    prepared = PreparedBatchTransport(
        source_profile_id="source",
        target_profile_id="target",
        batch_size=500,
        timeout_seconds=30,
        source_reader=source,
        staging_writer=target,
        source_profile=_profile("source", write_enabled=False),
        target_profile=_profile("target", write_enabled=True),
    )
    service = ProfileBoundBatchTransportService(
        lambda _profile_id: None,
        spark_settings=SparkTransportSettings(
            minimum_source_rows=3,
            jars_packages="net.snowflake:spark-snowflake_2.12:3.2.1",
        ),
    )

    selected = service.select_execution_strategy(
        prepared,
        replace(_table_with_primary_key(), estimated_row_count=3),
        target_database="warehouse",
        target_schema="landing",
    )
    small = service.select_execution_plan(
        prepared,
        replace(_table_with_primary_key(), estimated_row_count=2),
        target_database="warehouse",
        target_schema="landing",
    )

    assert isinstance(selected, SparkTransportStrategy)
    assert isinstance(small.strategy, SequentialBatchTransportStrategy)
    assert small.spark_selected is False
    assert small.fallback_reason == "SOURCE_ROW_ESTIMATE_BELOW_THRESHOLD"


def test_profile_bound_service_falls_back_when_the_snowflake_spark_package_is_missing() -> None:
    prepared = PreparedBatchTransport(
        source_profile_id="source",
        target_profile_id="target",
        batch_size=500,
        timeout_seconds=30,
        source_reader=Reader(()),
        staging_writer=Writer(),
        source_profile=_snowflake_profile("source", write_enabled=False),
        target_profile=_snowflake_profile("target", write_enabled=True),
    )
    selection = ProfileBoundBatchTransportService(
        lambda _profile_id: None,
        spark_settings=SparkTransportSettings(minimum_source_rows=3),
        spark_runtime_available=lambda: True,
    ).select_execution_plan(
        prepared,
        replace(_table(), catalog_name="ANALYTICS", schema_name="REPORTING", system="snowflake", estimated_row_count=3),
        target_database="ANALYTICS",
        target_schema="LANDING",
    )

    assert isinstance(selection.strategy, SequentialBatchTransportStrategy)
    assert selection.fallback_reason == "SNOWFLAKE_SPARK_CONNECTOR_UNAVAILABLE"


def test_profile_bound_service_selects_spark_for_an_eligible_large_snowflake_workflow() -> None:
    prepared = PreparedBatchTransport(
        source_profile_id="source",
        target_profile_id="target",
        batch_size=500,
        timeout_seconds=30,
        source_reader=Reader(()),
        staging_writer=Writer(),
        source_profile=_snowflake_profile("source", write_enabled=False),
        target_profile=_snowflake_profile("target", write_enabled=True),
    )
    service = ProfileBoundBatchTransportService(
        lambda _profile_id: None,
        spark_settings=SparkTransportSettings(
            minimum_source_rows=3,
            jars_packages="net.snowflake:spark-snowflake_2.12:3.2.1",
        ),
        spark_runtime_available=lambda: True,
    )

    selected = service.select_execution_strategy(
        prepared,
        replace(_table(), catalog_name="ANALYTICS", schema_name="REPORTING", system="snowflake", estimated_row_count=3),
        target_database="ANALYTICS",
        target_schema="LANDING",
    )

    assert isinstance(selected, SparkTransportStrategy)


def test_profile_bound_service_explains_runtime_and_ineligible_spark_fallbacks() -> None:
    prepared = PreparedBatchTransport(
        source_profile_id="source",
        target_profile_id="target",
        batch_size=500,
        timeout_seconds=30,
        source_reader=QueryReader(),
        staging_writer=QueryWriter(3),
        source_profile=_profile("source", write_enabled=False),
        target_profile=_profile("target", write_enabled=True),
    )
    unavailable = ProfileBoundBatchTransportService(
        lambda _profile_id: None,
        spark_settings=SparkTransportSettings(),
        spark_runtime_available=lambda: False,
    ).select_execution_plan(
        prepared, _table_with_primary_key(), target_database="warehouse", target_schema="landing"
    )
    missing_key = ProfileBoundBatchTransportService(
        lambda _profile_id: None,
        spark_settings=SparkTransportSettings(minimum_source_rows=1),
    ).select_execution_plan(
        prepared,
        replace(_table(), estimated_row_count=1),
        target_database="warehouse",
        target_schema="landing",
    )

    assert unavailable.fallback_reason == "SPARK_RUNTIME_UNAVAILABLE"
    assert missing_key.fallback_reason == "SPARK_JDBC_REQUIREMENTS_NOT_MET"
