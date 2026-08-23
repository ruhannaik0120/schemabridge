"""Verify SchemaBridge passes only its trusted JDBC configuration to Spark."""

from __future__ import annotations

from dataclasses import replace

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.discovery import ConstraintType, KeyConstraintMetadata
from schemabridge.transport.spark import (
    SparkJdbcDataFrameReader,
    SparkJdbcPartitionPlanner,
    SparkJdbcReadPlanFactory,
)
from tests.support.transport import transport_table as _table


class Reader:
    def __init__(self) -> None:
        self.format_name = None
        self.options = {}
        self.loaded = object()

    def format(self, value):
        self.format_name = value
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self):
        return self.loaded


class Session:
    def __init__(self) -> None:
        self.read = Reader()


def test_reader_uses_generated_subquery_and_partition_options() -> None:
    source = replace(
        _table(),
        primary_key=KeyConstraintMetadata(
            name="customers_pkey",
            constraint_type=ConstraintType.PRIMARY_KEY,
            columns=("customer_id",),
            vendor_metadata={},
        ),
    )
    profile = ConnectionProfile(
        profile_id="postgres-source",
        db_type="postgresql",
        host="database.internal",
        database="source_db",
        username="reader",
        password="private-value",
    )
    read_plan = SparkJdbcReadPlanFactory.build(profile, source)
    partition_plan = SparkJdbcPartitionPlanner.bind(
        read_plan, source, lower_bound=1, upper_bound=9_000, num_partitions=4
    )
    session = Session()

    result = SparkJdbcDataFrameReader.load(session, partition_plan)

    assert result is session.read.loaded
    assert session.read.format_name == "jdbc"
    assert session.read.options["dbtable"] == (
        '(SELECT "customer_id", "full_name" FROM "lab"."customers") AS sb_source'
    )
    assert session.read.options["partitionColumn"] == "customer_id"
    assert session.read.options["numPartitions"] == "4"
    assert session.read.options["password"] == "private-value"
