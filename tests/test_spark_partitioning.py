"""Verify Spark partitions only an unambiguous integer primary-key source."""

from __future__ import annotations

from dataclasses import replace

import pytest

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.discovery import ConstraintType, KeyConstraintMetadata
from schemabridge.transport.spark import (
    SparkJdbcPartitionPlanner,
    SparkJdbcPlanError,
    SparkJdbcReadPlanFactory,
)
from tests.support.transport import transport_table as _table


def _source():
    return replace(
        _table(),
        primary_key=KeyConstraintMetadata(
            name="customers_pkey",
            constraint_type=ConstraintType.PRIMARY_KEY,
            columns=("customer_id",),
            vendor_metadata={},
        ),
    )


def _profile():
    return ConnectionProfile(
        profile_id="postgres-source",
        db_type="postgresql",
        host="database.internal",
        database="source_db",
        username="reader",
        password="private-value",
    )


def test_partition_plan_uses_only_the_discovered_integer_primary_key() -> None:
    source = _source()
    read_plan = SparkJdbcReadPlanFactory.build(_profile(), source)

    assert SparkJdbcPartitionPlanner.bounds_query(read_plan, source) == (
        'SELECT MIN("customer_id") AS lower_bound, MAX("customer_id") AS upper_bound '
        'FROM "lab"."customers"'
    )
    partition = SparkJdbcPartitionPlanner.bind(
        read_plan, source, lower_bound=1, upper_bound=9_000, num_partitions=4
    )

    assert partition.spark_options() == {
        "partitionColumn": "customer_id",
        "lowerBound": "1",
        "upperBound": "9000",
        "numPartitions": "4",
    }


def test_partitioning_refuses_ambiguous_or_empty_ranges() -> None:
    source = _source()
    read_plan = SparkJdbcReadPlanFactory.build(_profile(), source)

    with pytest.raises(SparkJdbcPlanError, match="empty"):
        SparkJdbcPartitionPlanner.bind(
            read_plan, source, lower_bound=None, upper_bound=None, num_partitions=4
        )
    with pytest.raises(SparkJdbcPlanError, match="single-column"):
        SparkJdbcPartitionPlanner.partition_column(replace(source, primary_key=None))
