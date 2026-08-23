"""Verify trusted Snowflake Connector for Spark source-read plans."""

from __future__ import annotations

from dataclasses import replace

import pytest

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.metadata import CanonicalType
from schemabridge.models.transport import StagingColumn, StagingTableDefinition, TransportRelation
from schemabridge.spark import (
    SparkJdbcPlanError,
    SparkSnowflakeDataFrameReader,
    SparkSnowflakeDataFrameWriter,
    SparkSnowflakeReadPlanFactory,
    SparkSnowflakeWritePlanFactory,
)
from tests.test_batch_transport_service import _table


def _profile() -> ConnectionProfile:
    return ConnectionProfile(
        profile_id="snowflake-source",
        db_type="snowflake",
        host="organization-account",
        database="ANALYTICS",
        username="reader",
        password="private-value",
        connection_options={"warehouse": "COMPUTE_WH", "role": "ANALYST"},
    )


def _source():
    return replace(
        _table(),
        catalog_name="ANALYTICS",
        schema_name="REPORTING",
        system="snowflake",
    )


class Reader:
    def __init__(self) -> None:
        self.format_name = None
        self.options = {}
        self.loaded = False

    def format(self, value):
        self.format_name = value
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self):
        self.loaded = True
        return "dataframe"


class Session:
    def __init__(self) -> None:
        self.read = Reader()


class Writer(Reader):
    def __init__(self) -> None:
        super().__init__()
        self.mode_name = None

    def mode(self, value):
        self.mode_name = value
        return self

    def save(self):
        self.loaded = True


class DataFrame:
    def __init__(self) -> None:
        self.write = Writer()


def test_plan_uses_discovered_identifiers_and_redacts_credentials() -> None:
    plan = SparkSnowflakeReadPlanFactory.build(_profile(), _source())

    assert plan.query == 'SELECT "customer_id", "full_name" FROM "ANALYTICS"."REPORTING"."customers"'
    assert plan.safe_dict() == {"source": "net.snowflake.spark.snowflake", "column_count": 2, "role_configured": True}
    assert "private-value" not in repr(plan)


def test_reader_uses_the_official_data_source_with_only_trusted_options() -> None:
    plan = SparkSnowflakeReadPlanFactory.build(_profile(), _source())
    session = Session()

    assert SparkSnowflakeDataFrameReader.load(session, plan) == "dataframe"
    assert session.read.format_name == "net.snowflake.spark.snowflake"
    assert session.read.options["query"] == plan.query
    assert session.read.options["sfWarehouse"] == "COMPUTE_WH"
    assert session.read.loaded is True


def test_plan_refuses_wrong_database_or_missing_warehouse() -> None:
    with pytest.raises(SparkJdbcPlanError, match="does not match"):
        SparkSnowflakeReadPlanFactory.build(_profile(), replace(_source(), catalog_name="OTHER"))
    without_warehouse = ConnectionProfile(
        profile_id="snowflake-source",
        db_type="snowflake",
        host="organization-account",
        database="ANALYTICS",
        username="reader",
        password="private-value",
    )
    with pytest.raises(SparkJdbcPlanError, match="warehouse"):
        SparkSnowflakeReadPlanFactory.build(without_warehouse, _source())


def test_writer_appends_only_to_the_exact_managed_snowflake_staging_table() -> None:
    definition = StagingTableDefinition(
        relation=TransportRelation(catalog_name="ANALYTICS", schema_name="LANDING", object_name="SB_STAGE_1234"),
        columns=(StagingColumn(name="customer_id", canonical_type=CanonicalType.INTEGER, nullable=False),),
    )
    profile = ConnectionProfile(
        profile_id="snowflake-target",
        db_type="snowflake",
        host="organization-account",
        database="ANALYTICS",
        username="loader",
        password="private-value",
        connection_options={"warehouse": "COMPUTE_WH", "role": "LOADER"},
        write_enabled=True,
    )
    dataframe = DataFrame()

    plan = SparkSnowflakeWritePlanFactory.build(profile, definition)
    SparkSnowflakeDataFrameWriter.append(dataframe, plan)

    assert dataframe.write.options["dbtable"] == '"ANALYTICS"."LANDING"."SB_STAGE_1234"'
    assert dataframe.write.mode_name == "append"
    assert dataframe.write.loaded is True
    assert "private-value" not in repr(plan)


def test_writer_refuses_read_only_or_non_managed_targets() -> None:
    definition = StagingTableDefinition(
        relation=TransportRelation(catalog_name="ANALYTICS", schema_name="LANDING", object_name="people"),
        columns=(StagingColumn(name="customer_id", canonical_type=CanonicalType.INTEGER, nullable=False),),
    )
    with pytest.raises(SparkJdbcPlanError, match="managed"):
        SparkSnowflakeWritePlanFactory.build(_profile(), definition)
