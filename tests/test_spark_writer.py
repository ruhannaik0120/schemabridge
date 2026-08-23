"""Verify Spark can append only to a managed, write-enabled staging relation."""

from __future__ import annotations

import pytest

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.metadata import CanonicalType
from schemabridge.models.transport import StagingColumn, StagingTableDefinition, TransportRelation
from schemabridge.spark import (
    SparkJdbcDataFrameWriter,
    SparkJdbcPlanError,
    SparkJdbcWritePlanFactory,
)


DEFINITION = StagingTableDefinition(
    relation=TransportRelation(catalog_name="warehouse", schema_name="landing", object_name="SB_STAGE_1234"),
    columns=(StagingColumn(name="id", canonical_type=CanonicalType.INTEGER, nullable=False),),
)


class Writer:
    def __init__(self) -> None:
        self.format_name = None
        self.options = {}
        self.mode_name = None
        self.saved = False

    def format(self, value):
        self.format_name = value
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def mode(self, value):
        self.mode_name = value
        return self

    def save(self):
        self.saved = True


class DataFrame:
    def __init__(self) -> None:
        self.write = Writer()


def _profile(*, write_enabled=True) -> ConnectionProfile:
    return ConnectionProfile(
        profile_id="postgres-target",
        db_type="postgresql",
        host="database.internal",
        database="warehouse",
        username="loader",
        password="private-value",
        write_enabled=write_enabled,
    )


def test_writer_appends_to_the_exact_managed_staging_table() -> None:
    plan = SparkJdbcWritePlanFactory.build(_profile(), DEFINITION)
    dataframe = DataFrame()

    SparkJdbcDataFrameWriter.append(dataframe, plan)

    assert dataframe.write.format_name == "jdbc"
    assert dataframe.write.options["dbtable"] == '"landing"."SB_STAGE_1234"'
    assert dataframe.write.mode_name == "append"
    assert dataframe.write.saved is True
    assert "private-value" not in repr(plan)


def test_writer_refuses_read_only_profiles_and_non_managed_tables() -> None:
    with pytest.raises(SparkJdbcPlanError, match="write-enabled"):
        SparkJdbcWritePlanFactory.build(_profile(write_enabled=False), DEFINITION)
    unsafe = StagingTableDefinition(
        relation=TransportRelation(catalog_name="warehouse", schema_name="landing", object_name="people"),
        columns=DEFINITION.columns,
    )
    with pytest.raises(SparkJdbcPlanError, match="managed"):
        SparkJdbcWritePlanFactory.build(_profile(), unsafe)
