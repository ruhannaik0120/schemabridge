"""Verify Spark JDBC reads are generated from trusted profiles and metadata."""

from __future__ import annotations

from dataclasses import replace

import pytest

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.spark import SparkJdbcPlanError, SparkJdbcReadPlanFactory
from tests.test_batch_transport_service import _table


def _profile(database_type: str) -> ConnectionProfile:
    database = "source_db" if database_type == "postgresql" else "shop"
    return ConnectionProfile(
        profile_id=f"{database_type}-source",
        db_type=database_type,
        host="database.internal",
        database=database,
        username="reader",
        password="private-value",
        connection_options={"port": 5432 if database_type == "postgresql" else 3306},
    )


def test_postgresql_plan_quotes_discovered_identifiers_and_hides_credentials() -> None:
    plan = SparkJdbcReadPlanFactory.build(_profile("postgresql"), _table())

    assert plan.query == 'SELECT "customer_id", "full_name" FROM "lab"."customers"'
    assert plan.connection_properties["driver"] == "org.postgresql.Driver"
    assert "private-value" not in repr(plan)
    assert plan.safe_dict() == {
        "database_type": "postgresql",
        "driver": "org.postgresql.Driver",
        "column_count": 2,
    }


def test_mysql_plan_uses_database_table_convention() -> None:
    table = replace(_table(), catalog_name="shop", schema_name="shop", system="mysql")
    plan = SparkJdbcReadPlanFactory.build(_profile("mysql"), table)

    assert plan.query == "SELECT `customer_id`, `full_name` FROM `shop`.`customers`"
    assert plan.connection_properties["driver"] == "com.mysql.cj.jdbc.Driver"


def test_plan_rejects_wrong_profile_database_or_unsupported_source() -> None:
    with pytest.raises(SparkJdbcPlanError, match="does not match"):
        SparkJdbcReadPlanFactory.build(_profile("postgresql"), replace(_table(), catalog_name="other"))
    with pytest.raises(SparkJdbcPlanError, match="unsupported"):
        SparkJdbcReadPlanFactory.build(_profile("mysql"), _table())
