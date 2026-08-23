"""Opt-in live proof for Snowflake source through Spark-managed staging."""

from __future__ import annotations

import os
from dataclasses import replace
from uuid import uuid4

import pytest

from schemabridge.connectors.snowflake.connector import SnowflakeConnector
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.services.batch_transport import BatchTransportService
from schemabridge.spark import SparkTransportSettings, SparkTransportStrategy
from tests.test_batch_transport_service import _table


def _environment() -> dict[str, str]:
    if os.getenv("SCHEMABRIDGE_SPARK_SNOWFLAKE_INTEGRATION") != "1":
        pytest.skip("Spark Snowflake integration environment is not enabled.")
    names = (
        "SCHEMABRIDGE_SPARK_SNOWFLAKE_ACCOUNT",
        "SCHEMABRIDGE_SPARK_SNOWFLAKE_DATABASE",
        "SCHEMABRIDGE_SPARK_SNOWFLAKE_SCHEMA",
        "SCHEMABRIDGE_SPARK_SNOWFLAKE_WAREHOUSE",
        "SCHEMABRIDGE_SPARK_SNOWFLAKE_USERNAME",
        "SCHEMABRIDGE_SPARK_SNOWFLAKE_PASSWORD",
        "SCHEMABRIDGE_SPARK_JARS_PACKAGES",
    )
    values = {name: os.getenv(name, "") for name in names}
    if not all(values.values()):
        pytest.skip("Spark Snowflake integration environment is not configured.")
    role = os.getenv("SCHEMABRIDGE_SPARK_SNOWFLAKE_ROLE", "").strip()
    if role:
        values["role"] = role
    return values


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def test_snowflake_to_spark_to_snowflake_staging_when_explicitly_configured() -> None:
    environment = _environment()
    import snowflake.connector

    source_name = f"SB_SPARK_SOURCE_{uuid4().hex.upper()}"
    connection_options = {
        "warehouse": environment["SCHEMABRIDGE_SPARK_SNOWFLAKE_WAREHOUSE"],
        "schema": environment["SCHEMABRIDGE_SPARK_SNOWFLAKE_SCHEMA"],
    }
    if "role" in environment:
        connection_options["role"] = environment["role"]
    profile_base = dict(
        db_type="snowflake",
        host=environment["SCHEMABRIDGE_SPARK_SNOWFLAKE_ACCOUNT"],
        database=environment["SCHEMABRIDGE_SPARK_SNOWFLAKE_DATABASE"],
        username=environment["SCHEMABRIDGE_SPARK_SNOWFLAKE_USERNAME"],
        password=environment["SCHEMABRIDGE_SPARK_SNOWFLAKE_PASSWORD"],
        connection_options=connection_options,
        max_rows=100,
    )
    connection_kwargs = {
        "account": profile_base["host"],
        "database": profile_base["database"],
        "user": profile_base["username"],
        "password": profile_base["password"],
        "warehouse": connection_options["warehouse"],
        "schema": connection_options["schema"],
    }
    if "role" in connection_options:
        connection_kwargs["role"] = connection_options["role"]
    with snowflake.connector.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TRANSIENT TABLE {_quote(source_name)} "
                '("customer_id" INTEGER, "full_name" VARCHAR(100))'
            )
            cursor.execute(
                f"INSERT INTO {_quote(source_name)} (\"customer_id\", \"full_name\") "
                "VALUES (1, 'Asha'), (2, 'Rahul'), (3, 'Neha')"
            )

    source_profile = ConnectionProfile(profile_id="spark-proof-source", **profile_base)
    target_profile = ConnectionProfile(
        profile_id="spark-proof-target", write_enabled=True, **profile_base
    )
    source = SnowflakeConnector(profile=source_profile)
    target = SnowflakeConnector(profile=target_profile)
    transport_id = uuid4()
    source_table = replace(
        _table(),
        catalog_name=profile_base["database"],
        schema_name=connection_options["schema"],
        object_name=source_name,
        system="snowflake",
        estimated_row_count=3,
    )
    staging_relation = BatchTransportService.staging_relation(
        transport_id=transport_id,
        target_database=profile_base["database"],
        target_schema=connection_options["schema"],
    )
    staging_created = False
    try:
        result = SparkTransportStrategy(
            SparkTransportSettings(
                master=os.getenv("SCHEMABRIDGE_SPARK_MASTER") or "local[2]",
                num_partitions=2,
                jars_packages=environment["SCHEMABRIDGE_SPARK_JARS_PACKAGES"],
                application_name="SchemaBridge Snowflake Spark proof",
            )
        ).transfer(
            source_reader=source,
            staging_writer=target,
            source_profile=source_profile,
            target_profile=target_profile,
            transport_id=transport_id,
            source_table=source_table,
            target_database=profile_base["database"],
            target_schema=connection_options["schema"],
            batch_size=100,
            timeout_seconds=60,
            progress_reporter=None,
        )

        assert result.rows_read == result.rows_written == 3
        assert result.batch_count == 1
        staging_created = True
    finally:
        if staging_created:
            target.drop_staging_table(relation=staging_relation, timeout_seconds=60)
        with snowflake.connector.connect(**connection_kwargs) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE {_quote(source_name)}")
