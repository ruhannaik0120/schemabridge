"""Opt-in live proof for MySQL source through Spark-managed staging."""

from __future__ import annotations

import os
from dataclasses import replace
from uuid import uuid4

import pytest

from schemabridge.connectors.mysql.connector import MySQLConnector
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.discovery import ConstraintType, KeyConstraintMetadata
from schemabridge.services.batch_transport import BatchTransportService
from schemabridge.transport.spark import SparkTransportSettings, SparkTransportStrategy
from tests.support.transport import transport_table as _table


def _environment() -> dict[str, str]:
    if os.getenv("SCHEMABRIDGE_SPARK_MYSQL_INTEGRATION") != "1":
        pytest.skip("Spark MySQL integration environment is not enabled.")
    names = (
        "SCHEMABRIDGE_SPARK_MYSQL_HOST",
        "SCHEMABRIDGE_SPARK_MYSQL_DATABASE",
        "SCHEMABRIDGE_SPARK_MYSQL_USERNAME",
        "SCHEMABRIDGE_SPARK_MYSQL_PASSWORD",
    )
    values = {name: os.getenv(name, "") for name in names}
    if not all(values.values()):
        pytest.skip("Spark MySQL integration environment is not configured.")
    values["port"] = os.getenv("SCHEMABRIDGE_SPARK_MYSQL_PORT", "3306")
    values["packages"] = os.getenv(
        "SCHEMABRIDGE_SPARK_JARS_PACKAGES", "com.mysql:mysql-connector-j:8.4.0"
    )
    return values


def _table_metadata(database: str, table_name: str):
    return replace(
        _table(),
        catalog_name=database,
        schema_name=database,
        object_name=table_name,
        system="mysql",
        estimated_row_count=3,
        primary_key=KeyConstraintMetadata(
            name="customers_pkey",
            constraint_type=ConstraintType.PRIMARY_KEY,
            columns=("customer_id",),
            vendor_metadata={},
        ),
    )


def test_mysql_to_spark_to_mysql_staging_when_explicitly_configured() -> None:
    environment = _environment()
    import mysql.connector

    source_name = f"sb_spark_source_{uuid4().hex}"
    connection_kwargs = {
        "host": environment["SCHEMABRIDGE_SPARK_MYSQL_HOST"],
        "port": int(environment["port"]),
        "database": environment["SCHEMABRIDGE_SPARK_MYSQL_DATABASE"],
        "user": environment["SCHEMABRIDGE_SPARK_MYSQL_USERNAME"],
        "password": environment["SCHEMABRIDGE_SPARK_MYSQL_PASSWORD"],
    }
    with mysql.connector.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE `{source_name}` "
                "(`customer_id` BIGINT PRIMARY KEY, `full_name` VARCHAR(100) NOT NULL)"
            )
            cursor.executemany(
                f"INSERT INTO `{source_name}` (`customer_id`, `full_name`) VALUES (%s, %s)",
                [(1, "Asha"), (2, "Rahul"), (3, "Neha")],
            )
        connection.commit()

    profile_base = dict(
        db_type="mysql",
        host=connection_kwargs["host"],
        database=connection_kwargs["database"],
        username=connection_kwargs["user"],
        password=connection_kwargs["password"],
        connection_options={"port": connection_kwargs["port"]},
        max_rows=100,
    )
    source_profile = ConnectionProfile(profile_id="spark-proof-source", **profile_base)
    target_profile = ConnectionProfile(
        profile_id="spark-proof-target", write_enabled=True, **profile_base
    )
    source = MySQLConnector(profile=source_profile)
    target = MySQLConnector(profile=target_profile)
    transport_id = uuid4()
    table = _table_metadata(connection_kwargs["database"], source_name)
    staging_relation = BatchTransportService.staging_relation(
        transport_id=transport_id,
        target_database=connection_kwargs["database"],
        target_schema=connection_kwargs["database"],
    )
    staging_created = False
    try:
        result = SparkTransportStrategy(
            SparkTransportSettings(
                master="local[2]",
                num_partitions=2,
                jars_packages=environment["packages"],
                application_name="SchemaBridge MySQL Spark proof",
            )
        ).transfer(
            source_reader=source,
            staging_writer=target,
            source_profile=source_profile,
            target_profile=target_profile,
            transport_id=transport_id,
            source_table=table,
            target_database=connection_kwargs["database"],
            target_schema=connection_kwargs["database"],
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
        with mysql.connector.connect(**connection_kwargs) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE `{source_name}`")
            connection.commit()
