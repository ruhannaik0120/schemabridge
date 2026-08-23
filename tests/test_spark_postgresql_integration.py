"""Opt-in live proof for PostgreSQL source through Spark-managed staging."""

from __future__ import annotations

import os
from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from schemabridge.connectors.postgresql.connector import PostgreSQLConnector
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.discovery import ConstraintType, KeyConstraintMetadata
from schemabridge.services.batch_transport import BatchTransportService
from schemabridge.transport.spark import SparkTransportSettings, SparkTransportStrategy
from tests.support.transport import transport_table as _table


def _environment() -> dict[str, str]:
    if os.getenv("SCHEMABRIDGE_SPARK_POSTGRES_INTEGRATION") != "1":
        pytest.skip("Spark PostgreSQL integration environment is not enabled.")
    names = (
        "SCHEMABRIDGE_SPARK_POSTGRES_HOST",
        "SCHEMABRIDGE_SPARK_POSTGRES_DATABASE",
        "SCHEMABRIDGE_SPARK_POSTGRES_USERNAME",
        "SCHEMABRIDGE_SPARK_POSTGRES_PASSWORD",
    )
    values = {name: os.getenv(name, "") for name in names}
    if not all(values.values()):
        pytest.skip("Spark PostgreSQL integration environment is not configured.")
    values["port"] = os.getenv("SCHEMABRIDGE_SPARK_POSTGRES_PORT", "5432")
    values["packages"] = os.getenv(
        "SCHEMABRIDGE_SPARK_JARS_PACKAGES", "org.postgresql:postgresql:42.7.5"
    )
    return values


def _table_metadata(database: str, schema: str):
    return replace(
        _table(),
        catalog_name=database,
        schema_name=schema,
        object_name="customers",
        estimated_row_count=3,
        primary_key=KeyConstraintMetadata(
            name="customers_pkey",
            constraint_type=ConstraintType.PRIMARY_KEY,
            columns=("customer_id",),
            vendor_metadata={},
        ),
    )


def test_postgresql_to_spark_to_postgresql_staging_when_explicitly_configured() -> None:
    environment = _environment()
    import psycopg

    schema = f"sb_spark_proof_{uuid4().hex}"

    connection_kwargs = {
        "host": environment["SCHEMABRIDGE_SPARK_POSTGRES_HOST"],
        "port": int(environment["port"]),
        "dbname": environment["SCHEMABRIDGE_SPARK_POSTGRES_DATABASE"],
        "user": environment["SCHEMABRIDGE_SPARK_POSTGRES_USERNAME"],
        "password": environment["SCHEMABRIDGE_SPARK_POSTGRES_PASSWORD"],
    }
    with psycopg.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(
                f'CREATE TABLE "{schema}"."customers" '
                '("customer_id" BIGINT PRIMARY KEY, "full_name" VARCHAR(100) NOT NULL)'
            )
            cursor.executemany(
                f'INSERT INTO "{schema}"."customers" ("customer_id", "full_name") VALUES (%s, %s)',
                [(1, "Asha"), (2, "Rahul"), (3, "Neha")],
            )
        connection.commit()

    profile_base = dict(
        db_type="postgresql",
        host=connection_kwargs["host"],
        database=connection_kwargs["dbname"],
        username=connection_kwargs["user"],
        password=connection_kwargs["password"],
        connection_options={"port": connection_kwargs["port"]},
        max_rows=100,
    )
    source_profile = ConnectionProfile(profile_id="spark-proof-source", **profile_base)
    target_profile = ConnectionProfile(
        profile_id="spark-proof-target", write_enabled=True, **profile_base
    )
    source = PostgreSQLConnector(profile=source_profile)
    target = PostgreSQLConnector(profile=target_profile)
    table = _table_metadata(connection_kwargs["dbname"], schema)
    try:
        result = SparkTransportStrategy(
            SparkTransportSettings(
                master="local[2]",
                num_partitions=2,
                jars_packages=environment["packages"],
                application_name="SchemaBridge PostgreSQL Spark proof",
            )
        ).transfer(
            source_reader=source,
            staging_writer=target,
            source_profile=source_profile,
            target_profile=target_profile,
            transport_id=UUID("12345678-1234-5678-1234-567812345678"),
            source_table=table,
            target_database=connection_kwargs["dbname"],
            target_schema=schema,
            batch_size=100,
            timeout_seconds=60,
            progress_reporter=None,
        )

        assert result.rows_read == result.rows_written == 3
        assert result.batch_count == 1
    finally:
        with psycopg.connect(**connection_kwargs) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA "{schema}" CASCADE')
            connection.commit()
