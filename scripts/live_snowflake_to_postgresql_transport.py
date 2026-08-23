"""Run a disposable Snowflake-to-PostgreSQL batch-transport proof.

This is a connector-boundary proof, not a durable workflow demonstration.
It creates a tiny Snowflake source table and a managed PostgreSQL staging
table, verifies the copied count, then removes both tables in ``finally``.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import replace
from uuid import uuid4

from dotenv import load_dotenv

from schemabridge.connectors.factory import ConnectorFactory
from schemabridge.connectors.postgresql.connector import PostgreSQLConnector
from schemabridge.connectors.snowflake.connector import SnowflakeConnector
from schemabridge.models.transport import TransportRelation
from schemabridge.services.batch_transport import BatchTransportService
from schemabridge.services.profile_registry import ProfileRegistry


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm-live-write", action="store_true")
    parser.add_argument("--snowflake-profile", default="snowflake-target")
    parser.add_argument("--postgresql-profile", required=True)
    parser.add_argument("--postgresql-schema", default="public")
    parser.add_argument("--postgresql-port", type=int)
    return parser.parse_args()


def main() -> int:
    args = _arguments()
    if not args.confirm_live_write:
        raise SystemExit("Pass --confirm-live-write to create disposable remote tables.")

    load_dotenv()
    registry = ProfileRegistry.from_json(os.getenv("DB_PROFILES_JSON", ""))
    snowflake_profile = registry.resolve(args.snowflake_profile)
    configured_postgresql_profile = registry.resolve(args.postgresql_profile)
    connection_options = configured_postgresql_profile.connection_options_copy()
    if args.postgresql_port is not None:
        if args.postgresql_port <= 0 or args.postgresql_port > 65535:
            raise SystemExit("--postgresql-port must be between 1 and 65535.")
        connection_options["port"] = args.postgresql_port
    postgresql_profile = replace(
        configured_postgresql_profile,
        profile_id="live-postgresql-staging",
        write_enabled=True,
        connection_options=connection_options,
    )
    if snowflake_profile.db_type != "snowflake":
        raise SystemExit("--snowflake-profile must select a Snowflake profile.")
    if postgresql_profile.db_type != "postgresql":
        raise SystemExit("--postgresql-profile must select a PostgreSQL profile.")

    source = ConnectorFactory.create_for_profile(snowflake_profile)
    writer = ConnectorFactory.create_for_profile(postgresql_profile)
    if not isinstance(source, SnowflakeConnector) or not isinstance(
        writer, PostgreSQLConnector
    ):
        raise RuntimeError("Configured profiles did not resolve to the expected connectors.")
    source_schema = str(
        snowflake_profile.connection_options_copy().get("schema", "PUBLIC")
    )
    source_name = f"SB_LIVE_SOURCE_{uuid4().hex.upper()}"
    source_relation = TransportRelation(
        catalog_name=snowflake_profile.database,
        schema_name=source_schema,
        object_name=source_name,
    )
    staging_relation = None
    source_created = False
    source_sql = ".".join(
        source._quote_staging_identifier(value)
        for value in (
            snowflake_profile.database,
            source_schema,
            source_name,
        )
    )

    try:
        source.execute_query(
            f"CREATE TRANSIENT TABLE {source_sql} "
            '("customer_id" NUMBER(38,0) NOT NULL, "full_name" VARCHAR)',
            database=snowflake_profile.database,
            timeout_seconds=30,
        )
        source_created = True
        source.execute_query(
            f"INSERT INTO {source_sql} "
            '("customer_id", "full_name") VALUES (%s, %s), (%s, %s), (%s, %s)',
            parameters=(1, "Asha", 2, "Rahul", 3, "Neha"),
            database=snowflake_profile.database,
            timeout_seconds=30,
        )
        source_table = source.get_table_metadata(
            database=snowflake_profile.database,
            schema=source_schema,
            table=source_name,
            timeout_seconds=30,
        )
        if source_table is None:
            raise RuntimeError("The disposable Snowflake source table was not discovered.")

        result = BatchTransportService(
            source_reader=source,
            staging_writer=writer,
        ).transfer(
            transport_id=uuid4(),
            source_table=source_table,
            target_database=postgresql_profile.database,
            target_schema=args.postgresql_schema,
            batch_size=2,
            timeout_seconds=30,
        )
        staging_relation = result.staging_relation
        count_result = writer.execute_query(
            "SELECT COUNT(*) AS row_count FROM "
            + ".".join(
                writer._quote_transport_identifier(value)
                for value in (
                    staging_relation.schema_name,
                    staging_relation.object_name,
                )
            ),
            database=postgresql_profile.database,
            timeout_seconds=30,
            max_rows=1,
        )
        row_count = count_result["rows"][0]["row_count"]
        if row_count != result.rows_written:
            raise RuntimeError("PostgreSQL staging row count did not match transport evidence.")
        print(
            "Live transport proof passed: "
            f"{result.rows_written} rows in {result.batch_count} batches; cleanup follows."
        )
        return 0
    finally:
        if staging_relation is not None:
            writer.drop_staging_table(relation=staging_relation, timeout_seconds=30)
        if source_created:
            source.execute_query(
                f"DROP TABLE IF EXISTS {source_sql}",
                database=snowflake_profile.database,
                timeout_seconds=30,
            )


if __name__ == "__main__":
    raise SystemExit(main())
