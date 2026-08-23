"""Run a disposable PostgreSQL-to-MySQL batch-transport proof.

This connector-boundary proof creates a tiny PostgreSQL source table, loads it
into a SchemaBridge-managed MySQL staging table, verifies the copied count, and
removes both disposable tables in ``finally`` cleanup.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import replace
from uuid import uuid4

from dotenv import load_dotenv

from schemabridge.connectors.factory import ConnectorFactory
from schemabridge.connectors.mysql.connector import MySQLConnector
from schemabridge.connectors.postgresql.connector import PostgreSQLConnector
from schemabridge.models.transport import TransportRelation
from schemabridge.services.batch_transport import BatchTransportService
from schemabridge.services.profile_registry import ProfileRegistry


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm-live-write", action="store_true")
    parser.add_argument("--postgresql-profile", required=True)
    parser.add_argument("--mysql-profile", required=True)
    parser.add_argument("--postgresql-schema", default="public")
    parser.add_argument("--postgresql-port", type=int)
    parser.add_argument("--mysql-port", type=int)
    return parser.parse_args()


def _with_port(profile, port: int | None, profile_id: str):
    if port is None:
        return profile
    if port <= 0 or port > 65535:
        raise SystemExit("Port overrides must be between 1 and 65535.")
    options = profile.connection_options_copy()
    options["port"] = port
    return replace(profile, profile_id=profile_id, connection_options=options)


def main() -> int:
    args = _arguments()
    if not args.confirm_live_write:
        raise SystemExit("Pass --confirm-live-write to create disposable remote tables.")

    load_dotenv()
    registry = ProfileRegistry.from_json(os.getenv("DB_PROFILES_JSON", ""))
    postgresql_profile = _with_port(
        registry.resolve(args.postgresql_profile), args.postgresql_port, "live-postgresql-source"
    )
    configured_mysql_profile = registry.resolve(args.mysql_profile)
    mysql_profile = replace(
        _with_port(configured_mysql_profile, args.mysql_port, "live-mysql-staging"),
        write_enabled=True,
    )
    if postgresql_profile.db_type != "postgresql":
        raise SystemExit("--postgresql-profile must select a PostgreSQL profile.")
    if mysql_profile.db_type != "mysql":
        raise SystemExit("--mysql-profile must select a MySQL profile.")

    source = ConnectorFactory.create_for_profile(postgresql_profile)
    writer = ConnectorFactory.create_for_profile(mysql_profile)
    if not isinstance(source, PostgreSQLConnector) or not isinstance(writer, MySQLConnector):
        raise RuntimeError("Configured profiles did not resolve to the expected connectors.")

    source_name = f"sb_live_source_{uuid4().hex}"
    source_relation = TransportRelation(
        catalog_name=postgresql_profile.database,
        schema_name=args.postgresql_schema,
        object_name=source_name,
    )
    source_sql = ".".join(
        source._quote_transport_identifier(value)
        for value in (source_relation.schema_name, source_relation.object_name)
    )
    staging_relation = None
    source_created = False
    try:
        source.execute_query(
            f"CREATE TABLE {source_sql} (customer_id BIGINT NOT NULL, full_name TEXT)",
            database=postgresql_profile.database,
            timeout_seconds=30,
        )
        source_created = True
        source.execute_query(
            f"INSERT INTO {source_sql} (customer_id, full_name) VALUES (%s, %s), (%s, %s), (%s, %s)",
            parameters=(1, "Asha", 2, "Rahul", 3, "Neha"),
            database=postgresql_profile.database,
            timeout_seconds=30,
        )
        source_table = source.get_table_metadata(
            database=postgresql_profile.database,
            schema=args.postgresql_schema,
            table=source_name,
            timeout_seconds=30,
        )
        if source_table is None:
            raise RuntimeError("The disposable PostgreSQL source table was not discovered.")

        result = BatchTransportService(source_reader=source, staging_writer=writer).transfer(
            transport_id=uuid4(),
            source_table=source_table,
            target_database=mysql_profile.database,
            target_schema=mysql_profile.database,
            batch_size=2,
            timeout_seconds=30,
        )
        staging_relation = result.staging_relation
        count_result = writer.execute_query(
            "SELECT COUNT(*) AS row_count FROM "
            + ".".join(
                writer._quote_identifier(value)
                for value in (staging_relation.schema_name, staging_relation.object_name)
            ),
            database=mysql_profile.database,
            timeout_seconds=30,
            max_rows=1,
        )
        row_count = count_result["rows"][0]["row_count"]
        if row_count != result.rows_written:
            raise RuntimeError("MySQL staging row count did not match transport evidence.")
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
                database=postgresql_profile.database,
                timeout_seconds=30,
            )


if __name__ == "__main__":
    raise SystemExit(main())
