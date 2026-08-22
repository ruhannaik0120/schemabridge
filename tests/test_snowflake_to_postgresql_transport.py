"""End-to-end proof that Snowflake batches load through the PostgreSQL writer."""

from __future__ import annotations

from uuid import UUID

from schemabridge.connectors.postgresql.connector import PostgreSQLConnector
from schemabridge.connectors.snowflake.connector import SnowflakeConnector
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.services.batch_transport import BatchTransportService
from tests.test_batch_transport_service import _table


class SnowflakeCursor:
    def __init__(self): self.rows, self.query, self.closed = [(1, "Asha"), (2, "Rahul"), (3, "Neha")], None, False
    def execute(self, query, timeout=None): self.query = query
    def fetchmany(self, size): batch, self.rows = self.rows[:size], self.rows[size:]; return batch
    def close(self): self.closed = True


class SnowflakeConnection:
    def __init__(self, cursor): self.cursor_instance, self.closed = cursor, False
    def cursor(self): return self.cursor_instance
    def close(self): self.closed = True


class SnowflakeSource(SnowflakeConnector):
    def __init__(self, connection):
        super().__init__(profile=ConnectionProfile(profile_id="sf-source", db_type="snowflake", host="account", database="source_db", username="reader", password="secret", max_rows=10))
        self.connection = connection
    def _driver(self):
        connection = self.connection
        class Driver:
            def connect(self, **_kwargs): return connection
        return Driver()


class PostgreSQLCursor:
    def __init__(self, connection): self.connection, self.rowcount, self.closed = connection, 0, False
    def execute(self, query, parameters=None):
        self.connection.calls.append((query, parameters))
        if query.startswith("INSERT"):
            self.rowcount = len(parameters) // 2
    def close(self): self.closed = True


class PostgreSQLConnection:
    def __init__(self): self.calls, self.commits, self.rollbacks, self.closed = [], 0, 0, False
    def cursor(self, **_kwargs): return PostgreSQLCursor(self)
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1
    def close(self): self.closed = True


class PostgreSQLTarget(PostgreSQLConnector):
    def __init__(self, connection):
        super().__init__(profile=ConnectionProfile(profile_id="pg-target", db_type="postgresql", host="db", database="target_db", username="loader", password="secret", max_rows=10, write_enabled=True))
        self.connection = connection
    def _driver(self):
        connection = self.connection
        class Driver:
            def connect(self, **_kwargs): return connection
        return Driver()


def test_snowflake_source_to_postgresql_staging_proves_bounded_row_transfer() -> None:
    source_cursor = SnowflakeCursor()
    target_connection = PostgreSQLConnection()
    result = BatchTransportService(
        source_reader=SnowflakeSource(SnowflakeConnection(source_cursor)),
        staging_writer=PostgreSQLTarget(target_connection),
    ).transfer(
        transport_id=UUID("12345678-1234-5678-1234-567812345678"),
        source_table=_table(),
        target_database="target_db",
        target_schema="landing",
        batch_size=2,
        timeout_seconds=9,
    )

    assert result.batch_count == 2
    assert result.rows_read == result.rows_written == 3
    assert source_cursor.query == 'SELECT "customer_id", "full_name" FROM "source_db"."lab"."customers"'
    assert target_connection.calls[0][0].startswith('CREATE UNLOGGED TABLE "landing"."SB_STAGE_')
    assert [parameters for query, parameters in target_connection.calls if query.startswith("INSERT")] == [(1, "Asha", 2, "Rahul"), (3, "Neha")]
