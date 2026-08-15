"""Verify MySQL discovery, batch reading, and validation capabilities."""

from __future__ import annotations

from contextlib import contextmanager

from schemabridge.connectors.mysql.connector import MySQLConnector
from schemabridge.connectors.validation import ValidationQueryDialectProvider
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.mapping import SqlDialect
from schemabridge.models.metadata import CanonicalType
from schemabridge.models.transport import TransportRelation
from schemabridge.transport.base import BatchSourceReader
from schemabridge.services.database_service import DatabaseExecutionResult
from schemabridge.services.validation_execution import MigrationValidationExecutionService
from schemabridge.services.validation_sql import compile_validation_sql
from tests.test_transformation_sql import _approved
from tests.test_validation_execution import _request


def _profile() -> ConnectionProfile:
    return ConnectionProfile(
        profile_id="mysql-source",
        db_type="mysql",
        host="mysql.example.test",
        database="shop",
        username="reader",
        password="secret",
        timeout_seconds=20,
        max_rows=100,
    )


def test_mysql_connector_exposes_source_workflow_capabilities() -> None:
    connector = MySQLConnector(profile=_profile())

    assert isinstance(connector, BatchSourceReader)
    assert isinstance(connector, ValidationQueryDialectProvider)
    assert connector.validation_sql_dialect() is SqlDialect.MYSQL


class DiscoveryCursor:
    def __init__(self, responses):
        self.responses = list(responses)
        self.description = None
        self.rows = []
        self.closed = False
        self.queries = []

    def execute(self, sql, _parameters=None):
        self.queries.append(sql)
        columns, rows = self.responses.pop(0)
        self.description = [(column,) for column in columns]
        self.rows = rows

    def fetchall(self):
        return list(self.rows)

    def close(self):
        self.closed = True


class BatchCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.query = None
        self.closed = False

    def execute(self, query):
        self.query = query

    def fetchmany(self, size):
        result = self.rows[:size]
        del self.rows[:size]
        return result

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self.value = cursor
        self.rolled_back = False
        self.closed = False

    def cursor(self, **_kwargs):
        return self.value

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class FakeMySQLConnector(MySQLConnector):
    def __init__(self, connection):
        super().__init__(profile=_profile())
        self.connection = connection

    @contextmanager
    def _connection(self, database=None, timeout_seconds=None):
        yield self.connection

    def connect(self, database=None, timeout_seconds=None):
        return self.connection


def test_mysql_connection_check_avoids_reserved_utc_time_alias() -> None:
    cursor = DiscoveryCursor(
        (
            (
                ("server_name", "version", "logged_in_user", "connection_checked_at"),
                (("mysql", "8.4", "reader", "2026-08-15 00:00:00"),),
            ),
        )
    )

    result = FakeMySQLConnector(FakeConnection(cursor)).test_connection(database="shop")

    assert result["connection_status"] == "connected"
    assert "AS connection_checked_at" in cursor.queries[0]
    assert "AS utc_time" not in cursor.queries[0]


def test_mysql_discovery_returns_canonical_table_metadata() -> None:
    connection = FakeConnection(
        DiscoveryCursor(
            (
                (
                    ("TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "TABLE_COMMENT", "TABLE_ROWS"),
                    (("shop", "customers", "BASE TABLE", "customer data", 5),),
                ),
                (
                    (
                        "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "IS_NULLABLE",
                        "CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE",
                        "DATETIME_PRECISION", "COLUMN_DEFAULT", "COLUMN_COMMENT",
                        "COLLATION_NAME", "EXTRA", "GENERATION_EXPRESSION",
                    ),
                    (
                        ("customer_id", 1, "bigint", "NO", None, 19, 0, None, None, "", None, "auto_increment", ""),
                        ("full_name", 2, "varchar", "NO", 100, None, None, None, None, "", "utf8mb4", "", ""),
                    ),
                ),
                (
                    ("CONSTRAINT_NAME", "CONSTRAINT_TYPE", "COLUMN_NAME", "ORDINAL_POSITION"),
                    (("PRIMARY", "PRIMARY KEY", "customer_id", 1),),
                ),
            )
        )
    )

    metadata = FakeMySQLConnector(connection).get_table_metadata(
        database="shop", schema="shop", table="customers"
    )

    assert metadata is not None
    assert metadata.system == "mysql"
    assert metadata.primary_key.columns == ("customer_id",)
    assert [column.canonical_type for column in metadata.columns] == [
        CanonicalType.INTEGER,
        CanonicalType.STRING,
    ]


def test_mysql_reader_yields_bounded_batches_and_closes_resources() -> None:
    cursor = BatchCursor(((1, "A"), (2, "B"), (3, "C")))
    connection = FakeConnection(cursor)
    batches = tuple(
        FakeMySQLConnector(connection).read_batches(
            relation=TransportRelation(
                catalog_name="shop", schema_name="shop", object_name="customers"
            ),
            column_names=("customer_id", "full_name"),
            batch_size=2,
            timeout_seconds=10,
        )
    )

    assert [batch.row_count for batch in batches] == [2, 1]
    assert cursor.query == "SELECT `customer_id`, `full_name` FROM `shop`.`customers`"
    assert connection.rolled_back is True
    assert connection.closed is True


def test_mysql_validation_sql_uses_mysql_dialect_and_identifier_quotes() -> None:
    source, target = compile_validation_sql(
        _approved(),
        source_schema="shop",
        source_table="customers",
        target_database="warehouse",
        target_schema="public",
        target_table="customers",
        source_dialect=SqlDialect.MYSQL,
    )

    assert source.dialect is SqlDialect.MYSQL
    assert "FROM `shop`.`customers` AS `src`" in source.sql
    assert target.dialect is SqlDialect.SNOWFLAKE


def test_validation_execution_accepts_mysql_source_capability() -> None:
    metrics = {
        "row_count": 1,
        "m000_null_count": 0,
        "m000_distinct_count": 1,
        "m001_null_count": 0,
        "m001_distinct_count": 1,
    }

    class Service:
        def __init__(self, profile_id, dialect):
            self.profile_id = profile_id
            self.dialect = dialect

        def validation_execution_context(self, timeout):
            return {
                "profile_id": self.profile_id,
                "validation_dialect": self.dialect.value,
                "timeout_seconds": timeout,
            }

        def execute_validation_query(self, **_kwargs):
            return DatabaseExecutionResult(
                tuple(metrics), (tuple(metrics.values()),), None
            )

    services = {
        "pg": Service("pg", SqlDialect.MYSQL),
        "sf": Service("sf", SqlDialect.SNOWFLAKE),
    }
    report = MigrationValidationExecutionService(services.__getitem__).run(_request())

    assert report.source_sql_summary.dialect is SqlDialect.MYSQL
    assert report.validation_report.mismatched_count == 0
