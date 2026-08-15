"""Focused tests for bounded PostgreSQL source extraction."""

from __future__ import annotations

from typing import Any

import pytest

from schemabridge.config import ConfigError
from schemabridge.connectors.postgresql.connector import PostgreSQLConnector
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.transport import TransportRelation
from schemabridge.transport.base import (
    BatchSourceReader,
    BatchTransportConnectionError,
    BatchTransportError,
    BatchTransportTimeoutError,
)


def _profile() -> ConnectionProfile:
    return ConnectionProfile(
        profile_id="postgres-source",
        db_type="postgresql",
        host="source.invalid",
        database="source_db",
        username="reader",
        password="secret-marker",
    )


class FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]], error: BaseException | None = None):
        self.rows = list(rows)
        self.error = error
        self.query: str | None = None
        self.fetch_sizes: list[int] = []
        self.closed = False

    def execute(self, query: str) -> None:
        self.query = query

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        self.fetch_sizes.append(size)
        if self.error is not None:
            raise self.error
        result = self.rows[:size]
        del self.rows[:size]
        return result

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self.cursor_object = cursor
        self.cursor_name: str | None = None
        self.rollback_count = 0
        self.closed = False

    def cursor(self, *, name: str):
        self.cursor_name = name
        return self.cursor_object

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


class FakeDriver:
    def __init__(
        self,
        connection: FakeConnection,
        connect_error: BaseException | None = None,
    ):
        self.connection = connection
        self.connect_error = connect_error
        self.connect_kwargs: list[dict[str, Any]] = []

    def connect(self, **kwargs: Any) -> FakeConnection:
        self.connect_kwargs.append(dict(kwargs))
        if self.connect_error is not None:
            raise self.connect_error
        return self.connection


class FakeConnector(PostgreSQLConnector):
    def __init__(
        self,
        connection: FakeConnection,
        connect_error: BaseException | None = None,
    ):
        super().__init__(profile=_profile())
        self.driver = FakeDriver(connection, connect_error)

    def _driver(self):
        return self.driver


def _relation(**overrides: str) -> TransportRelation:
    values = {
        "catalog_name": "source_db",
        "schema_name": "lab",
        "object_name": "customers",
    }
    values.update(overrides)
    return TransportRelation(**values)


def test_reader_uses_server_cursor_and_yields_bounded_batches() -> None:
    cursor = FakeCursor([(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")])
    connection = FakeConnection(cursor)
    connector = FakeConnector(connection)

    batches = tuple(
        connector.read_batches(
            relation=_relation(),
            column_names=("customer_id", "full_name"),
            batch_size=2,
            timeout_seconds=9,
        )
    )

    assert isinstance(connector, BatchSourceReader)
    assert [batch.batch_number for batch in batches] == [1, 2, 3]
    assert [batch.row_count for batch in batches] == [2, 2, 1]
    assert batches[0].rows == ((1, "A"), (2, "B"))
    assert cursor.fetch_sizes == [2, 2, 2, 2]
    assert connection.cursor_name == "schemabridge_batch_reader"
    assert connection.rollback_count == 1
    assert cursor.closed is True and connection.closed is True
    assert connector.driver.connect_kwargs[0]["dbname"] == "source_db"
    assert connector.driver.connect_kwargs[0]["connect_timeout"] == 9


def test_reader_quotes_every_identifier_component() -> None:
    cursor = FakeCursor([(1, "value")])
    connector = FakeConnector(FakeConnection(cursor))

    tuple(
        connector.read_batches(
            relation=_relation(
                schema_name='lab"schema',
                object_name='customers; DROP TABLE audit',
            ),
            column_names=("id", 'odd"name'),
            batch_size=10,
            timeout_seconds=5,
        )
    )

    assert cursor.query == (
        'SELECT "id", "odd""name" '
        'FROM "lab""schema"."customers; DROP TABLE audit"'
    )


def test_reader_closes_resources_when_consumer_stops_early() -> None:
    cursor = FakeCursor([(1,), (2,), (3,)])
    connection = FakeConnection(cursor)
    batches = FakeConnector(connection).read_batches(
        relation=_relation(),
        column_names=("id",),
        batch_size=1,
        timeout_seconds=5,
    )

    assert next(batches).rows == ((1,),)
    batches.close()

    assert connection.rollback_count == 1
    assert cursor.closed is True and connection.closed is True


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (TimeoutError("secret-marker"), BatchTransportTimeoutError),
        (
            type("ConnectionFailure", (Exception,), {"sqlstate": "08006"})(
                "secret-marker"
            ),
            BatchTransportConnectionError,
        ),
        (RuntimeError("secret-marker"), BatchTransportError),
    ],
)
def test_reader_sanitizes_driver_failures(error: BaseException, expected: type[Exception]) -> None:
    cursor = FakeCursor([], error=error)
    connector = FakeConnector(FakeConnection(cursor))

    with pytest.raises(expected) as raised:
        tuple(
            connector.read_batches(
                relation=_relation(),
                column_names=("id",),
                batch_size=2,
                timeout_seconds=5,
            )
        )

    assert "secret-marker" not in str(raised.value)


def test_reader_sanitizes_connection_failure() -> None:
    connection = FakeConnection(FakeCursor([]))
    connector = FakeConnector(connection, RuntimeError("password=secret-marker"))

    with pytest.raises(BatchTransportConnectionError) as raised:
        tuple(
            connector.read_batches(
                relation=_relation(),
                column_names=("id",),
                batch_size=2,
                timeout_seconds=5,
            )
        )

    assert "secret-marker" not in str(raised.value)
    assert connection.closed is False


@pytest.mark.parametrize(
    ("columns", "batch_size", "timeout"),
    [
        ((), 2, 5),
        (("id", "id"), 2, 5),
        (("id",), 0, 5),
        (("id",), 2, 0),
    ],
)
def test_reader_rejects_invalid_requests_before_connecting(columns, batch_size, timeout) -> None:
    connector = FakeConnector(FakeConnection(FakeCursor([])))

    with pytest.raises(ConfigError):
        tuple(
            connector.read_batches(
                relation=_relation(),
                column_names=columns,
                batch_size=batch_size,
                timeout_seconds=timeout,
            )
        )

    assert connector.driver.connect_kwargs == []
