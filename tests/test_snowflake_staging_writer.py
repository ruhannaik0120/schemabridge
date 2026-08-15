"""Focused tests for safe Snowflake staging-table writes."""

from __future__ import annotations

from typing import Any

import pytest

from schemabridge.config import ConfigError
from schemabridge.connectors.snowflake.connector import SnowflakeConnector
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.metadata import CanonicalType
from schemabridge.models.transport import (
    DataBatch,
    StagingColumn,
    StagingTableDefinition,
    TransportRelation,
)
from schemabridge.transport.base import (
    BatchTransportConnectionError,
    BatchTransportError,
    BatchTransportTimeoutError,
    StagingTableWriter,
    UnsupportedStagingTypeError,
)


def _profile(*, write_enabled: bool = True) -> ConnectionProfile:
    return ConnectionProfile(
        profile_id="snowflake-target",
        db_type="snowflake",
        host="org-account",
        database="SCHEMABRIDGE_LAB",
        username="loader",
        password="secret-marker",
        connection_options={"warehouse": "SCHEMABRIDGE_WH", "role": "LOADER"},
        timeout_seconds=17,
        write_enabled=write_enabled,
    )


RELATION = TransportRelation(
    catalog_name="SCHEMABRIDGE_LAB",
    schema_name="PUBLIC",
    object_name="SB_STAGE_RUN_1",
)


def _definition(*, relation: TransportRelation = RELATION) -> StagingTableDefinition:
    return StagingTableDefinition(
        relation=relation,
        columns=(
            StagingColumn(
                name="customer_id",
                canonical_type=CanonicalType.INTEGER,
                nullable=False,
                numeric_precision=19,
                numeric_scale=0,
            ),
            StagingColumn(
                name="full_name",
                canonical_type=CanonicalType.STRING,
                nullable=True,
            ),
            StagingColumn(
                name="amount",
                canonical_type=CanonicalType.DECIMAL,
                nullable=True,
                numeric_precision=12,
                numeric_scale=2,
            ),
            StagingColumn(
                name="payload",
                canonical_type=CanonicalType.SEMI_STRUCTURED,
                nullable=True,
            ),
        ),
    )


class DriverFailure(Exception):
    def __init__(
        self,
        marker: str,
        *,
        sqlstate: str | None = None,
        errno: int | None = None,
    ):
        super().__init__(marker)
        self.sqlstate = sqlstate
        self.errno = errno


class FakeCursor:
    def __init__(
        self,
        connection: "FakeConnection",
        *,
        rowcount: int = 0,
        error: BaseException | None = None,
    ):
        self.connection = connection
        self.rowcount = rowcount
        self.error = error
        self.closed = False

    def execute(
        self,
        query: str,
        parameters: tuple[object, ...] | None = None,
        *,
        timeout: int,
    ) -> None:
        self.connection.executions.append((query, parameters, timeout))
        if self.error is not None:
            raise self.error

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(
        self,
        *,
        rowcount: int = 0,
        error: BaseException | None = None,
    ):
        self.cursor_object = FakeCursor(self, rowcount=rowcount, error=error)
        self.executions: list[tuple[str, tuple[object, ...] | None, int]] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.cursor_object

    def commit(self) -> None:
        self.commit_count += 1

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


class FakeConnector(SnowflakeConnector):
    def __init__(
        self,
        connection: FakeConnection,
        *,
        profile: ConnectionProfile | None = None,
        connect_error: BaseException | None = None,
    ):
        super().__init__(profile=profile or _profile())
        self.driver = FakeDriver(connection, connect_error)

    def _driver(self):
        return self.driver


def test_prepare_creates_exact_transient_table_and_commits() -> None:
    connection = FakeConnection()
    connector = FakeConnector(connection)

    connector.prepare_staging_table(definition=_definition(), timeout_seconds=11)

    assert isinstance(connector, StagingTableWriter)
    assert connection.executions == [
        (
            'CREATE TRANSIENT TABLE "SCHEMABRIDGE_LAB"."PUBLIC".'
            '"SB_STAGE_RUN_1" ("customer_id" NUMBER(38,0) NOT NULL, '
            '"full_name" VARCHAR, "amount" NUMBER(12,2), "payload" VARIANT)',
            None,
            11,
        )
    ]
    assert connection.commit_count == 1 and connection.rollback_count == 0
    assert connection.cursor_object.closed is True and connection.closed is True
    assert connector.driver.connect_kwargs[0]["autocommit"] is False
    assert connector.driver.connect_kwargs[0]["login_timeout"] == 11


def test_prepare_quotes_identifier_components_instead_of_executing_them() -> None:
    relation = TransportRelation(
        catalog_name="SCHEMABRIDGE_LAB",
        schema_name='PUB"LIC',
        object_name="stage; DROP TABLE target",
    )
    connection = FakeConnection()

    FakeConnector(connection).prepare_staging_table(
        definition=_definition(relation=relation),
        timeout_seconds=5,
    )

    assert '"PUB""LIC"."stage; DROP TABLE target"' in connection.executions[0][0]


def test_write_batch_binds_values_and_confirms_exact_row_count() -> None:
    connection = FakeConnection(rowcount=2)
    connector = FakeConnector(connection)
    batch = DataBatch(
        batch_number=3,
        column_names=("customer_id", "full_name", "amount", "payload"),
        rows=(
            (1, "Asha", "10.25", {"b": 2, "a": 1}),
            (2, "Rahul", "20.00", None),
        ),
    )

    result = connector.write_batch(
        definition=_definition(),
        batch=batch,
        timeout_seconds=13,
    )

    query, parameters, timeout = connection.executions[0]
    assert query == (
        'INSERT INTO "SCHEMABRIDGE_LAB"."PUBLIC"."SB_STAGE_RUN_1" '
        '("customer_id", "full_name", "amount", "payload") VALUES '
        '(%s, %s, %s, PARSE_JSON(%s)), (%s, %s, %s, PARSE_JSON(%s))'
    )
    assert parameters == (
        1,
        "Asha",
        "10.25",
        '{"a":1,"b":2}',
        2,
        "Rahul",
        "20.00",
        None,
    )
    assert timeout == 13
    assert result.batch_number == 3
    assert result.rows_received == result.rows_written == 2
    assert connection.commit_count == 1 and connection.rollback_count == 0


def test_partial_batch_confirmation_rolls_back_and_returns_no_evidence() -> None:
    connection = FakeConnection(rowcount=1)
    connector = FakeConnector(connection)
    batch = DataBatch(
        batch_number=1,
        column_names=("customer_id", "full_name", "amount", "payload"),
        rows=((1, "A", "1.00", None), (2, "B", "2.00", None)),
    )

    with pytest.raises(BatchTransportError, match="complete"):
        connector.write_batch(
            definition=_definition(),
            batch=batch,
            timeout_seconds=5,
        )

    assert connection.commit_count == 0 and connection.rollback_count == 1
    assert connection.closed is True


def test_write_rejects_wrong_column_order_before_connecting() -> None:
    connector = FakeConnector(FakeConnection())
    batch = DataBatch(
        batch_number=1,
        column_names=("full_name", "customer_id", "amount", "payload"),
        rows=(("A", 1, "1.00", None),),
    )

    with pytest.raises(BatchTransportError, match="columns"):
        connector.write_batch(
            definition=_definition(),
            batch=batch,
            timeout_seconds=5,
        )

    assert connector.driver.connect_kwargs == []


def test_drop_is_exact_idempotent_and_committed() -> None:
    connection = FakeConnection()

    FakeConnector(connection).drop_staging_table(
        relation=RELATION,
        timeout_seconds=7,
    )

    assert connection.executions == [
        (
            'DROP TABLE IF EXISTS "SCHEMABRIDGE_LAB"."PUBLIC"."SB_STAGE_RUN_1"',
            None,
            7,
        )
    ]
    assert connection.commit_count == 1


def test_staging_requires_operator_enabled_named_profile() -> None:
    connector = FakeConnector(
        FakeConnection(),
        profile=_profile(write_enabled=False),
    )

    with pytest.raises(ConfigError, match="write-enabled"):
        connector.prepare_staging_table(
            definition=_definition(),
            timeout_seconds=5,
        )

    assert connector.driver.connect_kwargs == []


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (DriverFailure("private", errno=604), BatchTransportTimeoutError),
        (
            DriverFailure("private", sqlstate="08006"),
            BatchTransportConnectionError,
        ),
        (DriverFailure("private"), BatchTransportError),
    ],
)
def test_staging_sanitizes_driver_failures(error, expected) -> None:
    connection = FakeConnection(error=error)

    with pytest.raises(expected) as raised:
        FakeConnector(connection).prepare_staging_table(
            definition=_definition(),
            timeout_seconds=5,
        )

    assert "private" not in str(raised.value)
    assert connection.rollback_count == 1 and connection.closed is True


def test_staging_sanitizes_connection_failure() -> None:
    connector = FakeConnector(
        FakeConnection(),
        connect_error=RuntimeError("password=private"),
    )

    with pytest.raises(BatchTransportConnectionError) as raised:
        connector.prepare_staging_table(
            definition=_definition(),
            timeout_seconds=5,
        )

    assert "private" not in str(raised.value)


def test_type_mapping_preserves_unsafe_decimal_as_text_and_rejects_unknown() -> None:
    wide_decimal = StagingColumn(
        name="wide_decimal",
        canonical_type=CanonicalType.DECIMAL,
        nullable=True,
        numeric_precision=50,
        numeric_scale=4,
    )
    unknown = StagingColumn(
        name="mystery",
        canonical_type=CanonicalType.UNKNOWN,
        nullable=True,
    )

    assert SnowflakeConnector._snowflake_staging_type(wide_decimal) == "VARCHAR"
    with pytest.raises(UnsupportedStagingTypeError):
        SnowflakeConnector._snowflake_staging_type(unknown)


def test_timestamp_without_source_precision_uses_safe_snowflake_maximum() -> None:
    timestamp = StagingColumn(
        name="created_at",
        canonical_type=CanonicalType.TIMESTAMP,
        nullable=True,
    )

    assert SnowflakeConnector._snowflake_staging_type(timestamp) == "TIMESTAMP_NTZ(9)"


def test_write_rejects_batch_above_profile_limit() -> None:
    connector = FakeConnector(FakeConnection(rowcount=501))
    batch = DataBatch(
        batch_number=1,
        column_names=("customer_id", "full_name", "amount", "payload"),
        rows=tuple((index, "A", "1.00", None) for index in range(501)),
    )

    with pytest.raises(ConfigError, match="profile limit"):
        connector.write_batch(
            definition=_definition(),
            batch=batch,
            timeout_seconds=5,
        )

    assert connector.driver.connect_kwargs == []
