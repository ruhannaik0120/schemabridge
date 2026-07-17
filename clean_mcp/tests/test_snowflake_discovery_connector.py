"""Focused tests for Stage 3A Snowflake canonical schema discovery."""

from __future__ import annotations

import inspect
import re
from copy import deepcopy
from decimal import Decimal
from typing import Any

import pytest

from config import Config, ConfigError
from connectors.discovery import (
    MalformedDiscoveryResultError,
    SchemaDiscoveryConnectionError,
    SchemaDiscoveryConnector,
    SchemaDiscoveryError,
    SchemaDiscoveryTimeoutError,
)
from connectors.snowflake import _discovery_queries as queries
from connectors.snowflake.connector import SnowflakeConnector
from models.connection_profile import ConnectionProfile
from models.discovery import DatabaseObjectType, ObjectPersistence


def _profile(
    database: str = "App DB",
    *,
    profile_id: str = "snowflake-discovery",
    password: str = "credential-marker",
) -> ConnectionProfile:
    return ConnectionProfile(
        profile_id=profile_id,
        db_type="snowflake",
        host="org-account",
        database=database,
        username="app",
        password=password,
        connection_options={"warehouse": "DISCOVERY_WH", "role": "DISCOVERY_ROLE"},
        timeout_seconds=17,
    )


class DriverFailure(Exception):
    def __init__(
        self,
        marker: str,
        sqlstate: str | None = None,
        errno: int | None = None,
    ):
        super().__init__(marker)
        self.msg = marker
        self.raw_msg = marker
        self.sqlstate = sqlstate
        self.errno = errno
        self.sfqid = marker


class FakeCursor:
    def __init__(self, connection: "FakeConnection"):
        self.connection = connection
        self.description: list[tuple[str]] | None = None
        self._raw_rows: list[tuple[Any, ...]] = []
        self.closed = False

    def execute(
        self,
        query: str,
        parameters: tuple[Any, ...] | None = None,
        *,
        timeout: int,
    ) -> None:
        bound = () if parameters is None else parameters
        self.connection.executions.append((query, bound, timeout))
        response = self.connection.response_for(query, bound)
        if isinstance(response, BaseException):
            raise response
        rows = list(response)
        columns = list(rows[0]) if rows else []
        self.description = [(name,) for name in columns]
        self._raw_rows = [tuple(row.get(name) for name in columns) for row in rows]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._raw_rows)

    def close(self) -> None:
        self.closed = True
        self.connection.closed_cursors += 1


class FakeConnection:
    def __init__(
        self,
        responses: dict[str, Any] | None = None,
        *,
        current_database: str = "App DB",
    ):
        self.responses = responses or {}
        self.current_database = current_database
        self.executions: list[tuple[str, tuple[Any, ...], int]] = []
        self.cursor_count = 0
        self.closed_cursors = 0
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def response_for(self, query: str, parameters: tuple[Any, ...]) -> Any:
        default = (
            [{"current_database": self.current_database}]
            if query == queries._CURRENT_DATABASE_QUERY
            else []
        )
        response = self.responses.get(query, default)
        return response(parameters) if callable(response) else response

    def cursor(self) -> FakeCursor:
        self.cursor_count += 1
        return FakeCursor(self)

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        raise AssertionError("Discovery must not use the connection as a context manager.")

    def __exit__(self, *args: Any):
        raise AssertionError("Discovery must not use the connection as a context manager.")


class FakeDriver:
    def __init__(self, connection: FakeConnection, error: BaseException | None = None):
        self.connection = connection
        self.error = error
        self.connect_kwargs: list[dict[str, Any]] = []

    def connect(self, **kwargs: Any) -> FakeConnection:
        self.connect_kwargs.append(dict(kwargs))
        if self.error is not None:
            raise self.error
        return self.connection


class FakeSnowflakeConnector(SnowflakeConnector):
    def __init__(
        self,
        connection: FakeConnection,
        *,
        profile: ConnectionProfile | None = None,
        connect_error: BaseException | None = None,
    ):
        super().__init__(profile=profile or _profile())
        self.fake_driver = FakeDriver(connection, connect_error)
        self.driver_loaded = False

    def _driver(self):
        self.driver_loaded = True
        return self.fake_driver


def _schema_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "catalog_name": "App DB",
        "schema_name": "Mixed Case",
        "owner": "owner",
        "comment": "comment",
        "is_system_managed": False,
        "schema_classification": "USER",
        "is_transient": False,
        "is_managed_access": True,
    }
    row.update(overrides)
    return row


def _object_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "catalog_name": "App DB",
        "schema_name": "Mixed Case",
        "object_name": "Orders",
        "owner": "owner",
        "comment": "comment",
        "table_type": "BASE TABLE",
        "is_transient": False,
        "is_temporary": False,
        "is_dynamic": False,
        "is_iceberg": False,
        "is_hybrid": False,
        "auto_clustering_on": False,
        "has_clustering_key": False,
        "estimated_row_count": 12,
    }
    row.update(overrides)
    return row


def test_stage_3a_signatures_are_exact_and_full_protocol_conformance_is_deferred():
    expected_schemas = ["self", "database", "timeout_seconds"]
    expected_objects = ["self", "database", "schema", "object_types", "timeout_seconds"]
    schemas_signature = inspect.signature(SnowflakeConnector.list_schemas)
    objects_signature = inspect.signature(SnowflakeConnector.list_objects)
    assert schemas_signature == inspect.signature(SchemaDiscoveryConnector.list_schemas)
    assert objects_signature == inspect.signature(SchemaDiscoveryConnector.list_objects)
    assert list(schemas_signature.parameters) == expected_schemas
    assert list(objects_signature.parameters) == expected_objects
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for name, parameter in schemas_signature.parameters.items()
        if name != "self"
    )
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for name, parameter in objects_signature.parameters.items()
        if name != "self"
    )
    connector = SnowflakeConnector(profile=_profile())
    # Stage 3B will add the real get_table_metadata operation and complete
    # structural SchemaDiscoveryConnector conformance. Stage 3A has no stub.
    assert not hasattr(connector, "get_table_metadata")
    assert not isinstance(connector, SchemaDiscoveryConnector)


def test_profile_bound_discovery_never_reads_global_config(monkeypatch):
    monkeypatch.setattr(
        Config,
        "connection_config",
        classmethod(lambda cls: (_ for _ in ()).throw(AssertionError("global Config read"))),
    )
    connection = FakeConnection({queries._SCHEMAS_QUERY: []})
    assert FakeSnowflakeConnector(connection).list_schemas() == ()


def test_independent_profiles_keep_exact_driver_settings():
    first_connection = FakeConnection(current_database="FIRST_DB")
    second_connection = FakeConnection(current_database="SECOND_DB")
    first = FakeSnowflakeConnector(
        first_connection,
        profile=_profile("FIRST_DB", profile_id="first"),
    )
    second = FakeSnowflakeConnector(
        second_connection,
        profile=_profile("SECOND_DB", profile_id="second"),
    )
    assert first.list_schemas() == ()
    assert second.list_schemas() == ()
    assert first.fake_driver.connect_kwargs[0]["database"] == "FIRST_DB"
    assert second.fake_driver.connect_kwargs[0]["database"] == "SECOND_DB"


@pytest.mark.parametrize("explicit", [None, "App DB"])
def test_database_resolution_preserves_configured_database_exactly(explicit):
    connection = FakeConnection()
    connector = FakeSnowflakeConnector(connection)
    connector.list_schemas(database=explicit)
    assert connector.fake_driver.connect_kwargs[0]["database"] == "App DB"


@pytest.mark.parametrize("explicit", ["app db", "App DB ", "Other"])
def test_database_mismatch_fails_before_driver_loading(explicit):
    connector = FakeSnowflakeConnector(FakeConnection())
    with pytest.raises(ConfigError, match="exactly match"):
        connector.list_schemas(database=explicit)
    assert connector.driver_loaded is False
    assert connector.fake_driver.connect_kwargs == []


def test_missing_database_fails_without_driver_loading():
    connector = FakeSnowflakeConnector(FakeConnection(), profile=_profile(""))
    with pytest.raises(ConfigError, match="database is required"):
        connector.list_schemas()
    assert connector.driver_loaded is False


@pytest.mark.parametrize("database", [" Exact DB ", " "])
def test_explicit_database_reaches_real_driver_path_without_normalization(database):
    connection = FakeConnection(current_database=database)
    connector = FakeSnowflakeConnector(connection, profile=_profile(""))
    connector.list_schemas(database=database)
    assert len(connector.fake_driver.connect_kwargs) == 1
    assert connector.fake_driver.connect_kwargs[0]["database"] == database


def test_current_database_mismatch_is_safe_and_closes_everything():
    connection = FakeConnection(current_database="OTHER_DB")
    with pytest.raises(SchemaDiscoveryConnectionError, match="does not match"):
        FakeSnowflakeConnector(connection).list_schemas()
    assert connection.closed
    assert connection.cursor_count == connection.closed_cursors == 1
    assert connection.commit_count == connection.rollback_count == 0


def test_malformed_current_database_result_fails_safely():
    connection = FakeConnection({queries._CURRENT_DATABASE_QUERY: []})
    with pytest.raises(MalformedDiscoveryResultError):
        FakeSnowflakeConnector(connection).list_schemas()
    assert connection.closed
    assert connection.cursor_count == connection.closed_cursors == 1


@pytest.mark.parametrize(
    "schema",
    ["", "bad\x00name", True, 7],
)
def test_schema_validation_rejects_invalid_shapes_before_driver_loading(schema):
    connector = FakeSnowflakeConnector(FakeConnection())
    with pytest.raises(ConfigError):
        connector.list_objects(schema=schema)
    assert connector.driver_loaded is False


def test_whitespace_and_sql_shaped_identifiers_are_preserved_and_bound():
    database = " "
    schema = " SELECT * FROM secrets;-- "
    connection = FakeConnection(current_database=database)
    connector = FakeSnowflakeConnector(connection, profile=_profile(""))
    assert connector.list_objects(database=database, schema=schema) == ()
    object_execution = next(item for item in connection.executions if item[0] == queries._OBJECTS_QUERY)
    assert object_execution[1] == (database, schema)
    assert schema not in object_execution[0]


def test_schema_mapping_includes_information_schema_and_deduplicates_deterministically():
    rows = [
        _schema_row(schema_name="zeta", comment=None),
        _schema_row(schema_name="INFORMATION_SCHEMA", is_managed_access=False),
        _schema_row(schema_name="zeta", comment="richer"),
        _schema_row(schema_name="Alpha", is_transient=True, is_managed_access=False),
    ]
    connection = FakeConnection({queries._SCHEMAS_QUERY: rows})
    schemas = FakeSnowflakeConnector(connection).list_schemas()
    assert [item.schema_name for item in schemas] == ["Alpha", "INFORMATION_SCHEMA", "zeta"]
    information_schema = schemas[1]
    assert information_schema.is_system_managed is True
    assert information_schema.vendor_metadata["classification"] == "INFORMATION_SCHEMA"
    assert schemas[0].vendor_metadata == {
        "classification": "USER",
        "is_transient": True,
        "is_managed_access": False,
    }
    assert schemas[2].comment == "richer"


def test_all_object_mappings_persistence_flags_and_row_counts():
    rows = [
        _object_row(object_name="table", table_type="BASE TABLE", estimated_row_count=5),
        _object_row(object_name="temporary", table_type="TEMPORARY TABLE", is_temporary=False),
        _object_row(object_name="transient", table_type="BASE TABLE", is_transient=True),
        _object_row(object_name="view", table_type="VIEW", estimated_row_count=99),
        _object_row(object_name="materialized", table_type="MATERIALIZED VIEW"),
        _object_row(object_name="external", table_type="EXTERNAL TABLE"),
        _object_row(object_name="dynamic", table_type="VIEW", is_dynamic=True),
        _object_row(object_name="future", table_type="EVENT TABLE", estimated_row_count=99),
    ]
    connection = FakeConnection({queries._OBJECTS_QUERY: rows})
    objects = FakeSnowflakeConnector(connection).list_objects(schema="Mixed Case")
    by_name = {item.object_name: item for item in objects}
    assert by_name["table"].object_type is DatabaseObjectType.TABLE
    assert by_name["temporary"].persistence is ObjectPersistence.TEMPORARY
    assert by_name["transient"].persistence is ObjectPersistence.TRANSIENT
    assert by_name["table"].persistence is ObjectPersistence.PERMANENT
    assert by_name["view"].object_type is DatabaseObjectType.VIEW
    assert by_name["view"].estimated_row_count is None
    assert by_name["materialized"].object_type is DatabaseObjectType.MATERIALIZED_VIEW
    assert by_name["external"].object_type is DatabaseObjectType.EXTERNAL_TABLE
    assert by_name["dynamic"].object_type is DatabaseObjectType.DYNAMIC_TABLE
    assert by_name["future"].object_type is DatabaseObjectType.UNKNOWN
    assert by_name["future"].persistence is ObjectPersistence.UNKNOWN
    assert by_name["future"].estimated_row_count is None


def test_malformed_persistence_flags_are_canonical_unknown():
    row = _object_row(is_temporary="NO", is_transient=None)
    objects = FakeSnowflakeConnector(
        FakeConnection({queries._OBJECTS_QUERY: [row]})
    ).list_objects(schema="Mixed Case")
    assert objects[0].object_type is DatabaseObjectType.TABLE
    assert objects[0].persistence is ObjectPersistence.UNKNOWN


def test_iceberg_hybrid_and_dynamic_precedence_are_preserved_safely():
    rows = [
        _object_row(object_name="iceberg", is_iceberg=True),
        _object_row(object_name="hybrid", is_hybrid=True),
        _object_row(object_name="dynamic_external", table_type="EXTERNAL TABLE", is_dynamic=True),
    ]
    objects = FakeSnowflakeConnector(
        FakeConnection({queries._OBJECTS_QUERY: rows})
    ).list_objects(schema="Mixed Case")
    by_name = {item.object_name: item for item in objects}
    assert by_name["iceberg"].object_type is DatabaseObjectType.TABLE
    assert by_name["iceberg"].vendor_metadata["is_iceberg"] is True
    assert by_name["hybrid"].object_type is DatabaseObjectType.TABLE
    assert by_name["hybrid"].vendor_metadata["is_hybrid"] is True
    assert by_name["dynamic_external"].object_type is DatabaseObjectType.DYNAMIC_TABLE


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, 0),
        (Decimal("12"), 12),
        (-1, None),
        (Decimal("1.5"), None),
        (True, None),
        ("12", None),
        (None, None),
    ],
)
def test_estimated_row_count_accepts_only_non_negative_integral_values(value, expected):
    row = _object_row(estimated_row_count=value)
    connection = FakeConnection({queries._OBJECTS_QUERY: [row]})
    objects = FakeSnowflakeConnector(connection).list_objects(schema="Mixed Case")
    assert objects[0].estimated_row_count == expected


def test_object_type_filters_are_validated_deduplicated_and_applied_after_mapping():
    original = (DatabaseObjectType.TABLE, DatabaseObjectType.TABLE)
    before = deepcopy(original)
    rows = [_object_row(), _object_row(object_name="v", table_type="VIEW")]
    objects = FakeSnowflakeConnector(
        FakeConnection({queries._OBJECTS_QUERY: rows})
    ).list_objects(schema="Mixed Case", object_types=original)
    assert original == before
    assert [item.object_name for item in objects] == ["Orders"]


def test_empty_filter_short_circuits_before_profile_config_driver_or_query(monkeypatch):
    monkeypatch.setattr(
        Config,
        "connection_config",
        classmethod(lambda cls: (_ for _ in ()).throw(AssertionError("global Config read"))),
    )
    connector = SnowflakeConnector()
    assert connector.list_objects(schema="anything", object_types=()) == ()


@pytest.mark.parametrize(
    "object_types",
    [
        [DatabaseObjectType.TABLE],
        (DatabaseObjectType.FOREIGN_TABLE,),
        (DatabaseObjectType.PARTITIONED_TABLE,),
        ("TABLE",),
    ],
)
def test_unsupported_object_filters_fail_before_driver_loading(object_types):
    connector = FakeSnowflakeConnector(FakeConnection())
    with pytest.raises(ConfigError):
        connector.list_objects(schema="s", object_types=object_types)
    assert connector.driver_loaded is False


def test_object_ordering_and_duplicate_selection_are_input_order_independent():
    rows = [
        _object_row(object_name="z", comment=None),
        _object_row(object_name="z", comment="richer"),
        _object_row(object_name="a", table_type="VIEW"),
        _object_row(object_name="b"),
    ]
    forward = FakeSnowflakeConnector(
        FakeConnection({queries._OBJECTS_QUERY: rows})
    ).list_objects(schema="Mixed Case")
    reverse = FakeSnowflakeConnector(
        FakeConnection({queries._OBJECTS_QUERY: list(reversed(rows))})
    ).list_objects(schema="Mixed Case")
    assert forward == reverse
    assert [(item.object_type.value, item.object_name) for item in forward] == [
        ("TABLE", "b"),
        ("TABLE", "z"),
        ("VIEW", "a"),
    ]
    assert forward[1].comment == "richer"


@pytest.mark.parametrize(
    "row",
    [
        _schema_row(schema_name=""),
        _schema_row(catalog_name=None),
        _object_row(object_name=""),
        _object_row(table_type=None),
        _object_row(schema_name="other"),
    ],
)
def test_malformed_or_out_of_scope_rows_fail_safely(row):
    query = queries._SCHEMAS_QUERY if "schema_classification" in row else queries._OBJECTS_QUERY
    connection = FakeConnection({query: [row]})
    connector = FakeSnowflakeConnector(connection)
    with pytest.raises(MalformedDiscoveryResultError):
        if query == queries._SCHEMAS_QUERY:
            connector.list_schemas()
        else:
            connector.list_objects(schema="Mixed Case")
    assert connection.closed
    assert connection.cursor_count == connection.closed_cursors


@pytest.mark.parametrize(
    ("failure", "exception_type"),
    [
        (DriverFailure("secret", errno=604), SchemaDiscoveryTimeoutError),
        (DriverFailure("secret", sqlstate="57014"), SchemaDiscoveryTimeoutError),
        (DriverFailure("secret", sqlstate="08006"), SchemaDiscoveryConnectionError),
        (DriverFailure("secret", sqlstate="42501"), SchemaDiscoveryError),
        (DriverFailure("secret", sqlstate="XX000"), SchemaDiscoveryError),
    ],
)
def test_query_errors_are_redacted_translated_and_fully_cleaned_up(
    failure,
    exception_type,
    caplog,
):
    connection = FakeConnection({queries._SCHEMAS_QUERY: failure})
    with pytest.raises(exception_type) as captured:
        FakeSnowflakeConnector(connection).list_schemas()
    assert "secret" not in str(captured.value)
    assert "secret" not in repr(captured.value)
    assert "secret" not in caplog.text
    assert connection.closed
    assert connection.cursor_count == connection.closed_cursors
    assert connection.commit_count == connection.rollback_count == 0


def test_connect_failure_is_redacted_and_translated_without_a_connection():
    marker = "password=top-secret account=private"
    connector = FakeSnowflakeConnector(
        FakeConnection(),
        connect_error=DriverFailure(marker, sqlstate="28000"),
    )
    with pytest.raises(SchemaDiscoveryConnectionError) as captured:
        connector.list_schemas()
    assert marker not in str(captured.value)


@pytest.mark.parametrize("timeout", [True, False, 0, -1, 1.5, "5"])
def test_invalid_timeouts_fail_before_driver_loading(timeout):
    connector = FakeSnowflakeConnector(FakeConnection())
    with pytest.raises(ConfigError):
        connector.list_schemas(timeout_seconds=timeout)
    assert connector.driver_loaded is False


def test_timeout_lifecycle_and_bound_parameters_use_one_connection_and_two_cursors():
    connection = FakeConnection({queries._SCHEMAS_QUERY: [_schema_row()]})
    connector = FakeSnowflakeConnector(connection)
    connector.list_schemas(timeout_seconds=9)
    assert connector.fake_driver.connect_kwargs[0]["login_timeout"] == 9
    assert [item[2] for item in connection.executions] == [9, 9]
    assert connection.executions[1][1] == ("App DB",)
    assert connection.cursor_count == connection.closed_cursors == 2
    assert connection.closed
    assert connection.commit_count == connection.rollback_count == 0


def test_every_snowflake_discovery_query_is_one_fixed_read_only_select():
    query_constants = {
        name: value
        for name, value in vars(queries).items()
        if name.startswith("_") and name.endswith("_QUERY") and isinstance(value, str)
    }
    assert set(query_constants) == {
        "_CURRENT_DATABASE_QUERY",
        "_SCHEMAS_QUERY",
        "_OBJECTS_QUERY",
    }
    forbidden = re.compile(
        r"\b(USE|SHOW|RESULT_SCAN|INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|"
        r"TRUNCATE|GRANT|REVOKE|COPY|CALL|SET|BEGIN|COMMIT|ROLLBACK)\b",
        re.I,
    )
    for name, query in query_constants.items():
        without_literals = re.sub(r"'[^']*'", "''", query)
        assert query.strip().upper().startswith("SELECT"), name
        assert len(re.findall(r"\bSELECT\b", without_literals, flags=re.I)) == 1, name
        assert ";" not in query, name
        assert forbidden.search(without_literals) is None, name
        assert "{" not in query and "}" not in query, name
    assert queries._SCHEMAS_QUERY.count("%s") == 1
    assert queries._OBJECTS_QUERY.count("%s") == 2
