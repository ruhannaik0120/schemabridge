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
from connectors.snowflake import _discovery_commands as commands
from connectors.snowflake.connector import SnowflakeConnector
from models.connection_profile import ConnectionProfile
from models.discovery import CoverageStatus, DatabaseObjectType, ObjectPersistence


def _profile(
    database: str = "App DB",
    *,
    profile_id: str = "snowflake-discovery",
    password: str = "credential-marker",
    connection_options: dict[str, Any] | None = None,
) -> ConnectionProfile:
    return ConnectionProfile(
        profile_id=profile_id,
        db_type="snowflake",
        host="org-account",
        database=database,
        username="app",
        password=password,
        connection_options=(
            {"warehouse": "DISCOVERY_WH", "role": "DISCOVERY_ROLE"}
            if connection_options is None
            else connection_options
        ),
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
        self._sfqid = marker

    @property
    def sfqid(self):
        raise AssertionError("Canonical discovery must not access query IDs.")


class CursorResult:
    def __init__(self, columns: list[str], rows: list[Any]):
        self.columns = columns
        self.rows = rows


class FakeCursor:
    def __init__(self, connection: "FakeConnection"):
        self.connection = connection
        self.description: list[tuple[str]] | None = None
        self._raw_rows: list[Any] = []
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
        if self.connection.transaction_poisoned:
            raise DriverFailure("poisoned transaction", sqlstate="25000")
        response = self.connection.response_for(query, bound)
        if isinstance(response, BaseException):
            if (
                getattr(response, "sqlstate", None) == "42501"
                and self.connection.autocommit is False
            ):
                self.connection.transaction_poisoned = True
            raise response
        if isinstance(response, CursorResult):
            self.description = [(name,) for name in response.columns]
            self._raw_rows = list(response.rows)
        else:
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
        self.autocommit: bool | None = None
        self.transaction_poisoned = False

    def response_for(self, query: str, parameters: tuple[Any, ...]) -> Any:
        if query == queries._CURRENT_DATABASE_QUERY:
            default: Any = [{"current_database": self.current_database}]
        elif query.startswith(commands._SHOW_PRIMARY_KEYS_PREFIX):
            default = CursorResult(
                [
                    "database_name",
                    "schema_name",
                    "table_name",
                    "constraint_name",
                    "column_name",
                    "key_sequence",
                    "comment",
                ],
                [],
            )
        else:
            default = []
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
        self.connection.autocommit = kwargs.get("autocommit")
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


def _table_metadata_row(**overrides: Any) -> dict[str, Any]:
    row = _object_row(
        is_immutable=False,
        clustering_expression=None,
    )
    row.update(overrides)
    return row


def _column_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "column_name": "Order Id",
        "ordinal_position": 1,
        "data_type": "NUMBER",
        "data_type_alias": "INT",
        "is_nullable": "NO",
        "character_maximum_length": None,
        "numeric_precision": 38,
        "numeric_scale": 0,
        "datetime_precision": None,
        "column_default": "1",
        "column_comment": "identifier",
        "collation_name": None,
        "is_identity": True,
        "identity_generation": "BY DEFAULT",
        "identity_start": 1,
        "identity_increment": 1,
        "identity_cycle": False,
        "identity_ordered": True,
        "generation_expression": None,
        "kind": "COLUMN",
        "dtd_identifier": "DT1",
    }
    row.update(overrides)
    return row


def _key_constraint_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "constraint_name": "Orders PK",
        "constraint_type": "PRIMARY KEY",
        "is_enforced": False,
        "is_rely": True,
        "is_deferrable": False,
        "initially_deferred": False,
        "comment": "key comment",
    }
    row.update(overrides)
    return row


def _foreign_key_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "constraint_name": "Orders Customer FK",
        "referenced_catalog": "App DB",
        "referenced_schema": "Mixed Case",
        "referenced_table": "Customers",
        "match_option": "NONE",
        "update_rule": "NO ACTION",
        "delete_rule": "CASCADE",
        "is_enforced": False,
        "is_rely": True,
        "is_deferrable": False,
        "initially_deferred": False,
        "comment": "fk comment",
    }
    row.update(overrides)
    return row


def _check_constraint_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "constraint_catalog": "App DB",
        "constraint_schema": "Mixed Case",
        "constraint_table": "Orders",
        "constraint_name": "Orders Amount Check",
        "expression": '"amount" > 0',
    }
    row.update(overrides)
    return row


def _show_primary_key_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "database_name": "App DB",
        "schema_name": "Mixed Case",
        "table_name": "Orders",
        "constraint_name": "Orders PK",
        "column_name": "Order Id",
        "key_sequence": 1,
        "comment": "key comment",
    }
    row.update(overrides)
    return row


def _show_primary_keys_result(
    rows: list[dict[str, Any]] | None = None,
    *,
    columns: list[str] | None = None,
) -> CursorResult:
    selected = columns or [
        "database_name",
        "schema_name",
        "table_name",
        "constraint_name",
        "column_name",
        "key_sequence",
        "comment",
    ]
    return CursorResult(
        selected,
        [tuple(row.get(name.casefold()) for name in selected) for row in (rows or [])],
    )


def _stage_3b_responses(**overrides: Any) -> dict[str, Any]:
    responses: dict[str, Any] = {
        queries._TABLE_METADATA_QUERY: [_table_metadata_row()],
        queries._COLUMNS_QUERY: [_column_row()],
        queries._KEY_CONSTRAINTS_QUERY: [],
        queries._FOREIGN_KEYS_QUERY: [],
        queries._CHECK_CONSTRAINTS_QUERY: [],
    }
    responses.update(overrides)
    return responses


def _show_command(
    database: str = "App DB",
    schema: str = "Mixed Case",
    table: str = "Orders",
) -> str:
    return commands._show_primary_keys_command(database, schema, table)


def test_stage_3b_signatures_are_exact_and_protocol_conformance_is_complete():
    expected_schemas = ["self", "database", "timeout_seconds"]
    expected_objects = ["self", "database", "schema", "object_types", "timeout_seconds"]
    expected_table = ["self", "database", "schema", "table", "timeout_seconds"]
    schemas_signature = inspect.signature(SnowflakeConnector.list_schemas)
    objects_signature = inspect.signature(SnowflakeConnector.list_objects)
    table_signature = inspect.signature(SnowflakeConnector.get_table_metadata)
    assert schemas_signature == inspect.signature(SchemaDiscoveryConnector.list_schemas)
    assert objects_signature == inspect.signature(SchemaDiscoveryConnector.list_objects)
    assert table_signature == inspect.signature(SchemaDiscoveryConnector.get_table_metadata)
    assert list(schemas_signature.parameters) == expected_schemas
    assert list(objects_signature.parameters) == expected_objects
    assert list(table_signature.parameters) == expected_table
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
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for name, parameter in table_signature.parameters.items()
        if name != "self"
    )
    connector = SnowflakeConnector(profile=_profile())
    assert isinstance(connector, SchemaDiscoveryConnector)


def test_profile_bound_discovery_never_reads_global_config(monkeypatch):
    monkeypatch.setattr(
        Config,
        "connection_config",
        classmethod(lambda cls: (_ for _ in ()).throw(AssertionError("global Config read"))),
    )
    connection = FakeConnection({queries._SCHEMAS_QUERY: []})
    assert FakeSnowflakeConnector(connection).list_schemas() == ()


@pytest.mark.parametrize("configured_autocommit", [False, True, None])
def test_discovery_always_uses_autocommit_without_mutating_profile_options(
    configured_autocommit,
):
    source_options: dict[str, Any] = {
        "warehouse": "DISCOVERY_WH",
        "session_parameters": {"QUERY_TAGS": ["canonical"]},
    }
    if configured_autocommit is not None:
        source_options["autocommit"] = configured_autocommit
    source_before = deepcopy(source_options)
    profile = _profile(connection_options=source_options)
    profile_before = profile.connection_options_copy()
    connection = FakeConnection()
    connector = FakeSnowflakeConnector(connection, profile=profile)

    assert connector.list_schemas() == ()
    assert connector.fake_driver.connect_kwargs[0]["autocommit"] is True
    assert connection.autocommit is True
    assert profile.connection_options_copy() == profile_before
    assert source_options == source_before

    connector.fake_driver.connect_kwargs[0]["session_parameters"]["QUERY_TAGS"].append(
        "driver-copy"
    )
    assert profile.connection_options_copy() == profile_before
    assert source_options == source_before


def test_discovery_autocommit_prevents_permission_failure_from_poisoning_later_queries():
    profile = _profile(
        connection_options={
            "warehouse": "DISCOVERY_WH",
            "role": "DISCOVERY_ROLE",
            "autocommit": False,
        }
    )
    profile_before = profile.connection_options_copy()
    responses = _stage_3b_responses(
        **{
            queries._COLUMNS_QUERY: DriverFailure("private", sqlstate="42501"),
            queries._CHECK_CONSTRAINTS_QUERY: [_check_constraint_row()],
        }
    )
    connection = FakeConnection(responses)
    connector = FakeSnowflakeConnector(connection, profile=profile)

    metadata = connector.get_table_metadata(schema="Mixed Case", table="Orders")

    assert metadata is not None
    assert metadata.coverage.columns is CoverageStatus.UNAVAILABLE
    assert metadata.coverage.primary_key is CoverageStatus.COMPLETE
    assert metadata.coverage.foreign_keys is CoverageStatus.COMPLETE
    assert metadata.coverage.check_constraints is CoverageStatus.COMPLETE
    assert metadata.check_constraints[0].name == "Orders Amount Check"
    executed_queries = [item[0] for item in connection.executions]
    assert executed_queries.index(queries._CHECK_CONSTRAINTS_QUERY) > executed_queries.index(
        queries._COLUMNS_QUERY
    )
    assert connection.autocommit is True
    assert connection.transaction_poisoned is False
    assert profile.connection_options_copy() == profile_before
    assert profile.connection_options["autocommit"] is False
    assert connection.commit_count == connection.rollback_count == 0
    assert connection.cursor_count == connection.closed_cursors
    assert connection.closed


def test_legacy_connect_preserves_configured_autocommit_behavior():
    profile = _profile(connection_options={"autocommit": False})
    connection = FakeConnection()
    connector = FakeSnowflakeConnector(connection, profile=profile)

    legacy_connection = connector.connect()

    assert connector.fake_driver.connect_kwargs[0]["autocommit"] is False
    assert legacy_connection.autocommit is False
    legacy_connection.close()


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
    assert first.fake_driver.connect_kwargs[0]["autocommit"] is True
    assert second.fake_driver.connect_kwargs[0]["autocommit"] is True


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


def test_get_table_metadata_returns_sanitized_metadata_with_partial_constraint_headers():
    column = _column_row(schema_evolution_record="query-id", masking_policy="secret")
    responses = _stage_3b_responses(
        **{
            queries._COLUMNS_QUERY: [
                _column_row(column_name="z", ordinal_position=2),
                column,
            ],
            queries._KEY_CONSTRAINTS_QUERY: [
                _key_constraint_row(),
                _key_constraint_row(
                    constraint_name="Orders Order Number Unique",
                    constraint_type="UNIQUE",
                    is_enforced=False,
                    is_rely=False,
                ),
            ],
            queries._FOREIGN_KEYS_QUERY: [_foreign_key_row()],
            queries._CHECK_CONSTRAINTS_QUERY: [_check_constraint_row()],
        }
    )
    connection = FakeConnection(responses)
    metadata = FakeSnowflakeConnector(connection).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert metadata is not None
    assert [column.column_name for column in metadata.columns] == ["Order Id", "z"]
    first_column = metadata.columns[0]
    assert first_column.ordinal_position == 1
    assert first_column.numeric_precision == 38
    assert first_column.is_identity is True
    assert first_column.is_auto_increment is True
    assert first_column.vendor_metadata == {
        "data_type_alias": "INT",
        "dtd_identifier": "DT1",
        "identity_start": 1,
        "identity_increment": 1,
        "identity_cycle": False,
        "identity_ordered": True,
        "column_kind": "COLUMN",
    }
    assert "schema_evolution_record" not in first_column.vendor_metadata
    assert "masking_policy" not in first_column.vendor_metadata
    assert metadata.primary_key is not None
    assert metadata.primary_key.columns == ()
    assert metadata.primary_key.is_enforced is False
    assert metadata.primary_key.is_rely is True
    assert metadata.unique_constraints[0].columns == ()
    assert metadata.foreign_keys[0].local_columns == ()
    assert metadata.foreign_keys[0].referenced_columns == ()
    assert metadata.foreign_keys[0].referenced_table == "Customers"
    assert metadata.foreign_keys[0].update_rule == "NO ACTION"
    assert metadata.foreign_keys[0].delete_rule == "CASCADE"
    assert metadata.check_constraints[0].expression == '"amount" > 0'
    assert metadata.check_constraints[0].is_enforced is None
    assert metadata.check_constraints[0].is_validated is None
    assert metadata.check_constraints[0].is_rely is None
    assert metadata.coverage.columns is CoverageStatus.COMPLETE
    assert metadata.coverage.primary_key is CoverageStatus.PARTIAL
    assert metadata.coverage.unique_constraints is CoverageStatus.PARTIAL
    assert metadata.coverage.foreign_keys is CoverageStatus.PARTIAL
    assert metadata.coverage.check_constraints is CoverageStatus.COMPLETE
    assert metadata.coverage.partitioning is CoverageStatus.NOT_APPLICABLE
    assert metadata.coverage.clustering is CoverageStatus.COMPLETE
    assert metadata.coverage.warnings == tuple(sorted(metadata.coverage.warnings))
    assert "PRIMARY_KEY_MEMBERSHIP_UNAVAILABLE" in metadata.coverage.warnings
    assert "UNIQUE_CONSTRAINT_MEMBERSHIP_UNAVAILABLE" in metadata.coverage.warnings
    assert "FOREIGN_KEY_MEMBERSHIP_UNAVAILABLE" in metadata.coverage.warnings
    assert connection.closed
    assert connection.cursor_count == connection.closed_cursors == 7
    assert connection.commit_count == connection.rollback_count == 0


def test_get_table_metadata_returns_none_only_for_a_missing_base_object():
    connection = FakeConnection({queries._TABLE_METADATA_QUERY: []})
    metadata = FakeSnowflakeConnector(connection).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert metadata is None
    assert connection.closed
    assert connection.cursor_count == connection.closed_cursors == 2
    assert connection.commit_count == connection.rollback_count == 0


@pytest.mark.parametrize(
    "base_rows",
    [
        [_table_metadata_row(), _table_metadata_row()],
        [_table_metadata_row(object_name="other")],
        [_table_metadata_row(table_type="EVENT TABLE")],
    ],
)
def test_get_table_metadata_rejects_duplicate_malformed_and_unsupported_bases(base_rows):
    connection = FakeConnection({queries._TABLE_METADATA_QUERY: base_rows})
    with pytest.raises(SchemaDiscoveryError):
        FakeSnowflakeConnector(connection).get_table_metadata(
            schema="Mixed Case",
            table="Orders",
        )
    assert connection.closed
    assert connection.cursor_count == connection.closed_cursors == 2


def test_get_table_metadata_preserves_exact_database_schema_and_table_as_parameters():
    database = " Exact DB "
    schema = " SELECT * FROM secrets;-- "
    table = "Drop Table?"
    response = _table_metadata_row(
        catalog_name=database,
        schema_name=schema,
        object_name=table,
    )
    connection = FakeConnection(
        _stage_3b_responses(**{queries._TABLE_METADATA_QUERY: [response]}),
        current_database=database,
    )
    connector = FakeSnowflakeConnector(connection, profile=_profile(""))
    connector.get_table_metadata(database=database, schema=schema, table=table)
    base_execution = next(
        item for item in connection.executions if item[0] == queries._TABLE_METADATA_QUERY
    )
    assert base_execution[1] == (database, schema, table)
    assert schema not in base_execution[0]
    assert table not in base_execution[0]
    assert connector.fake_driver.connect_kwargs[0]["database"] == database


@pytest.mark.parametrize(
    ("row", "view_rows", "expected_type", "expected_key", "expected_view"),
    [
        (_table_metadata_row(), None, DatabaseObjectType.TABLE, CoverageStatus.COMPLETE, CoverageStatus.NOT_APPLICABLE),
        (_table_metadata_row(table_type="VIEW", estimated_row_count=None), [{"view_definition": "SELECT 1", "is_secure": False}], DatabaseObjectType.VIEW, CoverageStatus.NOT_APPLICABLE, CoverageStatus.COMPLETE),
        (_table_metadata_row(table_type="MATERIALIZED VIEW", estimated_row_count=2), None, DatabaseObjectType.MATERIALIZED_VIEW, CoverageStatus.NOT_APPLICABLE, CoverageStatus.UNAVAILABLE),
        (_table_metadata_row(table_type="EXTERNAL TABLE"), None, DatabaseObjectType.EXTERNAL_TABLE, CoverageStatus.COMPLETE, CoverageStatus.NOT_APPLICABLE),
        (_table_metadata_row(is_dynamic=True), None, DatabaseObjectType.DYNAMIC_TABLE, CoverageStatus.COMPLETE, CoverageStatus.NOT_APPLICABLE),
    ],
)
def test_get_table_metadata_covers_each_supported_object_class(
    row,
    view_rows,
    expected_type,
    expected_key,
    expected_view,
):
    responses = _stage_3b_responses(**{queries._TABLE_METADATA_QUERY: [row]})
    if view_rows is not None:
        responses[queries._VIEW_DEFINITION_QUERY] = view_rows
    connection = FakeConnection(responses)
    metadata = FakeSnowflakeConnector(connection).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert metadata is not None
    assert metadata.object_type is expected_type
    assert metadata.coverage.primary_key is expected_key
    assert metadata.coverage.unique_constraints is expected_key
    assert metadata.coverage.view_definition is expected_view
    if expected_type is DatabaseObjectType.EXTERNAL_TABLE:
        assert metadata.coverage.clustering is CoverageStatus.NOT_APPLICABLE
        assert queries._KEY_CONSTRAINTS_QUERY in [item[0] for item in connection.executions]


@pytest.mark.parametrize(
    ("value", "expected_vendor_value", "expected_coverage", "expected_warning"),
    [
        (True, True, CoverageStatus.COMPLETE, None),
        (False, False, CoverageStatus.COMPLETE, None),
        (None, None, CoverageStatus.UNAVAILABLE, "CLUSTERING_UNAVAILABLE"),
        ("YES", None, CoverageStatus.PARTIAL, "CLUSTERING_PARTIAL"),
    ],
)
def test_get_table_metadata_preserves_native_auto_clustering_boolean_coverage(
    value,
    expected_vendor_value,
    expected_coverage,
    expected_warning,
):
    responses = _stage_3b_responses(
        **{
            queries._TABLE_METADATA_QUERY: [
                _table_metadata_row(auto_clustering_on=value)
            ]
        }
    )
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert metadata is not None
    assert metadata.vendor_metadata["auto_clustering_on"] is expected_vendor_value
    assert metadata.coverage.clustering is expected_coverage
    if expected_warning is None:
        assert "CLUSTERING_UNAVAILABLE" not in metadata.coverage.warnings
        assert "CLUSTERING_PARTIAL" not in metadata.coverage.warnings
    else:
        assert expected_warning in metadata.coverage.warnings


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", 0),
        ("1", 1),
        ("-1", -1),
        ("+1", 1),
        ("0001", 1),
        (1, 1),
        (Decimal("1"), 1),
    ],
)
def test_discovery_integer_accepts_only_complete_integral_values(value, expected):
    assert SnowflakeConnector._discovery_integer(value) == (expected, True)


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        " 1",
        "1 ",
        "1.0",
        "1e3",
        "+-1",
        "--1",
        "1abc",
        True,
        1.0,
        Decimal("1.5"),
        Decimal("NaN"),
        Decimal("Infinity"),
    ],
)
def test_discovery_integer_rejects_noncanonical_or_fractional_values(value):
    assert SnowflakeConnector._discovery_integer(value) == (None, False)


def test_get_table_metadata_normalizes_identity_numeric_text_without_mutating_rows():
    column_row = _column_row(
        identity_start="-1",
        identity_increment="+1",
    )
    before = deepcopy(column_row)
    responses = _stage_3b_responses(**{queries._COLUMNS_QUERY: [column_row]})

    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )

    assert metadata is not None
    assert metadata.coverage.columns is CoverageStatus.COMPLETE
    assert metadata.columns[0].is_identity is True
    assert metadata.columns[0].identity_generation == "BY DEFAULT"
    assert metadata.columns[0].vendor_metadata["identity_start"] == -1
    assert metadata.columns[0].vendor_metadata["identity_increment"] == 1
    assert column_row == before


@pytest.mark.parametrize("value", ["1.0", "1e3", " 1", True, Decimal("1.5")])
def test_get_table_metadata_marks_malformed_identity_numeric_values_partial(value):
    column_row = _column_row(identity_start=value)
    responses = _stage_3b_responses(**{queries._COLUMNS_QUERY: [column_row]})

    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )

    assert metadata is not None
    assert metadata.coverage.columns is CoverageStatus.PARTIAL
    assert "identity_start" not in metadata.columns[0].vendor_metadata


def test_non_identity_columns_ignore_identity_only_source_fields():
    column_row = _column_row(
        is_identity=False,
        identity_generation="malformed but irrelevant",
        identity_start="1.0",
        identity_increment=True,
        identity_cycle="YES",
        identity_ordered="NO",
    )
    responses = _stage_3b_responses(**{queries._COLUMNS_QUERY: [column_row]})

    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )

    assert metadata is not None
    column = metadata.columns[0]
    assert metadata.coverage.columns is CoverageStatus.COMPLETE
    assert column.is_identity is False
    assert column.is_auto_increment is False
    assert column.identity_generation is None
    assert "identity_start" not in column.vendor_metadata
    assert "identity_increment" not in column.vendor_metadata
    assert "identity_cycle" not in column.vendor_metadata
    assert "identity_ordered" not in column.vendor_metadata


def test_get_table_metadata_converts_decimal_columns_and_enriches_only_structured_arrays():
    array_column = _column_row(
        column_name="nested",
        ordinal_position=Decimal("2"),
        data_type="ARRAY",
        data_type_alias=None,
        numeric_precision=Decimal("38"),
        numeric_scale=Decimal("0"),
        dtd_identifier="ARRAY_DTD",
    )
    response = _stage_3b_responses(
        **{
            queries._COLUMNS_QUERY: [array_column],
            queries._ELEMENT_TYPES_QUERY: [
                {
                    "collection_type_identifier": "ARRAY_DTD",
                    "data_type": "NUMBER",
                    "dtd_identifier": None,
                }
            ],
        }
    )
    metadata = FakeSnowflakeConnector(FakeConnection(response)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert metadata is not None
    column = metadata.columns[0]
    assert column.ordinal_position == 2
    assert column.numeric_precision == 38
    assert column.array_dimensions == 1
    assert column.element_native_type == "NUMBER"


def test_get_table_metadata_marks_malformed_or_unavailable_array_elements_partial():
    array_column = _column_row(data_type="ARRAY", dtd_identifier="ARRAY_DTD")
    malformed = _stage_3b_responses(
        **{
            queries._COLUMNS_QUERY: [array_column],
            queries._ELEMENT_TYPES_QUERY: [
                {
                    "collection_type_identifier": "ARRAY_DTD",
                    "data_type": None,
                    "dtd_identifier": None,
                }
            ],
        }
    )
    malformed_metadata = FakeSnowflakeConnector(FakeConnection(malformed)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert malformed_metadata is not None
    assert malformed_metadata.coverage.columns is CoverageStatus.PARTIAL

    unavailable = _stage_3b_responses(
        **{
            queries._COLUMNS_QUERY: [array_column],
            queries._ELEMENT_TYPES_QUERY: DriverFailure("private", sqlstate="42501"),
        }
    )
    unavailable_metadata = FakeSnowflakeConnector(FakeConnection(unavailable)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert unavailable_metadata is not None
    assert unavailable_metadata.coverage.columns is CoverageStatus.PARTIAL
    assert "STRUCTURED_ARRAY_TYPES_UNAVAILABLE" in unavailable_metadata.coverage.warnings


def test_get_table_metadata_degrades_component_permissions_without_leaking_errors():
    responses = _stage_3b_responses(
        **{
            queries._COLUMNS_QUERY: DriverFailure("secret identifiers", sqlstate="42501"),
            queries._KEY_CONSTRAINTS_QUERY: DriverFailure("secret identifiers", sqlstate="42501"),
            queries._FOREIGN_KEYS_QUERY: DriverFailure("secret identifiers", sqlstate="42501"),
            queries._CHECK_CONSTRAINTS_QUERY: DriverFailure("secret identifiers", sqlstate="42501"),
        }
    )
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert metadata is not None
    assert metadata.columns == ()
    assert metadata.coverage.columns is CoverageStatus.UNAVAILABLE
    assert metadata.coverage.primary_key is CoverageStatus.UNAVAILABLE
    assert metadata.coverage.unique_constraints is CoverageStatus.UNAVAILABLE
    assert metadata.coverage.foreign_keys is CoverageStatus.UNAVAILABLE
    assert metadata.coverage.check_constraints is CoverageStatus.UNAVAILABLE
    assert all("SECRET" not in warning for warning in metadata.coverage.warnings)


def test_get_table_metadata_omits_foreign_keys_without_a_visible_target():
    responses = _stage_3b_responses(
        **{
            queries._FOREIGN_KEYS_QUERY: [_foreign_key_row(referenced_table=None)],
        }
    )
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert metadata is not None
    assert metadata.foreign_keys == ()
    assert metadata.coverage.foreign_keys is CoverageStatus.PARTIAL
    assert "FOREIGN_KEYS_PARTIAL" in metadata.coverage.warnings


def test_get_table_metadata_orders_direct_check_constraints_without_invented_reliability():
    responses = _stage_3b_responses(
        **{
            queries._CHECK_CONSTRAINTS_QUERY: [
                _check_constraint_row(
                    constraint_name="z_check",
                    expression='"z" > 0',
                ),
                _check_constraint_row(
                    constraint_name="a_check",
                    expression='"a" > 0',
                ),
            ]
        }
    )
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert metadata is not None
    assert [constraint.name for constraint in metadata.check_constraints] == [
        "a_check",
        "z_check",
    ]
    assert metadata.coverage.check_constraints is CoverageStatus.COMPLETE
    assert all(constraint.is_enforced is None for constraint in metadata.check_constraints)
    assert all(constraint.is_validated is None for constraint in metadata.check_constraints)
    assert all(constraint.is_rely is None for constraint in metadata.check_constraints)


@pytest.mark.parametrize(
    "rows",
    [
        [_check_constraint_row(expression=None)],
        [_check_constraint_row(expression="")],
        [_check_constraint_row(constraint_table="Other")],
    ],
)
def test_get_table_metadata_marks_malformed_direct_check_rows_partial(rows):
    responses = _stage_3b_responses(**{queries._CHECK_CONSTRAINTS_QUERY: rows})
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert metadata is not None
    assert metadata.check_constraints == ()
    assert metadata.coverage.check_constraints is CoverageStatus.PARTIAL
    assert "CHECK_CONSTRAINTS_PARTIAL" in metadata.coverage.warnings


def test_get_table_metadata_marks_successful_empty_checks_complete_for_tables():
    metadata = FakeSnowflakeConnector(
        FakeConnection(_stage_3b_responses())
    ).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None
    assert metadata.check_constraints == ()
    assert metadata.coverage.check_constraints is CoverageStatus.COMPLETE


@pytest.mark.parametrize(
    ("query", "failure", "exception_type"),
    [
        (queries._TABLE_METADATA_QUERY, DriverFailure("secret", errno=604), SchemaDiscoveryTimeoutError),
        (queries._COLUMNS_QUERY, DriverFailure("secret", sqlstate="57014"), SchemaDiscoveryTimeoutError),
        (queries._KEY_CONSTRAINTS_QUERY, DriverFailure("secret", sqlstate="08006"), SchemaDiscoveryConnectionError),
        (queries._CHECK_CONSTRAINTS_QUERY, DriverFailure("secret", sqlstate="XX000"), SchemaDiscoveryError),
    ],
)
def test_get_table_metadata_translates_fatal_failures_and_closes_resources(
    query,
    failure,
    exception_type,
):
    responses = _stage_3b_responses(**{query: failure})
    connection = FakeConnection(responses)
    with pytest.raises(exception_type) as captured:
        FakeSnowflakeConnector(connection).get_table_metadata(
            schema="Mixed Case",
            table="Orders",
        )
    assert "secret" not in str(captured.value)
    assert connection.closed
    assert connection.cursor_count == connection.closed_cursors
    assert connection.commit_count == connection.rollback_count == 0


def test_get_table_metadata_is_deterministic_under_shuffled_catalog_rows():
    first = _stage_3b_responses(
        **{
            queries._COLUMNS_QUERY: [
                _column_row(column_name="b", ordinal_position=2),
                _column_row(column_name="a", ordinal_position=1),
            ],
            queries._KEY_CONSTRAINTS_QUERY: [
                _key_constraint_row(constraint_name="z", constraint_type="UNIQUE"),
                _key_constraint_row(constraint_name="a", constraint_type="UNIQUE"),
            ],
        }
    )
    second = dict(first)
    second[queries._COLUMNS_QUERY] = list(reversed(first[queries._COLUMNS_QUERY]))
    second[queries._KEY_CONSTRAINTS_QUERY] = list(reversed(first[queries._KEY_CONSTRAINTS_QUERY]))
    forward = FakeSnowflakeConnector(FakeConnection(first)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    reverse = FakeSnowflakeConnector(FakeConnection(second)).get_table_metadata(
        schema="Mixed Case",
        table="Orders",
    )
    assert forward == reverse


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
        "_TABLE_METADATA_QUERY",
        "_COLUMNS_QUERY",
        "_ELEMENT_TYPES_QUERY",
        "_KEY_CONSTRAINTS_QUERY",
        "_FOREIGN_KEYS_QUERY",
        "_CHECK_CONSTRAINTS_QUERY",
        "_VIEW_DEFINITION_QUERY",
    }
    forbidden = re.compile(
        r"\b(USE|SHOW|RESULT_SCAN|GET_DDL|INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|"
        r"DROP|TRUNCATE|GRANT|REVOKE|COPY|CALL|PUT|GET|REMOVE|SET|UNSET|BEGIN|"
        r"COMMIT|ROLLBACK)\b",
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
    assert queries._TABLE_METADATA_QUERY.count("%s") == 3
    assert queries._COLUMNS_QUERY.count("%s") == 3
    assert queries._ELEMENT_TYPES_QUERY.count("%s") == 3
    assert queries._KEY_CONSTRAINTS_QUERY.count("%s") == 3
    assert queries._FOREIGN_KEYS_QUERY.count("%s") == 3
    assert queries._CHECK_CONSTRAINTS_QUERY.count("%s") == 3
    assert queries._VIEW_DEFINITION_QUERY.count("%s") == 3
    assert 'AUTO_CLUSTERING_ON AS "auto_clustering_on"' in queries._TABLE_METADATA_QUERY
    assert "AUTO_CLUSTERING_ON IN" not in queries._TABLE_METADATA_QUERY
    assert (
        "FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS"
        in queries._CHECK_CONSTRAINTS_QUERY
    )
    assert "TABLE_CONSTRAINTS" not in queries._CHECK_CONSTRAINTS_QUERY
    assert "CONSTRAINT_TYPE" not in queries._CHECK_CONSTRAINTS_QUERY
    assert "CONSTRAINT_CATALOG = %s" in queries._CHECK_CONSTRAINTS_QUERY
    assert "CONSTRAINT_SCHEMA = %s" in queries._CHECK_CONSTRAINTS_QUERY
    assert "CONSTRAINT_TABLE = %s" in queries._CHECK_CONSTRAINTS_QUERY


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("plain", '"plain"'),
        ("schema.with.dot", '"schema.with.dot"'),
        ('embedded"quote', '"embedded""quote"'),
        ('x"; DROP TABLE y; --', '"x""; DROP TABLE y; --"'),
        ("/*comment*/", '"/*comment*/"'),
        ("SELECT", '"SELECT"'),
        ("雪", '"雪"'),
        (" leading and trailing ", '" leading and trailing "'),
        ("db.schema.table", '"db.schema.table"'),
        ("IDENTIFIER('other')", '"IDENTIFIER(\'other\')"'),
    ],
)
def test_primary_key_identifier_component_serializer_preserves_and_quotes(value, expected):
    assert commands._quote_identifier_component(value) == expected


@pytest.mark.parametrize("value", ["", "bad\x00name", None, True, False, 1, Decimal("1")])
def test_primary_key_identifier_component_serializer_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="Invalid Snowflake discovery identifier"):
        commands._quote_identifier_component(value)


def test_primary_key_command_is_exactly_three_quoted_components_and_has_no_extra_clause():
    values = [" DB ", "schema.with.dot", 'x"; DROP TABLE y; --']
    original = deepcopy(values)
    command = commands._show_primary_keys_command(*values)
    assert command == 'SHOW PRIMARY KEYS IN TABLE " DB "."schema.with.dot"."x""; DROP TABLE y; --"'
    assert command.startswith(commands._SHOW_PRIMARY_KEYS_PREFIX)
    assert not command.endswith(";")
    assert values == original


def test_primary_key_header_absence_is_complete_and_skips_show():
    connection = FakeConnection(_stage_3b_responses())
    metadata = FakeSnowflakeConnector(connection).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None
    assert metadata.primary_key is None
    assert metadata.coverage.primary_key is CoverageStatus.COMPLETE
    assert not any(query.startswith(commands._SHOW_PRIMARY_KEYS_PREFIX) for query, _, _ in connection.executions)


def test_show_primary_keys_maps_shuffled_case_insensitive_description_and_preserves_header():
    columns = ["KEY_SEQUENCE", "COLUMN_NAME", "CONSTRAINT_NAME", "TABLE_NAME", "SCHEMA_NAME", "DATABASE_NAME", "COMMENT"]
    rows = [
        _show_primary_key_row(column_name="Second", key_sequence="+2"),
        _show_primary_key_row(column_name="Order Id", key_sequence=Decimal("1")),
    ]
    responses = _stage_3b_responses(**{
        queries._COLUMNS_QUERY: [_column_row(), _column_row(column_name="Second", ordinal_position=2, is_identity=False)],
        queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()],
        _show_command(): _show_primary_keys_result(rows, columns=columns),
    })
    connection = FakeConnection(responses)
    metadata = FakeSnowflakeConnector(connection).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None and metadata.primary_key is not None
    assert metadata.primary_key.columns == ("Order Id", "Second")
    assert metadata.primary_key.is_enforced is False
    assert metadata.primary_key.is_rely is True
    assert metadata.primary_key.is_deferrable is False
    assert metadata.primary_key.initially_deferred is False
    assert metadata.primary_key.comment == "key comment"
    assert metadata.coverage.primary_key is CoverageStatus.COMPLETE
    show_execution = next(item for item in connection.executions if item[0].startswith(commands._SHOW_PRIMARY_KEYS_PREFIX))
    assert show_execution == (_show_command(), (), 17)
    assert connection.cursor_count == connection.closed_cursors


def test_show_primary_keys_accepts_mapping_rows_without_retaining_unapproved_fields():
    row = _show_primary_key_row()
    row["query_id"] = "secret"
    responses = _stage_3b_responses(**{
        queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()],
        _show_command(): CursorResult(list(row), [deepcopy(row)]),
    })
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None and metadata.primary_key is not None
    assert metadata.primary_key.columns == ("Order Id",)
    assert "query_id" not in metadata.primary_key.vendor_metadata


@pytest.mark.parametrize("result", [
    CursorResult(["database_name"], []),
    CursorResult(["database_name", "DATABASE_NAME", "schema_name", "table_name", "constraint_name", "column_name", "key_sequence"], []),
    CursorResult(["database_name", "schema_name", "table_name", "constraint_name", "column_name", "key_sequence"], [("App DB", "Mixed Case")]),
    CursorResult(["database_name", "schema_name", "table_name", "constraint_name", "column_name", "key_sequence"], [("App DB", "Mixed Case", "Orders", "Orders PK", None, 1)]),
])
def test_malformed_show_description_or_rows_degrade_all_membership(result):
    responses = _stage_3b_responses(**{queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()], _show_command(): result})
    connection = FakeConnection(responses)
    metadata = FakeSnowflakeConnector(connection).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None and metadata.primary_key is not None
    assert metadata.primary_key.columns == ()
    assert metadata.coverage.primary_key is CoverageStatus.PARTIAL
    assert "PRIMARY_KEY_MEMBERSHIP_PARTIAL" in metadata.coverage.warnings
    assert connection.cursor_count == connection.closed_cursors


@pytest.mark.parametrize("sequence", [
    0, -1, "0", "-1", 1.0, Decimal("1.5"), Decimal("NaN"), Decimal("Infinity"),
    True, " 1", "1 ", "1e3", "1abc", "",
])
def test_invalid_primary_key_sequences_are_rejected_without_shortening(sequence):
    rows = [_show_primary_key_row(column_name="Order Id", key_sequence=1), _show_primary_key_row(column_name="Second", key_sequence=sequence)]
    responses = _stage_3b_responses(**{
        queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()],
        _show_command(): _show_primary_keys_result(rows),
    })
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None and metadata.primary_key is not None
    assert metadata.primary_key.columns == ()
    assert metadata.coverage.primary_key is CoverageStatus.PARTIAL


@pytest.mark.parametrize("rows", [
    [_show_primary_key_row(key_sequence=2)],
    [_show_primary_key_row(), _show_primary_key_row()],
    [_show_primary_key_row(column_name="A", key_sequence=1), _show_primary_key_row(column_name="B", key_sequence=1)],
    [_show_primary_key_row(column_name="A", key_sequence=1), _show_primary_key_row(column_name="A", key_sequence=2)],
])
def test_primary_key_sequence_gaps_duplicates_and_conflicts_are_all_or_nothing(rows):
    responses = _stage_3b_responses(**{queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()], _show_command(): _show_primary_keys_result(rows)})
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None and metadata.primary_key is not None
    assert metadata.primary_key.columns == ()
    assert "PRIMARY_KEY_MEMBERSHIP_PARTIAL" in metadata.coverage.warnings


@pytest.mark.parametrize("override", [
    {"database_name": "Other DB"}, {"schema_name": "Other Schema"},
    {"table_name": "Other Table"}, {"constraint_name": "Other PK"},
])
def test_unmatched_show_membership_never_creates_a_phantom_constraint(override):
    responses = _stage_3b_responses(**{
        queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()],
        _show_command(): _show_primary_keys_result([_show_primary_key_row(**override)]),
    })
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None and metadata.primary_key is not None
    assert metadata.primary_key.name == "Orders PK"
    assert metadata.primary_key.columns == ()
    assert "PRIMARY_KEY_MEMBERSHIP_UNMATCHED" in metadata.coverage.warnings


def test_show_comment_conflict_retains_header_and_reports_fixed_temporal_warning():
    responses = _stage_3b_responses(**{
        queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()],
        _show_command(): _show_primary_keys_result([_show_primary_key_row(comment="concurrent comment")]),
    })
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None and metadata.primary_key is not None
    assert metadata.primary_key.columns == ()
    assert metadata.primary_key.comment == "key comment"
    assert "PRIMARY_KEY_MEMBERSHIP_CONFLICT" in metadata.coverage.warnings


def test_complete_columns_validate_membership_but_partial_column_evidence_does_not_reject_it():
    show = _show_primary_keys_result([_show_primary_key_row(column_name="Not Visible")])
    complete_responses = _stage_3b_responses(**{queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()], _show_command(): show})
    complete = FakeSnowflakeConnector(FakeConnection(complete_responses)).get_table_metadata(schema="Mixed Case", table="Orders")
    assert complete is not None and complete.primary_key is not None
    assert complete.primary_key.columns == ()
    assert complete.coverage.primary_key is CoverageStatus.PARTIAL

    partial_responses = _stage_3b_responses(**{
        queries._COLUMNS_QUERY: [_column_row(ordinal_position="malformed")],
        queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()], _show_command(): show,
    })
    partial = FakeSnowflakeConnector(FakeConnection(partial_responses)).get_table_metadata(schema="Mixed Case", table="Orders")
    assert partial is not None and partial.primary_key is not None
    assert partial.primary_key.columns == ("Not Visible",)
    assert partial.coverage.columns is CoverageStatus.PARTIAL
    assert partial.coverage.primary_key is CoverageStatus.COMPLETE


def test_show_permission_denial_is_partial_and_later_component_query_succeeds():
    responses = _stage_3b_responses(**{
        queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()],
        _show_command(): DriverFailure("secret identifier", sqlstate="42501"),
        queries._CHECK_CONSTRAINTS_QUERY: [_check_constraint_row()],
    })
    connection = FakeConnection(responses)
    metadata = FakeSnowflakeConnector(connection).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None
    assert metadata.coverage.primary_key is CoverageStatus.PARTIAL
    assert "PRIMARY_KEY_MEMBERSHIP_UNAVAILABLE" in metadata.coverage.warnings
    executed = [query for query, _, _ in connection.executions]
    assert executed.index(queries._CHECK_CONSTRAINTS_QUERY) > executed.index(_show_command())
    assert connection.autocommit is True
    assert connection.commit_count == connection.rollback_count == 0
    assert connection.cursor_count == connection.closed_cursors
    assert connection.closed


@pytest.mark.parametrize(("error", "expected"), [
    (DriverFailure("secret timeout", sqlstate="57014"), SchemaDiscoveryTimeoutError),
    (DriverFailure("secret connection", sqlstate="08001"), SchemaDiscoveryConnectionError),
    (DriverFailure("secret unexpected", sqlstate="XX000"), SchemaDiscoveryError),
])
def test_show_failures_are_fatal_redacted_and_close_every_resource(error, expected):
    responses = _stage_3b_responses(**{queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()], _show_command(): error})
    connection = FakeConnection(responses)
    with pytest.raises(expected) as caught:
        FakeSnowflakeConnector(connection).get_table_metadata(schema="Mixed Case", table="Orders")
    message = str(caught.value)
    assert "secret" not in message and "Orders" not in message and "SHOW" not in message
    assert connection.cursor_count == connection.closed_cursors
    assert connection.closed


@pytest.mark.parametrize("base_row", [
    _table_metadata_row(), _table_metadata_row(is_transient=True),
    _table_metadata_row(is_temporary=True), _table_metadata_row(is_hybrid=True),
    _table_metadata_row(table_type="EXTERNAL TABLE"), _table_metadata_row(is_iceberg=True),
    _table_metadata_row(is_dynamic=True),
])
def test_primary_key_membership_is_applicable_to_supported_table_classes(base_row):
    responses = _stage_3b_responses(**{
        queries._TABLE_METADATA_QUERY: [base_row], queries._KEY_CONSTRAINTS_QUERY: [_key_constraint_row()],
        _show_command(): _show_primary_keys_result([_show_primary_key_row()]),
    })
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None and metadata.primary_key is not None
    assert metadata.primary_key.columns == ("Order Id",)
    assert metadata.coverage.primary_key is CoverageStatus.COMPLETE


def test_unique_and_foreign_key_membership_remain_header_only_in_stage_3c_1():
    responses = _stage_3b_responses(**{
        queries._KEY_CONSTRAINTS_QUERY: [
            _key_constraint_row(), _key_constraint_row(constraint_name="Orders UQ", constraint_type="UNIQUE")
        ],
        queries._FOREIGN_KEYS_QUERY: [_foreign_key_row()],
        _show_command(): _show_primary_keys_result([_show_primary_key_row()]),
    })
    metadata = FakeSnowflakeConnector(FakeConnection(responses)).get_table_metadata(schema="Mixed Case", table="Orders")
    assert metadata is not None
    assert metadata.primary_key is not None and metadata.primary_key.columns == ("Order Id",)
    assert metadata.unique_constraints[0].columns == ()
    assert metadata.foreign_keys[0].local_columns == ()
    assert metadata.coverage.unique_constraints is CoverageStatus.PARTIAL
    assert metadata.coverage.foreign_keys is CoverageStatus.PARTIAL
