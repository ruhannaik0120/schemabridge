"""Focused tests for PostgreSQL canonical schema discovery."""

from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest

from config import Config, ConfigError
from connectors.base import DatabaseConnector
from connectors.demo.connector import DemoConnector
from connectors.discovery import (
    MalformedDiscoveryResultError,
    SchemaDiscoveryConnectionError,
    SchemaDiscoveryConnector,
    SchemaDiscoveryError,
    SchemaDiscoveryTimeoutError,
)
from connectors.mysql.connector import MySQLConnector
from connectors.postgresql import _discovery_queries as queries
from connectors.postgresql.connector import PostgreSQLConnector, UnsupportedPostgreSQLVersionError
from connectors.snowflake.connector import SnowflakeConnector
from connectors.sqlserver.connector import SQLServerConnector
from models.connection_profile import ConnectionProfile
from models.discovery import CoverageStatus, DatabaseObjectType, ObjectPersistence


def _profile(database: str = "App DB", password: str = "credential-marker") -> ConnectionProfile:
    return ConnectionProfile(
        profile_id="pg-discovery",
        db_type="postgresql",
        host="db.invalid",
        database=database,
        username="app",
        password=password,
    )


class DriverFailure(Exception):
    def __init__(self, marker: str, sqlstate: str | None = None):
        super().__init__(marker)
        self.sqlstate = sqlstate


class FakeCursor:
    def __init__(self, connection: "FakeConnection"):
        self.connection = connection
        self.description: list[Any] | None = None
        self._raw_rows: list[tuple[Any, ...]] = []
        self.closed = False

    def execute(self, query: str, parameters: tuple[Any, ...] = ()) -> None:
        self.connection.executions.append((query, parameters))
        response = self.connection.response_for(query, parameters)
        if isinstance(response, BaseException):
            raise response
        rows = list(response)
        columns = list(rows[0]) if rows else []
        self.description = [SimpleNamespace(name=name) for name in columns]
        self._raw_rows = [tuple(row.get(name) for name in columns) for row in rows]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._raw_rows)

    def close(self) -> None:
        self.closed = True
        self.connection.closed_cursors += 1


class FakeConnection:
    def __init__(self, responses: dict[str, Any] | None = None):
        self.responses = responses or {}
        self.executions: list[tuple[str, tuple[Any, ...]]] = []
        self.cursor_count = 0
        self.closed_cursors = 0
        self.rollback_count = 0
        self.commit_count = 0
        self.closed = False

    def response_for(self, query: str, parameters: tuple[Any, ...]) -> Any:
        if query == queries._CAPABILITIES_QUERY:
            default = [{
                "current_database": "App DB",
                "server_version_num": 140005,
                "max_identifier_length": 63,
                "has_partition_key_helper": True,
                "has_partition_constraint_helper": True,
            }]
        elif query == queries._IDENTIFIER_LENGTH_QUERY:
            default = [{"byte_length": len(parameters[0].encode("utf-8"))}]
        else:
            default = []
        response = self.responses.get(query, default)
        return response(parameters) if callable(response) else response

    def cursor(self) -> FakeCursor:
        self.cursor_count += 1
        return FakeCursor(self)

    def rollback(self) -> None:
        self.rollback_count += 1

    def commit(self) -> None:
        self.commit_count += 1

    def close(self) -> None:
        self.closed = True


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


class FakePostgreSQLConnector(PostgreSQLConnector):
    def __init__(
        self,
        connection: FakeConnection,
        *,
        profile: ConnectionProfile | None = None,
        connect_error: BaseException | None = None,
    ):
        super().__init__(profile=profile or _profile())
        self.fake_connection = connection
        self.fake_driver = FakeDriver(connection, connect_error)
        self.driver_loaded = False

    def _driver(self):
        self.driver_loaded = True
        return self.fake_driver


def _base_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "relation_oid": 42,
        "catalog_name": "App DB",
        "schema_name": "Mixed Case",
        "object_name": "Order Items",
        "relkind": "r",
        "relpersistence": "p",
        "owner": "owner",
        "comment": "orders",
        "estimated_row_count": 12,
        "is_partition_child": False,
        "is_system_managed": False,
        "schema_classification": "USER",
    }
    row.update(overrides)
    return row


def _metadata_responses(**overrides: Any) -> dict[str, Any]:
    responses: dict[str, Any] = {
        queries._BASE_OBJECT_QUERY: [_base_row()],
        queries._COLUMNS_QUERY: [
            {
                "column_name": "tenant_id", "ordinal_position": 1, "data_type": "integer",
                "is_nullable": False, "numeric_precision": 32, "numeric_scale": 0,
                "column_default": "nextval('seq'::regclass)", "column_comment": "tenant",
                "is_identity": True, "identity_generation": "BY DEFAULT", "is_auto_increment": True,
                "is_generated": False, "generation_expression": None, "array_dimensions": 0,
                "element_native_type": None, "generation_kind": "NONE",
            },
            {
                "column_name": "amount", "ordinal_position": 2, "data_type": "numeric",
                "is_nullable": True, "numeric_precision": 8, "numeric_scale": -2,
                "column_default": None, "column_comment": None, "collation_name": None,
                "is_identity": False, "identity_generation": None, "is_auto_increment": False,
                "is_generated": True, "generation_expression": "tenant_id * 10", "array_dimensions": 0,
                "element_native_type": None, "generation_kind": "STORED",
            },
            {
                "column_name": "tags", "ordinal_position": 3, "data_type": "text[]",
                "is_nullable": True, "array_dimensions": 2, "element_native_type": "text",
                "is_identity": False, "is_auto_increment": False, "is_generated": False,
                "generation_kind": "NONE",
            },
        ],
        queries._KEY_CONSTRAINTS_QUERY_V14: [
            {"constraint_oid": "101", "constraint_name": "same_name", "constraint_type": "p", "column_name": "tenant_id", "key_sequence": 1, "is_enforced": True, "is_validated": True, "is_deferrable": False, "initially_deferred": False},
            {"constraint_oid": "101", "constraint_name": "same_name", "constraint_type": "p", "column_name": "amount", "key_sequence": 2, "is_enforced": True, "is_validated": True, "is_deferrable": False, "initially_deferred": False},
            {"constraint_oid": "102", "constraint_name": "same_name", "constraint_type": "u", "column_name": "amount", "key_sequence": 1, "is_enforced": True, "is_validated": True, "is_deferrable": True, "initially_deferred": True},
            {"constraint_oid": "103", "constraint_name": "same_name", "constraint_type": "u", "column_name": "tags", "key_sequence": 1, "is_enforced": True, "is_validated": True, "is_deferrable": False, "initially_deferred": False},
        ],
        queries._FOREIGN_KEYS_QUERY_V14: [
            {"constraint_oid": "201", "constraint_name": "fk_parent", "local_column_name": "tenant_id", "referenced_column_name": "tenant_id", "referenced_catalog": "App DB", "referenced_schema": "Parent Schema", "referenced_table": "Parent", "key_sequence": 1, "expected_column_count": 2, "match_option": "SIMPLE", "update_rule": "CASCADE", "delete_rule": "RESTRICT", "is_enforced": True, "is_validated": True, "is_deferrable": False, "initially_deferred": False},
            {"constraint_oid": "201", "constraint_name": "fk_parent", "local_column_name": "amount", "referenced_column_name": "amount", "referenced_catalog": "App DB", "referenced_schema": "Parent Schema", "referenced_table": "Parent", "key_sequence": 2, "expected_column_count": 2, "match_option": "SIMPLE", "update_rule": "CASCADE", "delete_rule": "RESTRICT", "is_enforced": True, "is_validated": True, "is_deferrable": False, "initially_deferred": False},
        ],
        queries._CHECK_CONSTRAINTS_QUERY_V14: [
            {"constraint_oid": "301", "constraint_name": "amount_positive", "expression": "amount > 0", "constraint_definition": "CHECK ((amount > 0))", "is_enforced": True, "is_validated": True},
        ],
        queries._PARTITION_QUERY: [{
            "is_partitioned": False, "is_partition_child": False, "parent_schema": None,
            "parent_table": None, "partition_strategy": None, "partitioning_expression": None,
            "partition_bound": None, "partition_constraint": None,
        }],
    }
    responses.update(overrides)
    return responses


def test_runtime_protocol_conformance_and_abstract_contract_are_unchanged():
    connector = PostgreSQLConnector(profile=_profile())
    assert isinstance(connector, SchemaDiscoveryConnector)
    assert DatabaseConnector.__abstractmethods__ == frozenset({
        "connect", "test_connection", "health_check", "list_databases", "list_tables",
        "describe_table", "execute_query", "close",
    })
    for connector_type in (DemoConnector, MySQLConnector, SnowflakeConnector, SQLServerConnector):
        assert connector_type()


def test_profile_bound_discovery_never_reads_global_config(monkeypatch):
    monkeypatch.setattr(Config, "connection_config", classmethod(lambda cls: (_ for _ in ()).throw(AssertionError())))
    connection = FakeConnection({queries._SCHEMAS_QUERY: []})
    assert FakePostgreSQLConnector(connection, profile=_profile()).list_schemas() == ()


@pytest.mark.parametrize("explicit", [None, "App DB"])
def test_database_resolution_accepts_configured_database_exactly(explicit):
    connection = FakeConnection({queries._SCHEMAS_QUERY: []})
    connector = FakePostgreSQLConnector(connection)
    connector.list_schemas(database=explicit)
    assert [kwargs["dbname"] for kwargs in connector.fake_driver.connect_kwargs] == ["App DB"]


@pytest.mark.parametrize("explicit", ["app db", "App DB ", "Other"])
def test_database_mismatch_is_rejected_before_driver_or_connection(explicit):
    connector = FakePostgreSQLConnector(FakeConnection())
    with pytest.raises(ConfigError, match="exactly match"):
        connector.list_schemas(database=explicit)
    assert connector.fake_driver.connect_kwargs == []
    assert connector.driver_loaded is False


def test_missing_database_never_falls_back_to_postgres():
    connector = FakePostgreSQLConnector(FakeConnection(), profile=_profile(database=""))
    with pytest.raises(ConfigError, match="database is required"):
        connector.list_schemas()
    assert connector.fake_driver.connect_kwargs == []


def test_explicit_database_is_accepted_when_profile_has_no_database():
    connection = FakeConnection({
        queries._CAPABILITIES_QUERY: [{
            "current_database": "Exact DB", "server_version_num": 140000, "max_identifier_length": 63,
            "has_partition_key_helper": True, "has_partition_constraint_helper": True,
        }],
        queries._SCHEMAS_QUERY: [],
    })
    connector = FakePostgreSQLConnector(connection, profile=_profile(database=""))
    connector.list_schemas(database="Exact DB")
    assert [kwargs["dbname"] for kwargs in connector.fake_driver.connect_kwargs] == ["Exact DB"]


@pytest.mark.parametrize("database", [" Exact DB ", " "])
def test_discovery_driver_receives_explicit_database_exactly_once_without_legacy_normalization(database):
    connection = FakeConnection({
        queries._CAPABILITIES_QUERY: [{
            "current_database": database, "server_version_num": 140000, "max_identifier_length": 63,
            "has_partition_key_helper": True, "has_partition_constraint_helper": True,
        }],
        queries._SCHEMAS_QUERY: [],
    })
    connector = FakePostgreSQLConnector(connection, profile=_profile(database=""))
    connector.list_schemas(database=database)
    assert len(connector.fake_driver.connect_kwargs) == 1
    assert connector.fake_driver.connect_kwargs[0]["dbname"] == database
    assert connector.fake_driver.connect_kwargs[0]["dbname"] != "postgres"


def test_actual_current_database_mismatch_is_safe_and_closes_connection():
    connection = FakeConnection({
        queries._CAPABILITIES_QUERY: [{
            "current_database": "other", "server_version_num": 140000, "max_identifier_length": 63,
            "has_partition_key_helper": True, "has_partition_constraint_helper": True,
        }]
    })
    with pytest.raises(SchemaDiscoveryConnectionError, match="does not match"):
        FakePostgreSQLConnector(connection).list_schemas()
    assert connection.closed
    assert connection.commit_count == 0


@pytest.mark.parametrize("field,value", [
    ("schema", ""), ("schema", "bad\x00name"), ("schema", True), ("schema", 7),
    ("table", ""), ("table", "bad\x00name"), ("table", False), ("table", object()),
])
def test_structural_identifier_validation_rejects_only_invalid_shapes(field, value):
    connector = FakePostgreSQLConnector(FakeConnection())
    kwargs = {"schema": "s", "table": "t"}
    kwargs[field] = value
    with pytest.raises(ConfigError):
        connector.get_table_metadata(**kwargs)
    assert connector.fake_driver.connect_kwargs == []


def test_whitespace_and_sql_shaped_identifiers_are_preserved_and_bound():
    connection = FakeConnection({
        queries._CAPABILITIES_QUERY: [{
            "current_database": " ", "server_version_num": 140000, "max_identifier_length": 63,
            "has_partition_key_helper": True, "has_partition_constraint_helper": True,
        }],
        queries._BASE_OBJECT_QUERY: [],
    })
    connector = FakePostgreSQLConnector(connection, profile=_profile(database=""))
    assert connector.get_table_metadata(database=" ", schema=" SELECT *;-- ", table='"Odd Name"') is None
    base_execution = next(item for item in connection.executions if item[0] == queries._BASE_OBJECT_QUERY)
    assert base_execution[1] == (" ", " SELECT *;-- ", '"Odd Name"')
    assert " SELECT *;-- " not in base_execution[0]
    assert '"Odd Name"' not in base_execution[0]


def test_server_encoded_identifier_byte_limit_is_enforced():
    connection = FakeConnection({
        queries._CAPABILITIES_QUERY: [{
            "current_database": "App DB", "server_version_num": 140000, "max_identifier_length": 4,
            "has_partition_key_helper": True, "has_partition_constraint_helper": True,
        }]
    })
    with pytest.raises(ConfigError, match="server limit"):
        FakePostgreSQLConnector(connection).list_objects(schema="ééé")
    assert connection.rollback_count == 1
    assert connection.closed


def test_all_schema_classifications_are_retained_with_deterministic_exact_order():
    schema_rows = [
        {"catalog_name": "App DB", "schema_name": name, "owner": "owner", "comment": None,
         "schema_classification": classification, "is_system_managed": managed}
        for name, classification, managed in [
            ("user", "USER", False), ("pg_toast_temp_4", "POSTGRESQL_TOAST_TEMPORARY", True),
            ("pg_temp_4", "POSTGRESQL_TEMPORARY", True), ("pg_toast_9", "POSTGRESQL_TOAST", True),
            ("pg_toast", "POSTGRESQL_TOAST", True), ("information_schema", "INFORMATION_SCHEMA", True),
            ("pg_catalog", "POSTGRESQL_CATALOG", True),
        ]
    ]
    connection = FakeConnection({queries._SCHEMAS_QUERY: schema_rows})
    result = FakePostgreSQLConnector(connection).list_schemas()
    assert [item.schema_name for item in result] == sorted(row["schema_name"] for row in schema_rows)
    assert sum(item.is_system_managed is True for item in result) == 6
    assert {item.vendor_metadata["classification"] for item in result} == {
        "USER", "POSTGRESQL_TOAST_TEMPORARY", "POSTGRESQL_TEMPORARY", "POSTGRESQL_TOAST",
        "INFORMATION_SCHEMA", "POSTGRESQL_CATALOG",
    }


def test_object_mappings_persistence_partition_child_and_ordering():
    rows = []
    for name, relkind, persistence in [
        ("z", "r", "p"), ("a", "p", "p"), ("view", "v", "t"),
        ("mat", "m", "u"), ("foreign", "f", "p"),
    ]:
        rows.append({
            "catalog_name": "App DB", "schema_name": "public", "object_name": name,
            "relkind": relkind, "relpersistence": persistence, "owner": "owner", "comment": None,
            "estimated_row_count": None, "is_partition_child": name == "z", "is_system_managed": False,
            "schema_classification": "USER",
        })
    connection = FakeConnection({queries._OBJECTS_QUERY: rows})
    result = FakePostgreSQLConnector(connection).list_objects(schema="public")
    assert {item.object_type for item in result} == {
        DatabaseObjectType.TABLE, DatabaseObjectType.PARTITIONED_TABLE, DatabaseObjectType.VIEW,
        DatabaseObjectType.MATERIALIZED_VIEW, DatabaseObjectType.FOREIGN_TABLE,
    }
    assert {item.persistence for item in result} == {
        ObjectPersistence.PERMANENT, ObjectPersistence.TEMPORARY, ObjectPersistence.UNLOGGED,
    }
    assert [(item.object_type.value, item.object_name) for item in result] == sorted(
        (item.object_type.value, item.object_name) for item in result
    )
    assert next(item for item in result if item.object_name == "z").vendor_metadata["is_partition_child"] is True
    execution = next(item for item in connection.executions if item[0] == queries._OBJECTS_QUERY)
    assert execution[1][0:2] == ("App DB", "public")
    assert set(execution[1][2]) == {"r", "p", "v", "m", "f"}


def test_empty_object_types_short_circuits_without_driver_connection_or_query():
    connector = FakePostgreSQLConnector(FakeConnection())
    assert connector.list_objects(schema="public", object_types=()) == ()
    assert connector.fake_driver.connect_kwargs == []
    assert connector.driver_loaded is False


@pytest.mark.parametrize("object_types", [
    (DatabaseObjectType.EXTERNAL_TABLE,), (DatabaseObjectType.DYNAMIC_TABLE,),
    (DatabaseObjectType.UNKNOWN,), ("TABLE",), [DatabaseObjectType.TABLE],
])
def test_unsupported_object_types_are_rejected_safely_before_connection(object_types):
    connector = FakePostgreSQLConnector(FakeConnection())
    with pytest.raises(ConfigError, match="object_types"):
        connector.list_objects(schema="public", object_types=object_types)
    assert connector.fake_driver.connect_kwargs == []


def test_complete_table_metadata_constraints_columns_and_coverage():
    connection = FakeConnection(_metadata_responses())
    table = FakePostgreSQLConnector(connection).get_table_metadata(
        schema="Mixed Case", table="Order Items"
    )
    assert table is not None
    assert [column.column_name for column in table.columns] == ["tenant_id", "amount", "tags"]
    assert table.columns[1].numeric_scale == -2
    assert table.columns[1].generation_expression == "tenant_id * 10"
    assert table.columns[2].array_dimensions == 2
    assert table.columns[2].element_native_type == "text"
    assert table.columns[0].is_identity is True
    assert table.columns[0].is_auto_increment is True
    assert table.primary_key is not None and table.primary_key.columns == ("tenant_id", "amount")
    assert len(table.unique_constraints) == 2
    assert {item.columns for item in table.unique_constraints} == {("amount",), ("tags",)}
    assert table.foreign_keys[0].local_columns == ("tenant_id", "amount")
    assert table.foreign_keys[0].referenced_columns == ("tenant_id", "amount")
    assert table.foreign_keys[0].update_rule == "CASCADE"
    assert table.check_constraints[0].expression == "amount > 0"
    assert table.coverage == table.coverage.__class__(
        columns=CoverageStatus.COMPLETE, primary_key=CoverageStatus.COMPLETE,
        unique_constraints=CoverageStatus.COMPLETE, foreign_keys=CoverageStatus.COMPLETE,
        check_constraints=CoverageStatus.COMPLETE, comments=CoverageStatus.COMPLETE,
        estimated_row_count=CoverageStatus.COMPLETE, view_definition=CoverageStatus.NOT_APPLICABLE,
        partitioning=CoverageStatus.COMPLETE, clustering=CoverageStatus.NOT_APPLICABLE, warnings=(),
    )
    assert [(column.is_primary_key, column.is_unique_key, column.is_foreign_key) for column in table.columns] == [
        (True, False, True), (True, True, True), (False, True, False),
    ]
    serialized = json.dumps(table.to_dict())
    assert "relation_oid" not in serialized
    assert "constraint_oid" not in serialized
    assert "credential-marker" not in serialized
    assert connection.commit_count == 0
    assert connection.rollback_count == 1
    assert connection.closed
    assert connection.cursor_count == connection.closed_cursors
    assert len({id(connection) for _ in connection.executions}) == 1


def test_incomplete_foreign_key_membership_is_partial_and_unknown_nonmembers_stay_none():
    responses = _metadata_responses()
    responses[queries._FOREIGN_KEYS_QUERY_V14] = [
        {"constraint_oid": "201", "constraint_name": "fk_parent", "local_column_name": "tenant_id",
         "referenced_column_name": "tenant_id", "referenced_catalog": "App DB",
         "referenced_schema": "public", "referenced_table": "parent", "key_sequence": 1,
         "expected_column_count": 2, "is_enforced": True, "is_validated": True},
    ]
    table = FakePostgreSQLConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case", table="Order Items"
    )
    assert table is not None
    assert table.coverage.foreign_keys is CoverageStatus.PARTIAL
    assert "FOREIGN_KEYS_PARTIAL" in table.coverage.warnings
    assert table.columns[0].is_foreign_key is True
    assert table.columns[1].is_foreign_key is None


def test_unpaired_referenced_fk_member_is_omitted_without_losing_known_local_participation():
    responses = _metadata_responses()
    raw_rows = [
        {"constraint_oid": "201", "constraint_name": "fk_parent", "local_column_name": "tenant_id",
         "referenced_column_name": "tenant_id", "referenced_catalog": "App DB",
         "referenced_schema": "public", "referenced_table": "parent", "key_sequence": 1,
         "expected_column_count": 2, "is_enforced": True, "is_validated": True},
        {"constraint_oid": "201", "constraint_name": "fk_parent", "local_column_name": "amount",
         "referenced_column_name": None, "referenced_catalog": "App DB",
         "referenced_schema": "public", "referenced_table": "parent", "key_sequence": 2,
         "expected_column_count": 2, "is_enforced": True, "is_validated": True},
    ]
    before = deepcopy(raw_rows)
    responses[queries._FOREIGN_KEYS_QUERY_V14] = raw_rows
    table = FakePostgreSQLConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case", table="Order Items"
    )
    assert table is not None
    assert table.coverage.foreign_keys is CoverageStatus.PARTIAL
    assert table.foreign_keys[0].local_columns == ("tenant_id",)
    assert table.foreign_keys[0].referenced_columns == ("tenant_id",)
    assert table.columns[0].is_foreign_key is True
    assert table.columns[1].is_foreign_key is True
    assert table.columns[2].is_foreign_key is None
    assert raw_rows == before


@pytest.mark.parametrize(
    "raw_row",
    [
        {"local_column_name": "tenant_id", "referenced_column_name": None},
        {"local_column_name": None, "referenced_column_name": "tenant_id"},
    ],
)
def test_fk_with_zero_valid_pairs_is_partial_and_does_not_abort_or_invent_membership(raw_row):
    responses = _metadata_responses()
    responses[queries._FOREIGN_KEYS_QUERY_V14] = [{
        "constraint_oid": "201", "constraint_name": "fk_parent",
        "referenced_catalog": "App DB", "referenced_schema": "public", "referenced_table": "parent",
        "key_sequence": 1, "expected_column_count": 1, "is_enforced": True, "is_validated": True,
        **raw_row,
    }]
    table = FakePostgreSQLConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case", table="Order Items"
    )
    assert table is not None
    assert table.coverage.foreign_keys is CoverageStatus.PARTIAL
    assert table.foreign_keys == ()
    if raw_row["local_column_name"] is not None:
        assert table.columns[0].is_foreign_key is True
    assert all(column.is_foreign_key is not False for column in table.columns)


def test_conflicting_duplicate_fk_sequences_are_partial_deterministic_and_detached():
    raw_rows = (
        {"constraint_oid": "201", "constraint_name": "fk_parent", "local_column_name": "tenant_id",
         "referenced_column_name": "z_id", "referenced_table": "parent", "key_sequence": 1,
         "expected_column_count": 2},
        {"constraint_oid": "201", "constraint_name": "fk_parent", "local_column_name": "tenant_id",
         "referenced_column_name": "a_id", "referenced_table": "parent", "key_sequence": 1,
         "expected_column_count": 2},
        {"constraint_oid": "201", "constraint_name": "fk_parent", "local_column_name": "amount",
         "referenced_column_name": "amount", "referenced_table": "parent", "key_sequence": 2,
         "expected_column_count": 2},
    )
    before = deepcopy(raw_rows)
    forward = FakePostgreSQLConnector._foreign_key_rows_for_normalization(raw_rows)
    reverse = FakePostgreSQLConnector._foreign_key_rows_for_normalization(tuple(reversed(raw_rows)))
    assert FakePostgreSQLConnector._foreign_key_coverage(raw_rows) is CoverageStatus.PARTIAL
    assert forward == reverse
    assert forward is not None
    assert [row["referenced_column_name"] for row in forward] == ["a_id", "amount"]
    assert all(row is not source for row in forward for source in raw_rows)
    assert raw_rows == before


@pytest.mark.parametrize("relkind", ["v", "m"])
def test_view_definition_is_preserved_and_partitioning_is_not_applicable(relkind):
    responses = _metadata_responses()
    responses[queries._BASE_OBJECT_QUERY] = [
        _base_row(relkind=relkind, object_name="Order View", estimated_row_count=None)
    ]
    responses[queries._VIEW_DEFINITION_QUERY] = [{"view_definition": " SELECT tenant_id FROM source;"}]
    connection = FakeConnection(responses)
    table = FakePostgreSQLConnector(connection).get_table_metadata(
        schema="Mixed Case", table="Order Items"
    )
    assert table is not None
    assert table.view_definition == " SELECT tenant_id FROM source;"
    assert table.coverage.view_definition is CoverageStatus.COMPLETE
    assert table.coverage.estimated_row_count is CoverageStatus.NOT_APPLICABLE
    assert table.coverage.partitioning is CoverageStatus.NOT_APPLICABLE
    assert table.is_partitioned is None
    assert table.partitioning_expression is None
    assert all(not warning.startswith("PARTITION") for warning in table.coverage.warnings)
    assert set(table.vendor_metadata) == {"classification"}
    executed = [query for query, _ in connection.executions]
    assert queries._VIEW_DEFINITION_QUERY in executed
    assert queries._PARTITION_QUERY not in executed
    assert queries._PARTITION_QUERY_WITHOUT_HELPERS not in executed
    assert connection.cursor_count == connection.closed_cursors
    assert connection.rollback_count == 1
    assert connection.commit_count == 0
    assert connection.closed


@pytest.mark.parametrize(
    ("base_overrides", "partition_row", "expected_partitioned", "expected_parent"),
    [
        (
            {"relkind": "p", "is_partition_child": False},
            {"is_partitioned": True, "is_partition_child": False, "parent_schema": None,
             "parent_table": None, "partition_strategy": "RANGE", "partitioning_expression": "RANGE (tenant_id)",
             "partition_bound": None, "partition_constraint": None},
            True,
            None,
        ),
        (
            {"relkind": "r", "is_partition_child": True},
            {"is_partitioned": False, "is_partition_child": True, "parent_schema": "Parent Schema",
             "parent_table": "Parent", "partition_strategy": "RANGE", "partitioning_expression": "RANGE (tenant_id)",
             "partition_bound": "FOR VALUES FROM (1) TO (10)", "partition_constraint": "tenant_id >= 1"},
            False,
            "Parent",
        ),
        (
            {"relkind": "f", "is_partition_child": True},
            {"is_partitioned": False, "is_partition_child": True, "parent_schema": "Parent Schema",
             "parent_table": "Parent", "partition_strategy": "RANGE", "partitioning_expression": "RANGE (tenant_id)",
             "partition_bound": "FOR VALUES FROM (1) TO (10)", "partition_constraint": "tenant_id >= 1"},
            False,
            "Parent",
        ),
    ],
)
def test_partitioned_parents_and_children_preserve_safe_partition_facts(
    base_overrides, partition_row, expected_partitioned, expected_parent
):
    responses = _metadata_responses()
    responses[queries._BASE_OBJECT_QUERY] = [_base_row(**base_overrides)]
    responses[queries._PARTITION_QUERY] = [partition_row]
    table = FakePostgreSQLConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case", table="Order Items"
    )
    assert table is not None
    assert table.is_partitioned is expected_partitioned
    assert table.partitioning_expression == "RANGE (tenant_id)"
    assert table.vendor_metadata["parent_table"] == expected_parent
    assert table.vendor_metadata["is_partition_child"] is base_overrides["is_partition_child"]
    assert table.coverage.partitioning is CoverageStatus.COMPLETE


def test_version_18_variants_and_partition_helper_fallback_are_selected_from_capabilities():
    responses = _metadata_responses()
    responses[queries._CAPABILITIES_QUERY] = [{
        "current_database": "App DB", "server_version_num": 180000, "max_identifier_length": 63,
        "has_partition_key_helper": False, "has_partition_constraint_helper": False,
    }]
    responses[queries._KEY_CONSTRAINTS_QUERY_V18] = responses.pop(queries._KEY_CONSTRAINTS_QUERY_V14)
    responses[queries._FOREIGN_KEYS_QUERY_V18] = responses.pop(queries._FOREIGN_KEYS_QUERY_V14)
    responses[queries._CHECK_CONSTRAINTS_QUERY_V18] = responses.pop(queries._CHECK_CONSTRAINTS_QUERY_V14)
    responses[queries._PARTITION_QUERY_WITHOUT_HELPERS] = responses.pop(queries._PARTITION_QUERY)
    connection = FakeConnection(responses)
    table = FakePostgreSQLConnector(connection).get_table_metadata(
        schema="Mixed Case", table="Order Items"
    )
    assert table is not None
    executed = {query for query, _ in connection.executions}
    assert queries._KEY_CONSTRAINTS_QUERY_V18 in executed
    assert queries._FOREIGN_KEYS_QUERY_V18 in executed
    assert queries._CHECK_CONSTRAINTS_QUERY_V18 in executed
    assert queries._PARTITION_QUERY_WITHOUT_HELPERS in executed
    assert table.coverage.partitioning is CoverageStatus.COMPLETE
    assert table.is_partitioned is False
    assert table.partitioning_expression is None
    assert "PARTITION_HELPERS_UNAVAILABLE" not in table.coverage.warnings


@pytest.mark.parametrize(
    ("relkind", "partition_row"),
    [
        (
            "p",
            {"is_partitioned": True, "is_partition_child": False, "parent_schema": None,
             "parent_table": None, "partition_strategy": "RANGE", "partitioning_expression": None,
             "partition_bound": None, "partition_constraint": None},
        ),
        (
            "r",
            {"is_partitioned": False, "is_partition_child": True, "parent_schema": "parent_schema",
             "parent_table": "parent", "partition_strategy": "RANGE", "partitioning_expression": None,
             "partition_bound": "FOR VALUES FROM (1) TO (10)", "partition_constraint": None},
        ),
        (
            "f",
            {"is_partitioned": False, "is_partition_child": True, "parent_schema": "parent_schema",
             "parent_table": "parent", "partition_strategy": "RANGE", "partitioning_expression": None,
             "partition_bound": "FOR VALUES FROM (1) TO (10)", "partition_constraint": None},
        ),
    ],
)
def test_partition_parent_and_children_without_helpers_are_partial_with_known_facts(
    relkind, partition_row
):
    responses = _metadata_responses()
    responses[queries._CAPABILITIES_QUERY] = [{
        "current_database": "App DB", "server_version_num": 140000, "max_identifier_length": 63,
        "has_partition_key_helper": False, "has_partition_constraint_helper": False,
    }]
    responses[queries._BASE_OBJECT_QUERY] = [
        _base_row(relkind=relkind, is_partition_child=partition_row["is_partition_child"])
    ]
    responses[queries._PARTITION_QUERY_WITHOUT_HELPERS] = [partition_row]
    connection = FakeConnection(responses)
    table = FakePostgreSQLConnector(connection).get_table_metadata(
        schema="Mixed Case", table="Order Items"
    )
    assert table is not None
    assert table.coverage.partitioning is CoverageStatus.PARTIAL
    assert table.is_partitioned is True
    assert table.partitioning_expression is None
    assert table.vendor_metadata["partition_strategy"] == "RANGE"
    assert table.vendor_metadata["parent_table"] == partition_row["parent_table"]
    assert table.vendor_metadata["partition_bound"] == partition_row["partition_bound"]
    assert table.coverage.warnings.count("PARTITION_HELPERS_UNAVAILABLE") == 1
    assert queries._PARTITION_QUERY_WITHOUT_HELPERS in {query for query, _ in connection.executions}


def test_ordinary_inheritance_is_not_reported_as_declarative_partitioning():
    responses = _metadata_responses()
    responses[queries._PARTITION_QUERY] = [{
        "is_partitioned": False, "is_partition_child": False, "parent_schema": None, "parent_table": None,
        "partition_strategy": None, "partitioning_expression": None, "partition_bound": None,
        "partition_constraint": None,
    }]
    table = FakePostgreSQLConnector(FakeConnection(responses)).get_table_metadata(
        schema="Mixed Case", table="Order Items"
    )
    assert table is not None
    assert table.is_partitioned is False
    assert table.vendor_metadata["is_partition_child"] is False
    assert table.vendor_metadata["parent_table"] is None


def test_optional_permission_failure_rolls_back_continues_and_marks_unavailable():
    responses = _metadata_responses()
    responses[queries._COLUMNS_QUERY] = DriverFailure("private-sql", "42501")
    connection = FakeConnection(responses)
    table = FakePostgreSQLConnector(connection).get_table_metadata(
        schema="Mixed Case", table="Order Items"
    )
    assert table is not None
    assert table.columns == ()
    assert table.coverage.columns is CoverageStatus.UNAVAILABLE
    assert table.coverage.warnings == ("COLUMNS_UNAVAILABLE",)
    assert any(query == queries._KEY_CONSTRAINTS_QUERY_V14 for query, _ in connection.executions)
    assert connection.rollback_count == 2
    assert connection.closed_cursors == connection.cursor_count


@pytest.mark.parametrize("sqlstate,exception_type", [
    ("57014", SchemaDiscoveryTimeoutError), ("08006", SchemaDiscoveryConnectionError),
    ("XX000", SchemaDiscoveryError),
])
def test_query_errors_are_safely_translated_and_cleanup_is_complete(
    sqlstate, exception_type, caplog
):
    marker = "raw-driver-credential-marker"
    connection = FakeConnection({queries._SCHEMAS_QUERY: DriverFailure(marker, sqlstate)})
    with pytest.raises(exception_type) as captured:
        FakePostgreSQLConnector(connection).list_schemas()
    assert marker not in str(captured.value)
    assert marker not in repr(captured.value)
    assert marker not in caplog.text
    assert connection.commit_count == 0
    assert connection.rollback_count == 1
    assert connection.closed
    assert connection.closed_cursors == connection.cursor_count


def test_connect_error_is_safely_translated_without_loading_global_state():
    marker = "password=top-secret"
    connector = FakePostgreSQLConnector(
        FakeConnection(), connect_error=DriverFailure(marker)
    )
    with pytest.raises(SchemaDiscoveryConnectionError) as captured:
        connector.list_schemas()
    assert marker not in str(captured.value)


def test_malformed_capabilities_and_old_versions_fail_safely():
    malformed = FakeConnection({queries._CAPABILITIES_QUERY: []})
    with pytest.raises(MalformedDiscoveryResultError):
        FakePostgreSQLConnector(malformed).list_schemas()
    old = FakeConnection({queries._CAPABILITIES_QUERY: [{
        "current_database": "App DB", "server_version_num": 130099, "max_identifier_length": 63,
        "has_partition_key_helper": True, "has_partition_constraint_helper": True,
    }]})
    with pytest.raises(UnsupportedPostgreSQLVersionError, match="14 or newer"):
        FakePostgreSQLConnector(old).list_schemas()


def test_missing_base_object_is_none_but_query_failure_is_not_none():
    missing = FakeConnection({queries._BASE_OBJECT_QUERY: []})
    assert FakePostgreSQLConnector(missing).get_table_metadata(schema="s", table="t") is None
    failed = FakeConnection({queries._BASE_OBJECT_QUERY: DriverFailure("raw", "XX000")})
    with pytest.raises(SchemaDiscoveryError):
        FakePostgreSQLConnector(failed).get_table_metadata(schema="s", table="t")


def test_every_catalog_query_is_one_fixed_read_only_select():
    query_constants = {
        name: value
        for name, value in vars(queries).items()
        if name.startswith("_") and "QUERY" in name and isinstance(value, str)
    }
    assert query_constants
    forbidden_statements = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|COPY|CALL|DO|SET|SHOW|BEGIN|COMMIT|ROLLBACK)\b", re.I)
    for name, query in query_constants.items():
        without_literals = re.sub(r"'[^']*'", "''", query)
        assert query.strip().upper().startswith("SELECT"), name
        assert len(re.findall(r"\bSELECT\b", without_literals, flags=re.I)) == 1, name
        assert ";" not in query, name
        assert forbidden_statements.search(without_literals) is None, name
        assert "{" not in query and "}" not in query, name
    assert "c.relkind::text = ANY(%s::text[])" in queries._OBJECTS_QUERY
    assert '"char"[]' not in queries._OBJECTS_QUERY


def test_discovery_does_not_mutate_environment_config_or_profile(monkeypatch):
    monkeypatch.setenv("DB_DATABASE", "environment-marker")
    original_config_database = Config.DATABASE
    profile = _profile()
    before = profile.to_safe_dict()
    connection = FakeConnection({queries._SCHEMAS_QUERY: []})
    FakePostgreSQLConnector(connection, profile=profile).list_schemas()
    assert os.environ["DB_DATABASE"] == "environment-marker"
    assert Config.DATABASE == original_config_database
    assert profile.to_safe_dict() == before


def test_postgresql_discovery_integration_when_explicitly_configured():
    """Opt-in harness suitable for one target in a future PostgreSQL 14-18 matrix."""
    if os.getenv("SCHEMABRIDGE_POSTGRES_INTEGRATION") != "1":
        pytest.skip("PostgreSQL discovery integration environment is not enabled.")
    host = os.getenv("SCHEMABRIDGE_POSTGRES_HOST")
    database = os.getenv("SCHEMABRIDGE_POSTGRES_DATABASE")
    if not host or not database:
        pytest.skip("PostgreSQL discovery integration host/database are not configured.")
    port = int(os.getenv("SCHEMABRIDGE_POSTGRES_PORT", "5432"))
    profile = ConnectionProfile(
        profile_id="pg-integration",
        db_type="postgresql",
        host=host,
        database=database,
        username=os.getenv("SCHEMABRIDGE_POSTGRES_USERNAME", ""),
        password=os.getenv("SCHEMABRIDGE_POSTGRES_PASSWORD", ""),
        connection_options={"port": port},
    )
    connector = PostgreSQLConnector(profile=profile)
    schemas = connector.list_schemas()
    assert isinstance(schemas, tuple)
    assert all(schema.catalog_name == database for schema in schemas)
