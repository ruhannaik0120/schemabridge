"""Focused tests for QueryService instances bound to immutable profiles."""

from __future__ import annotations

import json
from importlib import import_module
import logging

import pytest

from config import Config, ConfigError
from connectors.postgresql.connector import PostgreSQLConnector
from connectors.snowflake.connector import SnowflakeConnector
from connectors.factory import ConnectorFactory
from models.connection_profile import ConnectionProfile
from models.errors import ErrorCode
from services.query_service import QueryService


query_service_module = import_module("services.query_service")


class FakeConnector:
    """Record service inputs and optionally raise a poisoned driver error."""

    def __init__(self, *, label: str = "fake", failure: Exception | None = None):
        self.label = label
        self.failure = failure
        self.calls: list[tuple] = []

    def _result(self, call: tuple, payload: dict) -> dict:
        self.calls.append(call)
        if self.failure is not None:
            raise self.failure
        return payload

    def test_connection(self, database=None, timeout_seconds=None):
        return self._result(
            ("test_connection", database, timeout_seconds),
            {
                "connector_type": f"{self.label}Connector",
                "connection_status": "connected",
                "server_information": {"version": f"{self.label}-version"},
            },
        )

    def health_check(self, database=None, timeout_seconds=None):
        return self._result(
            ("health_check", database, timeout_seconds),
            {
                "connector_type": f"{self.label}Connector",
                "connection_status": "connected",
                "server_information": {"version": f"{self.label}-version"},
            },
        )

    def list_databases(self, timeout_seconds=None):
        return self._result(
            ("list_databases", timeout_seconds),
            {"count": 1, "databases": [{"name": f"{self.label}_database"}]},
        )

    def list_tables(self, database=None, schema=None, timeout_seconds=None):
        return self._result(
            ("list_tables", database, schema, timeout_seconds),
            {"count": 1, "tables": [{"TABLE_SCHEMA": schema, "TABLE_NAME": "orders"}]},
        )

    def describe_table(self, database=None, table=None, schema=None, timeout_seconds=None):
        return self._result(
            ("describe_table", database, table, schema, timeout_seconds),
            {
                "database": database,
                "schema": schema,
                "table": table,
                "column_count": 1,
                "columns": [{"COLUMN_NAME": "id", "DATA_TYPE": "integer"}],
            },
        )

    def execute_query(self, query, *, database=None, timeout_seconds=None, max_rows=None):
        return self._result(
            ("execute_query", query, database, timeout_seconds, max_rows),
            {"columns": ["id"], "rows": [(1,)], "rows_affected": 1},
        )

    def close(self):
        self.calls.append(("close",))


class _MessageHandler(logging.Handler):
    """Capture formatted project log messages without changing logger behavior."""

    def __init__(self):
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def _postgres_profile() -> ConnectionProfile:
    return ConnectionProfile(
        profile_id="postgres-source",
        db_type="postgresql",
        host="source.internal",
        database="source_db",
        username="source_user",
        password="source-password",
        connection_options={"application_name": "schemabridge", "nested": {"labels": ["source"]}},
        timeout_seconds=11,
        max_rows=25,
    )


def _snowflake_profile() -> ConnectionProfile:
    return ConnectionProfile(
        profile_id="snowflake-target",
        db_type="snowflake",
        host="acme.eu-west-1",
        database="TARGET_DB",
        username="TARGET_USER",
        password="target-password",
        connection_options={"warehouse": "INGEST_WH", "nested": {"labels": ["target"]}},
        timeout_seconds=29,
        max_rows=75,
    )


def _forbid_legacy_config(monkeypatch) -> None:
    def fail(*_args, **_kwargs):
        raise AssertionError("profile-bound service read legacy Config")

    monkeypatch.setattr(Config, "load", classmethod(fail))
    monkeypatch.setattr(Config, "connection_config", classmethod(fail))
    monkeypatch.setattr(Config, "diagnostics", classmethod(fail))
    monkeypatch.setattr(Config, "redact_text", classmethod(fail))
    monkeypatch.setattr(Config, "DB_TYPE", "poisoned")
    monkeypatch.setattr(Config, "DATABASE", "poisoned_database")
    monkeypatch.setattr(Config, "GLOBAL_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(Config, "GLOBAL_MAX_ROWS", 1)
    monkeypatch.setattr(query_service_module.os, "getenv", fail)


def test_constructor_preserves_legacy_factory_and_connector_injection(monkeypatch):
    connector = FakeConnector()
    load_calls: list[str] = []

    monkeypatch.setattr(Config, "load", classmethod(lambda cls: load_calls.append("load")))
    monkeypatch.setattr(ConnectorFactory, "create", staticmethod(lambda: connector))

    assert QueryService().connector is connector
    assert load_calls == ["load"]

    monkeypatch.setattr(Config, "load", classmethod(lambda cls: pytest.fail("unexpected Config.load")))
    injected = FakeConnector(label="injected")
    assert QueryService(injected).connector is injected


def test_profile_constructor_uses_profile_factory_without_config(monkeypatch):
    profile = _postgres_profile()
    connector = FakeConnector(label="postgres")
    selected: list[ConnectionProfile] = []
    _forbid_legacy_config(monkeypatch)
    monkeypatch.setattr(ConnectorFactory, "create", staticmethod(lambda: pytest.fail("legacy factory used")))
    monkeypatch.setattr(
        ConnectorFactory,
        "create_for_profile",
        staticmethod(lambda value: selected.append(value) or connector),
    )

    service = QueryService(profile=profile)

    assert service.connector is connector
    assert selected == [profile]


def test_profile_rejects_a_real_connector_for_another_vendor(monkeypatch):
    _forbid_legacy_config(monkeypatch)
    postgres_profile = _postgres_profile()
    connector = SnowflakeConnector(profile=_snowflake_profile())
    monkeypatch.setattr(connector, "connect", lambda *_args, **_kwargs: pytest.fail("connection attempted"))

    with pytest.raises(ConfigError, match="^Connector is not bound to the supplied connection profile$"):
        QueryService(connector, profile=postgres_profile)


def test_profile_rejects_an_unbound_real_connector(monkeypatch):
    _forbid_legacy_config(monkeypatch)
    connector = PostgreSQLConnector()
    monkeypatch.setattr(connector, "connect", lambda *_args, **_kwargs: pytest.fail("connection attempted"))

    with pytest.raises(ConfigError, match="^Connector is not bound to the supplied connection profile$"):
        QueryService(connector, profile=_postgres_profile())


def test_profile_rejects_a_same_vendor_connector_bound_to_another_profile(monkeypatch):
    _forbid_legacy_config(monkeypatch)
    other_profile = ConnectionProfile(
        profile_id="postgres-other",
        db_type="postgresql",
        host="other.internal",
        database="other_db",
        username="other_user",
        password="other-password",
    )
    connector = PostgreSQLConnector(profile=other_profile)
    monkeypatch.setattr(connector, "connect", lambda *_args, **_kwargs: pytest.fail("connection attempted"))

    with pytest.raises(ConfigError, match="^Connector is not bound to the supplied connection profile$"):
        QueryService(connector, profile=_postgres_profile())


def test_profile_accepts_a_real_connector_bound_to_that_profile(monkeypatch):
    _forbid_legacy_config(monkeypatch)
    profile = _postgres_profile()
    connector = PostgreSQLConnector(profile=profile)

    assert QueryService(connector, profile=profile).connector is connector


def test_profile_accepts_a_profileless_fake_connector(monkeypatch):
    _forbid_legacy_config(monkeypatch)
    connector = FakeConnector()

    assert QueryService(connector, profile=_postgres_profile()).connector is connector


def test_connector_profile_rejection_is_credential_safe(monkeypatch):
    _forbid_legacy_config(monkeypatch)
    postgres_profile = _postgres_profile()
    snowflake_profile = _snowflake_profile()
    connector = SnowflakeConnector(profile=snowflake_profile)
    sensitive_values = (
        postgres_profile.profile_id,
        postgres_profile.host,
        postgres_profile.database,
        postgres_profile.username,
        postgres_profile.password,
        snowflake_profile.profile_id,
        snowflake_profile.host,
        snowflake_profile.database,
        snowflake_profile.username,
        snowflake_profile.password,
        "INGEST_WH",
    )
    handler = _MessageHandler()
    query_service_module.logger.addHandler(handler)
    try:
        with pytest.raises(ConfigError) as captured:
            QueryService(connector, profile=postgres_profile)
    finally:
        query_service_module.logger.removeHandler(handler)

    error_text = str(captured.value)
    log_text = json.dumps(handler.messages)
    assert error_text == "Connector is not bound to the supplied connection profile"
    for sensitive_value in sensitive_values:
        assert sensitive_value not in error_text
        assert sensitive_value not in log_text


def test_profile_services_coexist_with_independent_operational_context(monkeypatch):
    _forbid_legacy_config(monkeypatch)
    dialects: list[str] = []

    def validate(query: str, dialect: str):
        dialects.append(dialect)
        return True, ""

    monkeypatch.setattr(query_service_module, "validate_query", validate)
    postgres_connector = FakeConnector(label="postgres")
    snowflake_connector = FakeConnector(label="snowflake")
    postgres = QueryService(postgres_connector, profile=_postgres_profile())
    snowflake = QueryService(snowflake_connector, profile=_snowflake_profile())

    postgres_connection = postgres.test_connection(timeout_seconds=99).to_dict()
    snowflake_health = snowflake.health(timeout_seconds=99).to_dict()
    postgres_databases = postgres.list_databases(timeout_seconds=99).to_dict()
    snowflake_tables = snowflake.list_tables(schema="PUBLIC", timeout_seconds=99).to_dict()
    postgres_columns = postgres.describe_table(table="orders", schema="public", timeout_seconds=99).to_dict()
    postgres_query = postgres.execute_query(sql="SELECT 1", timeout_seconds=99, max_rows=999).to_dict()
    snowflake_query = snowflake.execute_query(sql="SELECT 2", timeout_seconds=99, max_rows=999).to_dict()

    assert all(
        response["success"]
        for response in (
            postgres_connection,
            snowflake_health,
            postgres_databases,
            snowflake_tables,
            postgres_columns,
            postgres_query,
            snowflake_query,
        )
    )
    assert postgres_connection["database"] == "source_db"
    assert snowflake_health["metadata"]["profile"] == "snowflake-target"
    assert postgres_query["metadata"]["profile"] == "postgres-source"
    assert postgres_query["metadata"]["db_type"] == "postgresql"
    assert postgres_query["metadata"]["row_limit"] == 25
    assert snowflake_query["metadata"]["profile"] == "snowflake-target"
    assert snowflake_query["metadata"]["db_type"] == "snowflake"
    assert snowflake_query["metadata"]["row_limit"] == 75
    assert dialects == ["postgresql", "snowflake"]
    assert ("test_connection", "source_db", 11) in postgres_connector.calls
    assert ("list_databases", 11) in postgres_connector.calls
    assert ("describe_table", "source_db", "orders", "public", 11) in postgres_connector.calls
    assert ("execute_query", "SELECT 1", "source_db", 11, 25) in postgres_connector.calls
    assert ("health_check", "TARGET_DB", 29) in snowflake_connector.calls
    assert ("list_tables", "TARGET_DB", "PUBLIC", 29) in snowflake_connector.calls
    assert ("execute_query", "SELECT 2", "TARGET_DB", 29, 75) in snowflake_connector.calls


def test_profile_execution_rejects_a_different_database(monkeypatch):
    _forbid_legacy_config(monkeypatch)
    monkeypatch.setattr(query_service_module, "validate_query", lambda query, dialect: (True, ""))
    connector = FakeConnector()

    response = QueryService(connector, profile=_postgres_profile()).execute_query(
        sql="SELECT 1", database="another_database"
    ).to_dict()

    assert response["success"] is False
    assert response["error"]["code"] == ErrorCode.CONFIG_INVALID
    assert response["error"]["detail"] == "Database operation failed for the selected connection profile."
    assert connector.calls == []


@pytest.mark.parametrize(
    "operation",
    ["test_connection", "health", "list_tables", "describe_table", "execute_query"],
)
def test_profile_connector_errors_are_redacted_from_responses_and_logs(monkeypatch, operation):
    profile = _postgres_profile()
    secrets = [profile.password, profile.host, profile.username, "option-secret"]
    poisoned = RuntimeError("driver failed " + " ".join(secrets))
    connector = FakeConnector(failure=poisoned)
    service = QueryService(connector, profile=profile)
    _forbid_legacy_config(monkeypatch)
    monkeypatch.setattr(query_service_module, "validate_query", lambda query, dialect: (True, ""))
    handler = _MessageHandler()
    query_service_module.logger.addHandler(handler)
    try:
        if operation == "list_tables":
            response = service.list_tables(schema="public").to_dict()
        elif operation == "describe_table":
            response = service.describe_table(table="orders", schema="public").to_dict()
        elif operation == "execute_query":
            response = service.execute_query(sql="SELECT 1").to_dict()
        else:
            response = getattr(service, operation)().to_dict()
    finally:
        query_service_module.logger.removeHandler(handler)

    serialized_response = json.dumps(response, default=str)
    serialized_logs = json.dumps(handler.messages)
    assert response["success"] is False
    assert response["error"]["detail"] == "Database operation failed for the selected connection profile."
    for secret in secrets:
        assert secret not in serialized_response
        assert secret not in serialized_logs


def test_profile_health_and_diagnostics_are_safe(monkeypatch):
    profile = _snowflake_profile()
    service = QueryService(FakeConnector(label="snowflake"), profile=profile)
    _forbid_legacy_config(monkeypatch)

    health = service.health().to_dict()
    diagnostics = service.config_diagnostics().to_dict()
    combined = json.dumps({"health": health, "diagnostics": diagnostics}, default=str)

    assert health["environment_details"]["name"] == "snowflake-target"
    assert diagnostics["configuration"]["database"] == "TARGET_DB"
    assert diagnostics["configuration"]["username_present"] is True
    assert "username" not in diagnostics["configuration"]
    assert "password" not in diagnostics["configuration"]
    assert "connection_options" not in diagnostics["configuration"]
    for secret in (profile.host, profile.username, profile.password, "INGEST_WH"):
        assert secret not in combined


def test_direct_profile_operations_do_not_use_runtime_lock(monkeypatch):
    class ForbiddenLock:
        def __enter__(self):
            raise AssertionError("direct profile operation acquired runtime_lock")

        def __exit__(self, *_args):
            return False

    _forbid_legacy_config(monkeypatch)
    monkeypatch.setattr(query_service_module, "runtime_lock", ForbiddenLock())
    service = QueryService(FakeConnector(), profile=_postgres_profile())

    assert service.list_databases().success is True
