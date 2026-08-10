"""Focused tests for SchemaBridge's profile-bound database service."""

from __future__ import annotations

import json
from importlib import import_module
import logging

import pytest

from schemabridge.config import Config, ConfigError
from schemabridge.connectors.factory import ConnectorFactory
from schemabridge.connectors.postgresql.connector import PostgreSQLConnector
from schemabridge.connectors.snowflake.connector import SnowflakeConnector
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.services.database_service import DatabaseAccessError, DatabaseService


database_service_module = import_module("schemabridge.services.database_service")


class FakeConnector:
    def __init__(self, *, failure: Exception | None = None):
        self.failure = failure
        self.calls: list[tuple] = []

    def execute_query(
        self,
        query,
        *,
        parameters=None,
        database=None,
        timeout_seconds=None,
        max_rows=None,
    ):
        self.calls.append(
            ("execute_query", query, parameters, database, timeout_seconds, max_rows)
        )
        if self.failure is not None:
            raise self.failure
        return {"columns": ["value"], "rows": [(1,)], "rows_affected": 1}

    def get_table_metadata(
        self,
        *,
        database,
        schema,
        table,
        timeout_seconds=None,
    ):
        self.calls.append(
            ("get_table_metadata", database, schema, table, timeout_seconds)
        )
        if self.failure is not None:
            raise self.failure
        return {"table": table}

    def close(self):
        self.calls.append(("close",))


class MessageHandler(logging.Handler):
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
        connection_options={"application_name": "schemabridge"},
        timeout_seconds=11,
        max_rows=25,
    )


def _snowflake_profile(*, write_enabled: bool = True) -> ConnectionProfile:
    return ConnectionProfile(
        profile_id="snowflake-target",
        db_type="snowflake",
        host="acme.eu-west-1",
        database="TARGET_DB",
        username="TARGET_USER",
        password="target-password",
        connection_options={"warehouse": "INGEST_WH"},
        timeout_seconds=29,
        max_rows=75,
        write_enabled=write_enabled,
    )


def _forbid_global_config(monkeypatch) -> None:
    def fail(*_args, **_kwargs):
        raise AssertionError("profile-bound database access read global Config")

    monkeypatch.setattr(Config, "load", classmethod(fail))
    monkeypatch.setattr(Config, "connection_config", classmethod(fail))
    monkeypatch.setattr(Config, "diagnostics", classmethod(fail))
    monkeypatch.setattr(ConnectorFactory, "create", staticmethod(fail))


def test_profile_constructor_uses_profile_factory_without_global_config(monkeypatch):
    profile = _postgres_profile()
    connector = FakeConnector()
    selected: list[ConnectionProfile] = []
    _forbid_global_config(monkeypatch)
    monkeypatch.setattr(
        ConnectorFactory,
        "create_for_profile",
        staticmethod(lambda value: selected.append(value) or connector),
    )

    service = DatabaseService(profile)

    assert service.connector is connector
    assert selected == [profile]


@pytest.mark.parametrize("kind", ["other_vendor", "unbound", "other_profile"])
def test_real_connector_must_be_bound_to_exact_profile(monkeypatch, kind):
    profile = _postgres_profile()
    if kind == "other_vendor":
        connector = SnowflakeConnector(profile=_snowflake_profile())
    elif kind == "unbound":
        connector = PostgreSQLConnector()
    else:
        connector = PostgreSQLConnector(
            profile=ConnectionProfile(
                profile_id="postgres-other",
                db_type="postgresql",
                host="other.internal",
                database="other_db",
                username="other_user",
                password="other-password",
            )
        )
    monkeypatch.setattr(
        connector,
        "connect",
        lambda *_args, **_kwargs: pytest.fail("connection attempted"),
    )

    with pytest.raises(
        ConfigError,
        match="^Connector is not bound to the supplied connection profile$",
    ):
        DatabaseService(profile, connector)


def test_bound_real_and_profileless_fake_connectors_are_accepted():
    profile = _postgres_profile()
    real = PostgreSQLConnector(profile=profile)
    fake = FakeConnector()

    assert DatabaseService(profile, real).connector is real
    assert DatabaseService(profile, fake).connector is fake


def test_contexts_are_credential_free_and_clamp_timeout():
    profile = _snowflake_profile()
    service = DatabaseService(profile, FakeConnector())

    migration = service.migration_execution_context(99)
    validation = service.validation_execution_context(99)
    rendered = json.dumps({"migration": migration, "validation": validation})

    assert migration == {
        "profile_id": "snowflake-target",
        "db_type": "snowflake",
        "database": "TARGET_DB",
        "timeout_seconds": 29,
        "write_enabled": True,
        "connector_type": "snowflake",
    }
    assert validation == {
        "profile_id": "snowflake-target",
        "db_type": "snowflake",
        "timeout_seconds": 29,
    }
    assert all(
        secret not in rendered
        for secret in (profile.host, profile.username, profile.password, "INGEST_WH")
    )


def test_validation_execution_is_profile_bound_and_limited(monkeypatch):
    connector = FakeConnector()
    dialects: list[str] = []
    monkeypatch.setattr(
        database_service_module,
        "validate_query",
        lambda statement, dialect: dialects.append(dialect) or (True, ""),
    )
    service = DatabaseService(_postgres_profile(), connector)

    result = service.execute_validation_query(
        sql="SELECT %s AS value",
        parameters=("safe",),
        timeout_seconds=99,
    )

    assert result.columns == ("value",)
    assert result.rows == ((1,),)
    assert result.rows_affected == 1
    assert dialects == ["postgresql"]
    assert connector.calls == [
        ("execute_query", "SELECT %s AS value", ("safe",), "source_db", 11, 25)
    ]


def test_migration_execution_requires_write_enabled_and_uses_one_row_cap(monkeypatch):
    monkeypatch.setattr(
        database_service_module, "validate_query", lambda *_args: (True, "")
    )
    read_only = DatabaseService(_snowflake_profile(write_enabled=False), FakeConnector())
    with pytest.raises(DatabaseAccessError, match="not write enabled"):
        read_only.execute_migration_statement(sql="INSERT INTO T SELECT 1")

    connector = FakeConnector()
    writable = DatabaseService(_snowflake_profile(), connector)
    result = writable.execute_migration_statement(
        sql="INSERT INTO T SELECT %s",
        parameters=(1,),
        database="TARGET_DB",
        timeout_seconds=99,
    )

    assert result.rows_affected == 1
    assert connector.calls == [
        ("execute_query", "INSERT INTO T SELECT %s", (1,), "TARGET_DB", 29, 1)
    ]


def test_execution_rejects_other_database_and_unsafe_sql(monkeypatch):
    connector = FakeConnector()
    service = DatabaseService(_snowflake_profile(), connector)
    monkeypatch.setattr(
        database_service_module, "validate_query", lambda *_args: (True, "")
    )
    with pytest.raises(DatabaseAccessError, match="configured"):
        service.execute_migration_statement(
            sql="INSERT INTO T SELECT 1",
            database="OTHER_DB",
        )
    monkeypatch.setattr(
        database_service_module, "validate_query", lambda *_args: (False, "blocked")
    )
    with pytest.raises(DatabaseAccessError, match="safety policy"):
        service.execute_validation_query(sql="SELECT 1")
    assert connector.calls == []


def test_connector_failure_is_credential_safe(monkeypatch):
    profile = _postgres_profile()
    secrets = (profile.host, profile.username, profile.password, "driver-secret")
    connector = FakeConnector(failure=RuntimeError(" ".join(secrets)))
    service = DatabaseService(profile, connector)
    monkeypatch.setattr(
        database_service_module, "validate_query", lambda *_args: (True, "")
    )
    handler = MessageHandler()
    database_service_module.logger.addHandler(handler)
    try:
        with pytest.raises(DatabaseAccessError) as captured:
            service.execute_validation_query(sql="SELECT 1")
    finally:
        database_service_module.logger.removeHandler(handler)

    rendered = str(captured.value) + json.dumps(handler.messages)
    assert str(captured.value) == (
        "Database operation failed for the selected connection profile."
    )
    assert all(secret not in rendered for secret in secrets)


def test_discovery_delegates_with_profile_timeout_and_redacts_failure():
    connector = FakeConnector()
    service = DatabaseService(_postgres_profile(), connector)
    assert service.get_table_metadata(
        database="source_db",
        schema="public",
        table="orders",
    ) == {"table": "orders"}
    assert connector.calls == [
        ("get_table_metadata", "source_db", "public", "orders", 11)
    ]

    poisoned = DatabaseService(
        _postgres_profile(),
        FakeConnector(failure=RuntimeError("source-password source.internal")),
    )
    with pytest.raises(DatabaseAccessError) as captured:
        poisoned.get_table_metadata(
            database="source_db",
            schema="public",
            table="orders",
        )
    assert "source-password" not in str(captured.value)
    assert "source.internal" not in str(captured.value)
