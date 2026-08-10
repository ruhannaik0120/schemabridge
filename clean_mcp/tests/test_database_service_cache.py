"""Caching and lifecycle tests for named SchemaBridge database services."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
import json
import logging
import os
from threading import Lock
from time import sleep

import pytest

from config import Config, ConfigError
from connectors.factory import ConnectorFactory
from services.profile_registry import ProfileRegistryError


database_service_module = import_module("services.database_service")


class FakeConnector:
    def __init__(self, *, close_error: Exception | None = None):
        self.close_calls = 0
        self.close_error = close_error

    def close(self):
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


class MessageHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def _profile_document(*, postgres_database: str = "source_db") -> str:
    return json.dumps(
        {
            "Postgres-Source": {
                "db_type": "postgresql",
                "host": "postgres.internal",
                "database": postgres_database,
                "username": "source_user",
                "password": "postgres-secret",
                "connection_options": {"application_name": "schemabridge"},
                "timeout_seconds": 11,
                "max_rows": 25,
            },
            "Snowflake-Target": {
                "db_type": "snowflake",
                "host": "acme.eu-west-1",
                "database": "TARGET_DB",
                "username": "TARGET_USER",
                "password": "snowflake-secret",
                "connection_options": {"warehouse": "INGEST_WH"},
                "timeout_seconds": 29,
                "max_rows": 75,
                "write_enabled": True,
            },
        }
    )


@pytest.fixture(autouse=True)
def isolated_database_service_cache(monkeypatch):
    monkeypatch.setattr(database_service_module, "_PROFILE_REGISTRY", None)
    monkeypatch.setattr(database_service_module, "_DATABASE_SERVICES", {})
    monkeypatch.setattr(database_service_module, "_DATABASE_SERVICE_LOCK", Lock())


def _install_factory(monkeypatch, *, close_errors=None):
    created: list[tuple[object, FakeConnector]] = []
    errors = iter(close_errors or [])

    def create_for_profile(profile):
        connector = FakeConnector(close_error=next(errors, None))
        created.append((profile, connector))
        return connector

    monkeypatch.setattr(
        ConnectorFactory, "create_for_profile", staticmethod(create_for_profile)
    )
    return created


def test_case_insensitive_profiles_are_cached_once_and_vendors_coexist(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    created = _install_factory(monkeypatch)

    postgres = database_service_module.get_database_service("postgres-source")
    same_postgres = database_service_module.get_database_service("POSTGRES-SOURCE")
    snowflake = database_service_module.get_database_service("snowflake-target")

    assert postgres is same_postgres
    assert postgres is not snowflake
    assert [profile.db_type for profile, _ in created] == ["postgresql", "snowflake"]
    assert set(database_service_module._DATABASE_SERVICES) == {
        "postgres-source",
        "snowflake-target",
    }


def test_concurrent_same_profile_requests_create_exactly_one_service(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    created: list[tuple[object, FakeConnector]] = []
    guard = Lock()

    def create_for_profile(profile):
        sleep(0.02)
        connector = FakeConnector()
        with guard:
            created.append((profile, connector))
        return connector

    monkeypatch.setattr(
        ConnectorFactory, "create_for_profile", staticmethod(create_for_profile)
    )
    with ThreadPoolExecutor(max_workers=12) as executor:
        services = list(
            executor.map(
                database_service_module.get_database_service,
                ["POSTGRES-SOURCE"] * 24,
            )
        )

    assert len(created) == 1
    assert all(service is services[0] for service in services)


def test_named_creation_does_not_use_or_mutate_global_config(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    created = _install_factory(monkeypatch)
    environment_before = dict(os.environ)
    config_before = (
        Config.DB_TYPE,
        Config.DATABASE,
        Config.GLOBAL_TIMEOUT_SECONDS,
        Config.GLOBAL_MAX_ROWS,
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("database service used global connector state")

    monkeypatch.setattr(Config, "load", classmethod(forbidden))
    monkeypatch.setattr(Config, "connection_config", classmethod(forbidden))
    monkeypatch.setattr(ConnectorFactory, "create", staticmethod(forbidden))

    postgres = database_service_module.get_database_service("postgres-source")
    snowflake = database_service_module.get_database_service("snowflake-target")

    assert postgres is not snowflake
    assert len(created) == 2
    assert dict(os.environ) == environment_before
    assert (
        Config.DB_TYPE,
        Config.DATABASE,
        Config.GLOBAL_TIMEOUT_SECONDS,
        Config.GLOBAL_MAX_ROWS,
    ) == config_before


def test_registry_snapshot_is_loaded_only_once(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    _install_factory(monkeypatch)
    calls: list[str] = []
    original = database_service_module._load_profile_registry

    def load_registry():
        calls.append("load")
        return original()

    monkeypatch.setattr(database_service_module, "_load_profile_registry", load_registry)
    database_service_module.get_database_service("postgres-source")
    database_service_module.get_database_service("snowflake-target")
    database_service_module.get_database_service("POSTGRES-SOURCE")

    assert calls == ["load"]


def test_blank_profile_document_creates_empty_registry(monkeypatch):
    monkeypatch.delenv("DB_PROFILES_JSON", raising=False)

    with pytest.raises(ProfileRegistryError):
        database_service_module.get_database_service("missing")

    assert database_service_module._PROFILE_REGISTRY is not None
    assert len(database_service_module._PROFILE_REGISTRY) == 0
    assert database_service_module._DATABASE_SERVICES == {}


def test_malformed_json_is_not_cached_or_exposed(monkeypatch):
    secret = "malformed-profile-secret"
    raw = '{"postgres-source":{"password":"' + secret + '"}'
    monkeypatch.setenv("DB_PROFILES_JSON", raw)
    handler = MessageHandler()
    database_service_module.logger.addHandler(handler)
    try:
        with pytest.raises(ProfileRegistryError) as captured:
            database_service_module.get_database_service("postgres-source")
    finally:
        database_service_module.logger.removeHandler(handler)

    rendered = str(captured.value) + json.dumps(handler.messages)
    assert database_service_module._PROFILE_REGISTRY is None
    assert database_service_module._DATABASE_SERVICES == {}
    assert secret not in rendered
    assert raw not in rendered


def test_failed_service_construction_is_not_cached_and_is_redacted(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    secret = "constructor-secret"
    handler = MessageHandler()

    def fail_construction(*_args, **_kwargs):
        raise RuntimeError(f"{secret} postgres.internal source_user")

    monkeypatch.setattr(database_service_module, "DatabaseService", fail_construction)
    database_service_module.logger.addHandler(handler)
    try:
        with pytest.raises(ConfigError) as captured:
            database_service_module.get_database_service("postgres-source")
    finally:
        database_service_module.logger.removeHandler(handler)

    rendered = str(captured.value) + json.dumps(handler.messages)
    assert str(captured.value) == (
        "Unable to create a service for the selected connection profile"
    )
    assert database_service_module._DATABASE_SERVICES == {}
    assert all(
        value not in rendered
        for value in (secret, "postgres.internal", "source_user")
    )


def test_resetting_one_profile_is_case_insensitive_and_closes_once(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    _install_factory(monkeypatch)
    postgres = database_service_module.get_database_service("postgres-source")
    snowflake = database_service_module.get_database_service("snowflake-target")

    database_service_module.reset_database_services("POSTGRES-SOURCE")
    database_service_module.reset_database_services("postgres-source")

    assert postgres.connector.close_calls == 1
    assert snowflake.connector.close_calls == 0
    assert set(database_service_module._DATABASE_SERVICES) == {"snowflake-target"}


def test_reset_all_closes_services_redacts_failures_and_reloads_profiles(monkeypatch):
    monkeypatch.setenv(
        "DB_PROFILES_JSON", _profile_document(postgres_database="old_db")
    )
    secret = "close-secret"
    _install_factory(monkeypatch, close_errors=[RuntimeError(secret), None, None])
    old = database_service_module.get_database_service("postgres-source")
    snowflake = database_service_module.get_database_service("snowflake-target")
    handler = MessageHandler()
    database_service_module.logger.addHandler(handler)
    try:
        database_service_module.reset_database_services()
    finally:
        database_service_module.logger.removeHandler(handler)

    assert old.connector.close_calls == 1
    assert snowflake.connector.close_calls == 1
    assert database_service_module._DATABASE_SERVICES == {}
    assert database_service_module._PROFILE_REGISTRY is None
    assert handler.messages == [
        "Failed to close a cached profile-bound database service"
    ]
    assert secret not in json.dumps(handler.messages)

    monkeypatch.setenv(
        "DB_PROFILES_JSON", _profile_document(postgres_database="new_db")
    )
    new = database_service_module.get_database_service("postgres-source")
    assert new is not old
    assert new.profile.database == "new_db"
