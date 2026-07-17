"""Focused tests for named, profile-bound QueryService caching."""

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
from services import profile_service
from services.profile_registry import ProfileRegistryError


query_service_module = import_module("services.query_service")


class FakeConnector:
    """Minimal connector used to observe cache construction and cleanup."""

    def __init__(self, *, close_error: Exception | None = None):
        self.close_calls = 0
        self.close_error = close_error

    def close(self):
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


class FakeService:
    """Legacy cached-service stand-in with an observable connector."""

    def __init__(self, connector: FakeConnector):
        self.connector = connector


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
                "connection_options": {"application_name": "schemabridge", "token": "option-secret"},
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
            },
        }
    )


@pytest.fixture(autouse=True)
def isolated_query_service_caches(monkeypatch):
    monkeypatch.setattr(query_service_module, "_QUERY_SERVICE", None)
    monkeypatch.setattr(query_service_module, "_PROFILE_REGISTRY", None)
    monkeypatch.setattr(query_service_module, "_PROFILE_QUERY_SERVICES", {})
    monkeypatch.setattr(query_service_module, "_PROFILE_QUERY_SERVICE_LOCK", Lock())


def _install_named_factory(monkeypatch, *, close_errors: list[Exception | None] | None = None):
    created: list[tuple[object, FakeConnector]] = []
    errors = iter(close_errors or [])

    def create_for_profile(profile):
        connector = FakeConnector(close_error=next(errors, None))
        created.append((profile, connector))
        return connector

    monkeypatch.setattr(ConnectorFactory, "create_for_profile", staticmethod(create_for_profile))
    return created


def test_legacy_no_argument_singleton_behavior_is_unchanged(monkeypatch):
    connector = FakeConnector()
    load_calls: list[str] = []
    create_calls: list[str] = []

    monkeypatch.setattr(Config, "load", classmethod(lambda cls: load_calls.append("load")))
    monkeypatch.setattr(
        ConnectorFactory,
        "create",
        staticmethod(lambda: create_calls.append("create") or connector),
    )

    first = query_service_module.get_query_service()
    second = query_service_module.get_query_service()

    assert first is second
    assert first.connector is connector
    assert load_calls == ["load"]
    assert create_calls == ["create"]
    assert query_service_module._PROFILE_REGISTRY is None
    assert query_service_module._PROFILE_QUERY_SERVICES == {}


def test_legacy_and_named_resets_affect_only_their_own_services(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    created = _install_named_factory(monkeypatch)
    legacy_connector = FakeConnector()
    query_service_module._QUERY_SERVICE = FakeService(legacy_connector)

    named = query_service_module.get_query_service("postgres-source")
    query_service_module.reset_query_service()

    assert legacy_connector.close_calls == 1
    assert named.connector.close_calls == 0
    assert query_service_module.get_query_service("POSTGRES-SOURCE") is named

    query_service_module._QUERY_SERVICE = FakeService(legacy_connector)
    query_service_module.reset_profile_query_services("POSTGRES-SOURCE")

    assert named.connector.close_calls == 1
    assert legacy_connector.close_calls == 1
    assert len(created) == 1


def test_case_insensitive_profiles_are_cached_once_and_vendors_coexist(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    created = _install_named_factory(monkeypatch)

    postgres = query_service_module.get_query_service("postgres-source")
    same_postgres = query_service_module.get_query_service("POSTGRES-SOURCE")
    snowflake = query_service_module.get_query_service("snowflake-target")

    assert postgres is same_postgres
    assert postgres is not snowflake
    assert [profile.db_type for profile, _connector in created] == ["postgresql", "snowflake"]
    assert set(query_service_module._PROFILE_QUERY_SERVICES) == {"postgres-source", "snowflake-target"}


def test_concurrent_same_profile_requests_create_exactly_one_service(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    created: list[tuple[object, FakeConnector]] = []
    creation_guard = Lock()

    def create_for_profile(profile):
        sleep(0.02)
        connector = FakeConnector()
        with creation_guard:
            created.append((profile, connector))
        return connector

    monkeypatch.setattr(ConnectorFactory, "create_for_profile", staticmethod(create_for_profile))

    with ThreadPoolExecutor(max_workers=12) as executor:
        services = list(executor.map(query_service_module.get_query_service, ["POSTGRES-SOURCE"] * 24))

    assert len(created) == 1
    assert all(service is services[0] for service in services)


def test_named_creation_does_not_use_legacy_state_or_mutate_process_state(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    monkeypatch.setenv("DB_ACTIVE_PROFILE", "legacy-active")
    created = _install_named_factory(monkeypatch)
    environment_before = dict(os.environ)
    active_profile_before = profile_service._active_profile
    config_before = (Config.DB_TYPE, Config.DATABASE, Config.GLOBAL_TIMEOUT_SECONDS, Config.GLOBAL_MAX_ROWS)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("named service used legacy state")

    class ForbiddenRuntimeLock:
        def __enter__(self):
            forbidden()

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(Config, "load", classmethod(forbidden))
    monkeypatch.setattr(Config, "connection_config", classmethod(forbidden))
    monkeypatch.setattr(ConnectorFactory, "create", staticmethod(forbidden))
    monkeypatch.setattr(profile_service, "switch_connection_profile", forbidden)
    monkeypatch.setattr(query_service_module, "runtime_lock", ForbiddenRuntimeLock())

    postgres = query_service_module.get_query_service("postgres-source")
    snowflake = query_service_module.get_query_service("snowflake-target")

    assert postgres is not snowflake
    assert len(created) == 2
    assert dict(os.environ) == environment_before
    assert profile_service._active_profile == active_profile_before
    assert (Config.DB_TYPE, Config.DATABASE, Config.GLOBAL_TIMEOUT_SECONDS, Config.GLOBAL_MAX_ROWS) == config_before


def test_registry_snapshot_is_loaded_only_once_for_named_services(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    _install_named_factory(monkeypatch)
    load_calls: list[str] = []
    original_loader = query_service_module._load_profile_registry

    def load_registry():
        load_calls.append("load")
        return original_loader()

    monkeypatch.setattr(query_service_module, "_load_profile_registry", load_registry)

    query_service_module.get_query_service("postgres-source")
    query_service_module.get_query_service("snowflake-target")
    query_service_module.get_query_service("POSTGRES-SOURCE")

    assert load_calls == ["load"]


def test_blank_profile_document_creates_an_empty_registry(monkeypatch):
    monkeypatch.delenv("DB_PROFILES_JSON", raising=False)

    with pytest.raises(ProfileRegistryError):
        query_service_module.get_query_service("missing")

    assert query_service_module._PROFILE_REGISTRY is not None
    assert len(query_service_module._PROFILE_REGISTRY) == 0
    assert query_service_module._PROFILE_QUERY_SERVICES == {}


def test_malformed_json_does_not_cache_registry_or_expose_raw_input(monkeypatch):
    secret = "malformed-profile-secret"
    raw_json = '{"postgres-source":{"password":"' + secret + '"}'
    monkeypatch.setenv("DB_PROFILES_JSON", raw_json)
    handler = MessageHandler()
    query_service_module.logger.addHandler(handler)
    try:
        with pytest.raises(ProfileRegistryError) as captured:
            query_service_module.get_query_service("postgres-source")
    finally:
        query_service_module.logger.removeHandler(handler)

    assert query_service_module._PROFILE_REGISTRY is None
    assert query_service_module._PROFILE_QUERY_SERVICES == {}
    assert secret not in str(captured.value)
    assert raw_json not in str(captured.value)
    assert secret not in json.dumps(handler.messages)


def test_failed_service_construction_does_not_cache_and_is_safely_redacted(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    secret = "constructor-secret"
    handler = MessageHandler()

    def fail_construction(*_args, **_kwargs):
        raise RuntimeError(f"driver setup failed: {secret} postgres.internal source_user")

    monkeypatch.setattr(query_service_module, "QueryService", fail_construction)
    query_service_module.logger.addHandler(handler)
    try:
        with pytest.raises(ConfigError) as captured:
            query_service_module.get_query_service("postgres-source")
    finally:
        query_service_module.logger.removeHandler(handler)

    rendered = str(captured.value) + json.dumps(handler.messages)
    assert query_service_module._PROFILE_QUERY_SERVICES == {}
    assert str(captured.value) == "Unable to create a service for the selected connection profile"
    assert secret not in rendered
    assert "postgres.internal" not in rendered
    assert "source_user" not in rendered


def test_resetting_one_profile_is_case_insensitive_and_closes_once(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    _install_named_factory(monkeypatch)
    postgres = query_service_module.get_query_service("postgres-source")
    snowflake = query_service_module.get_query_service("snowflake-target")

    query_service_module.reset_profile_query_services("POSTGRES-SOURCE")
    query_service_module.reset_profile_query_services("postgres-source")

    assert postgres.connector.close_calls == 1
    assert snowflake.connector.close_calls == 0
    assert set(query_service_module._PROFILE_QUERY_SERVICES) == {"snowflake-target"}
    assert query_service_module._PROFILE_REGISTRY is not None


def test_reset_all_closes_every_service_and_redacts_close_failures(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document())
    secret = "close-secret"
    _install_named_factory(monkeypatch, close_errors=[RuntimeError(secret), None])
    postgres = query_service_module.get_query_service("postgres-source")
    snowflake = query_service_module.get_query_service("snowflake-target")
    handler = MessageHandler()
    query_service_module.logger.addHandler(handler)
    try:
        query_service_module.reset_profile_query_services()
    finally:
        query_service_module.logger.removeHandler(handler)

    assert postgres.connector.close_calls == 1
    assert snowflake.connector.close_calls == 1
    assert query_service_module._PROFILE_QUERY_SERVICES == {}
    assert query_service_module._PROFILE_REGISTRY is None
    assert handler.messages == ["Failed to close a cached profile-bound query service"]
    assert secret not in json.dumps(handler.messages)


def test_reset_all_reloads_changed_profile_document(monkeypatch):
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document(postgres_database="old_db"))
    _install_named_factory(monkeypatch)
    old_service = query_service_module.get_query_service("postgres-source")

    query_service_module.reset_profile_query_services()
    monkeypatch.setenv("DB_PROFILES_JSON", _profile_document(postgres_database="new_db"))
    new_service = query_service_module.get_query_service("postgres-source")

    assert old_service is not new_service
    assert old_service.connector.close_calls == 1
    assert new_service._database() == "new_db"
