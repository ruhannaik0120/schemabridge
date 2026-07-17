"""Focused tests for profile-aware MCP metadata routing."""

from __future__ import annotations

import asyncio
from importlib import import_module
import inspect
import json
import logging
import os
import sys

import pytest

from config import Config, ConfigError
from models.errors import ErrorCode
from services import profile_service
from services.profile_registry import ProfileRegistryError, UnknownProfileError
import tools.metadata as metadata_tools
import tools.service_routing as service_routing


class FakeResponse:
    def __init__(self, payload: dict):
        self.payload = payload

    def to_dict(self) -> dict:
        return self.payload


class MetadataService:
    def __init__(self, name: str, *, error: bool = False):
        self.name = name
        self.error = error
        self.calls: list[tuple[str, dict]] = []

    def _response(self, tool: str, kwargs: dict) -> FakeResponse:
        self.calls.append((tool, kwargs))
        payload = {
            "success": not self.error,
            "tool": tool,
            "request_id": f"{self.name}-request",
            "timestamp": "2026-07-17T00:00:00+00:00",
            "execution_time_ms": 1,
            "environment": self.name.upper(),
            "data": {"source": self.name},
            "metadata": {"profile": self.name},
        }
        if self.error:
            payload["error"] = {
                "code": "DATABASE_ERROR",
                "message": "Safe service error",
                "request_id": payload["request_id"],
                "timestamp": payload["timestamp"],
                "retryable": True,
                "context": {},
            }
        return FakeResponse(payload)

    def list_databases(self, **kwargs):
        return self._response("list_databases", kwargs)

    def list_tables(self, **kwargs):
        return self._response("list_tables", kwargs)

    def describe_table(self, **kwargs):
        return self._response("describe_table", kwargs)

    def suggest_columns(self, **kwargs):
        return self._response("suggest_columns", kwargs)


class TrackingLock:
    def __init__(self):
        self.entries = 0
        self.active = False

    def __enter__(self):
        self.entries += 1
        self.active = True
        return self

    def __exit__(self, *_args):
        self.active = False
        return False


class ForbiddenLock:
    def __enter__(self):
        raise AssertionError("named metadata path acquired runtime_lock")

    def __exit__(self, *_args):
        return False


class MessageHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def test_legacy_none_uses_no_argument_service_and_holds_runtime_lock(monkeypatch):
    service = MetadataService("legacy")
    lock = TrackingLock()
    acquisitions: list[tuple] = []

    def get_service(*args):
        acquisitions.append(args)
        return service

    monkeypatch.setattr(service_routing, "runtime_lock", lock)
    monkeypatch.setattr(service_routing, "get_query_service", get_service)

    response = service_routing.invoke_query_service(
        profile_id=None,
        tool_name="list_databases",
        operation=lambda selected: (
            selected.list_databases(lock_active=lock.active)
        ),
    )

    assert acquisitions == [()]
    assert lock.entries == 1
    assert service.calls == [("list_databases", {"lock_active": True})]
    assert response["data"] == {"source": "legacy"}


def test_named_service_bypasses_runtime_lock_and_legacy_path(monkeypatch):
    postgres = MetadataService("postgres-source")
    acquisitions: list[str] = []

    def get_service(profile_id):
        acquisitions.append(profile_id)
        return postgres

    monkeypatch.setattr(service_routing, "runtime_lock", ForbiddenLock())
    monkeypatch.setattr(service_routing, "get_query_service", get_service)

    response = service_routing.invoke_query_service(
        profile_id="postgres-source",
        tool_name="list_tables",
        operation=lambda selected: selected.list_tables(database="source_db"),
    )

    assert acquisitions == ["postgres-source"]
    assert response["metadata"]["profile"] == "postgres-source"


@pytest.mark.parametrize(
    ("profile_id", "error"),
    [
        ("", UnknownProfileError("blank profile password=secret")),
        ("   ", UnknownProfileError("whitespace profile host=private")),
        ("malformed-profile", ProfileRegistryError("raw registry JSON and token-secret")),
        ("unknown-profile", UnknownProfileError("unknown-profile source_user")),
    ],
)
def test_invalid_named_ids_never_fall_back_and_return_safe_errors(monkeypatch, profile_id, error):
    calls: list[str] = []

    def fail_named(selected_profile_id):
        calls.append(selected_profile_id)
        raise error

    handler = MessageHandler()
    monkeypatch.setattr(service_routing, "runtime_lock", ForbiddenLock())
    monkeypatch.setattr(service_routing, "get_query_service", fail_named)
    service_routing.logger.addHandler(handler)
    try:
        response = service_routing.invoke_query_service(
            profile_id=profile_id,
            tool_name="describe_table",
            operation=lambda _service: pytest.fail("operation should not run"),
        )
    finally:
        service_routing.logger.removeHandler(handler)

    rendered = json.dumps(response) + json.dumps(handler.messages)
    assert calls == [profile_id]
    assert response["success"] is False
    assert response["tool"] == "describe_table"
    assert response["error"]["code"] == ErrorCode.CONFIG_INVALID
    assert response["error"]["request_id"] == response["request_id"]
    assert handler.messages == ["Named query service resolution failed"]
    for sensitive in (profile_id.strip(), str(error), "secret", "private", "source_user", "token-secret"):
        if sensitive:
            assert sensitive not in rendered


def test_named_service_construction_errors_are_safe(monkeypatch):
    secret = "constructor-password"
    monkeypatch.setattr(
        service_routing,
        "get_query_service",
        lambda _profile_id: (_ for _ in ()).throw(ConfigError(f"failed {secret} postgres.internal")),
    )

    response = service_routing.invoke_query_service(
        profile_id="postgres-source",
        tool_name="list_databases",
        operation=lambda _service: pytest.fail("operation should not run"),
    )

    rendered = json.dumps(response)
    assert response["error"]["code"] == ErrorCode.CONFIG_INVALID
    assert secret not in rendered
    assert "postgres.internal" not in rendered


def test_metadata_operation_exceptions_are_not_broadly_caught(monkeypatch):
    expected = RuntimeError("operation failure remains outside routing boundary")
    monkeypatch.setattr(service_routing, "get_query_service", lambda _profile_id: MetadataService("postgres"))

    with pytest.raises(RuntimeError) as captured:
        service_routing.invoke_query_service(
            profile_id="postgres-source",
            tool_name="list_tables",
            operation=lambda _service: (_ for _ in ()).throw(expected),
        )

    assert captured.value is expected


def test_metadata_wrappers_route_all_four_tools_with_keyword_profile(monkeypatch):
    postgres = MetadataService("postgres-source")
    routed: list[tuple[str | None, str]] = []

    def invoke(*, profile_id, tool_name, operation):
        routed.append((profile_id, tool_name))
        return operation(postgres).to_dict()

    monkeypatch.setattr(metadata_tools, "invoke_query_service", invoke)

    metadata_tools.list_databases(profile_id="postgres-source")
    metadata_tools.list_tables(database="source_db", schema="public", profile_id="postgres-source")
    metadata_tools.describe_table(table="orders", profile_id="postgres-source")
    metadata_tools.suggest_columns(
        table="orders",
        missing_column="status",
        profile_id="postgres-source",
    )

    assert routed == [
        ("postgres-source", "list_databases"),
        ("postgres-source", "list_tables"),
        ("postgres-source", "describe_table"),
        ("postgres-source", "suggest_columns"),
    ]


def test_postgres_and_snowflake_wrappers_coexist_case_insensitively(monkeypatch):
    services = {
        "postgres-source": MetadataService("postgres-source"),
        "snowflake-target": MetadataService("snowflake-target"),
    }
    requested: list[str] = []

    def get_service(profile_id):
        requested.append(profile_id)
        return services[profile_id.casefold()]

    monkeypatch.setattr(service_routing, "runtime_lock", ForbiddenLock())
    monkeypatch.setattr(service_routing, "get_query_service", get_service)

    postgres = metadata_tools.list_tables(profile_id="POSTGRES-SOURCE")
    snowflake = metadata_tools.describe_table(table="ORDERS", profile_id="Snowflake-Target")

    assert requested == ["POSTGRES-SOURCE", "Snowflake-Target"]
    assert postgres["data"]["source"] == "postgres-source"
    assert snowflake["data"]["source"] == "snowflake-target"
    assert services["postgres-source"].calls != services["snowflake-target"].calls


def test_named_metadata_does_not_mutate_global_configuration(monkeypatch):
    service = MetadataService("postgres-source")
    environment_before = dict(os.environ)
    active_profile_before = profile_service._active_profile
    config_before = (Config.DB_TYPE, Config.DATABASE, Config.GLOBAL_TIMEOUT_SECONDS, Config.GLOBAL_MAX_ROWS)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("named metadata used legacy global state")

    monkeypatch.setattr(service_routing, "runtime_lock", ForbiddenLock())
    monkeypatch.setattr(service_routing, "get_query_service", lambda _profile_id: service)
    monkeypatch.setattr(profile_service, "switch_connection_profile", forbidden)

    metadata_tools.list_databases(profile_id="postgres-source")

    assert dict(os.environ) == environment_before
    assert profile_service._active_profile == active_profile_before
    assert (Config.DB_TYPE, Config.DATABASE, Config.GLOBAL_TIMEOUT_SECONDS, Config.GLOBAL_MAX_ROWS) == config_before


def test_service_success_and_error_responses_pass_through_unchanged(monkeypatch):
    success_service = MetadataService("postgres-source")
    error_service = MetadataService("snowflake-target", error=True)
    monkeypatch.setattr(
        service_routing,
        "get_query_service",
        lambda profile_id: success_service if profile_id == "postgres-source" else error_service,
    )

    success = metadata_tools.list_databases(profile_id="postgres-source")
    error = metadata_tools.list_databases(profile_id="snowflake-target")

    assert success == success_service._response("list_databases", {}).to_dict()
    assert error == error_service._response("list_databases", {}).to_dict()
    assert set(success) - {"error"} == set(error) - {"error"}


def test_metadata_profile_parameter_is_trailing_and_keyword_only():
    for function in (
        metadata_tools.list_databases,
        metadata_tools.list_tables,
        metadata_tools.describe_table,
        metadata_tools.suggest_columns,
    ):
        parameters = list(inspect.signature(function).parameters.values())
        assert parameters[-1].name == "profile_id"
        assert parameters[-1].kind is inspect.Parameter.KEYWORD_ONLY
        assert parameters[-1].default is None


def test_fastmcp_exposes_profile_id_on_exactly_the_metadata_tools(monkeypatch):
    monkeypatch.setattr(Config, "validate", classmethod(lambda cls: cls))
    sys.modules.pop("server", None)
    server = import_module("server")
    registered = {tool.name: tool for tool in asyncio.run(server.mcp.list_tools())}
    expected = {
        "tool_list_databases",
        "tool_list_tables",
        "tool_describe_table",
        "tool_suggest_columns",
    }
    exposed = {
        name
        for name, tool in registered.items()
        if "profile_id" in tool.inputSchema.get("properties", {})
    }

    assert exposed == expected
    for name in expected:
        schema = registered[name].inputSchema
        assert schema["properties"]["profile_id"] == {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "default": None,
            "title": "Profile Id",
        }
        assert "profile_id" not in schema.get("required", [])
