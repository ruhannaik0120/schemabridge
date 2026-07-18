"""Production-platform tests for the permanent FastAPI foundation."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, time, timezone
from decimal import Decimal
from enum import Enum
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType

import pytest
from fastapi import Depends, Response
from fastapi.testclient import TestClient
from pydantic import ValidationError
from pydantic_core import PydanticSerializationError
from starlette.types import Message, Scope

from api import __version__
from api.app import create_app
from api.config import ApiSettings
from api.dependencies import get_schema_mapping_service
from api.errors import ApiError
from api.middleware import PlatformMiddleware, safe_request_id
from api.schemas.common import ApiSchema, BoundedText


class _EchoInput(ApiSchema):
    value: BoundedText


def _add_test_routes(app) -> None:
    @app.post("/api/v1/echo", operation_id="test_echo")
    async def echo(payload: _EchoInput) -> dict[str, str]:
        return {"value": payload.value}

    @app.get("/api/v1/known-error", operation_id="test_known_error")
    async def known_error() -> None:
        raise ApiError(409, "KNOWN_CONFLICT", "The requested operation conflicts with current state.")

    @app.get("/api/v1/unexpected-error", operation_id="test_unexpected_error")
    async def unexpected_error() -> None:
        raise RuntimeError("secret SQL SELECT password FROM credentials at C:\\secret")

    @app.get("/api/v1/cache-policy", operation_id="test_cache_policy")
    async def cache_policy(response: Response) -> dict[str, bool]:
        response.headers["Cache-Control"] = "public, max-age=3600"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        return {"ok": True}


def test_create_app_is_repeatable_and_independent() -> None:
    first = create_app()
    second = create_app()

    assert first is not second
    assert first.state.settings is not second.state.settings
    assert first.dependency_overrides is not second.dependency_overrides
    assert first.exception_handlers is not second.exception_handlers
    first.state.ready = True
    assert second.state.ready is False


def test_routes_and_handlers_are_not_duplicated() -> None:
    for app in (create_app(), create_app()):
        paths = app.openapi()["paths"]
        operations = [
            (path, method)
            for path, path_item in paths.items()
            for method in path_item
            if method in {"get", "post", "put", "patch", "delete"}
        ]
        assert len(operations) == len(set(operations))
        assert operations.count(("/health/live", "get")) == 1
        assert operations.count(("/health/ready", "get")) == 1
        assert len(app.exception_handlers) == len(set(app.exception_handlers))


def test_exact_asgi_factory_target_imports() -> None:
    workspace_root = Path(__file__).resolve().parents[2]
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from uvicorn.importer import import_from_string; "
                "factory=import_from_string('clean_mcp.api.app:create_app'); "
                "assert factory().title == 'SchemaBridge API'; "
                "assert 'config' not in __import__('sys').modules; "
                "assert 'services.query_service' not in __import__('sys').modules; "
                "dependency=__import__('clean_mcp.api.dependencies', fromlist=['get_schema_mapping_service']); "
                "assert type(dependency.get_schema_mapping_service()).__name__ == 'SchemaMappingService'"
            ),
        ],
        cwd=workspace_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_import_and_openapi_do_not_load_database_runtime() -> None:
    app = create_app()
    before = set(sys.modules)
    app.openapi()
    newly_loaded = set(sys.modules) - before
    forbidden = {
        "config",
        "services.query_service",
        "connectors.postgresql.connector",
        "connectors.snowflake.connector",
    }
    assert forbidden.isdisjoint(newly_loaded)


def test_api_settings_require_a_positive_non_boolean_limit() -> None:
    assert ApiSettings().max_request_body_bytes == 1_048_576
    for value in (True, False, 0, -1, 1.5, "10"):
        with pytest.raises(ValueError):
            ApiSettings(max_request_body_bytes=value)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        create_app(object())  # type: ignore[arg-type]


def test_lifespan_owns_readiness_and_is_reusable() -> None:
    app = create_app()
    assert app.state.ready is False
    for _ in range(2):
        with TestClient(app) as client:
            assert app.state.ready is True
            assert client.get("/health/ready").status_code == 200
        assert app.state.ready is False


def test_startup_checks_hooks_without_resolving_them(monkeypatch) -> None:
    app_module = sys.modules[create_app.__module__]

    def must_not_resolve():
        pytest.fail("startup must not resolve a service dependency")

    monkeypatch.setattr(app_module, "REQUIRED_DEPENDENCY_HOOKS", (must_not_resolve,))
    app = create_app()
    with TestClient(app) as client:
        assert client.get("/health/ready").status_code == 200
    assert app.state.ready is False


def test_partial_startup_leaves_application_not_ready(monkeypatch) -> None:
    app_module = sys.modules[create_app.__module__]
    monkeypatch.setattr(app_module, "REQUIRED_DEPENDENCY_HOOKS", (None,))
    app = create_app()

    async def enter_lifespan() -> None:
        async with app.router.lifespan_context(app):
            pytest.fail("invalid startup must not enter the serving lifespan")

    with pytest.raises(RuntimeError, match="dependency hooks"):
        asyncio.run(enter_lifespan())
    assert app.state.ready is False


def test_shutdown_closes_only_an_already_loaded_supported_cache(monkeypatch) -> None:
    calls: list[str] = []
    fake = ModuleType("services.query_service")
    fake.reset_profile_query_services = lambda: calls.append("reset")  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "services.query_service", fake)

    with TestClient(create_app()):
        pass

    assert calls == ["reset"]


def test_liveness_and_readiness_are_safe_and_deterministic() -> None:
    app = create_app()
    not_ready = TestClient(app).get("/health/ready", headers={"X-Request-ID": "health-1"})
    assert not_ready.status_code == 503
    assert not_ready.json() == {
        "error": {
            "code": "SERVICE_NOT_READY",
            "message": "The application is not ready.",
            "request_id": "health-1",
        }
    }
    with TestClient(app) as client:
        expected = {"status": "ok", "service": "schemabridge-api", "version": __version__}
        assert client.get("/health/live").json() == expected
        assert client.get("/health/ready").json() == expected
        serialized = json.dumps(expected).casefold()
        assert all(term not in serialized for term in ("profile", "database", "credential", "host", "path"))


def test_request_id_generation_validation_and_isolation() -> None:
    app = create_app()
    with TestClient(app) as client:
        generated = client.get("/health/live").headers["X-Request-ID"]
        preserved = client.get("/health/live", headers={"X-Request-ID": "safe-id_123"})
        empty = client.get("/health/live", headers={"X-Request-ID": ""})
        oversized = client.get("/health/live", headers={"X-Request-ID": "a" * 65})
        second_generated = client.get("/health/live").headers["X-Request-ID"]

    assert len(generated) == 32
    assert preserved.headers["X-Request-ID"] == "safe-id_123"
    assert len(empty.headers["X-Request-ID"]) == 32
    assert len(oversized.headers["X-Request-ID"]) == 32
    assert generated != second_generated
    assert safe_request_id("control\nvalue") != "control\nvalue"
    assert safe_request_id("unsafe-\u2603") != "unsafe-\u2603"


def test_security_cache_and_cors_headers_cover_success_and_errors() -> None:
    app = create_app()
    _add_test_routes(app)
    with TestClient(app, raise_server_exceptions=False) as client:
        responses = (
            client.get("/health/live"),
            client.get("/missing"),
            client.get("/api/v1/known-error"),
            client.get("/api/v1/unexpected-error"),
        )
        cache = client.get("/api/v1/cache-policy")

    for response in responses:
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert response.headers["X-Frame-Options"] == "DENY"
        assert response.headers["Referrer-Policy"] == "no-referrer"
        assert response.headers["Cache-Control"] == "no-store"
        assert "Access-Control-Allow-Origin" not in response.headers
    assert cache.headers["Cache-Control"] == "no-store"
    assert cache.headers["X-Frame-Options"] == "SAMEORIGIN"


def test_body_limit_allows_under_limit_and_rejects_over_limit() -> None:
    app = create_app(ApiSettings(max_request_body_bytes=24))
    _add_test_routes(app)
    with TestClient(app) as client:
        accepted = client.post("/api/v1/echo", json={"value": "x"})
        rejected = client.post(
            "/api/v1/echo",
            content=b'{"value":"do-not-echo-this-secret"}',
            headers={"Content-Type": "application/json", "X-Request-ID": "body-limit"},
        )

    assert accepted.status_code == 200
    assert rejected.status_code == 413
    assert rejected.json() == {
        "error": {
            "code": "PAYLOAD_TOO_LARGE",
            "message": "Request body exceeds the allowed size.",
            "request_id": "body-limit",
        }
    }
    assert "do-not-echo" not in rejected.text


def test_stream_limit_enforces_actual_body_when_length_is_missing_or_false() -> None:
    async def downstream(scope: Scope, receive, send) -> None:
        while True:
            message = await receive()
            if not message.get("more_body", False):
                break
        response = Response("accepted")
        await response(scope, receive, send)

    async def exercise(headers: list[tuple[bytes, bytes]]) -> list[Message]:
        messages = iter(
            (
                {"type": "http.request", "body": b"123", "more_body": True},
                {"type": "http.request", "body": b"456", "more_body": False},
            )
        )
        sent: list[Message] = []

        async def receive() -> Message:
            return next(messages)  # type: ignore[return-value]

        async def send(message: Message) -> None:
            sent.append(message)

        scope: Scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/api/v1/echo",
            "raw_path": b"/api/v1/echo",
            "query_string": b"",
            "headers": headers,
            "client": ("test", 1),
            "server": ("test", 80),
        }
        await PlatformMiddleware(downstream, max_body_bytes=5)(scope, receive, send)
        return sent

    for headers in ([], [(b"content-length", b"1")]):
        sent = asyncio.run(exercise(headers))
        assert sent[0]["status"] == 413
        body = b"".join(message.get("body", b"") for message in sent)
        assert b"PAYLOAD_TOO_LARGE" in body


def test_invalid_content_length_is_rejected_safely() -> None:
    async def downstream(_scope, _receive, _send) -> None:
        pytest.fail("invalid Content-Length must not reach routing")

    async def exercise() -> list[Message]:
        sent: list[Message] = []

        async def receive() -> Message:
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message: Message) -> None:
            sent.append(message)

        scope: Scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/api/v1/echo",
            "raw_path": b"/api/v1/echo",
            "query_string": b"",
            "headers": [(b"content-length", b"invalid")],
            "client": ("test", 1),
            "server": ("test", 80),
        }
        await PlatformMiddleware(downstream, max_body_bytes=5)(scope, receive, send)
        return sent

    assert asyncio.run(exercise())[0]["status"] == 413


def test_known_validation_not_found_and_unexpected_errors_are_redacted() -> None:
    app = create_app()
    _add_test_routes(app)
    with TestClient(app, raise_server_exceptions=False) as client:
        known = client.get("/api/v1/known-error", headers={"X-Request-ID": "error-1"})
        validation = client.post(
            "/api/v1/echo",
            json={"value": 42, "password": "do-not-leak"},
            headers={"X-Request-ID": "error-2"},
        )
        missing = client.get("/does-not-exist", headers={"X-Request-ID": "error-3"})
        unexpected = client.get("/api/v1/unexpected-error", headers={"X-Request-ID": "error-4"})

    expected = (
        (known, 409, "KNOWN_CONFLICT", "error-1"),
        (validation, 422, "REQUEST_VALIDATION_FAILED", "error-2"),
        (missing, 404, "RESOURCE_NOT_FOUND", "error-3"),
        (unexpected, 500, "INTERNAL_ERROR", "error-4"),
    )
    for response, status, code, request_id in expected:
        assert response.status_code == status
        assert response.json()["error"]["code"] == code
        assert response.json()["error"]["request_id"] == request_id
        assert response.headers["X-Request-ID"] == request_id
    assert validation.json()["error"]["field"] == "value"
    combined = " ".join(response.text for response, *_ in expected).casefold()
    assert all(term not in combined for term in ("password", "do-not-leak", "select", "credentials", "c:\\secret", "traceback"))


def test_safe_logging_contains_operational_fields_not_payload(caplog) -> None:
    app = create_app()
    _add_test_routes(app)
    caplog.set_level("INFO", logger="schemabridge.api")
    with TestClient(app) as client:
        client.post("/api/v1/echo", json={"value": "private-value"}, headers={"X-Request-ID": "log-id"})

    record = next(record for record in caplog.records if record.message == "API request completed.")
    assert record.request_id == "log-id"  # type: ignore[attr-defined]
    assert record.method == "POST"  # type: ignore[attr-defined]
    assert record.route == "/api/v1/echo"  # type: ignore[attr-defined]
    assert record.status_code == 200  # type: ignore[attr-defined]
    assert "private-value" not in caplog.text


def test_pydantic_transport_serialization_is_explicit_and_deterministic() -> None:
    class Mode(str, Enum):
        ACTIVE = "active"

    class Transport(ApiSchema):
        amount: Decimal
        day: date
        timestamp: datetime
        clock: time
        mode: Mode
        label: BoundedText

    value = Transport(
        amount=Decimal("1.2300"),
        day=date(2026, 7, 18),
        timestamp=datetime(2026, 7, 18, 12, 30, tzinfo=timezone.utc),
        clock=time(8, 9, 10),
        mode=Mode.ACTIVE,
        label="  exact \u96ea  ",
    )
    assert json.loads(value.model_dump_json()) == {
        "amount": "1.2300",
        "day": "2026-07-18",
        "timestamp": "2026-07-18T12:30:00Z",
        "clock": "08:09:10",
        "mode": "active",
        "label": "  exact \u96ea  ",
    }
    assert value.model_dump_json() == value.model_dump_json()


def test_pydantic_conventions_reject_extra_wrong_and_arbitrary_values() -> None:
    class Text(ApiSchema):
        value: BoundedText

    with pytest.raises(ValidationError):
        Text(value="valid", extra="rejected")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        Text(value=123)  # type: ignore[arg-type]

    class Arbitrary(ApiSchema):
        value: object

    with pytest.raises(PydanticSerializationError):
        Arbitrary(value=object()).model_dump_json()


def test_dependency_hooks_are_lazy_overridable_and_app_scoped() -> None:
    first = create_app()
    second = create_app()
    sentinel = object()
    first.dependency_overrides[get_schema_mapping_service] = lambda: sentinel

    @first.get("/dependency-test", include_in_schema=False)
    async def dependency_test(service=Depends(get_schema_mapping_service)) -> dict[str, bool]:
        return {"overridden": service is sentinel}

    with TestClient(first) as client:
        assert client.get("/dependency-test").json() == {"overridden": True}
    assert get_schema_mapping_service not in second.dependency_overrides


def test_openapi_is_deterministic_unique_and_safe() -> None:
    app = create_app()
    first = app.openapi()
    second = app.openapi()
    assert first == second
    assert first["info"] == {
        "title": "SchemaBridge API",
        "description": "Production API for governed schema migration and validation workflows.",
        "version": __version__,
    }
    assert set(first["paths"]) == {"/health/live", "/health/ready"}
    operation_ids = [
        operation["operationId"]
        for path in first["paths"].values()
        for method, operation in path.items()
        if method in {"get", "post", "put", "patch", "delete"}
    ]
    assert operation_ids == ["health_live", "health_ready"]
    assert len(operation_ids) == len(set(operation_ids))
    assert "ErrorResponse" in first["components"]["schemas"]
    rendered = json.dumps(first).casefold()
    assert all(term not in rendered for term in ("password", "credential", "authorization", "raw_sql", "sql_parameter"))


def test_dependency_manifests_place_runtime_and_test_packages_correctly() -> None:
    project_root = Path(__file__).resolve().parents[1]
    runtime = (project_root / "requirements.txt").read_text(encoding="utf-8")
    testing = (project_root.parent / "requirements-e2e.txt").read_text(encoding="utf-8")
    assert "fastapi>=0.115,<1.0" in runtime
    assert "uvicorn[standard]>=0.30,<1.0" in runtime
    assert "httpx" not in runtime.casefold()
    assert "httpx>=0.27,<1.0" in testing
