"""Permanent ASGI application factory for SchemaBridge."""

from __future__ import annotations

import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import __version__
from .config import ApiSettings
from .dependencies import REQUIRED_DEPENDENCY_HOOKS
from .errors import install_error_handlers
from .middleware import install_middleware
from .routes.health import router as health_router
from .schemas.common import ErrorResponse


def _cleanup_services() -> None:
    """Close an existing query-service cache without importing it to do so."""

    seen: set[int] = set()
    for module_name in ("services.query_service", "clean_mcp.services.query_service"):
        module = sys.modules.get(module_name)
        reset = getattr(module, "reset_profile_query_services", None)
        if callable(reset) and id(reset) not in seen:
            seen.add(id(reset))
            try:
                reset()
            except Exception:
                # Shutdown remains safe after partial startup or connector failure.
                continue


@asynccontextmanager
async def _lifespan(app: FastAPI):
    app.state.ready = False
    try:
        if not isinstance(getattr(app.state, "settings", None), ApiSettings):
            raise RuntimeError("API settings are unavailable or invalid.")
        if not all(callable(hook) for hook in REQUIRED_DEPENDENCY_HOOKS):
            raise RuntimeError("Required API dependency hooks are unavailable.")
        app.state.ready = True
        yield
    finally:
        app.state.ready = False
        _cleanup_services()


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    if settings is not None and not isinstance(settings, ApiSettings):
        raise TypeError("settings must be an ApiSettings value.")
    effective_settings = settings if settings is not None else ApiSettings()
    app = FastAPI(
        title="SchemaBridge API",
        version=__version__,
        description="Production API for governed schema migration and validation workflows.",
        openapi_url="/openapi.json",
        lifespan=_lifespan,
        openapi_tags=[
            {"name": "health", "description": "Operational health checks."},
            {"name": "migrations", "description": "Versioned migration workflows."},
        ],
        responses={
            422: {"model": ErrorResponse, "description": "Request validation failed."},
            500: {"model": ErrorResponse, "description": "Unexpected application error."},
        },
    )
    app.state.ready = False
    app.state.settings = effective_settings
    install_error_handlers(app)
    install_middleware(app, max_body_bytes=effective_settings.max_request_body_bytes)
    app.include_router(health_router)
    return app


__all__ = ["create_app"]
