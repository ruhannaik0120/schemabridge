"""Assemble the SchemaBridge FastAPI application and process resources.

The factory installs the HTTP platform concerns and route groups without
opening source or target database connections.  Its lifespan owns the optional
control-plane repository and closes any lazily created connector services at
shutdown.
"""

from __future__ import annotations

import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import __version__
from .config import ApiSettings
from .dependencies import (
    REQUIRED_DEPENDENCY_HOOKS,
    build_workflow_repository,
)
from .errors import install_error_handlers
from .middleware import install_middleware
from .routes.health import router as health_router
from .schemas.common import ErrorResponse


def _cleanup_services() -> None:
    """Close an existing database-service cache without importing it to do so."""

    seen: set[int] = set()
    for module_name in ("schemabridge.services.database_service",):
        module = sys.modules.get(module_name)
        reset = getattr(module, "reset_database_services", None)
        if callable(reset) and id(reset) not in seen:
            seen.add(id(reset))
            try:
                reset()
            except Exception:
                # Shutdown remains safe after partial startup or connector failure.
                continue


def _cleanup_workflow_repository(app: FastAPI) -> None:
    """Detach and close the application-owned control-plane repository."""

    repository = getattr(app.state, "workflow_repository", None)
    # Detach first so a failed close cannot leave a stale object available to a
    # request or a second cleanup pass.
    app.state.workflow_repository = None
    close = getattr(repository, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            # Shutdown remains deterministic even after a persistence failure.
            pass


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Initialize optional durable persistence and guarantee orderly cleanup."""

    app.state.ready = False
    try:
        settings = getattr(app.state, "settings", None)
        if not isinstance(settings, ApiSettings):
            raise RuntimeError("API settings are unavailable or invalid.")
        if not all(callable(hook) for hook in REQUIRED_DEPENDENCY_HOOKS):
            raise RuntimeError("Required API dependency hooks are unavailable.")
        if settings.control_plane.enabled:
            # Repository construction is connection-free.  Individual
            # operations open their own transactions when a request arrives.
            app.state.workflow_repository = build_workflow_repository(settings.control_plane)
        app.state.ready = True
        yield
    finally:
        app.state.ready = False
        _cleanup_workflow_repository(app)
        _cleanup_services()


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    """Create an independently configured SchemaBridge ASGI application.

    Args:
        settings: Optional validated settings, primarily useful for isolated
            tests.  Environment-backed defaults are loaded when omitted.

    Returns:
        A fully routed FastAPI application whose remote resources remain lazy.

    Raises:
        TypeError: If ``settings`` is not an :class:`ApiSettings` instance.
    """

    if settings is not None and not isinstance(settings, ApiSettings):
        raise TypeError("settings must be an ApiSettings value.")
    effective_settings = settings if settings is not None else ApiSettings()
    # Import the heavier route graph only when the factory is invoked.  This
    # keeps package import and OpenAPI inspection independent of database
    # drivers, profiles, and live connections.
    from .routes.migrations import router as migrations_router
    from .routes.workflows import router as workflows_router

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
    app.state.workflow_repository = None
    install_error_handlers(app)
    install_middleware(app, max_body_bytes=effective_settings.max_request_body_bytes)
    app.include_router(health_router)
    app.include_router(migrations_router)
    app.include_router(workflows_router)
    return app


__all__ = ["create_app"]
