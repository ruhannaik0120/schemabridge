"""Metadata-oriented MCP tools for the MCP server.

These wrappers expose database discovery capabilities while leaving all
connector communication and response shaping to the service layer.
"""

from services import query_service
from services.runtime_state import runtime_lock


def list_databases(environment: str = "", timeout_seconds: int | None = None) -> dict:
    """Return all databases for the selected connector."""

    with runtime_lock:
        return query_service.list_databases(environment=environment, timeout_seconds=timeout_seconds).to_dict()


def list_tables(
    database: str = "",
    schema: str = "",
    environment: str = "",
    timeout_seconds: int | None = None,
) -> dict:
    """Return the tables for a database, optionally filtered by schema."""

    with runtime_lock:
        return query_service.list_tables(
            database=database,
            schema=schema,
            environment=environment,
            timeout_seconds=timeout_seconds,
        ).to_dict()


def describe_table(
    database: str = "",
    table: str = "",
    schema: str = "",
    environment: str = "",
    timeout_seconds: int | None = None,
) -> dict:
    """Return column metadata for a specific table."""

    with runtime_lock:
        return query_service.describe_table(
            database=database,
            table=table,
            schema=schema,
            environment=environment,
            timeout_seconds=timeout_seconds,
        ).to_dict()


def suggest_columns(
    table: str,
    missing_column: str,
    database: str = "",
    schema: str = "",
    environment: str = "",
    timeout_seconds: int | None = None,
    limit: int = 5,
) -> dict:
    """Suggest similar metadata column names without changing SQL."""

    with runtime_lock:
        return query_service.suggest_columns(
            table=table,
            missing_column=missing_column,
            database=database,
            schema=schema,
            environment=environment,
            timeout_seconds=timeout_seconds,
            limit=limit,
        ).to_dict()
