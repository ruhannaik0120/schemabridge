"""Metadata-oriented MCP tools for the MCP server.

These wrappers expose database discovery capabilities while leaving all
connector communication and response shaping to the service layer.
"""

from tools.service_routing import invoke_query_service


def list_databases(
    environment: str = "",
    timeout_seconds: int | None = None,
    *,
    profile_id: str | None = None,
) -> dict:
    """Return all databases for the selected connector."""

    return invoke_query_service(
        profile_id=profile_id,
        tool_name="list_databases",
        operation=lambda service: service.list_databases(
            environment=environment,
            timeout_seconds=timeout_seconds,
        ),
    )


def list_tables(
    database: str = "",
    schema: str = "",
    environment: str = "",
    timeout_seconds: int | None = None,
    *,
    profile_id: str | None = None,
) -> dict:
    """Return the tables for a database, optionally filtered by schema."""

    return invoke_query_service(
        profile_id=profile_id,
        tool_name="list_tables",
        operation=lambda service: service.list_tables(
            database=database,
            schema=schema,
            environment=environment,
            timeout_seconds=timeout_seconds,
        ),
    )


def describe_table(
    database: str = "",
    table: str = "",
    schema: str = "",
    environment: str = "",
    timeout_seconds: int | None = None,
    *,
    profile_id: str | None = None,
) -> dict:
    """Return column metadata for a specific table."""

    return invoke_query_service(
        profile_id=profile_id,
        tool_name="describe_table",
        operation=lambda service: service.describe_table(
            database=database,
            table=table,
            schema=schema,
            environment=environment,
            timeout_seconds=timeout_seconds,
        ),
    )


def suggest_columns(
    table: str,
    missing_column: str,
    database: str = "",
    schema: str = "",
    environment: str = "",
    timeout_seconds: int | None = None,
    limit: int = 5,
    *,
    profile_id: str | None = None,
) -> dict:
    """Suggest similar metadata column names without changing SQL."""

    return invoke_query_service(
        profile_id=profile_id,
        tool_name="suggest_columns",
        operation=lambda service: service.suggest_columns(
            table=table,
            missing_column=missing_column,
            database=database,
            schema=schema,
            environment=environment,
            timeout_seconds=timeout_seconds,
            limit=limit,
        ),
    )
