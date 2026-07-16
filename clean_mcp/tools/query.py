"""Query-oriented MCP wrappers for approved database command execution.

The wrappers contain no SQL or driver logic. They forward MCP arguments to the
service layer and serialize its standard response contract.
"""

from services import query_service
from services.runtime_state import runtime_lock


def execute_query(
    sql: str = "",
    query: str = "",
    database: str = "",
    schema: str = "",
    environment: str = "",
    timeout_seconds: int | None = None,
    max_rows: int | None = None,
) -> dict:
    """Execute approved SQL against the active profile's configured database."""

    with runtime_lock:
        return query_service.execute_query(
            sql=sql,
            query=query,
            database=database,
            schema=schema,
            environment=environment,
            timeout_seconds=timeout_seconds,
            max_rows=max_rows,
        ).to_dict()


def execute_select_query(
    sql: str = "",
    query: str = "",
    database: str = "",
    schema: str = "",
    environment: str = "",
    timeout_seconds: int | None = None,
    max_rows: int | None = None,
) -> dict:
    """Deprecated compatibility alias for the generic execution tool."""

    with runtime_lock:
        return query_service.execute_select_query(
            sql=sql,
            query=query,
            database=database,
            schema=schema,
            environment=environment,
            timeout_seconds=timeout_seconds,
            max_rows=max_rows,
        ).to_dict()
