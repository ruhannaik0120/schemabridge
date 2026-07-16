"""Connection-oriented MCP tools for the MCP server.

These wrappers expose health and connection checks without embedding connector
logic directly in the MCP entrypoint.
"""

from services import query_service
from services.runtime_state import runtime_lock


def test_connection(
    environment: str = "",
    database: str = "",
    timeout_seconds: int | None = None,
) -> dict:
    """Verify connectivity and return server metadata.

    Args:
        environment: Retained for compatibility; connector selection now comes from DB_TYPE.
        database: Database used for the connection check.
        timeout_seconds: Optional command timeout override.

    Returns:
        A structured response dictionary.
    """

    with runtime_lock:
        return query_service.test_connection(
            environment=environment,
            database=database,
            timeout_seconds=timeout_seconds,
        ).to_dict()


def health(environment: str = "", timeout_seconds: int | None = None) -> dict:
    """Return a diagnostic health summary for the active connector."""

    with runtime_lock:
        return query_service.health(environment=environment, timeout_seconds=timeout_seconds).to_dict()
