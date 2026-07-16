"""Database connector abstraction used by the MCP runtime."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from models.connection_profile import ConnectionProfile


def unique_column_names(columns: list[object]) -> list[str]:
    """Return deterministic JSON keys without dropping duplicate result columns."""

    used: set[str] = set()
    normalized: list[str] = []
    for raw_name in columns:
        base_name = str(raw_name)
        candidate = base_name
        suffix = 2
        while candidate in used:
            candidate = f"{base_name}_{suffix}"
            suffix += 1
        used.add(candidate)
        normalized.append(candidate)
    return normalized


class DatabaseConnector(ABC):
    """Stable contract implemented by every database backend.

    The MCP tools and service layer depend on this interface rather than a
    vendor driver. Consequently, adding another backend normally means adding
    one connector implementation and one factory registration while leaving
    the shared execution, logging, response, and profile-switching code intact.
    """

    profile_db_type: ClassVar[str] = ""

    def __init__(self, *, profile: ConnectionProfile | None = None) -> None:
        """Bind an optional immutable profile without exposing its values."""

        if profile is not None:
            from models.connection_profile import ConnectionProfile

            if not isinstance(profile, ConnectionProfile):
                raise TypeError("Injected profile must be a ConnectionProfile.")
            if not self.profile_db_type or profile.db_type != self.profile_db_type:
                raise ValueError("Injected profile does not match the connector type.")
        self._connection_profile = profile

    def matches_profile(self, profile: object) -> bool:
        """Return whether this connector is bound to the supplied profile."""

        if self._connection_profile is None:
            return False
        from models.connection_profile import ConnectionProfile

        return isinstance(profile, ConnectionProfile) and self._connection_profile == profile

    @abstractmethod
    def connect(self, database: str | None = None, timeout_seconds: int | None = None) -> Any:
        """Open a connection to the target database."""

    @abstractmethod
    def test_connection(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Run a lightweight connection check and return diagnostics."""

    @abstractmethod
    def health_check(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Return a health/diagnostic summary for the connector."""

    @abstractmethod
    def list_databases(self, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Return a list of available databases for the current connector."""

    @abstractmethod
    def list_tables(
        self,
        database: str | None = None,
        schema: str | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Return the tables available in a database."""

    @abstractmethod
    def describe_table(
        self,
        database: str | None = None,
        table: str | None = None,
        schema: str | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Return column metadata for a table."""

    @abstractmethod
    def execute_query(
        self,
        query: str,
        *,
        database: str | None = None,
        timeout_seconds: int | None = None,
        max_rows: int | None = None,
    ) -> Any:
        """Execute a database query and return the result payload."""

    @abstractmethod
    def close(self) -> None:
        """Close any open resources held by the connector."""
