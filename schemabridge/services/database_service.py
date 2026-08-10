"""Profile-bound database access used by SchemaBridge workflows."""

from __future__ import annotations

from dataclasses import dataclass
import os
from threading import Lock
from typing import Any

from schemabridge.config import ConfigError
from schemabridge.connectors.base import DatabaseConnector
from schemabridge.connectors.factory import ConnectorFactory
from schemabridge.logger import logger
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.services.profile_registry import ProfileRegistry
from schemabridge.validation.sql_guard import validate_query


class DatabaseAccessError(RuntimeError):
    """Raised when a profile-bound database operation cannot be completed safely."""


@dataclass(frozen=True, slots=True)
class DatabaseExecutionResult:
    """Internal normalized result for workflow-controlled SQL execution."""

    columns: tuple[str, ...]
    rows: tuple[object, ...]
    rows_affected: int | None


class DatabaseService:
    """Resolve one immutable profile and mediate its connector operations."""

    def __init__(
        self,
        profile: ConnectionProfile,
        connector: object | None = None,
    ) -> None:
        if not isinstance(profile, ConnectionProfile):
            raise TypeError("profile must be a ConnectionProfile.")
        if (
            isinstance(connector, DatabaseConnector)
            and not connector.matches_profile(profile)
        ):
            raise ConfigError("Connector is not bound to the supplied connection profile")
        self.profile = profile
        self.connector = (
            ConnectorFactory.create_for_profile(profile)
            if connector is None
            else connector
        )

    def _effective_timeout(self, timeout_seconds: int | None) -> int:
        if isinstance(timeout_seconds, bool) or (
            timeout_seconds is not None and timeout_seconds <= 0
        ):
            raise DatabaseAccessError("timeout_seconds must be a positive integer.")
        return min(
            timeout_seconds or self.profile.timeout_seconds,
            self.profile.timeout_seconds,
        )

    def _effective_row_limit(self, max_rows: int | None) -> int:
        if isinstance(max_rows, bool) or (max_rows is not None and max_rows <= 0):
            raise DatabaseAccessError("max_rows must be a positive integer.")
        return min(max_rows or self.profile.max_rows, self.profile.max_rows)

    def _execution_database(self, database: str | None) -> str:
        requested = (database or "").strip()
        configured = self.profile.database.strip()
        if requested and configured and requested.casefold() != configured.casefold():
            raise DatabaseAccessError(
                "Database operations must use the database configured by the selected profile."
            )
        return requested or configured

    def migration_execution_context(
        self,
        timeout_seconds: int | None = None,
    ) -> dict[str, object]:
        """Return credential-free settings required by approval-gated execution."""

        return {
            "profile_id": self.profile.profile_id,
            "db_type": self.profile.db_type,
            "database": self.profile.database,
            "timeout_seconds": self._effective_timeout(timeout_seconds),
            "write_enabled": self.profile.write_enabled,
            "connector_type": self.profile.db_type,
        }

    def validation_execution_context(
        self,
        timeout_seconds: int | None = None,
    ) -> dict[str, object]:
        """Return credential-free settings required by read-only validation."""

        return {
            "profile_id": self.profile.profile_id,
            "db_type": self.profile.db_type,
            "timeout_seconds": self._effective_timeout(timeout_seconds),
        }

    def get_table_metadata(
        self,
        *,
        database: str | None,
        schema: str,
        table: str,
    ):
        """Discover one table through the connector bound to this profile."""

        try:
            discover = getattr(self.connector, "get_table_metadata")
            return discover(
                database=database,
                schema=schema,
                table=table,
                timeout_seconds=self.profile.timeout_seconds,
            )
        except Exception:
            logger.error("Profile-bound schema discovery failed")
            raise DatabaseAccessError(
                "Schema discovery failed for the selected connection profile."
            ) from None

    @staticmethod
    def _parameters(
        parameters: tuple[object, ...] | list[object] | None,
    ) -> tuple[object, ...]:
        if parameters is None:
            return ()
        if isinstance(parameters, (str, bytes, dict)) or not isinstance(
            parameters, (tuple, list)
        ):
            raise DatabaseAccessError("Query parameters must be a sequence.")
        return tuple(parameters)

    def _execute_controlled_query(
        self,
        *,
        sql: str,
        parameters: tuple[object, ...] | list[object] | None,
        database: str | None,
        timeout_seconds: int | None,
        max_rows: int | None,
        read_only: bool,
        require_write_enabled: bool,
    ) -> DatabaseExecutionResult:
        try:
            if not isinstance(sql, str) or not sql.strip():
                raise DatabaseAccessError("A non-empty SQL statement is required.")
            statement = sql.strip()
            normalized = statement.lstrip().upper()
            if read_only and not normalized.startswith("SELECT"):
                raise DatabaseAccessError("Validation execution requires a SELECT statement.")
            if require_write_enabled and self.profile.write_enabled is not True:
                raise DatabaseAccessError("The selected profile is not write enabled.")
            valid, _reason = validate_query(statement, self.profile.db_type)
            if not valid:
                raise DatabaseAccessError(
                    "The generated statement was rejected by the SQL safety policy."
                )
            row_limit = self._effective_row_limit(max_rows)
            target_database = self._execution_database(database)
            bound_parameters = self._parameters(parameters)
            kwargs: dict[str, Any] = {
                "database": target_database,
                "timeout_seconds": self._effective_timeout(timeout_seconds),
                "max_rows": row_limit,
            }
            if bound_parameters:
                kwargs["parameters"] = bound_parameters
            payload = self.connector.execute_query(statement, **kwargs)
            if not isinstance(payload, dict):
                raise DatabaseAccessError("The database connector returned an invalid result.")
            columns = payload.get("columns", [])
            rows = payload.get("rows", [])
            if not isinstance(columns, (list, tuple)) or not isinstance(
                rows, (list, tuple)
            ):
                raise DatabaseAccessError("The database connector returned an invalid result.")
            rows_affected = payload.get("rows_affected")
            if isinstance(rows_affected, bool) or (
                rows_affected is not None and not isinstance(rows_affected, int)
            ):
                rows_affected = None
            return DatabaseExecutionResult(
                columns=tuple(str(column) for column in columns),
                rows=tuple(rows),
                rows_affected=rows_affected,
            )
        except DatabaseAccessError:
            raise
        except Exception:
            logger.error("Profile-bound database execution failed")
            raise DatabaseAccessError(
                "Database operation failed for the selected connection profile."
            ) from None

    def execute_validation_query(
        self,
        *,
        sql: str,
        parameters: tuple[object, ...] | list[object] | None = None,
        timeout_seconds: int | None = None,
    ) -> DatabaseExecutionResult:
        """Execute one generated read-only validation query."""

        return self._execute_controlled_query(
            sql=sql,
            parameters=parameters,
            database=None,
            timeout_seconds=timeout_seconds,
            max_rows=None,
            read_only=True,
            require_write_enabled=False,
        )

    def execute_migration_statement(
        self,
        *,
        sql: str,
        parameters: tuple[object, ...] | list[object] | None = None,
        database: str | None = None,
        timeout_seconds: int | None = None,
    ) -> DatabaseExecutionResult:
        """Execute one compiler-produced statement through a write-enabled profile."""

        return self._execute_controlled_query(
            sql=sql,
            parameters=parameters,
            database=database,
            timeout_seconds=timeout_seconds,
            max_rows=1,
            read_only=False,
            require_write_enabled=True,
        )

    def close(self) -> None:
        """Release connector-owned resources without exposing profile details."""

        self.connector.close()


_PROFILE_REGISTRY: ProfileRegistry | None = None
_DATABASE_SERVICES: dict[str, DatabaseService] = {}
_DATABASE_SERVICE_LOCK = Lock()


def _load_profile_registry() -> ProfileRegistry:
    return ProfileRegistry.from_json(os.getenv("DB_PROFILES_JSON", ""))


def get_database_service(profile_id: str) -> DatabaseService:
    """Return a cached service for an explicitly named connection profile."""

    global _PROFILE_REGISTRY
    with _DATABASE_SERVICE_LOCK:
        registry = _PROFILE_REGISTRY
        if registry is None:
            registry = _load_profile_registry()
            _PROFILE_REGISTRY = registry
        profile = registry.resolve(profile_id)
        key = profile.normalized_profile_id
        service = _DATABASE_SERVICES.get(key)
        if service is None:
            try:
                service = DatabaseService(profile)
            except Exception:
                logger.error("Failed to create a profile-bound database service")
                raise ConfigError(
                    "Unable to create a service for the selected connection profile"
                ) from None
            _DATABASE_SERVICES[key] = service
        return service


def reset_database_services(profile_id: str | None = None) -> None:
    """Detach and close one or all cached profile-bound services."""

    global _PROFILE_REGISTRY
    detached: list[DatabaseService] = []
    with _DATABASE_SERVICE_LOCK:
        if profile_id is None:
            detached = list(_DATABASE_SERVICES.values())
            _DATABASE_SERVICES.clear()
            _PROFILE_REGISTRY = None
        else:
            registry = _PROFILE_REGISTRY
            if registry is None:
                registry = _load_profile_registry()
                _PROFILE_REGISTRY = registry
            profile = registry.resolve(profile_id)
            service = _DATABASE_SERVICES.pop(profile.normalized_profile_id, None)
            if service is not None:
                detached.append(service)
    for service in detached:
        try:
            service.close()
        except Exception:
            logger.error("Failed to close a cached profile-bound database service")


__all__ = [
    "DatabaseAccessError",
    "DatabaseExecutionResult",
    "DatabaseService",
    "get_database_service",
    "reset_database_services",
]
