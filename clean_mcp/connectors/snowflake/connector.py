"""Snowflake implementation of the database connector interface."""

from __future__ import annotations

import contextlib
import re
from typing import Any

from config import Config, ConfigError, ConnectionConfig
from connectors.base import DatabaseConnector, unique_column_names


class SnowflakeConnector(DatabaseConnector):
    """Connector implementation for Snowflake via snowflake-connector-python."""

    def _driver(self):
        """Load the optional Snowflake driver only when selected."""
        # Snowflake is an optional and comparatively heavy dependency, so it is
        # imported only when this connector is actually selected.
        try:
            import snowflake.connector  # type: ignore
        except ImportError as exc:
            raise ConfigError("Install snowflake-connector-python to use the Snowflake connector.") from exc
        return snowflake.connector

    def _profile(self) -> ConnectionConfig:
        """Return the active profile after checking cloud account requirements."""
        profile = Config.connection_config()
        if not profile.host:
            raise ConfigError("DB_HOST is required for the Snowflake connector and should contain the account identifier.")
        if not profile.username:
            raise ConfigError("DB_USERNAME is required for the Snowflake connector.")
        return profile

    def _normalize_database(self, database: str | None, fallback: str) -> str:
        """Select an explicit database or the configured Snowflake default."""
        return (database or fallback or "").strip()

    def _connection_kwargs(self, profile: ConnectionConfig, database: str | None = None) -> dict[str, Any]:
        """Translate neutral settings into Snowflake account/session arguments."""
        options = dict(profile.connection_options or {})
        schema = options.pop("schema", None)
        kwargs: dict[str, Any] = {
            "account": profile.host,
            "user": profile.username,
            "password": profile.password,
            "login_timeout": profile.timeout_seconds,
        }
        if database:
            kwargs["database"] = database
        if schema:
            kwargs["schema"] = schema
        kwargs.update(options)
        return kwargs

    def _row_limit_sql(self, sql: str, max_rows: int) -> str:
        """Apply the configured result cap to row-returning Snowflake statements."""
        normalized_sql = sql.strip().rstrip(";")
        if not re.match(r"(?is)^\s*SELECT\b", normalized_sql):
            return normalized_sql
        # Preserve either supported row-limiting form before appending LIMIT.
        limit_match = re.search(r"\bLIMIT\s+(\d+)\b", normalized_sql, flags=re.I)
        if limit_match:
            safe_limit = min(int(limit_match.group(1)), max_rows)
            return normalized_sql[: limit_match.start(1)] + str(safe_limit) + normalized_sql[limit_match.end(1) :]
        fetch_match = re.search(r"\bFETCH\s+NEXT\s+(\d+)\s+ROWS\b", normalized_sql, flags=re.I)
        if fetch_match:
            safe_limit = min(int(fetch_match.group(1)), max_rows)
            return normalized_sql[: fetch_match.start(1)] + str(safe_limit) + normalized_sql[fetch_match.end(1) :]
        return f"{normalized_sql} LIMIT {max_rows}"

    def _execute(self, cursor: Any, query: str, params: Any = None, timeout_seconds: int | None = None) -> Any:
        """Execute one Snowflake statement with the framework command timeout."""

        effective_timeout = timeout_seconds or self._profile().timeout_seconds
        if params is None:
            return cursor.execute(query, timeout=effective_timeout)
        return cursor.execute(query, params, timeout=effective_timeout)

    def _fetch_rows(self, cursor, max_rows: int | None = None) -> dict[str, Any]:
        """Convert Snowflake tuples into JSON-ready dictionaries."""
        columns = unique_column_names([column[0] for column in cursor.description]) if cursor.description else []
        raw_rows = cursor.fetchmany(max_rows) if columns and max_rows and hasattr(cursor, "fetchmany") else cursor.fetchall() if columns else []
        rows = [dict(zip(columns, row)) for row in raw_rows[:max_rows] if columns] if max_rows else [dict(zip(columns, row)) for row in raw_rows]
        return {"columns": columns, "rows": rows}

    def connect(self, database: str | None = None, timeout_seconds: int | None = None) -> Any:
        """Open a Snowflake session using the active account profile."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        kwargs = self._connection_kwargs(profile, target_database or None)
        if timeout_seconds is not None:
            kwargs["login_timeout"] = timeout_seconds
        return self._driver().connect(**kwargs)

    @contextlib.contextmanager
    def _connection(self, database: str | None = None, timeout_seconds: int | None = None):
        """Yield an operation-scoped cloud session and always close it."""
        connection = self.connect(database=database, timeout_seconds=timeout_seconds)
        try:
            yield connection
        finally:
            connection.close()

    def test_connection(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Verify the session and return non-secret account context."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        with self._connection(database=target_database or None, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            try:
                self._execute(
                    cursor,
                    """
                    SELECT
                        CURRENT_ACCOUNT() AS server_name,
                        CURRENT_VERSION() AS version,
                        CURRENT_USER() AS logged_in_user,
                        CURRENT_TIMESTAMP() AS utc_time
                    """,
                    timeout_seconds=timeout_seconds,
                )
                snapshot = self._fetch_rows(cursor)
            finally:
                cursor.close()
        return {
            "connector_type": self.__class__.__name__,
            "db_type": profile.db_type,
            "database": target_database,
            "connection_status": "connected",
            "server_information": snapshot["rows"][0] if snapshot["rows"] else {},
        }

    def health_check(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Reuse the lightweight session test as the Snowflake health check."""
        return self.test_connection(database=database, timeout_seconds=timeout_seconds)

    def list_databases(self, timeout_seconds: int | None = None) -> dict[str, Any]:
        """List databases visible to the active Snowflake role."""
        profile = self._profile()
        with self._connection(timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            try:
                self._execute(
                    cursor,
                    """
                    SELECT DATABASE_NAME AS name
                    FROM INFORMATION_SCHEMA.DATABASES
                    ORDER BY DATABASE_NAME
                    """,
                    timeout_seconds=timeout_seconds,
                )
                payload = self._fetch_rows(cursor)
            finally:
                cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "count": len(payload["rows"]), "databases": payload["rows"]}

    def list_tables(self, database: str | None = None, schema: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """List tables and views for the requested database/schema scope."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        if not target_database:
            raise ConfigError("Database name is required to list Snowflake tables.")
        target_schema = schema or str((profile.connection_options or {}).get("schema", "PUBLIC"))
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            try:
                self._execute(
                    cursor,
                    """
                    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                    """,
                    (target_schema.upper(),),
                    timeout_seconds=timeout_seconds,
                )
                payload = self._fetch_rows(cursor)
            finally:
                cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "database": target_database, "schema": target_schema, "count": len(payload["rows"]), "tables": payload["rows"]}

    def describe_table(self, database: str | None = None, table: str | None = None, schema: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Return ordered column definitions for one Snowflake table."""
        if not table:
            raise ConfigError("Table name is required.")
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        if not target_database:
            raise ConfigError("Database name is required to describe a Snowflake table.")
        target_schema = schema or str((profile.connection_options or {}).get("schema", "PUBLIC"))
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            try:
                self._execute(
                    cursor,
                    """
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """,
                    (target_schema.upper(), table.upper()),
                    timeout_seconds=timeout_seconds,
                )
                payload = self._fetch_rows(cursor)
            finally:
                cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "database": target_database, "schema": target_schema, "table": table, "column_count": len(payload["rows"]), "columns": payload["rows"]}

    def execute_query(self, query: str, *, database: str | None = None, timeout_seconds: int | None = None, max_rows: int | None = None) -> Any:
        """Execute validated SQL and normalize read or committed write output."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        limited_query = self._row_limit_sql(query, max_rows or profile.max_rows)
        with self._connection(database=target_database or None, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, limited_query, timeout_seconds=timeout_seconds)
                payload = self._fetch_rows(cursor, max_rows or profile.max_rows)
                rows_affected = cursor.rowcount if cursor.description is None else len(payload["rows"])
                if cursor.description is None:
                    # Commit explicitly so behavior remains consistent even if
                    # Snowflake autocommit settings are changed by a profile.
                    conn.commit()
            finally:
                cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "database": target_database, "columns": payload["columns"], "rows": payload["rows"], "rows_affected": rows_affected}

    def close(self) -> None:
        """Satisfy the connector contract; sessions are already per-call."""
        return None


Connector = SnowflakeConnector
