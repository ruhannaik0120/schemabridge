"""PostgreSQL implementation of the database connector interface."""

from __future__ import annotations

import contextlib
import re
from typing import Any

from config import Config, ConfigError, ConnectionConfig
from connectors.base import DatabaseConnector, unique_column_names


class PostgreSQLConnector(DatabaseConnector):
    """Connector implementation for PostgreSQL via psycopg."""

    def _driver(self):
        """Load the optional PostgreSQL driver only when selected."""
        # Driver imports remain inside connectors to preserve the architecture
        # boundary checked by tests/test_architecture.py.
        try:
            import psycopg  # type: ignore
        except ImportError as exc:
            raise ConfigError("Install psycopg[binary] to use the PostgreSQL connector.") from exc
        return psycopg

    def _profile(self) -> ConnectionConfig:
        """Return the active neutral profile after checking host configuration."""
        profile = Config.connection_config()
        if not profile.host:
            raise ConfigError("DB_HOST is required for the PostgreSQL connector.")
        return profile

    def _normalize_database(self, database: str | None, fallback: str) -> str:
        """Select an explicit database, configured default, or postgres."""
        return (database or fallback or "postgres").strip() or "postgres"

    def _connection_kwargs(
        self,
        profile: ConnectionConfig,
        database: str,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Translate framework configuration into psycopg arguments."""
        options = dict(profile.connection_options or {})
        port = int(options.pop("port", 5432))
        existing_server_options = str(options.pop("options", "")).strip()
        effective_timeout = timeout_seconds if timeout_seconds is not None else profile.timeout_seconds
        statement_timeout = f"-c statement_timeout={effective_timeout * 1000}"
        kwargs: dict[str, Any] = {
            "host": profile.host,
            "port": port,
            "dbname": database,
            "user": profile.username or None,
            "password": profile.password or None,
            "connect_timeout": effective_timeout,
            "options": f"{existing_server_options} {statement_timeout}".strip(),
        }
        kwargs.update(options)
        return {key: value for key, value in kwargs.items() if value is not None}

    def _row_limit_sql(self, sql: str, max_rows: int) -> str:
        """Apply the configured result cap to row-returning PostgreSQL statements."""
        normalized_sql = sql.strip().rstrip(";")
        if not re.match(r"(?is)^\s*SELECT\b", normalized_sql):
            return normalized_sql
        # PostgreSQL and MySQL share LIMIT syntax, but keep this logic local so
        # future dialect behavior cannot leak into the service layer.
        limit_match = re.search(r"\bLIMIT\s+(\d+)\b", normalized_sql, flags=re.I)
        if limit_match:
            safe_limit = min(int(limit_match.group(1)), max_rows)
            return normalized_sql[: limit_match.start(1)] + str(safe_limit) + normalized_sql[limit_match.end(1) :]
        return f"{normalized_sql} LIMIT {max_rows}"

    def _fetch_rows(self, cursor, max_rows: int | None = None) -> dict[str, Any]:
        """Convert driver tuples into JSON-ready dictionaries by column name."""
        columns = unique_column_names([column.name for column in cursor.description]) if cursor.description else []
        raw_rows = cursor.fetchmany(max_rows) if columns and max_rows and hasattr(cursor, "fetchmany") else cursor.fetchall() if columns else []
        rows = [dict(zip(columns, row)) for row in raw_rows[:max_rows] if columns] if max_rows else [dict(zip(columns, row)) for row in raw_rows]
        return {"columns": columns, "rows": rows}

    def connect(self, database: str | None = None, timeout_seconds: int | None = None) -> Any:
        """Open a PostgreSQL connection with the active profile and timeout."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        kwargs = self._connection_kwargs(profile, target_database, timeout_seconds)
        return self._driver().connect(**kwargs)

    @contextlib.contextmanager
    def _connection(self, database: str | None = None, timeout_seconds: int | None = None):
        """Yield an operation-scoped connection and always close it."""
        connection = self.connect(database=database, timeout_seconds=timeout_seconds)
        try:
            yield connection
        finally:
            connection.close()

    def test_connection(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Verify connectivity and return safe PostgreSQL server metadata."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    inet_server_addr()::text AS server_name,
                    version() AS version,
                    current_user AS logged_in_user,
                    now() AT TIME ZONE 'UTC' AS utc_time
                """
            )
            snapshot = self._fetch_rows(cursor)
            cursor.close()
        return {
            "connector_type": self.__class__.__name__,
            "db_type": profile.db_type,
            "database": target_database,
            "connection_status": "connected",
            "server_information": snapshot["rows"][0] if snapshot["rows"] else {},
        }

    def health_check(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Reuse the lightweight connection test as the health check."""
        return self.test_connection(database=database, timeout_seconds=timeout_seconds)

    def list_databases(self, timeout_seconds: int | None = None) -> dict[str, Any]:
        """List non-template databases visible to the active PostgreSQL role."""
        profile = self._profile()
        with self._connection(database="postgres", timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT datname AS name
                FROM pg_database
                WHERE datistemplate = false
                ORDER BY datname
                """
            )
            payload = self._fetch_rows(cursor)
            cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "count": len(payload["rows"]), "databases": payload["rows"]}

    def list_tables(self, database: str | None = None, schema: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """List tables and views for the requested PostgreSQL schema scope."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        target_schema = schema or "public"
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT table_schema AS TABLE_SCHEMA, table_name AS TABLE_NAME, table_type AS TABLE_TYPE
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
                """,
                (target_schema,),
            )
            payload = self._fetch_rows(cursor)
            cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "database": target_database, "schema": target_schema, "count": len(payload["rows"]), "tables": payload["rows"]}

    def describe_table(self, database: str | None = None, table: str | None = None, schema: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Return ordered column definitions for one PostgreSQL table."""
        if not table:
            raise ConfigError("Table name is required.")
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        target_schema = schema or "public"
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT column_name AS COLUMN_NAME, data_type AS DATA_TYPE, is_nullable AS IS_NULLABLE,
                       character_maximum_length AS CHARACTER_MAXIMUM_LENGTH, ordinal_position AS ORDINAL_POSITION
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (target_schema, table),
            )
            payload = self._fetch_rows(cursor)
            cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "database": target_database, "schema": target_schema, "table": table, "column_count": len(payload["rows"]), "columns": payload["rows"]}

    def execute_query(self, query: str, *, database: str | None = None, timeout_seconds: int | None = None, max_rows: int | None = None) -> Any:
        """Execute validated SQL and normalize read or committed write output."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        limited_query = self._row_limit_sql(query, max_rows or profile.max_rows)
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(limited_query)
            payload = self._fetch_rows(cursor, max_rows or profile.max_rows)
            rows_affected = cursor.rowcount if cursor.description is None else len(payload["rows"])
            # psycopg opens a transaction automatically. Commit every successful
            # execution so DML with RETURNING is not rolled back on close.
            conn.commit()
            cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "database": target_database, "columns": payload["columns"], "rows": payload["rows"], "rows_affected": rows_affected}

    def close(self) -> None:
        """Satisfy the connector contract; connections are already per-call."""
        return None


Connector = PostgreSQLConnector
