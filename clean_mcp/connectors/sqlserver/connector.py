"""SQL Server implementation of the database connector interface."""

from __future__ import annotations

import contextlib
import re
from typing import Any

from config import Config, ConfigError, ConnectionConfig
from connectors.base import DatabaseConnector, unique_column_names
from logger import logger


class SQLServerConnector(DatabaseConnector):
    """Connector implementation for SQL Server via pyodbc."""

    @staticmethod
    def _odbc_value(value: object) -> str:
        """Brace-escape an ODBC value so delimiters cannot add attributes."""

        return "{" + str(value).replace("}", "}}") + "}"

    def _driver(self):
        """Load pyodbc only when the SQL Server backend is selected."""
        # Lazy loading prevents an absent ODBC installation from breaking other
        # connectors during MCP server startup.
        try:
            import pyodbc  # type: ignore
        except ImportError as exc:
            raise ConfigError("Install pyodbc to use the SQL Server connector.") from exc
        pyodbc.pooling = True
        return pyodbc

    @property
    def odbc_version(self) -> str:
        """Return the installed pyodbc version for diagnostics."""
        return self._driver().version

    def _profile(self) -> ConnectionConfig:
        """Return the active profile after checking SQL Server requirements."""
        profile = Config.connection_config()
        if not profile.host:
            raise ConfigError("DB_HOST is required for the SQL Server connector.")
        if not profile.database:
            raise ConfigError("DB_DATABASE is required for the SQL Server connector.")
        return profile

    def _normalize_database(self, database: str | None, fallback: str) -> str:
        """Select an explicit database or the configured default."""
        return (database or fallback or "master").strip() or "master"

    def _connection_options(self, profile: ConnectionConfig) -> str:
        """Build secure ODBC options from generic and SQL-specific settings."""
        options = dict(profile.connection_options or {})
        driver = str(options.pop("driver", "ODBC Driver 18 for SQL Server")).strip() or "ODBC Driver 18 for SQL Server"
        parts = [f"DRIVER={self._odbc_value(driver)}", f"SERVER={self._odbc_value(profile.host)}"]
        # Explicit credentials take precedence; otherwise local Windows trusted
        # authentication supports a password-free developer setup.
        if bool(profile.username) != bool(profile.password):
            raise ConfigError("DB_USERNAME and DB_PASSWORD must either both be set or both be empty.")
        if profile.username and profile.password:
            parts.extend(
                [
                    f"UID={self._odbc_value(profile.username)}",
                    f"PWD={self._odbc_value(profile.password)}",
                ]
            )
        else:
            parts.append(f"Trusted_Connection={self._odbc_value('yes')}")

        # ODBC 18 enables encryption by default. Local SQL Express instances
        # normally lack a trusted TLS certificate, while remote environments
        # should remain encrypted unless a profile explicitly says otherwise.
        server_name = profile.host.split("\\", 1)[0].split(",", 1)[0].strip().lower()
        is_local = server_name in {"localhost", ".", "(local)", "127.0.0.1", "::1"}
        normalized_options = {str(key).lower(): value for key, value in options.items()}
        if "encrypt" not in normalized_options:
            parts.append(f"Encrypt={self._odbc_value('no' if is_local else 'yes')}")
        if "trustservercertificate" not in normalized_options:
            parts.append(f"TrustServerCertificate={self._odbc_value('yes' if is_local else 'no')}")

        for key, value in options.items():
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9 _-]*", str(key)):
                raise ConfigError(f"Invalid ODBC connection option name: {key!r}.")
            rendered_value = "yes" if value is True else "no" if value is False else value
            parts.append(f"{key}={self._odbc_value(rendered_value)}")
        return ";".join(parts) + ";"

    def _build_connection_string(self, profile: ConnectionConfig, database: str) -> str:
        """Compose the complete ODBC connection string for one database."""
        return self._connection_options(profile) + f"DATABASE={self._odbc_value(database)};"

    def _row_limit_sql(self, sql: str, max_rows: int) -> str:
        """Apply SQL Server TOP limits to eligible row-returning statements."""
        normalized_sql = sql.strip()
        upper_sql = normalized_sql.upper()

        # SQL Server uses TOP/FETCH rather than LIMIT. Existing limits are kept,
        # while ordinary SELECT statements receive a safe TOP cap.
        top_match = re.search(r"\bTOP\s*\(?\s*(\d+)\s*\)?", normalized_sql, flags=re.I)
        if top_match:
            safe_limit = min(int(top_match.group(1)), max_rows)
            return normalized_sql[: top_match.start(1)] + str(safe_limit) + normalized_sql[top_match.end(1) :]
        fetch_match = re.search(r"\bFETCH\s+NEXT\s+(\d+)\s+ROWS\b", normalized_sql, flags=re.I)
        if fetch_match:
            safe_limit = min(int(fetch_match.group(1)), max_rows)
            return normalized_sql[: fetch_match.start(1)] + str(safe_limit) + normalized_sql[fetch_match.end(1) :]
        if upper_sql.startswith("WITH"):
            return normalized_sql

        select_match = re.match(r"(?is)^\s*SELECT\s+(DISTINCT\s+)?", normalized_sql)
        if select_match:
            prefix = select_match.group(0)
            distinct = select_match.group(1) or ""
            remainder = normalized_sql[len(prefix):].lstrip()
            return f"SELECT {distinct}TOP {max_rows} {remainder}"

        return normalized_sql

    def connect(self, database: str | None = None, timeout_seconds: int | None = None) -> Any:
        """Open an ODBC connection using the active profile and timeout."""
        profile = self._profile()
        normalized_database = self._normalize_database(database, profile.database)
        conn_str = self._build_connection_string(profile, normalized_database)
        command_timeout = timeout_seconds if timeout_seconds is not None else profile.timeout_seconds

        logger.info(
            "Connecting to database.",
            extra={
                "tool": "connector.connect",
                "db_type": profile.db_type,
                "database": normalized_database,
                "execution_time_ms": None,
            },
        )

        driver = self._driver()
        try:
            connection = driver.connect(conn_str, timeout=command_timeout)
            connection.autocommit = True
            # pyodbc's connection timeout controls subsequent statement execution.
            connection.timeout = command_timeout
            logger.info(
                "Connection established successfully.",
                extra={
                    "tool": "connector.connect",
                    "db_type": profile.db_type,
                    "database": normalized_database,
                    "success": True,
                },
            )
            return connection
        except driver.Error:
            logger.exception(
                "Connection failed.",
                extra={
                    "tool": "connector.connect",
                    "db_type": profile.db_type,
                    "database": normalized_database,
                    "success": False,
                },
            )
            raise

    @contextlib.contextmanager
    def _connection(self, database: str | None = None, timeout_seconds: int | None = None):
        """Yield an operation-scoped ODBC connection and always close it."""
        profile = self._profile()
        normalized_database = self._normalize_database(database, profile.database)
        connection = self.connect(database=normalized_database, timeout_seconds=timeout_seconds)

        try:
            yield connection
        finally:
            connection.close()
            logger.info(
                "Connection closed.",
                extra={
                    "tool": "connector.connection",
                    "db_type": profile.db_type,
                    "database": normalized_database,
                    "success": True,
                },
            )

    def _fetch_rows(self, cursor, max_rows: int | None = None) -> dict[str, Any]:
        """Convert ODBC rows into JSON-ready dictionaries by column name."""
        columns = unique_column_names([column[0] for column in cursor.description]) if cursor.description else []
        raw_rows = cursor.fetchmany(max_rows) if columns and max_rows and hasattr(cursor, "fetchmany") else cursor.fetchall() if columns else []
        rows = [dict(zip(columns, row)) for row in raw_rows[:max_rows] if columns] if max_rows else [dict(zip(columns, row)) for row in raw_rows]
        return {"columns": columns, "rows": rows}

    def test_connection(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Verify connectivity and return safe SQL Server metadata."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    @@SERVERNAME AS server_name,
                    @@VERSION AS version,
                    SYSTEM_USER AS logged_in_user,
                    GETUTCDATE() AS utc_time
                """
            )
            snapshot = self._fetch_rows(cursor)
        return {
            "connector_type": self.__class__.__name__,
            "db_type": profile.db_type,
            "database": target_database,
            "connection_status": "connected",
            "server_information": snapshot["rows"][0] if snapshot["rows"] else {},
            "driver_version": self.odbc_version,
        }

    def health_check(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Return SQL Server liveness information through the common contract."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        snapshot = self.test_connection(database=target_database, timeout_seconds=timeout_seconds)
        snapshot["environment"] = profile.db_type
        snapshot["database"] = target_database
        return snapshot

    def list_databases(self, timeout_seconds: int | None = None) -> dict[str, Any]:
        """List online databases visible to the active SQL Server login."""
        profile = self._profile()
        with self._connection(database="master", timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    name,
                    database_id,
                    create_date,
                    state_desc
                FROM sys.databases
                WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
                ORDER BY name
                """
            )
            payload = self._fetch_rows(cursor)
        return {
            "connector_type": self.__class__.__name__,
            "db_type": profile.db_type,
            "count": len(payload["rows"]),
            "databases": payload["rows"],
        }

    def list_tables(
        self,
        database: str | None = None,
        schema: str | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """List SQL Server tables, optionally restricted to one schema."""

        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            sql = """
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
            """
            params: list[str] = []
            if schema:
                sql += " AND TABLE_SCHEMA = ?"
                params.append(schema)
            sql += " ORDER BY TABLE_SCHEMA, TABLE_NAME"
            cursor.execute(sql, *params)
            payload = self._fetch_rows(cursor)
        return {
            "connector_type": self.__class__.__name__,
            "db_type": profile.db_type,
            "database": target_database,
            "schema": schema or "",
            "count": len(payload["rows"]),
            "tables": payload["rows"],
        }

    def describe_table(
        self,
        database: str | None = None,
        table: str | None = None,
        schema: str | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Return ordered information-schema columns for one SQL Server table."""

        if not table:
            raise ConfigError("Table name is required.")

        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            sql = """
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    CHARACTER_MAXIMUM_LENGTH,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
            """
            params: list[str] = [table]
            if schema:
                sql += " AND TABLE_SCHEMA = ?"
                params.append(schema)
            sql += " ORDER BY ORDINAL_POSITION"
            cursor.execute(sql, *params)
            payload = self._fetch_rows(cursor)

        return {
            "connector_type": self.__class__.__name__,
            "db_type": profile.db_type,
            "database": target_database,
            "schema": schema or "",
            "table": table,
            "column_count": len(payload["rows"]),
            "columns": payload["rows"],
        }

    def execute_query(
        self,
        query: str,
        *,
        database: str | None = None,
        timeout_seconds: int | None = None,
        max_rows: int | None = None,
    ) -> Any:
        """Execute validated SQL and normalize read or autocommitted write output."""

        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        limited_query = self._row_limit_sql(query, max_rows or profile.max_rows)
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(limited_query)
            payload = self._fetch_rows(cursor, max_rows or profile.max_rows)
            rows_affected = cursor.rowcount if cursor.description is None else len(payload["rows"])
        return {
            "connector_type": self.__class__.__name__,
            "db_type": profile.db_type,
            "database": target_database,
            "columns": payload["columns"],
            "rows": payload["rows"],
            "rows_affected": rows_affected,
        }

    def close(self) -> None:
        """Satisfy the connector contract; connections are already per-call."""
        return None


Connector = SQLServerConnector
