"""Snowflake implementation of the database connector interface."""

from __future__ import annotations

import contextlib
import re
from collections.abc import Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any, NoReturn

from config import Config, ConfigError, ConnectionConfig
from connectors.base import DatabaseConnector, unique_column_names
from connectors.discovery import (
    MalformedDiscoveryResultError,
    SchemaDiscoveryConnectionError,
    SchemaDiscoveryError,
    SchemaDiscoveryTimeoutError,
)
from connectors.snowflake import _discovery_queries as discovery_queries
from models.discovery import DatabaseObjectMetadata, DatabaseObjectType, SchemaMetadata
from normalizers._discovery_common import deduplicate_models
from normalizers.snowflake_discovery import normalize_snowflake_object, normalize_snowflake_schema

if TYPE_CHECKING:
    from models.connection_profile import ConnectionProfile


_SUPPORTED_DISCOVERY_OBJECT_TYPES = frozenset({
    DatabaseObjectType.TABLE,
    DatabaseObjectType.VIEW,
    DatabaseObjectType.MATERIALIZED_VIEW,
    DatabaseObjectType.EXTERNAL_TABLE,
    DatabaseObjectType.DYNAMIC_TABLE,
    DatabaseObjectType.UNKNOWN,
})

_SNOWFLAKE_OBJECT_TYPES = {
    "BASE TABLE": DatabaseObjectType.TABLE,
    "TEMPORARY TABLE": DatabaseObjectType.TABLE,
    "EXTERNAL TABLE": DatabaseObjectType.EXTERNAL_TABLE,
    "VIEW": DatabaseObjectType.VIEW,
    "MATERIALIZED VIEW": DatabaseObjectType.MATERIALIZED_VIEW,
}


class SnowflakeConnector(DatabaseConnector):
    """Connector implementation for Snowflake via snowflake-connector-python."""

    profile_db_type = "snowflake"

    def _driver(self):
        """Load the optional Snowflake driver only when selected."""
        # Snowflake is an optional and comparatively heavy dependency, so it is
        # imported only when this connector is actually selected.
        try:
            import snowflake.connector  # type: ignore
        except ImportError as exc:
            raise ConfigError("Install snowflake-connector-python to use the Snowflake connector.") from exc
        return snowflake.connector

    def _profile(self) -> ConnectionConfig | ConnectionProfile:
        """Return the active profile after checking cloud account requirements."""
        profile = self._connection_profile
        if profile is None:
            profile = Config.connection_config()
        if not profile.host:
            raise ConfigError("DB_HOST is required for the Snowflake connector and should contain the account identifier.")
        if not profile.username:
            raise ConfigError("DB_USERNAME is required for the Snowflake connector.")
        return profile

    def _normalize_database(self, database: str | None, fallback: str) -> str:
        """Select an explicit database or the configured Snowflake default."""
        return (database or fallback or "").strip()

    def _connection_kwargs(
        self,
        profile: ConnectionConfig | ConnectionProfile,
        database: str | None = None,
    ) -> dict[str, Any]:
        """Translate neutral settings into Snowflake account/session arguments."""
        options = (
            profile.connection_options_copy()
            if self._connection_profile is not None
            else dict(profile.connection_options or {})
        )
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

    @staticmethod
    def _validate_discovery_identifier(value: object, field_name: str) -> str:
        """Validate an exact identifier without changing caller-controlled text."""
        if isinstance(value, bool) or not isinstance(value, str):
            raise ConfigError(f"{field_name} must be a string.")
        if value == "":
            raise ConfigError(f"{field_name} must not be empty.")
        if "\x00" in value:
            raise ConfigError(f"{field_name} must not contain NUL characters.")
        return value

    @staticmethod
    def _validate_discovery_timeout(timeout_seconds: int | None) -> None:
        if timeout_seconds is None:
            return
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int):
            raise ConfigError("timeout_seconds must be an integer.")
        if timeout_seconds <= 0:
            raise ConfigError("timeout_seconds must be greater than zero.")

    def _resolve_discovery_database(
        self,
        database: str | None,
        profile: ConnectionConfig | ConnectionProfile,
    ) -> str:
        """Resolve an exact database without legacy normalization or fallback."""
        configured = profile.database
        if configured is not None and configured != "":
            configured = self._validate_discovery_identifier(configured, "Configured database")
        if database is None:
            if not configured:
                raise ConfigError("A database is required for schema discovery.")
            return configured
        requested = self._validate_discovery_identifier(database, "database")
        if configured and requested != configured:
            raise ConfigError("Requested database must exactly match the configured database.")
        return requested

    @staticmethod
    def _resolve_discovery_object_types(
        object_types: tuple[DatabaseObjectType, ...] | None,
    ) -> frozenset[DatabaseObjectType]:
        if object_types is None:
            return _SUPPORTED_DISCOVERY_OBJECT_TYPES
        if not isinstance(object_types, tuple):
            raise ConfigError("object_types must be a tuple.")
        resolved: set[DatabaseObjectType] = set()
        for object_type in object_types:
            if (
                not isinstance(object_type, DatabaseObjectType)
                or object_type not in _SUPPORTED_DISCOVERY_OBJECT_TYPES
            ):
                raise ConfigError("object_types contains an unsupported database object type.")
            resolved.add(object_type)
        return frozenset(resolved)

    @staticmethod
    def _raise_discovery_error(error: BaseException, *, connection_phase: bool = False) -> NoReturn:
        """Translate Snowflake failures without exposing driver-controlled text."""
        sqlstate = getattr(error, "sqlstate", None)
        error_number = getattr(error, "errno", None)
        if isinstance(error, TimeoutError) or error_number == 604 or sqlstate == "57014":
            raise SchemaDiscoveryTimeoutError("Schema discovery timed out.") from None
        if connection_phase or (isinstance(sqlstate, str) and sqlstate.startswith("08")):
            raise SchemaDiscoveryConnectionError("Schema discovery connection failed.") from None
        if sqlstate == "42501":
            raise SchemaDiscoveryError(
                "Schema discovery is not available for the current role."
            ) from None
        raise SchemaDiscoveryError("Schema discovery failed.") from None

    @contextlib.contextmanager
    def _discovery_connection(
        self,
        profile: ConnectionConfig | ConnectionProfile,
        database: str,
        timeout_seconds: int | None,
    ):
        """Open an exact-database Snowflake session and always close it."""
        connection = None
        try:
            kwargs = self._connection_kwargs(profile, database)
            if timeout_seconds is not None:
                kwargs["login_timeout"] = timeout_seconds
            connection = self._driver().connect(**kwargs)
            try:
                yield connection
            finally:
                connection.close()
        except (ConfigError, SchemaDiscoveryError):
            raise
        except Exception as error:
            self._raise_discovery_error(error, connection_phase=connection is None)

    def _execute_discovery_query(
        self,
        connection: Any,
        query: str,
        parameters: tuple[Any, ...],
        timeout_seconds: int,
    ) -> tuple[dict[str, Any], ...]:
        """Execute one fixed SELECT with a separately closed cursor."""
        cursor = connection.cursor()
        try:
            if parameters:
                cursor.execute(query, parameters, timeout=timeout_seconds)
            else:
                cursor.execute(query, timeout=timeout_seconds)
            return tuple(self._fetch_rows(cursor)["rows"])
        finally:
            cursor.close()

    def _verify_discovery_database(
        self,
        connection: Any,
        database: str,
        timeout_seconds: int,
    ) -> None:
        rows = self._execute_discovery_query(
            connection,
            discovery_queries._CURRENT_DATABASE_QUERY,
            (),
            timeout_seconds,
        )
        if len(rows) != 1 or not isinstance(rows[0].get("current_database"), str):
            raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
        if rows[0]["current_database"] != database:
            raise SchemaDiscoveryConnectionError(
                "Connected database does not match the requested database."
            )

    @staticmethod
    def _optional_discovery_bool(value: Any) -> bool | None:
        return value if isinstance(value, bool) else None

    @staticmethod
    def _estimated_row_count(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value if value >= 0 else None
        if (
            isinstance(value, Decimal)
            and value.is_finite()
            and value >= 0
            and value == value.to_integral_value()
        ):
            return int(value)
        return None

    @staticmethod
    def _safe_schema_row(row: Mapping[str, Any]) -> dict[str, Any]:
        catalog_name = row.get("catalog_name")
        schema_name = row.get("schema_name")
        owner = row.get("owner")
        comment = row.get("comment")
        if (
            not isinstance(catalog_name, str)
            or catalog_name == ""
            or "\x00" in catalog_name
            or not isinstance(schema_name, str)
            or schema_name == ""
            or "\x00" in schema_name
            or (owner is not None and not isinstance(owner, str))
            or (comment is not None and not isinstance(comment, str))
        ):
            raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
        is_information_schema = schema_name == "INFORMATION_SCHEMA"
        return {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "owner": owner,
            "comment": comment,
            "is_system_managed": is_information_schema,
            "vendor_metadata": {
                "classification": "INFORMATION_SCHEMA" if is_information_schema else "USER",
                "is_transient": SnowflakeConnector._optional_discovery_bool(
                    row.get("is_transient")
                ),
                "is_managed_access": SnowflakeConnector._optional_discovery_bool(
                    row.get("is_managed_access")
                ),
            },
        }

    @staticmethod
    def _safe_object_row(row: Mapping[str, Any]) -> dict[str, Any]:
        catalog_name = row.get("catalog_name")
        schema_name = row.get("schema_name")
        object_name = row.get("object_name")
        table_type = row.get("table_type")
        owner = row.get("owner")
        comment = row.get("comment")
        if (
            not isinstance(catalog_name, str)
            or catalog_name == ""
            or "\x00" in catalog_name
            or not isinstance(schema_name, str)
            or schema_name == ""
            or "\x00" in schema_name
            or not isinstance(object_name, str)
            or object_name == ""
            or "\x00" in object_name
            or not isinstance(table_type, str)
            or table_type == ""
            or (owner is not None and not isinstance(owner, str))
            or (comment is not None and not isinstance(comment, str))
        ):
            raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")

        normalized_table_type = table_type.strip().upper()
        is_dynamic = SnowflakeConnector._optional_discovery_bool(row.get("is_dynamic"))
        object_type = (
            DatabaseObjectType.DYNAMIC_TABLE
            if is_dynamic is True
            else _SNOWFLAKE_OBJECT_TYPES.get(normalized_table_type, DatabaseObjectType.UNKNOWN)
        )
        is_temporary = SnowflakeConnector._optional_discovery_bool(row.get("is_temporary"))
        is_transient = SnowflakeConnector._optional_discovery_bool(row.get("is_transient"))
        if is_temporary is True or normalized_table_type == "TEMPORARY TABLE":
            persistence = "temporary"
        elif is_transient is True:
            persistence = "transient"
        elif (
            is_temporary is False
            and is_transient is False
            and object_type is not DatabaseObjectType.UNKNOWN
        ):
            persistence = "permanent"
        else:
            persistence = "unknown"

        estimated_row_count = SnowflakeConnector._estimated_row_count(
            row.get("estimated_row_count")
        )
        if object_type in {DatabaseObjectType.VIEW, DatabaseObjectType.UNKNOWN}:
            estimated_row_count = None

        is_iceberg = SnowflakeConnector._optional_discovery_bool(row.get("is_iceberg"))
        is_hybrid = SnowflakeConnector._optional_discovery_bool(row.get("is_hybrid"))
        auto_clustering_on = SnowflakeConnector._optional_discovery_bool(
            row.get("auto_clustering_on")
        )
        has_clustering_key = SnowflakeConnector._optional_discovery_bool(
            row.get("has_clustering_key")
        )
        return {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "object_name": object_name,
            "object_type": object_type.value.replace("_", " "),
            "persistence": persistence,
            "owner": owner,
            "comment": comment,
            "estimated_row_count": estimated_row_count,
            "is_system_managed": schema_name == "INFORMATION_SCHEMA",
            "vendor_metadata": {
                "table_type": table_type,
                "is_dynamic": is_dynamic,
                "is_external": object_type is DatabaseObjectType.EXTERNAL_TABLE,
                "is_iceberg": is_iceberg,
                "is_hybrid": is_hybrid,
                "auto_clustering_on": auto_clustering_on,
                "has_clustering_key": has_clustering_key,
            },
        }

    def list_schemas(
        self,
        *,
        database: str | None = None,
        timeout_seconds: int | None = None,
    ) -> tuple[SchemaMetadata, ...]:
        """Return every role-visible Snowflake schema as canonical metadata."""
        self._validate_discovery_timeout(timeout_seconds)
        profile = self._profile()
        target_database = self._resolve_discovery_database(database, profile)
        effective_timeout = timeout_seconds or profile.timeout_seconds
        with self._discovery_connection(profile, target_database, timeout_seconds) as connection:
            self._verify_discovery_database(connection, target_database, effective_timeout)
            rows = self._execute_discovery_query(
                connection,
                discovery_queries._SCHEMAS_QUERY,
                (target_database,),
                effective_timeout,
            )
            try:
                schemas = tuple(normalize_snowflake_schema(self._safe_schema_row(row)) for row in rows)
                if any(item.catalog_name != target_database for item in schemas):
                    raise MalformedDiscoveryResultError(
                        "Schema discovery returned malformed data."
                    )
            except MalformedDiscoveryResultError:
                raise
            except (TypeError, ValueError, KeyError):
                raise MalformedDiscoveryResultError(
                    "Schema discovery returned malformed data."
                ) from None
        schemas = deduplicate_models(
            schemas,
            lambda item: (item.catalog_name, item.schema_name),
        )
        return tuple(sorted(schemas, key=lambda item: item.schema_name))

    def list_objects(
        self,
        *,
        database: str | None = None,
        schema: str,
        object_types: tuple[DatabaseObjectType, ...] | None = None,
        timeout_seconds: int | None = None,
    ) -> tuple[DatabaseObjectMetadata, ...]:
        """Return supported objects in one exact Snowflake schema."""
        self._validate_discovery_timeout(timeout_seconds)
        target_schema = self._validate_discovery_identifier(schema, "schema")
        selected_types = self._resolve_discovery_object_types(object_types)
        if not selected_types:
            return ()
        profile = self._profile()
        target_database = self._resolve_discovery_database(database, profile)
        effective_timeout = timeout_seconds or profile.timeout_seconds
        with self._discovery_connection(profile, target_database, timeout_seconds) as connection:
            self._verify_discovery_database(connection, target_database, effective_timeout)
            rows = self._execute_discovery_query(
                connection,
                discovery_queries._OBJECTS_QUERY,
                (target_database, target_schema),
                effective_timeout,
            )
            try:
                objects = tuple(normalize_snowflake_object(self._safe_object_row(row)) for row in rows)
                if any(
                    item.catalog_name != target_database or item.schema_name != target_schema
                    for item in objects
                ):
                    raise MalformedDiscoveryResultError(
                        "Schema discovery returned malformed data."
                    )
            except MalformedDiscoveryResultError:
                raise
            except (TypeError, ValueError, KeyError):
                raise MalformedDiscoveryResultError(
                    "Schema discovery returned malformed data."
                ) from None
        objects = tuple(item for item in objects if item.object_type in selected_types)
        objects = deduplicate_models(
            objects,
            lambda item: (
                item.catalog_name,
                item.schema_name,
                item.object_name,
                item.object_type,
            ),
        )
        return tuple(sorted(objects, key=lambda item: (item.object_type.value, item.object_name)))

    def close(self) -> None:
        """Satisfy the connector contract; sessions are already per-call."""
        return None


Connector = SnowflakeConnector
