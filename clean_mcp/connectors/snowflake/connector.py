"""Snowflake implementation of the database connector interface."""

from __future__ import annotations

import contextlib
import re
from collections.abc import Mapping
from decimal import Decimal
from numbers import Integral
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
from connectors.snowflake._discovery_commands import _show_primary_keys_command
from models.discovery import (
    ConstraintType,
    CoverageStatus,
    DatabaseObjectMetadata,
    DatabaseObjectType,
    DiscoveryCoverage,
    SchemaMetadata,
    TableMetadata,
)
from normalizers._discovery_common import deduplicate_and_sort_columns, deduplicate_models
from normalizers.snowflake import normalize_snowflake_column
from normalizers.snowflake_discovery import (
    normalize_snowflake_check_constraint,
    normalize_snowflake_foreign_key,
    normalize_snowflake_key_constraint,
    normalize_snowflake_object,
    normalize_snowflake_schema,
)

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
            kwargs["autocommit"] = True
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
        is_immutable = SnowflakeConnector._optional_discovery_bool(row.get("is_immutable"))
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
                "is_immutable": is_immutable,
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

    @staticmethod
    def _discovery_integer(
        value: Any,
        *,
        minimum: int | None = None,
    ) -> tuple[int | None, bool]:
        """Return a safely converted integral catalog value and its validity."""
        if value is None:
            return None, True
        if isinstance(value, bool):
            return None, False
        if isinstance(value, Integral):
            result = int(value)
        elif (
            isinstance(value, Decimal)
            and value.is_finite()
            and value == value.to_integral_value()
        ):
            result = int(value)
        elif isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value):
            result = int(value)
        else:
            return None, False
        if minimum is not None and result < minimum:
            return None, False
        return result, True

    @staticmethod
    def _safe_discovery_text(value: Any) -> tuple[str | None, bool]:
        if value is None or isinstance(value, str):
            return value, True
        return None, False

    @staticmethod
    def _safe_discovery_bool(value: Any) -> tuple[bool | None, bool]:
        if value is None or isinstance(value, bool):
            return value, True
        return None, False

    @staticmethod
    def _is_insufficient_privilege(error: BaseException) -> bool:
        return getattr(error, "sqlstate", None) == "42501"

    def _optional_discovery_query(
        self,
        connection: Any,
        query: str,
        parameters: tuple[Any, ...],
        timeout_seconds: int,
    ) -> tuple[dict[str, Any], ...] | None:
        """Run an optional component query without surfacing privilege details."""
        try:
            return self._execute_discovery_query(
                connection,
                query,
                parameters,
                timeout_seconds,
            )
        except Exception as error:
            if self._is_insufficient_privilege(error):
                return None
            self._raise_discovery_error(error)

    def _primary_key_membership_rows(
        self,
        connection: Any,
        *,
        database: str,
        schema: str,
        table: str,
        timeout_seconds: int,
    ) -> tuple[tuple[dict[str, Any], ...] | None, bool]:
        """Fetch allowlisted SHOW PRIMARY KEYS fields directly from its cursor."""
        cursor = connection.cursor()
        try:
            try:
                cursor.execute(
                    _show_primary_keys_command(database, schema, table),
                    timeout=timeout_seconds,
                )
                description = cursor.description
                if not isinstance(description, (list, tuple)):
                    return (), True
                names: list[str] = []
                indexes: dict[str, int] = {}
                for index, field in enumerate(description):
                    if not isinstance(field, (list, tuple)) or not field or not isinstance(field[0], str):
                        return (), True
                    normalized = field[0].casefold()
                    if normalized in indexes:
                        return (), True
                    names.append(field[0])
                    indexes[normalized] = index
                required = {
                    "database_name",
                    "schema_name",
                    "table_name",
                    "constraint_name",
                    "column_name",
                    "key_sequence",
                }
                if not required.issubset(indexes):
                    return (), True

                projected: list[dict[str, Any]] = []
                for raw_row in cursor.fetchall():
                    if isinstance(raw_row, Mapping):
                        folded: dict[str, Any] = {}
                        duplicate = False
                        for key, value in raw_row.items():
                            if not isinstance(key, str):
                                duplicate = True
                                break
                            normalized = key.casefold()
                            if normalized not in required and normalized != "comment":
                                continue
                            if normalized in folded:
                                duplicate = True
                                break
                            folded[normalized] = value
                        if duplicate or not required.issubset(folded):
                            return (), True
                        value_for = folded.get
                    elif isinstance(raw_row, (list, tuple)) and len(raw_row) == len(names):
                        value_for = lambda name, row=raw_row: row[indexes[name]]
                    else:
                        return (), True
                    item = {name: value_for(name) for name in required}
                    if "comment" in indexes or isinstance(raw_row, Mapping):
                        item["comment"] = value_for("comment")
                    if any(item[name] is None for name in required):
                        return (), True
                    projected.append(item)
                return tuple(projected), False
            except Exception as error:
                if self._is_insufficient_privilege(error):
                    return None, False
                self._raise_discovery_error(error)
        finally:
            cursor.close()

    def _reconcile_primary_key_membership(
        self,
        header_rows: tuple[dict[str, Any], ...],
        membership_rows: tuple[dict[str, Any], ...],
        *,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        complete_columns: frozenset[str] | None,
    ) -> tuple[tuple[dict[str, Any], ...], str | None]:
        """Attach one complete ordered PK membership without weakening its header."""
        primary_headers = [
            dict(row) for row in header_rows if row.get("constraint_type") == "PRIMARY KEY"
        ]
        header_by_name = {row["constraint_name"]: row for row in primary_headers}
        if len(header_by_name) != len(primary_headers):
            return header_rows, "PRIMARY_KEY_MEMBERSHIP_PARTIAL"
        if not membership_rows:
            return header_rows, "PRIMARY_KEY_MEMBERSHIP_UNAVAILABLE"

        grouped: dict[str, list[tuple[int, str]]] = {}
        seen_rows: set[tuple[str, int, str]] = set()
        for row in membership_rows:
            name = row.get("constraint_name")
            identity = (
                row.get("database_name"),
                row.get("schema_name"),
                row.get("table_name"),
            )
            if identity != (catalog_name, schema_name, table_name) or name not in header_by_name:
                return header_rows, "PRIMARY_KEY_MEMBERSHIP_UNMATCHED"
            column_name = row.get("column_name")
            sequence, valid_sequence = self._discovery_integer(row.get("key_sequence"), minimum=1)
            if (
                not isinstance(column_name, str)
                or column_name == ""
                or "\x00" in column_name
                or not valid_sequence
                or sequence is None
            ):
                return header_rows, "PRIMARY_KEY_MEMBERSHIP_PARTIAL"
            row_identity = (name, sequence, column_name)
            if row_identity in seen_rows:
                return header_rows, "PRIMARY_KEY_MEMBERSHIP_PARTIAL"
            seen_rows.add(row_identity)
            grouped.setdefault(name, []).append((sequence, column_name))

            show_comment = row.get("comment")
            header_comment = header_by_name[name].get("comment")
            if show_comment is not None and (
                not isinstance(show_comment, str)
                or (header_comment is not None and show_comment != header_comment)
            ):
                return header_rows, "PRIMARY_KEY_MEMBERSHIP_CONFLICT"

        if set(grouped) != set(header_by_name):
            return header_rows, "PRIMARY_KEY_MEMBERSHIP_UNAVAILABLE"
        for name, members in grouped.items():
            positions = [position for position, _ in members]
            columns = [column for _, column in members]
            if (
                len(set(positions)) != len(positions)
                or len(set(columns)) != len(columns)
                or sorted(positions) != list(range(1, len(positions) + 1))
            ):
                return header_rows, "PRIMARY_KEY_MEMBERSHIP_PARTIAL"
            ordered_columns = tuple(column for _, column in sorted(members))
            if complete_columns is not None and any(
                column not in complete_columns for column in ordered_columns
            ):
                return header_rows, "PRIMARY_KEY_MEMBERSHIP_PARTIAL"
            header_by_name[name]["columns"] = ordered_columns

        reconciled = tuple(
            header_by_name[row["constraint_name"]]
            if row.get("constraint_type") == "PRIMARY KEY"
            else row
            for row in header_rows
        )
        return reconciled, None

    def _safe_column_row(
        self,
        row: Mapping[str, Any],
    ) -> tuple[dict[str, Any] | None, bool]:
        """Keep only modeled, non-sensitive column facts from one source row."""
        column_name = row.get("column_name")
        native_type = row.get("data_type")
        ordinal_position, ordinal_valid = self._discovery_integer(
            row.get("ordinal_position"),
            minimum=1,
        )
        if (
            not isinstance(column_name, str)
            or column_name == ""
            or "\x00" in column_name
            or not isinstance(native_type, str)
            or native_type == ""
            or ordinal_position is None
            or not ordinal_valid
        ):
            return None, True

        malformed = False
        cleaned: dict[str, Any] = {
            "column_name": column_name,
            "ordinal_position": ordinal_position,
            "data_type": native_type,
        }
        vendor_metadata: dict[str, Any] = {}

        nullable = row.get("is_nullable")
        if nullable is None or isinstance(nullable, bool) or (
            isinstance(nullable, str) and nullable.strip().upper() in {"YES", "NO"}
        ):
            cleaned["is_nullable"] = nullable
        else:
            malformed = True

        for source_name, target_name, minimum in (
            ("character_maximum_length", "character_maximum_length", 0),
            ("numeric_precision", "numeric_precision", 0),
            ("numeric_scale", "numeric_scale", 0),
            ("datetime_precision", "datetime_precision", 0),
        ):
            value, valid = self._discovery_integer(row.get(source_name), minimum=minimum)
            if valid:
                cleaned[target_name] = value
            else:
                malformed = True

        for source_name, target_name in (
            ("column_default", "column_default"),
            ("column_comment", "column_comment"),
            ("collation_name", "collation_name"),
            ("generation_expression", "generation_expression"),
            ("kind", "kind"),
        ):
            value, valid = self._safe_discovery_text(row.get(source_name))
            if valid:
                cleaned[target_name] = value
            else:
                malformed = True

        identity, identity_valid = self._safe_discovery_bool(row.get("is_identity"))
        if identity_valid:
            cleaned["is_identity"] = identity
            cleaned["is_auto_increment"] = identity
        else:
            malformed = True

        for source_name in ("data_type_alias", "dtd_identifier"):
            value, valid = self._safe_discovery_text(row.get(source_name))
            if valid and value is not None:
                vendor_metadata[source_name] = value
            elif not valid:
                malformed = True
        if identity is True:
            identity_generation, generation_valid = self._safe_discovery_text(
                row.get("identity_generation")
            )
            if generation_valid:
                cleaned["identity_generation"] = identity_generation
            else:
                malformed = True
            for source_name in ("identity_start", "identity_increment"):
                value, valid = self._discovery_integer(row.get(source_name))
                if valid and value is not None:
                    vendor_metadata[source_name] = value
                elif not valid:
                    malformed = True
            for source_name in ("identity_cycle", "identity_ordered"):
                value, valid = self._safe_discovery_bool(row.get(source_name))
                if valid and value is not None:
                    vendor_metadata[source_name] = value
                elif not valid:
                    malformed = True
        kind = cleaned.get("kind")
        if isinstance(kind, str):
            vendor_metadata["column_kind"] = kind
        cleaned["vendor_metadata"] = vendor_metadata
        return cleaned, malformed

    @staticmethod
    def _array_element_metadata(
        rows: tuple[dict[str, Any], ...],
    ) -> tuple[dict[str, tuple[str, str | None]], bool]:
        """Index structured ARRAY element definitions without retaining raw rows."""
        element_types: dict[str, tuple[str, str | None]] = {}
        invalid_identifiers: set[str] = set()
        malformed = False
        for row in rows:
            collection_id = row.get("collection_type_identifier")
            data_type = row.get("data_type")
            dtd_identifier = row.get("dtd_identifier")
            if (
                not isinstance(collection_id, str)
                or collection_id == ""
                or "\x00" in collection_id
                or not isinstance(data_type, str)
                or data_type == ""
                or (dtd_identifier is not None and not isinstance(dtd_identifier, str))
            ):
                malformed = True
                continue
            candidate = (data_type, dtd_identifier)
            if collection_id in invalid_identifiers:
                continue
            existing = element_types.get(collection_id)
            if existing is None:
                element_types[collection_id] = candidate
            elif existing != candidate:
                malformed = True
                invalid_identifiers.add(collection_id)
                element_types.pop(collection_id, None)
        return element_types, malformed

    @staticmethod
    def _structured_array_details(
        dtd_identifier: str,
        element_types: Mapping[str, tuple[str, str | None]],
    ) -> tuple[dict[str, Any] | None, bool]:
        """Resolve a finite structured-array chain from exact DTD identifiers."""
        current_identifier = dtd_identifier
        dimensions = 0
        seen: set[str] = set()
        while current_identifier in element_types:
            if current_identifier in seen:
                return None, True
            seen.add(current_identifier)
            native_type, nested_identifier = element_types[current_identifier]
            dimensions += 1
            if native_type.strip().casefold() != "array" or not nested_identifier:
                return {
                    "array_dimensions": dimensions,
                    "element_native_type": native_type,
                }, False
            current_identifier = nested_identifier
        return None, False

    @staticmethod
    def _safe_key_constraint_rows(
        rows: tuple[dict[str, Any], ...],
    ) -> tuple[tuple[dict[str, Any], ...], frozenset[ConstraintType]]:
        cleaned: list[dict[str, Any]] = []
        malformed: set[ConstraintType] = set()
        for row in rows:
            raw_type = row.get("constraint_type")
            constraint_type = {
                "PRIMARY KEY": ConstraintType.PRIMARY_KEY,
                "UNIQUE": ConstraintType.UNIQUE,
            }.get(raw_type)
            if constraint_type is None:
                continue
            name = row.get("constraint_name")
            if not isinstance(name, str) or name == "" or "\x00" in name:
                malformed.add(constraint_type)
                continue
            item: dict[str, Any] = {
                "constraint_name": name,
                "constraint_type": raw_type,
                "vendor_metadata": {},
            }
            valid = True
            for field_name in (
                "is_enforced",
                "is_rely",
                "is_deferrable",
                "initially_deferred",
            ):
                value = row.get(field_name)
                if value is None or isinstance(value, bool):
                    item[field_name] = value
                else:
                    valid = False
            comment = row.get("comment")
            if comment is None or isinstance(comment, str):
                item["comment"] = comment
            else:
                valid = False
            if not valid:
                malformed.add(constraint_type)
                continue
            cleaned.append(item)
        return tuple(cleaned), frozenset(malformed)

    @staticmethod
    def _safe_foreign_key_rows(
        rows: tuple[dict[str, Any], ...],
    ) -> tuple[tuple[dict[str, Any], ...], bool]:
        cleaned: list[dict[str, Any]] = []
        malformed = False
        for row in rows:
            required = (
                row.get("constraint_name"),
                row.get("referenced_catalog"),
                row.get("referenced_schema"),
                row.get("referenced_table"),
            )
            if any(
                not isinstance(value, str) or value == "" or "\x00" in value
                for value in required
            ):
                malformed = True
                continue
            item: dict[str, Any] = {
                "constraint_name": required[0],
                "referenced_catalog": required[1],
                "referenced_schema": required[2],
                "referenced_table": required[3],
                "local_columns": (),
                "referenced_columns": (),
                "vendor_metadata": {},
            }
            valid = True
            for field_name in ("match_option", "update_rule", "delete_rule", "comment"):
                value = row.get(field_name)
                if value is None or isinstance(value, str):
                    item[field_name] = value
                else:
                    valid = False
            for field_name in (
                "is_enforced",
                "is_rely",
                "is_deferrable",
                "initially_deferred",
            ):
                value = row.get(field_name)
                if value is None or isinstance(value, bool):
                    item[field_name] = value
                else:
                    valid = False
            if not valid:
                malformed = True
                continue
            cleaned.append(item)
        return tuple(cleaned), malformed

    @staticmethod
    def _safe_check_constraint_rows(
        rows: tuple[dict[str, Any], ...],
        *,
        catalog_name: str,
        schema_name: str,
        table_name: str,
    ) -> tuple[tuple[dict[str, Any], ...], bool]:
        cleaned: list[dict[str, Any]] = []
        malformed = False
        for row in rows:
            identity = (
                row.get("constraint_catalog"),
                row.get("constraint_schema"),
                row.get("constraint_table"),
            )
            name = row.get("constraint_name")
            expression = row.get("expression")
            if (
                identity != (catalog_name, schema_name, table_name)
                or not isinstance(name, str)
                or name == ""
                or "\x00" in name
                or not isinstance(expression, str)
                or not expression.strip()
            ):
                malformed = True
                continue
            cleaned.append(
                {
                    "constraint_name": name,
                    "expression": expression,
                    "vendor_metadata": {},
                }
            )
        return tuple(cleaned), malformed

    @staticmethod
    def _sorted_key_constraints(
        rows: tuple[dict[str, Any], ...],
        constraint_type: ConstraintType,
    ) -> tuple[Any, ...]:
        models = tuple(
            normalize_snowflake_key_constraint(row, constraint_type=constraint_type)
            for row in rows
            if row.get("constraint_type")
            == ("PRIMARY KEY" if constraint_type is ConstraintType.PRIMARY_KEY else "UNIQUE")
        )
        models = deduplicate_models(models, lambda item: (item.constraint_type, item.name, item.columns))
        return tuple(sorted(models, key=lambda item: (item.name is None, item.name or "", item.columns)))

    @staticmethod
    def _sorted_foreign_keys(rows: tuple[dict[str, Any], ...]) -> tuple[Any, ...]:
        models = tuple(normalize_snowflake_foreign_key(row) for row in rows)
        models = deduplicate_models(
            models,
            lambda item: (
                item.name,
                item.local_columns,
                item.referenced_catalog,
                item.referenced_schema,
                item.referenced_table,
                item.referenced_columns,
            ),
        )
        return tuple(
            sorted(
                models,
                key=lambda item: (
                    item.name is None,
                    item.name or "",
                    item.referenced_catalog or "",
                    item.referenced_schema or "",
                    item.referenced_table,
                ),
            )
        )

    @staticmethod
    def _sorted_check_constraints(rows: tuple[dict[str, Any], ...]) -> tuple[Any, ...]:
        models = tuple(normalize_snowflake_check_constraint(row) for row in rows)
        models = deduplicate_models(models, lambda item: (item.name, item.expression))
        return tuple(sorted(models, key=lambda item: (item.name is None, item.name or "", item.expression)))

    def get_table_metadata(
        self,
        *,
        database: str | None = None,
        schema: str,
        table: str,
        timeout_seconds: int | None = None,
    ) -> TableMetadata | None:
        """Return canonical metadata for one Snowflake table-like object."""
        self._validate_discovery_timeout(timeout_seconds)
        target_schema = self._validate_discovery_identifier(schema, "schema")
        target_table = self._validate_discovery_identifier(table, "table")
        profile = self._profile()
        target_database = self._resolve_discovery_database(database, profile)
        effective_timeout = timeout_seconds or profile.timeout_seconds
        parameters = (target_database, target_schema, target_table)

        with self._discovery_connection(profile, target_database, timeout_seconds) as connection:
            self._verify_discovery_database(connection, target_database, effective_timeout)
            base_rows = self._execute_discovery_query(
                connection,
                discovery_queries._TABLE_METADATA_QUERY,
                parameters,
                effective_timeout,
            )
            if not base_rows:
                return None
            if len(base_rows) != 1:
                raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
            safe_object = self._safe_object_row(base_rows[0])
            if (
                safe_object["catalog_name"] != target_database
                or safe_object["schema_name"] != target_schema
                or safe_object["object_name"] != target_table
            ):
                raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
            try:
                object_metadata = normalize_snowflake_object(safe_object)
            except (TypeError, ValueError, KeyError):
                raise MalformedDiscoveryResultError(
                    "Schema discovery returned malformed data."
                ) from None
            if object_metadata.object_type not in {
                DatabaseObjectType.TABLE,
                DatabaseObjectType.VIEW,
                DatabaseObjectType.MATERIALIZED_VIEW,
                DatabaseObjectType.EXTERNAL_TABLE,
                DatabaseObjectType.DYNAMIC_TABLE,
            }:
                raise SchemaDiscoveryError(
                    "Table metadata discovery does not support this object type."
                )

            warnings: set[str] = set()
            raw_row_count = base_rows[0].get("estimated_row_count")
            normalized_row_count = self._estimated_row_count(raw_row_count)
            if raw_row_count is None:
                estimated_row_count_status = CoverageStatus.UNAVAILABLE
                warnings.add("ESTIMATED_ROW_COUNT_UNAVAILABLE")
            elif normalized_row_count is None:
                estimated_row_count_status = CoverageStatus.PARTIAL
                warnings.add("ESTIMATED_ROW_COUNT_PARTIAL")
            else:
                estimated_row_count_status = CoverageStatus.COMPLETE

            raw_clustering = base_rows[0].get("clustering_expression")
            raw_auto_clustering = base_rows[0].get("auto_clustering_on")
            raw_has_clustering_key = base_rows[0].get("has_clustering_key")
            if object_metadata.object_type in {
                DatabaseObjectType.VIEW,
                DatabaseObjectType.EXTERNAL_TABLE,
            }:
                clustering_expression = None
                clustering_status = CoverageStatus.NOT_APPLICABLE
            elif not (
                raw_clustering is None or isinstance(raw_clustering, str)
            ) or not (
                raw_has_clustering_key is None
                or isinstance(raw_has_clustering_key, bool)
            ):
                clustering_expression = None
                clustering_status = CoverageStatus.PARTIAL
                warnings.add("CLUSTERING_PARTIAL")
            elif raw_auto_clustering is None:
                clustering_expression = raw_clustering
                clustering_status = CoverageStatus.UNAVAILABLE
                warnings.add("CLUSTERING_UNAVAILABLE")
            elif isinstance(raw_auto_clustering, bool):
                clustering_expression = raw_clustering
                clustering_status = CoverageStatus.COMPLETE
            else:
                clustering_expression = None
                clustering_status = CoverageStatus.PARTIAL
                warnings.add("CLUSTERING_PARTIAL")

            column_rows = self._optional_discovery_query(
                connection,
                discovery_queries._COLUMNS_QUERY,
                parameters,
                effective_timeout,
            )
            safe_columns: list[dict[str, Any]] = []
            columns_malformed = False
            elements_unavailable = False
            if column_rows is None:
                columns_status = CoverageStatus.UNAVAILABLE
                comments_status = CoverageStatus.PARTIAL
                warnings.add("COLUMNS_UNAVAILABLE")
            else:
                for row in column_rows:
                    safe_column, malformed = self._safe_column_row(row)
                    columns_malformed = columns_malformed or malformed
                    if safe_column is not None:
                        safe_columns.append(safe_column)
                array_columns = [
                    row
                    for row in safe_columns
                    if row["data_type"].strip().casefold() == "array"
                ]
                if array_columns:
                    element_rows = self._optional_discovery_query(
                        connection,
                        discovery_queries._ELEMENT_TYPES_QUERY,
                        parameters,
                        effective_timeout,
                    )
                    if element_rows is None:
                        elements_unavailable = True
                        warnings.add("STRUCTURED_ARRAY_TYPES_UNAVAILABLE")
                    else:
                        element_types, malformed_elements = self._array_element_metadata(element_rows)
                        columns_malformed = columns_malformed or malformed_elements
                        for row in safe_columns:
                            dtd_identifier = row["vendor_metadata"].get("dtd_identifier")
                            if (
                                row["data_type"].strip().casefold() == "array"
                                and isinstance(dtd_identifier, str)
                            ):
                                details, malformed = self._structured_array_details(
                                    dtd_identifier,
                                    element_types,
                                )
                                columns_malformed = columns_malformed or malformed
                                if details is not None:
                                    row.update(details)
                if not safe_columns:
                    columns_status = CoverageStatus.PARTIAL
                    comments_status = CoverageStatus.PARTIAL
                    warnings.add("COLUMNS_PARTIAL")
                elif columns_malformed or elements_unavailable:
                    columns_status = CoverageStatus.PARTIAL
                    comments_status = CoverageStatus.PARTIAL
                    warnings.add("COLUMNS_PARTIAL")
                else:
                    columns_status = CoverageStatus.COMPLETE
                    comments_status = CoverageStatus.COMPLETE

            key_rows: tuple[dict[str, Any], ...] | None = None
            safe_key_rows: tuple[dict[str, Any], ...] = ()
            malformed_key_types: frozenset[ConstraintType] = frozenset()
            key_applicable = object_metadata.object_type in {
                DatabaseObjectType.TABLE,
                DatabaseObjectType.EXTERNAL_TABLE,
                DatabaseObjectType.DYNAMIC_TABLE,
            }
            if key_applicable:
                key_rows = self._optional_discovery_query(
                    connection,
                    discovery_queries._KEY_CONSTRAINTS_QUERY,
                    parameters,
                    effective_timeout,
                )
                if key_rows is not None:
                    safe_key_rows, malformed_key_types = self._safe_key_constraint_rows(key_rows)

            if not key_applicable:
                primary_key_status = CoverageStatus.NOT_APPLICABLE
                unique_status = CoverageStatus.NOT_APPLICABLE
            elif key_rows is None:
                primary_key_status = CoverageStatus.UNAVAILABLE
                unique_status = CoverageStatus.UNAVAILABLE
                warnings.update({"PRIMARY_KEY_UNAVAILABLE", "UNIQUE_CONSTRAINTS_UNAVAILABLE"})
            else:
                has_primary = any(
                    row.get("constraint_type") == "PRIMARY KEY" for row in safe_key_rows
                )
                has_unique = any(
                    row.get("constraint_type") == "UNIQUE" for row in safe_key_rows
                )
                primary_key_status = (
                    CoverageStatus.PARTIAL
                    if has_primary or ConstraintType.PRIMARY_KEY in malformed_key_types
                    else CoverageStatus.COMPLETE
                )
                unique_status = (
                    CoverageStatus.PARTIAL
                    if has_unique or ConstraintType.UNIQUE in malformed_key_types
                    else CoverageStatus.COMPLETE
                )
                if has_primary:
                    warnings.add("PRIMARY_KEY_MEMBERSHIP_UNAVAILABLE")
                elif ConstraintType.PRIMARY_KEY in malformed_key_types:
                    warnings.add("PRIMARY_KEY_PARTIAL")
                if has_unique:
                    warnings.add("UNIQUE_CONSTRAINT_MEMBERSHIP_UNAVAILABLE")
                elif ConstraintType.UNIQUE in malformed_key_types:
                    warnings.add("UNIQUE_CONSTRAINTS_PARTIAL")

            has_primary_header = any(
                row.get("constraint_type") == "PRIMARY KEY" for row in safe_key_rows
            )
            if key_applicable and key_rows is not None and has_primary_header:
                membership_rows, malformed_membership = self._primary_key_membership_rows(
                    connection,
                    database=target_database,
                    schema=target_schema,
                    table=target_table,
                    timeout_seconds=effective_timeout,
                )
                if membership_rows is None:
                    primary_key_status = CoverageStatus.PARTIAL
                    warnings.add("PRIMARY_KEY_MEMBERSHIP_UNAVAILABLE")
                elif malformed_membership:
                    primary_key_status = CoverageStatus.PARTIAL
                    warnings.discard("PRIMARY_KEY_MEMBERSHIP_UNAVAILABLE")
                    warnings.add("PRIMARY_KEY_MEMBERSHIP_PARTIAL")
                else:
                    safe_key_rows, membership_warning = self._reconcile_primary_key_membership(
                        safe_key_rows,
                        membership_rows,
                        catalog_name=target_database,
                        schema_name=target_schema,
                        table_name=target_table,
                        complete_columns=(
                            frozenset(row["column_name"] for row in safe_columns)
                            if columns_status is CoverageStatus.COMPLETE
                            else None
                        ),
                    )
                    warnings.discard("PRIMARY_KEY_MEMBERSHIP_UNAVAILABLE")
                    if membership_warning is None:
                        primary_key_status = CoverageStatus.COMPLETE
                    else:
                        primary_key_status = CoverageStatus.PARTIAL
                        warnings.add(membership_warning)

            foreign_rows: tuple[dict[str, Any], ...] | None = None
            safe_foreign_rows: tuple[dict[str, Any], ...] = ()
            malformed_foreign_keys = False
            if object_metadata.object_type is DatabaseObjectType.TABLE:
                foreign_rows = self._optional_discovery_query(
                    connection,
                    discovery_queries._FOREIGN_KEYS_QUERY,
                    parameters,
                    effective_timeout,
                )
                if foreign_rows is not None:
                    safe_foreign_rows, malformed_foreign_keys = self._safe_foreign_key_rows(
                        foreign_rows
                    )
            if object_metadata.object_type is not DatabaseObjectType.TABLE:
                foreign_key_status = CoverageStatus.NOT_APPLICABLE
            elif foreign_rows is None:
                foreign_key_status = CoverageStatus.UNAVAILABLE
                warnings.add("FOREIGN_KEYS_UNAVAILABLE")
            elif safe_foreign_rows or malformed_foreign_keys:
                foreign_key_status = CoverageStatus.PARTIAL
                warnings.add(
                    "FOREIGN_KEY_MEMBERSHIP_UNAVAILABLE"
                    if safe_foreign_rows
                    else "FOREIGN_KEYS_PARTIAL"
                )
            else:
                foreign_key_status = CoverageStatus.COMPLETE

            is_hybrid = object_metadata.vendor_metadata.get("is_hybrid") is True
            check_rows: tuple[dict[str, Any], ...] | None = None
            safe_check_rows: tuple[dict[str, Any], ...] = ()
            malformed_checks = False
            check_applicable = (
                object_metadata.object_type is DatabaseObjectType.TABLE and not is_hybrid
            )
            if check_applicable:
                check_rows = self._optional_discovery_query(
                    connection,
                    discovery_queries._CHECK_CONSTRAINTS_QUERY,
                    parameters,
                    effective_timeout,
                )
                if check_rows is not None:
                    safe_check_rows, malformed_checks = self._safe_check_constraint_rows(
                        check_rows,
                        catalog_name=target_database,
                        schema_name=target_schema,
                        table_name=target_table,
                    )
            if not check_applicable:
                check_status = CoverageStatus.NOT_APPLICABLE
            elif check_rows is None:
                check_status = CoverageStatus.UNAVAILABLE
                warnings.add("CHECK_CONSTRAINTS_UNAVAILABLE")
            elif malformed_checks:
                check_status = CoverageStatus.PARTIAL
                warnings.add("CHECK_CONSTRAINTS_PARTIAL")
            else:
                check_status = CoverageStatus.COMPLETE

            view_definition = None
            if object_metadata.object_type is DatabaseObjectType.VIEW:
                view_rows = self._optional_discovery_query(
                    connection,
                    discovery_queries._VIEW_DEFINITION_QUERY,
                    parameters,
                    effective_timeout,
                )
                if (
                    view_rows is not None
                    and len(view_rows) == 1
                    and view_rows[0].get("is_secure") is not True
                    and isinstance(view_rows[0].get("view_definition"), str)
                    and view_rows[0]["view_definition"].strip()
                ):
                    view_definition = view_rows[0]["view_definition"]
                    view_status = CoverageStatus.COMPLETE
                else:
                    view_status = CoverageStatus.UNAVAILABLE
                    warnings.add("VIEW_DEFINITION_UNAVAILABLE")
            elif object_metadata.object_type is DatabaseObjectType.MATERIALIZED_VIEW:
                view_status = CoverageStatus.UNAVAILABLE
                warnings.add("VIEW_DEFINITION_UNAVAILABLE")
            else:
                view_status = CoverageStatus.NOT_APPLICABLE

            try:
                columns = deduplicate_and_sort_columns(
                    normalize_snowflake_column(
                        row,
                        catalog_name=object_metadata.catalog_name,
                        schema_name=object_metadata.schema_name,
                        table_name=object_metadata.object_name,
                    )
                    for row in safe_columns
                )
                primary_keys = self._sorted_key_constraints(
                    safe_key_rows,
                    ConstraintType.PRIMARY_KEY,
                )
                unique_constraints = self._sorted_key_constraints(
                    safe_key_rows,
                    ConstraintType.UNIQUE,
                )
                foreign_keys = self._sorted_foreign_keys(safe_foreign_rows)
                check_constraints = self._sorted_check_constraints(safe_check_rows)
                if len(primary_keys) > 1:
                    primary_key_status = CoverageStatus.PARTIAL
                    warnings.add("PRIMARY_KEY_PARTIAL")
                coverage = DiscoveryCoverage(
                    columns=columns_status,
                    primary_key=primary_key_status,
                    unique_constraints=unique_status,
                    foreign_keys=foreign_key_status,
                    check_constraints=check_status,
                    comments=comments_status,
                    estimated_row_count=estimated_row_count_status,
                    view_definition=view_status,
                    partitioning=CoverageStatus.NOT_APPLICABLE,
                    clustering=clustering_status,
                    warnings=tuple(sorted(warnings)),
                )
                return TableMetadata(
                    catalog_name=object_metadata.catalog_name,
                    schema_name=object_metadata.schema_name,
                    object_name=object_metadata.object_name,
                    system=object_metadata.system,
                    object_type=object_metadata.object_type,
                    persistence=object_metadata.persistence,
                    owner=object_metadata.owner,
                    comment=object_metadata.comment,
                    estimated_row_count=object_metadata.estimated_row_count,
                    is_system_managed=object_metadata.is_system_managed,
                    columns=columns,
                    primary_key=primary_keys[0] if primary_keys else None,
                    unique_constraints=unique_constraints,
                    foreign_keys=foreign_keys,
                    check_constraints=check_constraints,
                    view_definition=view_definition,
                    clustering_expression=clustering_expression,
                    is_partitioned=None,
                    partitioning_expression=None,
                    coverage=coverage,
                    vendor_metadata=object_metadata.vendor_metadata,
                )
            except (TypeError, ValueError, KeyError):
                raise MalformedDiscoveryResultError(
                    "Schema discovery returned malformed data."
                ) from None

    def close(self) -> None:
        """Satisfy the connector contract; sessions are already per-call."""
        return None


Connector = SnowflakeConnector
