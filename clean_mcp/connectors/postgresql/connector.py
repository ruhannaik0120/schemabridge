"""PostgreSQL implementation of the database connector interface."""

from __future__ import annotations

import contextlib
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, NoReturn

from config import Config, ConfigError, ConnectionConfig
from connectors.base import DatabaseConnector, unique_column_names
from connectors.discovery import (
    MalformedDiscoveryResultError,
    SchemaDiscoveryConnectionError,
    SchemaDiscoveryError,
    SchemaDiscoveryTimeoutError,
)
from connectors.postgresql import _discovery_queries as discovery_queries
from models.discovery import (
    CoverageStatus,
    DatabaseObjectMetadata,
    DatabaseObjectType,
    DiscoveryCoverage,
    SchemaMetadata,
    TableMetadata,
)
from normalizers._discovery_common import (
    _preference_key,
    foreign_key_coverage,
    key_constraint_coverage,
    stable_constraint_identity,
)
from normalizers.postgresql_discovery import (
    normalize_postgresql_object,
    normalize_postgresql_schema,
    normalize_postgresql_table,
)

if TYPE_CHECKING:
    from models.connection_profile import ConnectionProfile


class UnsupportedPostgreSQLVersionError(SchemaDiscoveryError):
    """Raised when a server predates the supported discovery catalogs."""


_MINIMUM_DISCOVERY_SERVER_VERSION = 140000
_POSTGRESQL_18_SERVER_VERSION = 180000
_SUPPORTED_DISCOVERY_OBJECT_TYPES = {
    DatabaseObjectType.TABLE: "r",
    DatabaseObjectType.PARTITIONED_TABLE: "p",
    DatabaseObjectType.VIEW: "v",
    DatabaseObjectType.MATERIALIZED_VIEW: "m",
    DatabaseObjectType.FOREIGN_TABLE: "f",
}


class PostgreSQLConnector(DatabaseConnector):
    """Connector implementation for PostgreSQL via psycopg."""

    profile_db_type = "postgresql"

    def _driver(self):
        """Load the optional PostgreSQL driver only when selected."""
        # Driver imports remain inside connectors to preserve the architecture
        # boundary checked by tests/test_architecture.py.
        try:
            import psycopg  # type: ignore
        except ImportError as exc:
            raise ConfigError("Install psycopg[binary] to use the PostgreSQL connector.") from exc
        return psycopg

    def _profile(self) -> ConnectionConfig | ConnectionProfile:
        """Return the active neutral profile after checking host configuration."""
        profile = self._connection_profile
        if profile is None:
            profile = Config.connection_config()
        if not profile.host:
            raise ConfigError("DB_HOST is required for the PostgreSQL connector.")
        return profile

    def _normalize_database(self, database: str | None, fallback: str) -> str:
        """Select an explicit database, configured default, or postgres."""
        return (database or fallback or "postgres").strip() or "postgres"

    def _connection_kwargs(
        self,
        profile: ConnectionConfig | ConnectionProfile,
        database: str,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Translate framework configuration into psycopg arguments."""
        options = (
            profile.connection_options_copy()
            if self._connection_profile is not None
            else dict(profile.connection_options or {})
        )
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

    @staticmethod
    def _validate_discovery_identifier(value: object, field_name: str) -> str:
        """Validate an exact identifier without changing any caller text."""
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
        """Resolve discovery databases exactly, without legacy fallback behavior."""
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
    ) -> tuple[str, ...]:
        if object_types is None:
            return tuple(_SUPPORTED_DISCOVERY_OBJECT_TYPES.values())
        if not isinstance(object_types, tuple):
            raise ConfigError("object_types must be a tuple.")
        relkinds: list[str] = []
        for object_type in object_types:
            if not isinstance(object_type, DatabaseObjectType) or object_type not in _SUPPORTED_DISCOVERY_OBJECT_TYPES:
                raise ConfigError("object_types contains an unsupported database object type.")
            relkind = _SUPPORTED_DISCOVERY_OBJECT_TYPES[object_type]
            if relkind not in relkinds:
                relkinds.append(relkind)
        return tuple(relkinds)

    @staticmethod
    def _raise_discovery_error(error: BaseException, *, connection_phase: bool = False) -> NoReturn:
        """Translate driver failures without exposing their text or context."""
        sqlstate = getattr(error, "sqlstate", None)
        if isinstance(error, TimeoutError) or sqlstate == "57014":
            raise SchemaDiscoveryTimeoutError("Schema discovery timed out.") from None
        if connection_phase or (isinstance(sqlstate, str) and sqlstate.startswith("08")):
            raise SchemaDiscoveryConnectionError("Schema discovery connection failed.") from None
        raise SchemaDiscoveryError("Schema discovery failed.") from None

    @contextlib.contextmanager
    def _discovery_connection(self, database: str, timeout_seconds: int | None):
        """Open an exact-database discovery session and always close it."""
        opened = False
        try:
            profile = self._profile()
            kwargs = self._connection_kwargs(profile, database, timeout_seconds)
            connection = self._driver().connect(**kwargs)
            opened = True
            try:
                try:
                    yield connection
                except BaseException:
                    with contextlib.suppress(Exception):
                        connection.rollback()
                    raise
                else:
                    connection.rollback()
            finally:
                connection.close()
        except (ConfigError, SchemaDiscoveryError):
            raise
        except Exception as error:
            self._raise_discovery_error(error, connection_phase=not opened)

    def _execute_discovery_query(
        self,
        connection: Any,
        query: str,
        parameters: tuple[Any, ...] = (),
    ) -> tuple[dict[str, Any], ...]:
        """Execute one fixed query with one separately closed cursor."""
        cursor = connection.cursor()
        try:
            cursor.execute(query, parameters)
            return tuple(self._fetch_rows(cursor)["rows"])
        finally:
            cursor.close()

    def _execute_optional_discovery_query(
        self,
        connection: Any,
        query: str,
        parameters: tuple[Any, ...],
    ) -> tuple[dict[str, Any], ...] | None:
        """Treat only insufficient privilege as optional unavailability."""
        try:
            return self._execute_discovery_query(connection, query, parameters)
        except BaseException as error:
            if getattr(error, "sqlstate", None) != "42501":
                raise
            connection.rollback()
            return None

    def _discovery_capabilities(
        self,
        connection: Any,
        requested_database: str,
    ) -> dict[str, Any]:
        rows = self._execute_discovery_query(connection, discovery_queries._CAPABILITIES_QUERY)
        if len(rows) != 1:
            raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
        row = rows[0]
        current_database = row.get("current_database")
        server_version = row.get("server_version_num")
        max_identifier_length = row.get("max_identifier_length")
        if not isinstance(current_database, str):
            raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
        if current_database != requested_database:
            raise SchemaDiscoveryConnectionError("Connected database does not match the requested database.")
        if (
            isinstance(server_version, bool)
            or not isinstance(server_version, int)
            or isinstance(max_identifier_length, bool)
            or not isinstance(max_identifier_length, int)
            or max_identifier_length <= 0
            or not isinstance(row.get("has_partition_key_helper"), bool)
            or not isinstance(row.get("has_partition_constraint_helper"), bool)
        ):
            raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
        if server_version < _MINIMUM_DISCOVERY_SERVER_VERSION:
            raise UnsupportedPostgreSQLVersionError(
                "PostgreSQL 14 or newer is required for schema discovery."
            )
        return {
            "server_version_num": server_version,
            "max_identifier_length": max_identifier_length,
            "has_partition_key_helper": row.get("has_partition_key_helper") is True,
            "has_partition_constraint_helper": row.get("has_partition_constraint_helper") is True,
        }

    def _enforce_identifier_lengths(
        self,
        connection: Any,
        capabilities: Mapping[str, Any],
        *identifiers: str,
    ) -> None:
        limit = capabilities["max_identifier_length"]
        for identifier in identifiers:
            rows = self._execute_discovery_query(
                connection,
                discovery_queries._IDENTIFIER_LENGTH_QUERY,
                (identifier,),
            )
            length = rows[0].get("byte_length") if len(rows) == 1 else None
            if isinstance(length, bool) or not isinstance(length, int) or length < 0:
                raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
            if length > limit:
                raise ConfigError("Discovery identifier exceeds the server limit.")

    @staticmethod
    def _safe_schema_row(row: Mapping[str, Any]) -> dict[str, Any]:
        classification = row.get("schema_classification")
        if (
            not isinstance(row.get("catalog_name"), str)
            or not isinstance(row.get("schema_name"), str)
            or row.get("schema_name") == ""
            or not isinstance(classification, str)
            or not isinstance(row.get("is_system_managed"), bool)
            or (row.get("owner") is not None and not isinstance(row.get("owner"), str))
            or (row.get("comment") is not None and not isinstance(row.get("comment"), str))
        ):
            raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
        return {
            "catalog_name": row.get("catalog_name"),
            "schema_name": row.get("schema_name"),
            "owner": row.get("owner"),
            "comment": row.get("comment"),
            "is_system_managed": row.get("is_system_managed"),
            "vendor_metadata": {"classification": classification},
        }

    @staticmethod
    def _safe_object_row(row: Mapping[str, Any]) -> dict[str, Any]:
        classification = row.get("schema_classification")
        estimated_row_count = row.get("estimated_row_count")
        if (
            not isinstance(row.get("catalog_name"), str)
            or not isinstance(row.get("schema_name"), str)
            or row.get("schema_name") == ""
            or not isinstance(row.get("object_name"), str)
            or row.get("object_name") == ""
            or not isinstance(row.get("relkind"), str)
            or not isinstance(row.get("relpersistence"), str)
            or not isinstance(classification, str)
            or not isinstance(row.get("is_system_managed"), bool)
            or not isinstance(row.get("is_partition_child"), bool)
            or (row.get("owner") is not None and not isinstance(row.get("owner"), str))
            or (row.get("comment") is not None and not isinstance(row.get("comment"), str))
            or isinstance(estimated_row_count, bool)
            or (estimated_row_count is not None and not isinstance(estimated_row_count, int))
        ):
            raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
        return {
            "catalog_name": row.get("catalog_name"),
            "schema_name": row.get("schema_name"),
            "object_name": row.get("object_name"),
            "relkind": row.get("relkind"),
            "relpersistence": row.get("relpersistence"),
            "owner": row.get("owner"),
            "comment": row.get("comment"),
            "estimated_row_count": row.get("estimated_row_count"),
            "is_system_managed": row.get("is_system_managed"),
            "vendor_metadata": {
                "classification": classification,
                "is_partition_child": row.get("is_partition_child"),
            },
        }

    def list_schemas(
        self,
        *,
        database: str | None = None,
        timeout_seconds: int | None = None,
    ) -> tuple[SchemaMetadata, ...]:
        """Return every visible PostgreSQL schema as canonical metadata."""
        self._validate_discovery_timeout(timeout_seconds)
        profile = self._profile()
        target_database = self._resolve_discovery_database(database, profile)
        with self._discovery_connection(target_database, timeout_seconds) as connection:
            capabilities = self._discovery_capabilities(connection, target_database)
            self._enforce_identifier_lengths(connection, capabilities, target_database)
            rows = self._execute_discovery_query(
                connection,
                discovery_queries._SCHEMAS_QUERY,
                (target_database,),
            )
            try:
                schemas = tuple(normalize_postgresql_schema(self._safe_schema_row(row)) for row in rows)
            except MalformedDiscoveryResultError:
                raise
            except (TypeError, ValueError, KeyError):
                raise MalformedDiscoveryResultError("Schema discovery returned malformed data.") from None
        return tuple(sorted(schemas, key=lambda item: item.schema_name))

    def list_objects(
        self,
        *,
        database: str | None = None,
        schema: str,
        object_types: tuple[DatabaseObjectType, ...] | None = None,
        timeout_seconds: int | None = None,
    ) -> tuple[DatabaseObjectMetadata, ...]:
        """Return supported relations in one exact PostgreSQL schema."""
        self._validate_discovery_timeout(timeout_seconds)
        target_schema = self._validate_discovery_identifier(schema, "schema")
        profile = self._profile()
        target_database = self._resolve_discovery_database(database, profile)
        relkinds = self._resolve_discovery_object_types(object_types)
        if not relkinds:
            return ()
        with self._discovery_connection(target_database, timeout_seconds) as connection:
            capabilities = self._discovery_capabilities(connection, target_database)
            self._enforce_identifier_lengths(connection, capabilities, target_database, target_schema)
            rows = self._execute_discovery_query(
                connection,
                discovery_queries._OBJECTS_QUERY,
                (target_database, target_schema, list(relkinds)),
            )
            try:
                objects = tuple(normalize_postgresql_object(self._safe_object_row(row)) for row in rows)
                if any(item.object_type is DatabaseObjectType.UNKNOWN for item in objects):
                    raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
            except MalformedDiscoveryResultError:
                raise
            except (TypeError, ValueError, KeyError):
                raise MalformedDiscoveryResultError("Schema discovery returned malformed data.") from None
        return tuple(sorted(objects, key=lambda item: (item.object_type.value, item.object_name)))

    @staticmethod
    def _constraint_coverage(
        rows: tuple[dict[str, Any], ...] | None,
        constraint_type: str,
    ) -> CoverageStatus:
        if rows is None:
            return CoverageStatus.UNAVAILABLE
        selected = tuple(row for row in rows if row.get("constraint_type") == constraint_type)
        if any(
            row.get("constraint_oid") in (None, "")
            or not isinstance(row.get("column_name"), str)
            or row.get("column_name") == ""
            or isinstance(row.get("key_sequence"), bool)
            or not isinstance(row.get("key_sequence"), int)
            or row.get("key_sequence") <= 0
            for row in selected
        ):
            return CoverageStatus.PARTIAL
        return key_constraint_coverage(selected)

    @staticmethod
    def _foreign_key_coverage(rows: tuple[dict[str, Any], ...] | None) -> CoverageStatus:
        status = foreign_key_coverage(rows)
        if status is not CoverageStatus.COMPLETE or not rows:
            return status
        groups: dict[object, list[dict[str, Any]]] = {}
        for row in rows:
            identity = row.get("constraint_oid")
            groups.setdefault(identity, []).append(row)
        for identity, fragments in groups.items():
            expected = {row.get("expected_column_count") for row in fragments}
            sequences = {row.get("key_sequence") for row in fragments}
            local_columns = [row.get("local_column_name") for row in fragments]
            referenced_columns = [row.get("referenced_column_name") for row in fragments]
            if (
                identity in (None, "")
                or len(expected) != 1
                or isinstance(next(iter(expected)), bool)
                or not isinstance(next(iter(expected)), int)
                or next(iter(expected)) <= 0
                or len(sequences) != next(iter(expected))
                or len(fragments) != next(iter(expected))
                or any(
                    isinstance(sequence, bool)
                    or not isinstance(sequence, int)
                    or sequence <= 0
                    or sequence > next(iter(expected))
                    for sequence in sequences
                )
                or any(not isinstance(value, str) or value == "" for value in local_columns)
                or any(not isinstance(value, str) or value == "" for value in referenced_columns)
                or len(set(local_columns)) != len(local_columns)
                or len(set(referenced_columns)) != len(referenced_columns)
            ):
                return CoverageStatus.PARTIAL
        return CoverageStatus.COMPLETE

    @staticmethod
    def _foreign_key_rows_for_normalization(
        rows: tuple[dict[str, Any], ...] | None,
    ) -> tuple[dict[str, Any], ...] | None:
        """Detach deterministic valid FK pairs while retaining raw rows elsewhere."""
        if rows is None:
            return None
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for index, row in enumerate(rows):
            identity = stable_constraint_identity(row, index, "foreign-key")
            grouped.setdefault(identity, []).append(row)

        normalized: list[dict[str, Any]] = []
        for identity in sorted(grouped):
            candidates_by_sequence: dict[int, list[dict[str, Any]]] = {}
            for row in grouped[identity]:
                local_column = row.get("local_column_name")
                referenced_column = row.get("referenced_column_name")
                referenced_table = row.get("referenced_table")
                sequence = row.get("key_sequence")
                if (
                    not isinstance(local_column, str)
                    or local_column == ""
                    or not isinstance(referenced_column, str)
                    or referenced_column == ""
                    or not isinstance(referenced_table, str)
                    or referenced_table == ""
                    or isinstance(sequence, bool)
                    or not isinstance(sequence, int)
                    or sequence <= 0
                ):
                    continue
                candidates_by_sequence.setdefault(sequence, []).append(row)
            for sequence in sorted(candidates_by_sequence):
                preferred = min(candidates_by_sequence[sequence], key=_preference_key)
                detached = dict(preferred)
                detached["vendor_metadata"] = {}
                normalized.append(detached)
        return tuple(normalized)

    @staticmethod
    def _enrich_column_rows(
        rows: tuple[dict[str, Any], ...],
        primary_rows: tuple[dict[str, Any], ...] | None,
        unique_rows: tuple[dict[str, Any], ...] | None,
        foreign_rows: tuple[dict[str, Any], ...] | None,
        coverage: DiscoveryCoverage,
    ) -> tuple[dict[str, Any], ...]:
        def members(source: tuple[dict[str, Any], ...] | None, field: str) -> set[str]:
            return {
                value
                for row in source or ()
                if isinstance((value := row.get(field)), str) and value != ""
            }

        primary_members = members(primary_rows, "column_name")
        unique_members = members(unique_rows, "column_name")
        foreign_members = members(foreign_rows, "local_column_name")

        def marker(name: str, known: set[str], status: CoverageStatus) -> bool | None:
            if name in known:
                return True
            if status is CoverageStatus.COMPLETE:
                return False
            return None

        enriched: list[dict[str, Any]] = []
        for row in rows:
            name = row.get("column_name")
            if not isinstance(name, str) or name == "":
                continue
            item = dict(row)
            item["is_primary_key"] = marker(name, primary_members, coverage.primary_key)
            item["is_unique_key"] = marker(name, unique_members, coverage.unique_constraints)
            item["is_foreign_key"] = marker(name, foreign_members, coverage.foreign_keys)
            item["vendor_metadata"] = {"generation_kind": row.get("generation_kind")}
            enriched.append(item)
        return tuple(enriched)

    def get_table_metadata(
        self,
        *,
        database: str | None = None,
        schema: str,
        table: str,
        timeout_seconds: int | None = None,
    ) -> TableMetadata | None:
        """Discover one relation using independent fixed catalog queries."""
        self._validate_discovery_timeout(timeout_seconds)
        target_schema = self._validate_discovery_identifier(schema, "schema")
        target_table = self._validate_discovery_identifier(table, "table")
        profile = self._profile()
        target_database = self._resolve_discovery_database(database, profile)

        with self._discovery_connection(target_database, timeout_seconds) as connection:
            capabilities = self._discovery_capabilities(connection, target_database)
            self._enforce_identifier_lengths(
                connection,
                capabilities,
                target_database,
                target_schema,
                target_table,
            )
            base_rows = self._execute_discovery_query(
                connection,
                discovery_queries._BASE_OBJECT_QUERY,
                (target_database, target_schema, target_table),
            )
            if not base_rows:
                return None
            if len(base_rows) != 1:
                raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
            base_row = base_rows[0]
            relation_oid = base_row.get("relation_oid")
            if isinstance(relation_oid, bool) or not isinstance(relation_oid, int) or relation_oid <= 0:
                raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")
            try:
                safe_object_row = self._safe_object_row(base_row)
                object_metadata = normalize_postgresql_object(safe_object_row)
            except MalformedDiscoveryResultError:
                raise
            except (TypeError, ValueError, KeyError):
                raise MalformedDiscoveryResultError("Schema discovery returned malformed data.") from None
            if object_metadata.object_type is DatabaseObjectType.UNKNOWN:
                raise MalformedDiscoveryResultError("Schema discovery returned malformed data.")

            warnings: list[str] = []
            column_rows = self._execute_optional_discovery_query(
                connection, discovery_queries._COLUMNS_QUERY, (relation_oid,)
            )
            if column_rows is None:
                warnings.append("COLUMNS_UNAVAILABLE")

            server_version = capabilities["server_version_num"]
            key_query = (
                discovery_queries._KEY_CONSTRAINTS_QUERY_V18
                if server_version >= _POSTGRESQL_18_SERVER_VERSION
                else discovery_queries._KEY_CONSTRAINTS_QUERY_V14
            )
            key_rows = self._execute_optional_discovery_query(connection, key_query, (relation_oid,))
            if key_rows is None:
                warnings.append("KEY_CONSTRAINTS_UNAVAILABLE")

            foreign_query = (
                discovery_queries._FOREIGN_KEYS_QUERY_V18
                if server_version >= _POSTGRESQL_18_SERVER_VERSION
                else discovery_queries._FOREIGN_KEYS_QUERY_V14
            )
            foreign_rows = self._execute_optional_discovery_query(
                connection, foreign_query, (relation_oid,)
            )
            if foreign_rows is None:
                warnings.append("FOREIGN_KEYS_UNAVAILABLE")

            check_query = (
                discovery_queries._CHECK_CONSTRAINTS_QUERY_V18
                if server_version >= _POSTGRESQL_18_SERVER_VERSION
                else discovery_queries._CHECK_CONSTRAINTS_QUERY_V14
            )
            check_rows = self._execute_optional_discovery_query(connection, check_query, (relation_oid,))
            if check_rows is None:
                warnings.append("CHECK_CONSTRAINTS_UNAVAILABLE")

            is_view = object_metadata.object_type in {
                DatabaseObjectType.VIEW,
                DatabaseObjectType.MATERIALIZED_VIEW,
            }
            view_definition: str | None = None
            if is_view:
                view_rows = self._execute_optional_discovery_query(
                    connection, discovery_queries._VIEW_DEFINITION_QUERY, (relation_oid,)
                )
                if view_rows and isinstance(view_rows[0].get("view_definition"), str):
                    view_definition = view_rows[0]["view_definition"]
                    view_coverage = CoverageStatus.COMPLETE
                else:
                    view_coverage = CoverageStatus.UNAVAILABLE
                    warnings.append("VIEW_DEFINITION_UNAVAILABLE")
            else:
                view_coverage = CoverageStatus.NOT_APPLICABLE

            partition_applicable = object_metadata.object_type in {
                DatabaseObjectType.TABLE,
                DatabaseObjectType.PARTITIONED_TABLE,
                DatabaseObjectType.FOREIGN_TABLE,
            }
            partition_row: dict[str, Any] = {}
            if not partition_applicable:
                partition_coverage = CoverageStatus.NOT_APPLICABLE
            else:
                has_partition_helpers = (
                    capabilities["has_partition_key_helper"]
                    and capabilities["has_partition_constraint_helper"]
                )
                partition_query = (
                    discovery_queries._PARTITION_QUERY
                    if has_partition_helpers
                    else discovery_queries._PARTITION_QUERY_WITHOUT_HELPERS
                )
                partition_rows = self._execute_optional_discovery_query(
                    connection, partition_query, (relation_oid,)
                )
                if partition_rows is None:
                    partition_coverage = CoverageStatus.UNAVAILABLE
                    warnings.append("PARTITIONING_UNAVAILABLE")
                elif (
                    len(partition_rows) != 1
                    or not isinstance(partition_rows[0].get("is_partitioned"), bool)
                    or not isinstance(partition_rows[0].get("is_partition_child"), bool)
                ):
                    partition_coverage = CoverageStatus.PARTIAL
                    warnings.append("PARTITIONING_PARTIAL")
                else:
                    partition_row = partition_rows[0]
                    partition_needs_helpers = (
                        partition_row["is_partitioned"] or partition_row["is_partition_child"]
                    )
                    if has_partition_helpers or not partition_needs_helpers:
                        partition_coverage = CoverageStatus.COMPLETE
                    else:
                        partition_coverage = CoverageStatus.PARTIAL
                        if partition_row["is_partition_child"]:
                            partition_row = dict(partition_row)
                            partition_row["is_partitioned"] = True
                        warnings.append("PARTITION_HELPERS_UNAVAILABLE")

            primary_coverage = self._constraint_coverage(key_rows, "p")
            unique_coverage = self._constraint_coverage(key_rows, "u")
            foreign_coverage = self._foreign_key_coverage(foreign_rows)
            normalization_foreign_rows = self._foreign_key_rows_for_normalization(foreign_rows)
            columns_coverage = CoverageStatus.UNAVAILABLE if column_rows is None else CoverageStatus.COMPLETE
            valid_columns = tuple(
                row
                for row in column_rows or ()
                if isinstance(row.get("column_name"), str)
                and row.get("column_name") != ""
                and isinstance(row.get("ordinal_position"), int)
                and not isinstance(row.get("ordinal_position"), bool)
                and row.get("ordinal_position") > 0
                and isinstance(row.get("data_type"), str)
                and row.get("data_type") != ""
            )
            if column_rows is not None and len(valid_columns) != len(column_rows):
                columns_coverage = CoverageStatus.PARTIAL
                warnings.append("COLUMNS_PARTIAL")

            valid_checks = tuple(
                row
                for row in check_rows or ()
                if isinstance(row.get("expression"), str) and row.get("expression") != ""
            )
            check_coverage = CoverageStatus.UNAVAILABLE if check_rows is None else CoverageStatus.COMPLETE
            if check_rows is not None and len(valid_checks) != len(check_rows):
                check_coverage = CoverageStatus.PARTIAL
                warnings.append("CHECK_CONSTRAINTS_PARTIAL")

            if primary_coverage is CoverageStatus.PARTIAL:
                warnings.append("PRIMARY_KEY_PARTIAL")
            if unique_coverage is CoverageStatus.PARTIAL:
                warnings.append("UNIQUE_CONSTRAINTS_PARTIAL")
            if foreign_coverage is CoverageStatus.PARTIAL:
                warnings.append("FOREIGN_KEYS_PARTIAL")

            coverage = DiscoveryCoverage(
                columns=columns_coverage,
                primary_key=primary_coverage,
                unique_constraints=unique_coverage,
                foreign_keys=foreign_coverage,
                check_constraints=check_coverage,
                comments=CoverageStatus.COMPLETE,
                estimated_row_count=(
                    CoverageStatus.NOT_APPLICABLE
                    if is_view
                    else CoverageStatus.COMPLETE
                ),
                view_definition=view_coverage,
                partitioning=partition_coverage,
                clustering=CoverageStatus.NOT_APPLICABLE,
                warnings=tuple(sorted(set(warnings))),
            )

            primary_rows = (
                None if key_rows is None else tuple(row for row in key_rows if row.get("constraint_type") == "p")
            )
            unique_rows = (
                None if key_rows is None else tuple(row for row in key_rows if row.get("constraint_type") == "u")
            )
            enriched_columns = self._enrich_column_rows(
                valid_columns,
                primary_rows,
                unique_rows,
                foreign_rows,
                coverage,
            )

            object_row = dict(safe_object_row)
            object_row["view_definition"] = view_definition
            object_row["is_partitioned"] = partition_row.get("is_partitioned")
            object_row["partitioning_expression"] = partition_row.get("partitioning_expression")
            object_vendor_metadata = dict(safe_object_row["vendor_metadata"])
            if partition_applicable:
                object_vendor_metadata.update(
                    {
                        "partition_strategy": partition_row.get("partition_strategy"),
                        "parent_schema": partition_row.get("parent_schema"),
                        "parent_table": partition_row.get("parent_table"),
                        "partition_bound": partition_row.get("partition_bound"),
                        "partition_constraint": partition_row.get("partition_constraint"),
                    }
                )
            else:
                object_vendor_metadata.pop("is_partition_child", None)
            object_row["vendor_metadata"] = object_vendor_metadata
            try:
                return normalize_postgresql_table(
                    object_row,
                    column_rows=enriched_columns,
                    primary_key_rows=primary_rows,
                    unique_constraint_rows=unique_rows,
                    foreign_key_rows=normalization_foreign_rows,
                    check_constraint_rows=None if check_rows is None else valid_checks,
                    coverage=coverage,
                )
            except (TypeError, ValueError, KeyError):
                raise MalformedDiscoveryResultError("Schema discovery returned malformed data.") from None

    def close(self) -> None:
        """Satisfy the connector contract; connections are already per-call."""
        return None


Connector = PostgreSQLConnector
