"""MySQL implementation of the database connector interface."""

from __future__ import annotations

import contextlib
import json
import re
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, NoReturn

from schemabridge.config import Config, ConfigError, ConnectionConfig
from schemabridge.connectors.base import DatabaseConnector, unique_column_names
from schemabridge.models.mapping import SqlDialect
from schemabridge.models.discovery import (
    ConstraintType,
    CoverageStatus,
    DatabaseObjectType,
    DiscoveryCoverage,
    KeyConstraintMetadata,
    ObjectPersistence,
    TableMetadata,
)
from schemabridge.models.transport import (
    BatchWriteResult,
    DataBatch,
    StagingColumn,
    StagingTableDefinition,
    TransportRelation,
)
from schemabridge.models.metadata import CanonicalType
from schemabridge.normalizers.mysql import normalize_mysql_column
from schemabridge.transport.base import (
    BatchTransportConnectionError,
    BatchTransportError,
    BatchTransportTimeoutError,
    UnsupportedStagingTypeError,
)
from schemabridge.connectors.discovery import (
    SchemaDiscoveryConnectionError,
    SchemaDiscoveryError,
    SchemaDiscoveryTimeoutError,
)

if TYPE_CHECKING:
    from schemabridge.models.connection_profile import ConnectionProfile


class MySQLConnector(DatabaseConnector):
    """Connector implementation for MySQL via mysql-connector-python."""

    profile_db_type = "mysql"

    def validation_sql_dialect(self) -> SqlDialect:
        """Advertise generated MySQL validation-query support."""

        return SqlDialect.MYSQL

    def _driver(self):
        """Load the optional MySQL driver only when this backend is selected."""
        # Import on first use so installations that do not need MySQL can still
        # start SchemaBridge's shared connector layer.
        try:
            import mysql.connector  # type: ignore
        except ImportError as exc:
            raise ConfigError("Install mysql-connector-python to use the MySQL connector.") from exc
        return mysql.connector

    def _profile(self) -> ConnectionConfig | ConnectionProfile:
        """Return the active neutral profile after checking MySQL requirements."""
        profile = self._connection_profile
        if profile is None:
            profile = Config.connection_config()
        if not profile.host:
            raise ConfigError("DB_HOST is required for the MySQL connector.")
        return profile

    def _normalize_database(self, database: str | None, fallback: str) -> str:
        """Select an explicit database or fall back to the configured default."""
        return (database or fallback or "").strip()

    def _connection_kwargs(
        self,
        profile: ConnectionConfig | ConnectionProfile,
        database: str | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Translate framework configuration into MySQL driver arguments."""
        options = (
            profile.connection_options_copy()
            if self._connection_profile is not None
            else dict(profile.connection_options or {})
        )
        port = int(options.pop("port", 3306))
        effective_timeout = timeout_seconds if timeout_seconds is not None else profile.timeout_seconds
        kwargs: dict[str, Any] = {
            "host": profile.host,
            "port": port,
            "user": profile.username,
            "password": profile.password,
            "connection_timeout": effective_timeout,
            "read_timeout": effective_timeout,
            "write_timeout": effective_timeout,
        }
        if database:
            kwargs["database"] = database
        kwargs.update(options)
        return kwargs

    def _row_limit_sql(self, sql: str, max_rows: int) -> str:
        """Apply the configured result cap to row-returning MySQL statements."""
        normalized_sql = sql.strip().rstrip(";")
        if not re.match(r"(?is)^\s*SELECT\b", normalized_sql):
            return normalized_sql
        # Respect an explicit LIMIT; otherwise enforce the framework-wide cap
        # using MySQL's native syntax.
        limit_match = re.search(r"\bLIMIT\s+(\d+)\b", normalized_sql, flags=re.I)
        if limit_match:
            safe_limit = min(int(limit_match.group(1)), max_rows)
            return normalized_sql[: limit_match.start(1)] + str(safe_limit) + normalized_sql[limit_match.end(1) :]
        return f"{normalized_sql} LIMIT {max_rows}"

    def _fetch_rows(self, cursor, max_rows: int | None = None) -> dict[str, Any]:
        """Convert driver tuples into JSON-ready dictionaries by column name."""
        columns = unique_column_names([column[0] for column in cursor.description]) if cursor.description else []
        raw_rows = cursor.fetchmany(max_rows) if columns and max_rows and hasattr(cursor, "fetchmany") else cursor.fetchall() if columns else []
        rows = [dict(zip(columns, row)) for row in raw_rows[:max_rows] if columns] if max_rows else [dict(zip(columns, row)) for row in raw_rows]
        return {"columns": columns, "rows": rows}

    def connect(self, database: str | None = None, timeout_seconds: int | None = None) -> Any:
        """Open a MySQL connection with the active profile and timeout."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        kwargs = self._connection_kwargs(profile, target_database or None, timeout_seconds)
        return self._driver().connect(**kwargs)

    @contextlib.contextmanager
    def _connection(self, database: str | None = None, timeout_seconds: int | None = None):
        """Yield an operation-scoped connection and always close it."""
        connection = self.connect(database=database, timeout_seconds=timeout_seconds)
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _identifier(value: str, field_name: str) -> str:
        """Validate one exact MySQL identifier without interpreting it as SQL."""

        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > 64
            or "\x00" in value
        ):
            raise ConfigError(f"{field_name} is invalid.")
        return value

    @classmethod
    def _quote_identifier(cls, value: str) -> str:
        """Safely backtick-quote one MySQL identifier."""

        cls._identifier(value, "identifier")
        return "`" + value.replace("`", "``") + "`"

    def _exact_database(self, requested: str | None) -> str:
        """Require the operation database to match the immutable profile."""

        profile = self._profile()
        configured = self._identifier(profile.database, "configured database")
        database = configured if requested is None else self._identifier(requested, "database")
        if database != configured:
            raise ConfigError("Requested database must exactly match the configured database.")
        return database

    @staticmethod
    def _raise_transport_error(error: BaseException, *, connection_phase: bool = False) -> NoReturn:
        errno = getattr(error, "errno", None)
        if isinstance(error, TimeoutError) or errno in {3024}:
            raise BatchTransportTimeoutError("Batch extraction timed out.") from None
        if connection_phase or errno in {2002, 2003, 2005, 2006, 2013}:
            raise BatchTransportConnectionError("Batch extraction connection failed.") from None
        raise BatchTransportError("Batch extraction failed.") from None

    @staticmethod
    def _raise_discovery_error(error: BaseException, *, connection_phase: bool = False) -> NoReturn:
        errno = getattr(error, "errno", None)
        if isinstance(error, TimeoutError) or errno in {3024}:
            raise SchemaDiscoveryTimeoutError("Schema discovery timed out.") from None
        if connection_phase or errno in {2002, 2003, 2005, 2006, 2013}:
            raise SchemaDiscoveryConnectionError("Schema discovery connection failed.") from None
        raise SchemaDiscoveryError("Schema discovery failed.") from None

    def _require_staging_write_profile(self) -> ConnectionConfig | ConnectionProfile:
        """Require an explicitly write-enabled named target profile."""

        profile = self._profile()
        if self._connection_profile is None or profile.write_enabled is not True:
            raise ConfigError(
                "A write-enabled named profile is required for staging operations."
            )
        return profile

    @classmethod
    def _staging_relation_sql(cls, relation: TransportRelation) -> str:
        if relation.catalog_name is None:
            raise ConfigError("A database is required for a MySQL staging table.")
        return ".".join(
            cls._quote_identifier(value)
            for value in (relation.catalog_name, relation.object_name)
        )

    @staticmethod
    def _mysql_staging_type(column: StagingColumn) -> str:
        """Choose a lossless MySQL landing type without guessing."""

        kind = column.canonical_type
        if kind is CanonicalType.STRING:
            return "LONGTEXT"
        if kind is CanonicalType.INTEGER:
            return "DECIMAL(38,0)"
        if kind is CanonicalType.DECIMAL:
            precision, scale = column.numeric_precision, column.numeric_scale
            if precision is not None and scale is not None and 1 <= precision <= 65 and 0 <= scale <= min(30, precision):
                return f"DECIMAL({precision},{scale})"
            return "LONGTEXT"
        if kind is CanonicalType.FLOAT:
            return "DOUBLE"
        if kind is CanonicalType.BOOLEAN:
            return "BOOLEAN"
        if kind is CanonicalType.DATE:
            return "DATE"
        if kind is CanonicalType.TIME:
            return "TIME" if column.datetime_precision is None else f"TIME({column.datetime_precision})" if column.datetime_precision <= 6 else "LONGTEXT"
        if kind is CanonicalType.TIMESTAMP:
            return "DATETIME" if column.datetime_precision is None else f"DATETIME({column.datetime_precision})" if column.datetime_precision <= 6 else "LONGTEXT"
        if kind is CanonicalType.TIMESTAMP_TZ:
            return "LONGTEXT"
        if kind is CanonicalType.BINARY:
            return "LONGBLOB"
        if kind is CanonicalType.SEMI_STRUCTURED:
            return "JSON"
        raise UnsupportedStagingTypeError(
            "The source type cannot be represented safely in MySQL staging."
        )

    @classmethod
    def _staging_column_sql(cls, column: StagingColumn) -> str:
        rendered = f"{cls._quote_identifier(column.name)} {cls._mysql_staging_type(column)}"
        return rendered + (" NOT NULL" if column.nullable is False else "")

    @staticmethod
    def _validate_staging_timeout(timeout_seconds: int) -> None:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
            raise ConfigError("timeout_seconds must be a positive integer.")

    @staticmethod
    def _raise_staging_error(error: BaseException, *, connection_phase: bool = False) -> NoReturn:
        errno = getattr(error, "errno", None)
        if isinstance(error, TimeoutError) or errno == 3024:
            raise BatchTransportTimeoutError("Staging operation timed out.") from None
        if connection_phase or errno in {2002, 2003, 2005, 2006, 2013}:
            raise BatchTransportConnectionError("Staging connection failed.") from None
        raise BatchTransportError("Staging operation failed.") from None

    @contextlib.contextmanager
    def _staging_connection(self, database: str, timeout_seconds: int):
        connection = None
        try:
            profile = self._require_staging_write_profile()
            connection = self._driver().connect(
                **self._connection_kwargs(profile, database, timeout_seconds)
            )
            try:
                yield connection
            except BaseException:
                with contextlib.suppress(Exception):
                    connection.rollback()
                raise
            finally:
                connection.close()
        except (ConfigError, BatchTransportError):
            raise
        except Exception as error:
            self._raise_staging_error(error, connection_phase=connection is None)

    @staticmethod
    def _staging_bind_value(value: object, column: StagingColumn) -> object:
        if value is None:
            return None
        if column.canonical_type is CanonicalType.SEMI_STRUCTURED:
            try:
                return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            except (TypeError, ValueError):
                raise BatchTransportError("A semi-structured staging value is invalid.") from None
        if MySQLConnector._mysql_staging_type(column) == "LONGTEXT" and column.canonical_type is not CanonicalType.STRING:
            return str(value)
        return value

    def prepare_staging_table(self, *, definition: StagingTableDefinition, timeout_seconds: int) -> None:
        """Create one exact, non-overwriting MySQL landing table."""

        if not isinstance(definition, StagingTableDefinition):
            raise TypeError("definition must be a StagingTableDefinition.")
        self._validate_staging_timeout(timeout_seconds)
        profile = self._require_staging_write_profile()
        database = self._exact_database(definition.relation.catalog_name)
        if definition.relation.schema_name != database:
            raise ConfigError("MySQL schema must exactly match the configured database.")
        query = f"CREATE TABLE {self._staging_relation_sql(definition.relation)} ({', '.join(self._staging_column_sql(column) for column in definition.columns)})"
        with self._staging_connection(database, timeout_seconds) as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(query)
                connection.commit()
            finally:
                cursor.close()

    def write_batch(self, *, definition: StagingTableDefinition, batch: DataBatch, timeout_seconds: int) -> BatchWriteResult:
        """Insert one complete, bound batch and require exact row evidence."""

        if not isinstance(definition, StagingTableDefinition) or not isinstance(batch, DataBatch):
            raise TypeError("definition must be a StagingTableDefinition and batch must be a DataBatch.")
        self._validate_staging_timeout(timeout_seconds)
        if batch.column_names != tuple(column.name for column in definition.columns):
            raise BatchTransportError("Batch columns do not match the staging table definition.")
        profile = self._require_staging_write_profile()
        if batch.row_count > profile.max_rows:
            raise ConfigError("Batch row count exceeds the selected profile limit.")
        database = self._exact_database(definition.relation.catalog_name)
        if definition.relation.schema_name != database:
            raise ConfigError("MySQL schema must exactly match the configured database.")
        columns_sql = ", ".join(self._quote_identifier(name) for name in batch.column_names)
        # MySQL validates a bound JSON document when assigning it to a JSON
        # column. Binding the serialized document directly is portable across
        # supported server versions (unlike CAST(... AS JSON)).
        placeholders = "(" + ", ".join("%s" for _ in definition.columns) + ")"
        query = f"INSERT INTO {self._staging_relation_sql(definition.relation)} ({columns_sql}) VALUES {', '.join(placeholders for _ in batch.rows)}"
        parameters = tuple(self._staging_bind_value(value, column) for row in batch.rows for value, column in zip(row, definition.columns))
        with self._staging_connection(database, timeout_seconds) as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(query, parameters)
                rows_written = cursor.rowcount
                if isinstance(rows_written, bool) or not isinstance(rows_written, int) or rows_written != batch.row_count:
                    raise BatchTransportError("MySQL did not confirm the complete staging batch.")
                connection.commit()
            finally:
                cursor.close()
        return BatchWriteResult(batch_number=batch.batch_number, rows_received=batch.row_count, rows_written=rows_written)

    def drop_staging_table(self, *, relation: TransportRelation, timeout_seconds: int) -> None:
        """Idempotently remove one exact MySQL staging table."""

        if not isinstance(relation, TransportRelation):
            raise TypeError("relation must be a TransportRelation.")
        self._validate_staging_timeout(timeout_seconds)
        self._require_staging_write_profile()
        database = self._exact_database(relation.catalog_name)
        if relation.schema_name != database:
            raise ConfigError("MySQL schema must exactly match the configured database.")
        with self._staging_connection(database, timeout_seconds) as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {self._staging_relation_sql(relation)}")
                connection.commit()
            finally:
                cursor.close()

    def test_connection(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Verify connectivity and return a small non-secret server snapshot."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        with self._connection(database=target_database or None, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    @@hostname AS server_name,
                    VERSION() AS version,
                    USER() AS logged_in_user,
                    UTC_TIMESTAMP() AS connection_checked_at
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
        """Reuse the lightweight connection test as the MySQL health check."""
        return self.test_connection(database=database, timeout_seconds=timeout_seconds)

    def list_databases(self, timeout_seconds: int | None = None) -> dict[str, Any]:
        """List databases visible to the configured MySQL account."""
        profile = self._profile()
        with self._connection(timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT SCHEMA_NAME AS name
                FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                ORDER BY SCHEMA_NAME
                """
            )
            payload = self._fetch_rows(cursor)
            cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "count": len(payload["rows"]), "databases": payload["rows"]}

    def list_tables(self, database: str | None = None, schema: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """List tables and views through MySQL information_schema metadata."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        if not target_database:
            raise ConfigError("Database name is required to list MySQL tables.")
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
                """,
                (target_database,),
            )
            payload = self._fetch_rows(cursor)
            cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "database": target_database, "schema": schema or "", "count": len(payload["rows"]), "tables": payload["rows"]}

    def describe_table(self, database: str | None = None, table: str | None = None, schema: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Return ordered column definitions for one MySQL table."""
        if not table:
            raise ConfigError("Table name is required.")
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        if not target_database:
            raise ConfigError("Database name is required to describe a MySQL table.")
        with self._connection(database=target_database, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (target_database, table),
            )
            payload = self._fetch_rows(cursor)
            cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "database": target_database, "schema": schema or target_database, "table": table, "column_count": len(payload["rows"]), "columns": payload["rows"]}

    def get_table_metadata(
        self,
        *,
        database: str | None = None,
        schema: str,
        table: str,
        timeout_seconds: int | None = None,
    ) -> TableMetadata | None:
        """Discover one MySQL base table as canonical SchemaBridge metadata."""

        target_database = self._exact_database(database or schema)
        if self._identifier(schema, "schema") != target_database:
            raise ConfigError("MySQL schema must exactly match the configured database.")
        table_name = self._identifier(table, "table")
        opened = False
        try:
            with self._connection(
                database=target_database,
                timeout_seconds=timeout_seconds,
            ) as connection:
                opened = True
                cursor = connection.cursor()
                try:
                    cursor.execute(
                        """
                        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, TABLE_COMMENT, TABLE_ROWS
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                          AND TABLE_TYPE = 'BASE TABLE'
                        """,
                        (target_database, table_name),
                    )
                    object_rows = self._fetch_rows(cursor)["rows"]
                    if not object_rows:
                        return None
                    if len(object_rows) != 1:
                        raise SchemaDiscoveryError("Schema discovery failed.")
                    cursor.execute(
                        """
                        SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE,
                               CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
                               DATETIME_PRECISION, COLUMN_DEFAULT, COLUMN_COMMENT,
                               COLLATION_NAME, EXTRA, GENERATION_EXPRESSION
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                        ORDER BY ORDINAL_POSITION
                        """,
                        (target_database, table_name),
                    )
                    column_rows = self._fetch_rows(cursor)["rows"]
                    cursor.execute(
                        """
                        SELECT tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE,
                               kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                          ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                         AND tc.TABLE_NAME = kcu.TABLE_NAME
                         AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                        WHERE tc.TABLE_SCHEMA = %s AND tc.TABLE_NAME = %s
                          AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
                        ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
                        """,
                        (target_database, table_name),
                    )
                    key_rows = self._fetch_rows(cursor)["rows"]
                finally:
                    cursor.close()
        except (ConfigError, SchemaDiscoveryError):
            raise
        except Exception as error:
            self._raise_discovery_error(error, connection_phase=not opened)

        grouped: dict[tuple[str, str], list[tuple[int, str]]] = {}
        for raw in key_rows:
            row = {str(key).casefold(): value for key, value in raw.items()}
            name = self._identifier(str(row.get("constraint_name", "")), "constraint")
            kind = str(row.get("constraint_type", "")).upper()
            column = self._identifier(str(row.get("column_name", "")), "column")
            ordinal = int(row.get("ordinal_position") or 0)
            grouped.setdefault((name, kind), []).append((ordinal, column))
        primary_key = None
        unique_constraints = []
        primary_columns: set[str] = set()
        unique_columns: set[str] = set()
        for (name, kind), values in sorted(grouped.items()):
            columns = tuple(column for _, column in sorted(values))
            constraint_type = (
                ConstraintType.PRIMARY_KEY
                if kind == "PRIMARY KEY"
                else ConstraintType.UNIQUE
            )
            model = KeyConstraintMetadata(
                name=name,
                constraint_type=constraint_type,
                columns=columns,
                is_enforced=True,
                is_validated=True,
                vendor_metadata={},
            )
            if constraint_type is ConstraintType.PRIMARY_KEY:
                primary_key = model
                primary_columns.update(columns)
            else:
                unique_constraints.append(model)
                unique_columns.update(columns)
        columns = tuple(
            sorted(
                (
                    normalize_mysql_column(
                        row,
                        catalog_name=target_database,
                        schema_name=target_database,
                        table_name=table_name,
                        is_primary_key=str(
                            {str(k).casefold(): v for k, v in row.items()}.get(
                                "column_name", ""
                            )
                        )
                        in primary_columns,
                        is_unique_key=str(
                            {str(k).casefold(): v for k, v in row.items()}.get(
                                "column_name", ""
                            )
                        )
                        in unique_columns,
                    )
                    for row in column_rows
                ),
                key=lambda item: (item.ordinal_position or 0, item.column_name),
            )
        )
        if not columns:
            raise SchemaDiscoveryError("Schema discovery failed.")
        object_row = {
            str(key).casefold(): value for key, value in object_rows[0].items()
        }
        estimated = object_row.get("table_rows")
        return TableMetadata(
            catalog_name=target_database,
            schema_name=target_database,
            object_name=table_name,
            system="mysql",
            object_type=DatabaseObjectType.TABLE,
            persistence=ObjectPersistence.PERMANENT,
            comment=str(object_row.get("table_comment") or "") or None,
            estimated_row_count=(int(estimated) if estimated is not None else None),
            is_system_managed=False,
            columns=columns,
            primary_key=primary_key,
            unique_constraints=tuple(unique_constraints),
            coverage=DiscoveryCoverage(
                columns=CoverageStatus.COMPLETE,
                primary_key=CoverageStatus.COMPLETE,
                unique_constraints=CoverageStatus.COMPLETE,
                foreign_keys=CoverageStatus.UNAVAILABLE,
                check_constraints=CoverageStatus.UNAVAILABLE,
                comments=CoverageStatus.COMPLETE,
                estimated_row_count=CoverageStatus.COMPLETE,
                view_definition=CoverageStatus.NOT_APPLICABLE,
                partitioning=CoverageStatus.UNAVAILABLE,
                clustering=CoverageStatus.NOT_APPLICABLE,
                warnings=("MYSQL_CHECKS_NOT_DISCOVERED", "MYSQL_FOREIGN_KEYS_NOT_DISCOVERED"),
            ),
            vendor_metadata=object_row,
        )

    def execute_query(self, query: str, *, parameters: tuple[object, ...] | None = None, database: str | None = None, timeout_seconds: int | None = None, max_rows: int | None = None) -> Any:
        """Execute validated SQL and normalize read or committed write output."""
        profile = self._profile()
        target_database = self._normalize_database(database, profile.database)
        limited_query = self._row_limit_sql(query, max_rows or profile.max_rows)
        with self._connection(database=target_database or None, timeout_seconds=timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(limited_query, parameters) if parameters else cursor.execute(limited_query)
            payload = self._fetch_rows(cursor, max_rows or profile.max_rows)
            rows_affected = cursor.rowcount if cursor.description is None else len(payload["rows"])
            if cursor.description is None:
                # MySQL does not persist data-changing statements until commit.
                conn.commit()
            cursor.close()
        return {"connector_type": self.__class__.__name__, "db_type": profile.db_type, "database": target_database, "columns": payload["columns"], "rows": payload["rows"], "rows_affected": rows_affected}

    def read_batches(
        self,
        *,
        relation: TransportRelation,
        column_names: tuple[str, ...],
        batch_size: int,
        timeout_seconds: int,
    ) -> Iterator[DataBatch]:
        """Yield MySQL source rows incrementally without buffering the table."""

        if not isinstance(relation, TransportRelation):
            raise TypeError("relation must be a TransportRelation.")
        if not isinstance(column_names, tuple) or not column_names:
            raise ConfigError("column_names must be a non-empty tuple.")
        if len(set(column_names)) != len(column_names):
            raise ConfigError("column_names must be unique.")
        for column_name in column_names:
            self._identifier(column_name, "column_name")
        for value, name in (
            (batch_size, "batch_size"),
            (timeout_seconds, "timeout_seconds"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ConfigError(f"{name} must be a positive integer.")
        profile = self._profile()
        if batch_size > profile.max_rows:
            raise ConfigError("batch_size exceeds the selected profile limit.")
        requested_database = relation.catalog_name or relation.schema_name
        database = self._exact_database(requested_database)
        if relation.schema_name != database:
            raise ConfigError("MySQL schema must exactly match the configured database.")
        columns_sql = ", ".join(
            self._quote_identifier(name) for name in column_names
        )
        relation_sql = ".".join(
            (
                self._quote_identifier(database),
                self._quote_identifier(relation.object_name),
            )
        )
        query = f"SELECT {columns_sql} FROM {relation_sql}"
        opened = False
        try:
            connection = self.connect(
                database=database,
                timeout_seconds=timeout_seconds,
            )
            opened = True
            try:
                cursor = connection.cursor(buffered=False)
                try:
                    cursor.execute(query)
                    batch_number = 1
                    while True:
                        rows = tuple(
                            tuple(row) for row in cursor.fetchmany(batch_size)
                        )
                        if not rows:
                            break
                        yield DataBatch(
                            batch_number=batch_number,
                            column_names=column_names,
                            rows=rows,
                        )
                        batch_number += 1
                finally:
                    cursor.close()
                connection.rollback()
            finally:
                connection.close()
        except (ConfigError, BatchTransportError):
            raise
        except Exception as error:
            if opened:
                with contextlib.suppress(Exception):
                    connection.rollback()
                with contextlib.suppress(Exception):
                    connection.close()
            self._raise_transport_error(error, connection_phase=not opened)

    def close(self) -> None:
        """Satisfy the connector contract; connections are already per-call."""
        return None


Connector = MySQLConnector
