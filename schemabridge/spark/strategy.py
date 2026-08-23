"""Guarded distributed JDBC transport into SchemaBridge staging tables."""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.discovery import TableMetadata
from schemabridge.models.transport import BatchTransportResult, TransportRelation
from schemabridge.services.batch_transport import BatchTransportService
from schemabridge.transport.base import (
    BatchProgressReporter,
    BatchSourceReader,
    BatchTransportError,
    StagingTableWriter,
)

from .config import SparkTransportSettings
from .jdbc import SparkJdbcReadPlanFactory
from .partitioning import SparkJdbcPartitionPlanner
from .reader import SparkJdbcDataFrameReader
from .runtime import SparkSessionFactory
from .snowflake_connector import (
    SparkSnowflakeDataFrameReader,
    SparkSnowflakeDataFrameWriter,
    SparkSnowflakeReadPlanFactory,
    SparkSnowflakeWritePlanFactory,
)
from .writer import SparkJdbcDataFrameWriter, SparkJdbcWritePlanFactory


class SparkTransportStrategy:
    """Move one eligible table through Spark, never directly to final target."""

    strategy_name = "SPARK_JDBC_PARTITIONED"

    def __init__(
        self,
        settings: SparkTransportSettings,
        *,
        session_factory: SparkSessionFactory | None = None,
        dataframe_reader: SparkJdbcDataFrameReader | None = None,
        dataframe_writer: SparkJdbcDataFrameWriter | None = None,
        snowflake_dataframe_reader: SparkSnowflakeDataFrameReader | None = None,
        snowflake_dataframe_writer: SparkSnowflakeDataFrameWriter | None = None,
    ) -> None:
        if not isinstance(settings, SparkTransportSettings):
            raise TypeError("settings must be SparkTransportSettings.")
        self.settings = settings
        self.session_factory = session_factory or SparkSessionFactory()
        self.dataframe_reader = dataframe_reader or SparkJdbcDataFrameReader()
        self.dataframe_writer = dataframe_writer or SparkJdbcDataFrameWriter()
        self.snowflake_dataframe_reader = snowflake_dataframe_reader or SparkSnowflakeDataFrameReader()
        self.snowflake_dataframe_writer = snowflake_dataframe_writer or SparkSnowflakeDataFrameWriter()

    @staticmethod
    def _execute_query(connector: object) -> Callable[..., object]:
        execute = getattr(connector, "execute_query", None)
        if not callable(execute):
            raise BatchTransportError("Spark requires query-capable source and staging connectors.")
        return execute

    @staticmethod
    def _integer(result: object, key: str) -> int | None:
        if not isinstance(result, dict):
            raise BatchTransportError("Spark transport query returned invalid evidence.")
        rows = result.get("rows")
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
            raise BatchTransportError("Spark transport query returned invalid evidence.")
        value = rows[0].get(key)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise BatchTransportError("Spark transport query returned invalid evidence.")
        return value

    def _load_dataframe(
        self,
        session: object,
        source_reader: BatchSourceReader,
        source_profile: ConnectionProfile,
        source_table: TableMetadata,
        *,
        timeout_seconds: int,
    ) -> object:
        if source_profile.db_type == "snowflake":
            return self.snowflake_dataframe_reader.load(
                session, SparkSnowflakeReadPlanFactory.build(source_profile, source_table)
            )
        read_plan = SparkJdbcReadPlanFactory.build(source_profile, source_table)
        source_query = self._execute_query(source_reader)
        bounds = source_query(
            SparkJdbcPartitionPlanner.bounds_query(read_plan, source_table),
            database=source_profile.database,
            timeout_seconds=timeout_seconds,
            max_rows=1,
        )
        partition_plan = SparkJdbcPartitionPlanner.bind(
            read_plan,
            source_table,
            lower_bound=self._integer(bounds, "lower_bound"),
            upper_bound=self._integer(bounds, "upper_bound"),
            num_partitions=self.settings.num_partitions,
        )
        return self.dataframe_reader.load(session, partition_plan)

    def _append_dataframe(
        self,
        dataframe: object,
        target_profile: ConnectionProfile,
        definition: object,
    ) -> object:
        if target_profile.db_type == "snowflake":
            write_plan = SparkSnowflakeWritePlanFactory.build(target_profile, definition)
            self.snowflake_dataframe_writer.append(dataframe, write_plan)
            return write_plan
        write_plan = SparkJdbcWritePlanFactory.build(target_profile, definition)
        self.dataframe_writer.append(dataframe, write_plan)
        return write_plan

    def transfer(
        self,
        *,
        source_reader: BatchSourceReader,
        staging_writer: StagingTableWriter,
        source_profile: ConnectionProfile | None,
        target_profile: ConnectionProfile | None,
        transport_id: UUID,
        source_table: TableMetadata,
        target_database: str,
        target_schema: str,
        batch_size: int,
        timeout_seconds: int,
        progress_reporter: BatchProgressReporter | None,
    ) -> BatchTransportResult:
        """Create staging, Spark-read/write it, then prove exact row equality."""

        del batch_size, progress_reporter
        if not isinstance(source_profile, ConnectionProfile) or not isinstance(target_profile, ConnectionProfile):
            raise BatchTransportError("Spark transport requires resolved connection profiles.")
        try:
            staging_relation = BatchTransportService.staging_relation(
                transport_id=transport_id,
                target_database=target_database,
                target_schema=target_schema,
            )
            definition = BatchTransportService.staging_definition(source_table, staging_relation)
            staging_writer.prepare_staging_table(definition=definition, timeout_seconds=timeout_seconds)

            session = self.session_factory.create(self.settings)
            try:
                dataframe = self._load_dataframe(
                    session,
                    source_reader,
                    source_profile,
                    source_table,
                    timeout_seconds=timeout_seconds,
                )
                rows_read = dataframe.count()
                if isinstance(rows_read, bool) or not isinstance(rows_read, int) or rows_read < 0:
                    raise BatchTransportError("Spark returned an invalid source row count.")
                write_plan = self._append_dataframe(dataframe, target_profile, definition)
            finally:
                self.session_factory.stop(session)

            target_query = self._execute_query(staging_writer)
            rows_written = self._integer(
                target_query(
                    f"SELECT COUNT(*) AS row_count FROM {write_plan.staging_table}",
                    database=target_profile.database,
                    timeout_seconds=timeout_seconds,
                    max_rows=1,
                ),
                "row_count",
            )
            if rows_written is None or rows_written != rows_read:
                raise BatchTransportError("Spark staging row counts did not match.")
            return BatchTransportResult(
                transport_id=transport_id,
                source_relation=TransportRelation(
                    catalog_name=source_table.catalog_name,
                    schema_name=source_table.schema_name,
                    object_name=source_table.object_name,
                ),
                staging_relation=staging_relation,
                # Spark does one logical distributed write; its JDBC partitions
                # are not connector batches and must not be reported as such.
                batch_size=1,
                batch_count=1,
                column_count=len(definition.columns),
                rows_read=rows_read,
                rows_written=rows_written,
            )
        except BatchTransportError:
            raise
        except Exception:
            raise BatchTransportError("Spark staging transport failed.") from None
