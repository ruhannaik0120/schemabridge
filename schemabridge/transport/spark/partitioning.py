"""Conservative parallel JDBC partition planning for Spark source reads."""

from __future__ import annotations

from dataclasses import dataclass

from schemabridge.models.discovery import TableMetadata
from schemabridge.models.metadata import CanonicalType

from .jdbc import SparkJdbcPlanError, SparkJdbcReadPlan, _quote


@dataclass(frozen=True, slots=True)
class SparkJdbcPartitionPlan:
    """Database-proven integer bounds for one parallel Spark JDBC read."""

    read_plan: SparkJdbcReadPlan
    partition_column: str
    lower_bound: int
    upper_bound: int
    num_partitions: int

    def __post_init__(self) -> None:
        if not isinstance(self.read_plan, SparkJdbcReadPlan):
            raise TypeError("read_plan must be SparkJdbcReadPlan.")
        if not isinstance(self.partition_column, str) or not self.partition_column:
            raise SparkJdbcPlanError("Spark partition column is invalid.")
        for value, name in ((self.lower_bound, "lower_bound"), (self.upper_bound, "upper_bound"), (self.num_partitions, "num_partitions")):
            if isinstance(value, bool) or not isinstance(value, int):
                raise SparkJdbcPlanError(f"Spark {name} is invalid.")
        if self.lower_bound > self.upper_bound or self.num_partitions <= 0:
            raise SparkJdbcPlanError("Spark partition bounds are invalid.")
        if self.partition_column not in self.read_plan.selected_columns:
            raise SparkJdbcPlanError("Spark partition column is not selected.")

    def spark_options(self) -> dict[str, str]:
        """Return only Spark JDBC partition options, never credentials."""

        return {
            "partitionColumn": self.partition_column,
            "lowerBound": str(self.lower_bound),
            "upperBound": str(self.upper_bound),
            "numPartitions": str(self.num_partitions),
        }


class SparkJdbcPartitionPlanner:
    """Allow parallelism only for a discovered single-column integer primary key."""

    @staticmethod
    def partition_column(source_table: TableMetadata) -> str:
        if not isinstance(source_table, TableMetadata):
            raise TypeError("source_table must be TableMetadata.")
        primary_key = source_table.primary_key
        if primary_key is None or len(primary_key.columns) != 1:
            raise SparkJdbcPlanError("Spark requires a single-column primary key for parallel reads.")
        name = primary_key.columns[0]
        column = next((item for item in source_table.columns if item.column_name == name), None)
        if column is None or column.canonical_type is not CanonicalType.INTEGER:
            raise SparkJdbcPlanError("Spark requires an integer primary key for parallel reads.")
        return name

    @classmethod
    def bounds_query(cls, read_plan: SparkJdbcReadPlan, source_table: TableMetadata) -> str:
        """Generate the trusted query that obtains exact JDBC split bounds."""

        if not isinstance(read_plan, SparkJdbcReadPlan) or not isinstance(source_table, TableMetadata):
            raise TypeError("read_plan and source_table must be canonical models.")
        if source_table.system.casefold() != read_plan.database_type:
            raise SparkJdbcPlanError("Spark partition source does not match its JDBC plan.")
        column = cls.partition_column(source_table)
        quoted_column = _quote(read_plan.database_type, column)
        if read_plan.database_type == "postgresql":
            relation = ".".join(
                (_quote("postgresql", source_table.schema_name), _quote("postgresql", source_table.object_name))
            )
        elif read_plan.database_type == "mysql":
            relation = ".".join(
                (_quote("mysql", source_table.schema_name), _quote("mysql", source_table.object_name))
            )
        else:
            raise SparkJdbcPlanError("Spark partition source is unsupported.")
        return f"SELECT MIN({quoted_column}) AS lower_bound, MAX({quoted_column}) AS upper_bound FROM {relation}"

    @classmethod
    def bind(
        cls,
        read_plan: SparkJdbcReadPlan,
        source_table: TableMetadata,
        *,
        lower_bound: int | None,
        upper_bound: int | None,
        num_partitions: int,
    ) -> SparkJdbcPartitionPlan:
        """Bind bounds obtained from the source database to a read plan."""

        column = cls.partition_column(source_table)
        if lower_bound is None or upper_bound is None:
            raise SparkJdbcPlanError("Spark cannot partition an empty source table.")
        return SparkJdbcPartitionPlan(
            read_plan=read_plan,
            partition_column=column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
        )
