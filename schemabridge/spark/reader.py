"""Apply trusted SchemaBridge JDBC plans to Spark DataFrame reads."""

from __future__ import annotations

from .jdbc import SparkJdbcPlanError
from .partitioning import SparkJdbcPartitionPlan


class SparkJdbcReadError(RuntimeError):
    """Raised when Spark cannot start the approved JDBC source read."""


class SparkJdbcDataFrameReader:
    """Load one partitioned DataFrame without accepting caller-provided SQL."""

    @staticmethod
    def load(session: object, partition_plan: SparkJdbcPartitionPlan) -> object:
        """Configure Spark JDBC from an internally generated plan and load it."""

        if not isinstance(partition_plan, SparkJdbcPartitionPlan):
            raise TypeError("partition_plan must be SparkJdbcPartitionPlan.")
        try:
            reader = session.read.format("jdbc")
            reader = reader.option("url", partition_plan.read_plan.jdbc_url)
            # Spark's partition options require dbtable rather than its query
            # option.  The alias is fixed code, not a user-controlled value.
            reader = reader.option(
                "dbtable", f"({partition_plan.read_plan.query}) AS sb_source"
            )
            for key, value in partition_plan.read_plan.connection_properties.items():
                reader = reader.option(key, value)
            for key, value in partition_plan.spark_options().items():
                reader = reader.option(key, value)
            return reader.load()
        except (AttributeError, TypeError, ValueError, SparkJdbcPlanError):
            raise SparkJdbcReadError("Spark JDBC source read failed.") from None
