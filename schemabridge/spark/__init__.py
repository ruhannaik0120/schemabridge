"""Optional Spark support for large SchemaBridge transport operations."""

from .config import SparkTransportSettings
from .jdbc import SparkJdbcPlanError, SparkJdbcReadPlan, SparkJdbcReadPlanFactory
from .partitioning import SparkJdbcPartitionPlan, SparkJdbcPartitionPlanner
from .reader import SparkJdbcDataFrameReader, SparkJdbcReadError
from .writer import (
    SparkJdbcDataFrameWriter,
    SparkJdbcWriteError,
    SparkJdbcWritePlan,
    SparkJdbcWritePlanFactory,
)
from .runtime import SparkRuntimeUnavailableError, SparkSessionFactory
from .strategy import SparkTransportStrategy

__all__ = [
    "SparkJdbcPlanError",
    "SparkJdbcReadPlan",
    "SparkJdbcReadPlanFactory",
    "SparkJdbcPartitionPlan",
    "SparkJdbcPartitionPlanner",
    "SparkJdbcDataFrameReader",
    "SparkJdbcReadError",
    "SparkJdbcDataFrameWriter",
    "SparkJdbcWriteError",
    "SparkJdbcWritePlan",
    "SparkJdbcWritePlanFactory",
    "SparkRuntimeUnavailableError",
    "SparkSessionFactory",
    "SparkTransportSettings",
    "SparkTransportStrategy",
]
