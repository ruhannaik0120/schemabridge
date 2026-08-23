"""Build and execute guarded Spark JDBC writes into managed staging tables."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.transport import StagingTableDefinition

from .jdbc import SparkJdbcPlanError, _JDBC_DRIVERS, _quote


class SparkJdbcWriteError(RuntimeError):
    """Raised when Spark cannot append to the managed staging table."""


@dataclass(frozen=True, slots=True, repr=False)
class SparkJdbcWritePlan:
    """Private JDBC credentials plus one exact SchemaBridge staging relation."""

    database_type: str
    jdbc_url: str = field(repr=False)
    staging_table: str
    connection_properties: Mapping[str, str] = field(repr=False)

    def __post_init__(self) -> None:
        if self.database_type not in _JDBC_DRIVERS:
            raise SparkJdbcPlanError("Spark JDBC target is unsupported.")
        if not isinstance(self.jdbc_url, str) or not self.jdbc_url.startswith("jdbc:"):
            raise SparkJdbcPlanError("Spark JDBC URL is invalid.")
        if not isinstance(self.staging_table, str) or not self.staging_table:
            raise SparkJdbcPlanError("Spark staging relation is invalid.")
        values = {str(key): str(value) for key, value in self.connection_properties.items()}
        if values.get("driver") != _JDBC_DRIVERS[self.database_type]:
            raise SparkJdbcPlanError("Spark JDBC driver is invalid.")
        object.__setattr__(self, "connection_properties", MappingProxyType(values))

    def safe_dict(self) -> dict[str, str]:
        """Expose no host, URL, username, password, or table name."""

        return {"database_type": self.database_type, "driver": self.connection_properties["driver"]}


class SparkJdbcWritePlanFactory:
    """Permit Spark writes only to a write-enabled profile's managed staging table."""

    @staticmethod
    def build(profile: ConnectionProfile, definition: StagingTableDefinition) -> SparkJdbcWritePlan:
        if not isinstance(profile, ConnectionProfile) or not isinstance(definition, StagingTableDefinition):
            raise TypeError("profile and definition must be canonical models.")
        database_type = profile.db_type
        if database_type not in _JDBC_DRIVERS or profile.write_enabled is not True:
            raise SparkJdbcPlanError("Spark JDBC target is not write-enabled.")
        relation = definition.relation
        if relation.catalog_name != profile.database or not relation.object_name.startswith("SB_STAGE_"):
            raise SparkJdbcPlanError("Spark JDBC target is not a managed staging table.")
        options = profile.connection_options_copy()
        port = options.get("port", 5432 if database_type == "postgresql" else 3306)
        if isinstance(port, bool) or not isinstance(port, (int, str)) or not str(port).isdigit():
            raise SparkJdbcPlanError("Spark JDBC port is invalid.")
        if database_type == "postgresql":
            staging_table = ".".join((_quote(database_type, relation.schema_name), _quote(database_type, relation.object_name)))
            jdbc_url = f"jdbc:postgresql://{profile.host}:{int(port)}/{profile.database}"
        else:
            if relation.schema_name != profile.database:
                raise SparkJdbcPlanError("MySQL Spark staging schema must match its profile database.")
            staging_table = ".".join((_quote(database_type, profile.database), _quote(database_type, relation.object_name)))
            jdbc_url = f"jdbc:mysql://{profile.host}:{int(port)}/{profile.database}"
        return SparkJdbcWritePlan(
            database_type=database_type,
            jdbc_url=jdbc_url,
            staging_table=staging_table,
            connection_properties={
                "user": profile.username,
                "password": profile.password,
                "driver": _JDBC_DRIVERS[database_type],
            },
        )


class SparkJdbcDataFrameWriter:
    """Append a distributed DataFrame into the pre-created managed staging table."""

    @staticmethod
    def append(dataframe: object, write_plan: SparkJdbcWritePlan) -> None:
        if not isinstance(write_plan, SparkJdbcWritePlan):
            raise TypeError("write_plan must be SparkJdbcWritePlan.")
        try:
            writer = dataframe.write.format("jdbc")
            writer = writer.option("url", write_plan.jdbc_url)
            writer = writer.option("dbtable", write_plan.staging_table)
            for key, value in write_plan.connection_properties.items():
                writer = writer.option(key, value)
            writer.mode("append").save()
        except (AttributeError, TypeError, ValueError):
            raise SparkJdbcWriteError("Spark JDBC staging write failed.") from None
