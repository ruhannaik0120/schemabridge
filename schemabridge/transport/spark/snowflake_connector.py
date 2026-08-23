"""Build trusted Snowflake Connector for Spark source-read plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.discovery import TableMetadata
from schemabridge.models.transport import StagingTableDefinition

from .jdbc import SparkJdbcPlanError, _quote


_SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"


@dataclass(frozen=True, slots=True, repr=False)
class SparkSnowflakeReadPlan:
    """One metadata-derived Snowflake Spark query and private connector options."""

    query: str = field(repr=False)
    connection_properties: Mapping[str, str] = field(repr=False)
    selected_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.query, str) or not self.query.startswith("SELECT "):
            raise SparkJdbcPlanError("Snowflake Spark query is invalid.")
        if not isinstance(self.selected_columns, tuple) or not self.selected_columns:
            raise SparkJdbcPlanError("Snowflake Spark columns are invalid.")
        if not isinstance(self.connection_properties, Mapping):
            raise TypeError("connection_properties must be a mapping.")
        values = {str(key): str(value) for key, value in self.connection_properties.items()}
        required = {"sfURL", "sfUser", "sfPassword", "sfDatabase", "sfSchema", "sfWarehouse"}
        if not required.issubset(values) or not all(values[key] for key in required):
            raise SparkJdbcPlanError("Snowflake Spark connection options are incomplete.")
        object.__setattr__(self, "connection_properties", MappingProxyType(values))

    def safe_dict(self) -> dict[str, object]:
        """Return diagnostics that exclude endpoint, identity, and credentials."""

        return {
            "source": _SNOWFLAKE_SOURCE,
            "column_count": len(self.selected_columns),
            "role_configured": "sfRole" in self.connection_properties,
        }


class SparkSnowflakeReadPlanFactory:
    """Translate an immutable Snowflake profile and discovered table safely."""

    @staticmethod
    def build(profile: ConnectionProfile, source_table: TableMetadata) -> SparkSnowflakeReadPlan:
        if not isinstance(profile, ConnectionProfile) or not isinstance(source_table, TableMetadata):
            raise TypeError("profile and source_table must be canonical models.")
        if profile.db_type != "snowflake" or source_table.system.casefold() != "snowflake":
            raise SparkJdbcPlanError("Snowflake Spark source is unsupported.")
        if source_table.catalog_name != profile.database:
            raise SparkJdbcPlanError("Snowflake Spark source database does not match its profile.")
        columns = tuple(column.column_name for column in source_table.columns)
        if not columns:
            raise SparkJdbcPlanError("Snowflake Spark source has no columns.")
        options = profile.connection_options_copy()
        warehouse = options.get("warehouse")
        role = options.get("role")
        if not isinstance(warehouse, str) or not warehouse.strip():
            raise SparkJdbcPlanError("Snowflake Spark warehouse is required.")
        if role is not None and (not isinstance(role, str) or not role.strip()):
            raise SparkJdbcPlanError("Snowflake Spark role is invalid.")
        quoted_columns = ", ".join(_quote("snowflake", name) for name in columns)
        relation = ".".join(
            _quote("snowflake", value)
            for value in (source_table.catalog_name, source_table.schema_name, source_table.object_name)
        )
        connection_properties = {
            "sfURL": f"{profile.host}.snowflakecomputing.com",
            "sfUser": profile.username,
            "sfPassword": profile.password,
            "sfDatabase": profile.database,
            "sfSchema": source_table.schema_name,
            "sfWarehouse": warehouse.strip(),
        }
        if role is not None:
            connection_properties["sfRole"] = role.strip()
        return SparkSnowflakeReadPlan(
            query=f"SELECT {quoted_columns} FROM {relation}",
            connection_properties=connection_properties,
            selected_columns=columns,
        )


class SparkSnowflakeDataFrameReader:
    """Load a DataFrame through the official Snowflake Spark data source."""

    @staticmethod
    def load(session: object, read_plan: SparkSnowflakeReadPlan) -> object:
        if not isinstance(read_plan, SparkSnowflakeReadPlan):
            raise TypeError("read_plan must be SparkSnowflakeReadPlan.")
        try:
            reader = session.read.format(_SNOWFLAKE_SOURCE)
            for key, value in read_plan.connection_properties.items():
                reader = reader.option(key, value)
            return reader.option("query", read_plan.query).load()
        except (AttributeError, TypeError, ValueError, SparkJdbcPlanError):
            raise SparkJdbcPlanError("Snowflake Spark source read failed.") from None


@dataclass(frozen=True, slots=True, repr=False)
class SparkSnowflakeWritePlan:
    """Private Snowflake options plus one exact managed staging table."""

    staging_table: str = field(repr=False)
    connection_properties: Mapping[str, str] = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.staging_table, str) or not self.staging_table:
            raise SparkJdbcPlanError("Snowflake Spark staging relation is invalid.")
        if not isinstance(self.connection_properties, Mapping):
            raise TypeError("connection_properties must be a mapping.")
        values = {str(key): str(value) for key, value in self.connection_properties.items()}
        required = {"sfURL", "sfUser", "sfPassword", "sfDatabase", "sfSchema", "sfWarehouse"}
        if not required.issubset(values) or not all(values[key] for key in required):
            raise SparkJdbcPlanError("Snowflake Spark connection options are incomplete.")
        object.__setattr__(self, "connection_properties", MappingProxyType(values))

    def safe_dict(self) -> dict[str, object]:
        return {"target": _SNOWFLAKE_SOURCE, "role_configured": "sfRole" in self.connection_properties}


class SparkSnowflakeWritePlanFactory:
    """Allow only write-enabled Snowflake profiles to append to managed staging."""

    @staticmethod
    def build(profile: ConnectionProfile, definition: StagingTableDefinition) -> SparkSnowflakeWritePlan:
        if not isinstance(profile, ConnectionProfile) or not isinstance(definition, StagingTableDefinition):
            raise TypeError("profile and definition must be canonical models.")
        relation = definition.relation
        if (
            profile.db_type != "snowflake"
            or profile.write_enabled is not True
            or relation.catalog_name != profile.database
            or not relation.object_name.startswith("SB_STAGE_")
        ):
            raise SparkJdbcPlanError("Snowflake Spark target is not a managed write-enabled staging table.")
        options = profile.connection_options_copy()
        warehouse = options.get("warehouse")
        role = options.get("role")
        if not isinstance(warehouse, str) or not warehouse.strip():
            raise SparkJdbcPlanError("Snowflake Spark warehouse is required.")
        if role is not None and (not isinstance(role, str) or not role.strip()):
            raise SparkJdbcPlanError("Snowflake Spark role is invalid.")
        properties = {
            "sfURL": f"{profile.host}.snowflakecomputing.com",
            "sfUser": profile.username,
            "sfPassword": profile.password,
            "sfDatabase": profile.database,
            "sfSchema": relation.schema_name,
            "sfWarehouse": warehouse.strip(),
        }
        if role is not None:
            properties["sfRole"] = role.strip()
        return SparkSnowflakeWritePlan(
            staging_table=".".join(
                _quote("snowflake", value)
                for value in (relation.catalog_name, relation.schema_name, relation.object_name)
            ),
            connection_properties=properties,
        )


class SparkSnowflakeDataFrameWriter:
    """Append a DataFrame only to a pre-created SchemaBridge staging table."""

    @staticmethod
    def append(dataframe: object, write_plan: SparkSnowflakeWritePlan) -> None:
        if not isinstance(write_plan, SparkSnowflakeWritePlan):
            raise TypeError("write_plan must be SparkSnowflakeWritePlan.")
        try:
            writer = dataframe.write.format(_SNOWFLAKE_SOURCE)
            for key, value in write_plan.connection_properties.items():
                writer = writer.option(key, value)
            writer.option("dbtable", write_plan.staging_table).mode("append").save()
        except (AttributeError, TypeError, ValueError, SparkJdbcPlanError):
            raise SparkJdbcPlanError("Snowflake Spark staging write failed.") from None
