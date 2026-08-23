"""Build safe, profile-bound JDBC reads for a future Spark transport runner."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.discovery import TableMetadata


class SparkJdbcPlanError(ValueError):
    """Raised when a source cannot be represented as a safe Spark JDBC read."""


_JDBC_DRIVERS = {
    "postgresql": "org.postgresql.Driver",
    "mysql": "com.mysql.cj.jdbc.Driver",
}


def _quote(database_type: str, value: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise SparkJdbcPlanError("Spark JDBC identifiers are invalid.")
    if database_type == "mysql":
        if len(value) > 64:
            raise SparkJdbcPlanError("Spark JDBC identifiers are invalid.")
        return "`" + value.replace("`", "``") + "`"
    return '"' + value.replace('"', '""') + '"'


@dataclass(frozen=True, slots=True, repr=False)
class SparkJdbcReadPlan:
    """One internally generated JDBC query plus private driver properties."""

    database_type: str
    jdbc_url: str = field(repr=False)
    query: str = field(repr=False)
    connection_properties: Mapping[str, str] = field(repr=False)
    selected_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.database_type not in _JDBC_DRIVERS:
            raise SparkJdbcPlanError("Spark JDBC source is unsupported.")
        if not isinstance(self.jdbc_url, str) or not self.jdbc_url.startswith("jdbc:"):
            raise SparkJdbcPlanError("Spark JDBC URL is invalid.")
        if not isinstance(self.query, str) or not self.query.startswith("SELECT "):
            raise SparkJdbcPlanError("Spark JDBC query is invalid.")
        if not isinstance(self.selected_columns, tuple) or not self.selected_columns:
            raise SparkJdbcPlanError("Spark JDBC columns are invalid.")
        if not isinstance(self.connection_properties, Mapping):
            raise TypeError("connection_properties must be a mapping.")
        values = {str(key): str(value) for key, value in self.connection_properties.items()}
        if values.get("driver") != _JDBC_DRIVERS[self.database_type]:
            raise SparkJdbcPlanError("Spark JDBC driver is invalid.")
        object.__setattr__(self, "connection_properties", MappingProxyType(values))

    def safe_dict(self) -> dict[str, object]:
        """Return diagnostics without an endpoint, username, or password."""

        return {
            "database_type": self.database_type,
            "driver": self.connection_properties["driver"],
            "column_count": len(self.selected_columns),
        }


class SparkJdbcReadPlanFactory:
    """Translate immutable profile and discovered metadata into a JDBC plan."""

    @staticmethod
    def build(profile: ConnectionProfile, source_table: TableMetadata) -> SparkJdbcReadPlan:
        if not isinstance(profile, ConnectionProfile) or not isinstance(source_table, TableMetadata):
            raise TypeError("profile and source_table must be canonical models.")
        database_type = profile.db_type
        if database_type not in _JDBC_DRIVERS or source_table.system.casefold() != database_type:
            raise SparkJdbcPlanError("Spark JDBC source is unsupported.")
        if source_table.catalog_name != profile.database:
            raise SparkJdbcPlanError("Spark JDBC source database does not match its profile.")
        columns = tuple(column.column_name for column in source_table.columns)
        if not columns:
            raise SparkJdbcPlanError("Spark JDBC source has no columns.")
        options = profile.connection_options_copy()
        port = options.get("port", 5432 if database_type == "postgresql" else 3306)
        if isinstance(port, bool) or not isinstance(port, (int, str)) or not str(port).isdigit():
            raise SparkJdbcPlanError("Spark JDBC port is invalid.")
        quoted_columns = ", ".join(_quote(database_type, name) for name in columns)
        if database_type == "postgresql":
            relation = ".".join(
                (_quote(database_type, source_table.schema_name), _quote(database_type, source_table.object_name))
            )
            jdbc_url = f"jdbc:postgresql://{profile.host}:{int(port)}/{profile.database}"
        else:
            if source_table.schema_name != profile.database:
                raise SparkJdbcPlanError("MySQL Spark JDBC schema must match its profile database.")
            relation = ".".join(
                (_quote(database_type, profile.database), _quote(database_type, source_table.object_name))
            )
            jdbc_url = f"jdbc:mysql://{profile.host}:{int(port)}/{profile.database}"
        return SparkJdbcReadPlan(
            database_type=database_type,
            jdbc_url=jdbc_url,
            query=f"SELECT {quoted_columns} FROM {relation}",
            connection_properties={
                "user": profile.username,
                "password": profile.password,
                "driver": _JDBC_DRIVERS[database_type],
            },
            selected_columns=columns,
        )
