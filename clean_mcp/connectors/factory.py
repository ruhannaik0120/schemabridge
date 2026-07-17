"""Factory for creating database connectors based on configuration."""

from __future__ import annotations

from importlib import import_module

from connectors.base import DatabaseConnector

SUPPORTED_CONNECTORS: dict[str, str] = {
    "demo": "connectors.demo.connector",
    "sqlserver": "connectors.sqlserver.connector",
    "mysql": "connectors.mysql.connector",
    "postgresql": "connectors.postgresql.connector",
    "snowflake": "connectors.snowflake.connector",
}


class ConnectorFactory:
    """Instantiate the appropriate connector based on configuration."""

    @staticmethod
    def supported_connectors() -> tuple[str, ...]:
        """Return registered connector names in deterministic display order."""

        return tuple(sorted(SUPPORTED_CONNECTORS))

    @staticmethod
    def create(connector_type: str | None = None) -> DatabaseConnector:
        """Build the requested connector, falling back to runtime configuration."""

        # Normalization makes profile and environment values case-insensitive.
        connector_name = (connector_type or "").strip().lower()
        if not connector_name:
            from config import Config

            connector_name = Config.DB_TYPE.strip().lower()

        if not connector_name:
            raise ValueError("DB_TYPE is required to select a connector.")

        module_path = SUPPORTED_CONNECTORS.get(connector_name)
        if module_path is None:
            supported = ", ".join(ConnectorFactory.supported_connectors())
            raise ValueError(
                f"Unsupported connector type: {connector_name}. Supported values: {supported}."
            )

        # Lazy imports isolate optional drivers: using PostgreSQL should not
        # require importing Snowflake or SQL Server dependencies at startup.
        module = import_module(module_path)
        # Every connector exports the same alias, so adding a backend requires
        # one registry entry and no changes to the service or MCP tool layers.
        connector_class = getattr(module, "Connector", None)
        if connector_class is None:
            raise ValueError(
                f"Connector module {module_path} must expose Connector."
            )

        return connector_class()

    @staticmethod
    def create_for_profile(profile: object) -> DatabaseConnector:
        """Build a connector bound exclusively to an immutable profile."""

        # Keep this import local: ConnectionProfile reads the connector-name
        # registry from this module while it is imported.
        from models.connection_profile import ConnectionProfile

        if not isinstance(profile, ConnectionProfile):
            raise TypeError("profile must be a ConnectionProfile.")

        module_path = SUPPORTED_CONNECTORS.get(profile.db_type)
        if module_path is None:
            raise ValueError("The profile selects an unsupported connector type.")

        module = import_module(module_path)
        connector_class = getattr(module, "Connector", None)
        if connector_class is None:
            raise ValueError("The selected connector module is invalid.")
        return connector_class(profile=profile)
