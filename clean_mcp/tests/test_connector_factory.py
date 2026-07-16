"""Verify connector registration, selection, and unsupported-type handling."""

from connectors.base import DatabaseConnector
from connectors.demo.connector import DemoConnector
from connectors.factory import ConnectorFactory, SUPPORTED_CONNECTORS
from connectors.postgresql.connector import PostgreSQLConnector
from connectors.sqlserver.connector import SQLServerConnector
from config import Config
import pytest


# Registry discovery feeds diagnostics and profile-switch validation.
def test_factory_lists_supported_connectors():
    assert "demo" in ConnectorFactory.supported_connectors()
    assert set(ConnectorFactory.supported_connectors()) == set(SUPPORTED_CONNECTORS)


# Omitted selection deliberately falls back to the configured DB_TYPE.
def test_factory_returns_sqlserver_connector_by_default():
    Config.DB_TYPE = "sqlserver"
    connector = ConnectorFactory.create()

    assert isinstance(connector, DatabaseConnector)
    assert isinstance(connector, SQLServerConnector)


# Explicit selections must construct their matching concrete implementation.
def test_factory_returns_mysql_connector_for_mysql_type():
    connector = ConnectorFactory.create("mysql")

    assert isinstance(connector, DatabaseConnector)
    assert connector.__class__.__name__ == "MySQLConnector"


def test_factory_returns_postgresql_connector_for_postgresql_type():
    connector = ConnectorFactory.create("postgresql")

    assert isinstance(connector, DatabaseConnector)
    assert isinstance(connector, PostgreSQLConnector)


def test_factory_returns_snowflake_connector_for_snowflake_type():
    connector = ConnectorFactory.create("snowflake")

    assert isinstance(connector, DatabaseConnector)
    assert connector.__class__.__name__ == "SnowflakeConnector"


def test_factory_returns_demo_connector_for_demo_type():
    connector = ConnectorFactory.create("demo")

    assert isinstance(connector, DemoConnector)


# Unknown names fail at the factory boundary, before any driver is imported.
def test_factory_rejects_invalid_connector_type():
    with pytest.raises(ValueError, match="Unsupported connector type"):
        ConnectorFactory.create("oracle")
