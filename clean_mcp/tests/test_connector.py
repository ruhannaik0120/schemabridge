"""Connector tests for the packaged SQL Server connector."""

from config import Config
from connectors.sqlserver.connector import SQLServerConnector


# These ODBC doubles verify connector behavior without a live SQL Server.
class FakeCursor:
    """Return deterministic metadata rows for connector assertions."""
    def __init__(self):
        self.description = [("server_name",), ("version",), ("logged_in_user",), ("utc_time",)]
        self._rows = [("server", "version", "user", "time")]
        self.executed = []

    def execute(self, sql, *params):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows


class FakeConnection:
    """Track cursor access and closure for lifecycle assertions."""
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.closed = False
        self.autocommit = False
        self.timeout = 0

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


class FakeDriver:
    """Capture pyodbc arguments while returning an in-memory connection."""
    Error = RuntimeError
    version = "fake-odbc-1.0"

    def __init__(self, connection=None):
        self.connection = connection or FakeConnection()
        self.captured = {}

    def connect(self, conn_str, timeout=30):
        self.captured = {"conn_str": conn_str, "timeout": timeout}
        return self.connection


# Establish one known profile so tests isolate only connector behavior.
def _configure_generic_settings(monkeypatch):
    monkeypatch.setenv("DB_TYPE", "sqlserver")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_DATABASE", "devdb")
    monkeypatch.setenv("DB_USERNAME", "")
    monkeypatch.setenv("DB_PASSWORD", "")
    monkeypatch.setenv("DB_CONNECTION_OPTIONS", '{"driver": "ODBC Driver 18 for SQL Server"}')
    monkeypatch.setenv("DB_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("DB_MAX_ROWS", "50")
    Config.load()


# Context-managed operations must always close their ODBC connection.
def test_connection_opens_and_closes(monkeypatch):
    _configure_generic_settings(monkeypatch)
    sql_connector = SQLServerConnector()
    driver = FakeDriver()
    monkeypatch.setattr(sql_connector, "_driver", lambda: driver)
    connection = sql_connector.connect(database="devdb")

    assert isinstance(connection, FakeConnection)
    assert "SERVER={localhost}" in driver.captured["conn_str"]
    assert "DATABASE={devdb}" in driver.captured["conn_str"]
    assert "Trusted_Connection={yes}" in driver.captured["conn_str"]
    assert "Encrypt={no}" in driver.captured["conn_str"]
    assert "TrustServerCertificate={yes}" in driver.captured["conn_str"]
    assert driver.captured["timeout"] == 45
    assert connection.timeout == 45


# Remote hosts receive secure encryption defaults unless explicitly overridden.
def test_remote_sqlserver_defaults_to_validated_encryption(monkeypatch):
    _configure_generic_settings(monkeypatch)
    monkeypatch.setenv("DB_HOST", "sql.company.internal")
    Config.load()
    sql_connector = SQLServerConnector()

    connection_string = sql_connector._connection_options(Config.connection_config())

    assert "Encrypt={yes}" in connection_string
    assert "TrustServerCertificate={no}" in connection_string


# A successful check returns useful metadata through the common connector shape.
def test_test_connection_returns_server_snapshot(monkeypatch):
    _configure_generic_settings(monkeypatch)
    fake_connection = FakeConnection()

    sql_connector = SQLServerConnector()
    monkeypatch.setattr(sql_connector, "_driver", lambda: FakeDriver(fake_connection))
    snapshot = sql_connector.test_connection(database="devdb")

    assert snapshot["connection_status"] == "connected"
    assert snapshot["db_type"] == "sqlserver"
    assert snapshot["server_information"]["server_name"] == "server"
    assert fake_connection.closed is True


def test_odbc_values_escape_connection_string_delimiters(monkeypatch):
    _configure_generic_settings(monkeypatch)
    monkeypatch.setenv("DB_USERNAME", "qa-user")
    monkeypatch.setenv("DB_PASSWORD", "p}ass;word")
    Config.load()
    connector = SQLServerConnector()

    connection_string = connector._build_connection_string(
        Config.connection_config(),
        "qa};SERVER=shadow",
    )

    assert "PWD={p}}ass;word}" in connection_string
    assert connection_string.endswith("DATABASE={qa}};SERVER=shadow};")


def test_sqlserver_rejects_partial_explicit_credentials(monkeypatch):
    _configure_generic_settings(monkeypatch)
    monkeypatch.setenv("DB_USERNAME", "qa-user")
    monkeypatch.setenv("DB_PASSWORD", "")
    Config.load()

    connector = SQLServerConnector()

    import pytest

    with pytest.raises(Exception, match="must either both be set or both be empty"):
        connector._connection_options(Config.connection_config())
