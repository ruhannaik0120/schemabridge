"""Service-layer tests for delegation and response shaping."""

from config import Config
from models.errors import ErrorCode
from services.query_service import QueryService


class FakeConnector:
    """Record service delegation while returning deterministic connector data."""

    def __init__(self):
        self.calls = []

    def test_connection(self, database=None, timeout_seconds=None):
        self.calls.append(("test_connection", database, timeout_seconds))
        return {
            "connector_type": "FakeConnector",
            "connection_status": "connected",
            "server_information": {"server_name": "server", "version": "version-string"},
        }

    def health_check(self, database=None, timeout_seconds=None):
        self.calls.append(("health_check", database, timeout_seconds))
        return {
            "connector_type": "FakeConnector",
            "connection_status": "connected",
            "server_information": {"server_name": "server", "version": "version-string"},
        }

    def list_databases(self, timeout_seconds=None):
        self.calls.append(("list_databases", timeout_seconds))
        return {"count": 2, "databases": [{"name": "alpha"}, {"name": "beta"}]}

    def list_tables(self, database=None, schema=None, timeout_seconds=None):
        self.calls.append(("list_tables", database, schema, timeout_seconds))
        return {"count": 1, "tables": [{"TABLE_SCHEMA": schema or "dbo", "TABLE_NAME": "items"}]}

    def describe_table(self, database=None, table=None, schema=None, timeout_seconds=None):
        self.calls.append(("describe_table", database, table, schema, timeout_seconds))
        return {
            "database": database,
            "schema": schema,
            "table": table,
            "column_count": 1,
            "columns": [{"COLUMN_NAME": "name", "DATA_TYPE": "nvarchar"}],
        }

    def execute_query(self, query, *, database=None, timeout_seconds=None, max_rows=None):
        self.calls.append(("execute_query", query, database, timeout_seconds, max_rows))
        return {"columns": ["name"], "rows": [("alpha",), ("beta",)]}

    def close(self):
        self.calls.append(("close",))


def _configure_settings(monkeypatch):
    """Set stable policy values without loading credentials or live drivers."""

    monkeypatch.setenv("DB_TYPE", "sqlserver")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_DATABASE", "sales")
    monkeypatch.setenv("DB_USERNAME", "dev_user")
    monkeypatch.setenv("DB_PASSWORD", "dev_pass")
    monkeypatch.setenv("DB_CONNECTION_OPTIONS", '{"driver": "ODBC Driver 18 for SQL Server"}')
    monkeypatch.setenv("DB_TIMEOUT_SECONDS", "20")
    monkeypatch.setenv("DB_MAX_ROWS", "25")
    monkeypatch.setenv("DB_ACTIVE_PROFILE", "sqlserver-sandbox")
    Config.load()


def test_execute_select_query_delegates_to_connector(monkeypatch):
    _configure_settings(monkeypatch)
    connector = FakeConnector()
    service = QueryService(connector)

    response = service.execute_select_query(sql="SELECT name FROM sys.databases", environment="ignored").to_dict()

    assert response["success"] is True
    assert response["tool"] == "execute_select_query"
    assert response["environment"] == "SQLSERVER"
    assert response["row_count"] == 2
    assert response["metadata"]["row_limit"] == 25
    assert connector.calls[0][0] == "execute_query"
    assert connector.calls[0][1] == "SELECT name FROM sys.databases"


def test_health_returns_structured_status(monkeypatch):
    _configure_settings(monkeypatch)
    connector = FakeConnector()
    service = QueryService(connector)

    response = service.health(environment="ignored").to_dict()

    assert response["success"] is True
    assert response["tool"] == "health"
    assert response["environment"] == "SQLSERVER"
    assert response["connection_status"] == "connected"
    assert response["server_information"]["server_name"] == "server"
    assert connector.calls[0][0] == "health_check"


def test_metadata_calls_delegate(monkeypatch):
    _configure_settings(monkeypatch)
    connector = FakeConnector()
    service = QueryService(connector)

    databases = service.list_databases().to_dict()
    tables = service.list_tables(database="sales", schema="dbo").to_dict()
    details = service.describe_table(database="sales", table="items", schema="dbo").to_dict()

    assert databases["count"] == 2
    assert tables["count"] == 1
    assert details["column_count"] == 1
    assert [call[0] for call in connector.calls[:3]] == ["list_databases", "list_tables", "describe_table"]


def test_suggest_columns_uses_metadata_without_executing_sql(monkeypatch):
    _configure_settings(monkeypatch)

    class MetadataConnector(FakeConnector):
        def describe_table(self, database=None, table=None, schema=None, timeout_seconds=None):
            self.calls.append(("describe_table", database, table, schema, timeout_seconds))
            return {
                "database": database,
                "schema": schema,
                "table": table,
                "columns": [
                    {"COLUMN_NAME": "order_id", "DATA_TYPE": "integer"},
                    {"COLUMN_NAME": "order_status", "DATA_TYPE": "varchar"},
                    {"COLUMN_NAME": "created_at", "DATA_TYPE": "timestamp"},
                ],
            }

    connector = MetadataConnector()
    response = QueryService(connector).suggest_columns(
        table="orders",
        missing_column="status",
        database="sales",
        schema="dbo",
    ).to_dict()

    assert response["success"] is True
    assert response["suggestions"][0]["column"] == "order_status"
    assert response["sql_modified"] is False
    assert response["sql_executed"] is False
    assert response["approval_required_before_revised_sql"] is True
    assert [call[0] for call in connector.calls] == ["describe_table"]


def test_suggest_columns_requires_table_and_column(monkeypatch):
    _configure_settings(monkeypatch)
    connector = FakeConnector()

    response = QueryService(connector).suggest_columns(table="", missing_column="status").to_dict()

    assert response["success"] is False
    assert response["error"]["code"] == ErrorCode.CONFIG_INVALID
    assert connector.calls == []


def test_response_preserves_reserved_fields(monkeypatch):
    _configure_settings(monkeypatch)
    service = QueryService(FakeConnector())

    response = service._response(
        tool="execute_select_query",
        environment="SQLSERVER",
        success=True,
        request_id="req-1",
        start_time=0.0,
        data={"success": False, "request_id": "shadow", "tool": "shadow-tool", "environment": "shadow-env", "custom": "value"},
    ).to_dict()

    assert response["success"] is True
    assert response["request_id"] == "req-1"
    assert response["tool"] == "execute_select_query"
    assert response["environment"] == "SQLSERVER"
    assert response["data"]["success"] is False
    assert response["data"]["request_id"] == "shadow"
    assert response["custom"] == "value"
    assert response["metadata"]["session_isolation"] == "one_client_per_process"
    assert response["metadata"]["runtime_id"]


def test_execute_query_executes_approved_write_statement(monkeypatch):
    _configure_settings(monkeypatch)
    connector = FakeConnector()
    service = QueryService(connector)

    response = service.execute_query(sql="DELETE FROM items", environment="ignored").to_dict()

    assert response["success"] is True
    assert response["tool"] == "execute_query"
    assert response["query"] == "DELETE FROM items"
    assert connector.calls[0][1] == "DELETE FROM items"


def test_execute_query_rejects_conflicting_sql_arguments(monkeypatch):
    _configure_settings(monkeypatch)
    connector = FakeConnector()
    service = QueryService(connector)

    response = service.execute_query(sql="SELECT 1", query="DELETE FROM items").to_dict()

    assert response["success"] is False
    assert response["error"]["code"] == ErrorCode.CONFIG_INVALID
    assert connector.calls == []


def test_execute_query_rejects_database_outside_active_profile(monkeypatch):
    _configure_settings(monkeypatch)
    connector = FakeConnector()
    service = QueryService(connector)

    response = service.execute_query(sql="SELECT 1", database="another_database").to_dict()

    assert response["success"] is False
    assert response["error"]["code"] == ErrorCode.CONFIG_INVALID
    assert "profile switch" in response["error"]["detail"]
    assert connector.calls == []


def test_deprecated_alias_uses_same_generic_execution_path(monkeypatch):
    _configure_settings(monkeypatch)
    connector = FakeConnector()
    service = QueryService(connector)

    response = service.execute_select_query(sql="UPDATE items SET active = 1").to_dict()

    assert response["success"] is True
    assert response["tool"] == "execute_select_query"
    assert connector.calls[0][0] == "execute_query"


def test_request_row_limit_cannot_exceed_configured_cap(monkeypatch):
    _configure_settings(monkeypatch)
    connector = FakeConnector()
    service = QueryService(connector)

    response = service.execute_select_query(sql="SELECT name FROM items", max_rows=10_000).to_dict()

    assert response["success"] is True
    assert response["metadata"]["row_limit"] == 25
    assert response["metadata"]["profile"] == "sqlserver-sandbox"
    assert connector.calls[0][4] == 25


def test_non_positive_row_limit_is_rejected(monkeypatch):
    _configure_settings(monkeypatch)
    service = QueryService(FakeConnector())

    response = service.execute_select_query(sql="SELECT 1", max_rows=0).to_dict()

    assert response["success"] is False
    assert response["error"]["code"] == ErrorCode.CONFIG_INVALID


def test_non_positive_timeout_is_rejected(monkeypatch):
    _configure_settings(monkeypatch)
    service = QueryService(FakeConnector())

    response = service.health(timeout_seconds=-1).to_dict()

    assert response["success"] is False
    assert response["error"]["code"] == ErrorCode.CONFIG_INVALID


def test_connector_errors_redact_configured_password(monkeypatch):
    _configure_settings(monkeypatch)

    class FailingConnector(FakeConnector):
        def execute_query(self, query, **kwargs):
            raise RuntimeError("Authentication failed for password dev_pass")

    response = QueryService(FailingConnector()).execute_query(sql="SELECT 1").to_dict()

    assert response["success"] is False
    assert "dev_pass" not in str(response)
    assert "[REDACTED]" in response["error"]["detail"]
