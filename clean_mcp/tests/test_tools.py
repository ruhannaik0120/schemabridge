"""MCP wrapper tests to ensure the tool surface remains stable."""

import tools.connection as connection_tools
import tools.metadata as metadata_tools
import tools.query as query_tools


class FakeResponse:
    """Provide the serializable response contract expected by tool wrappers."""

    def __init__(self, payload):
        self._payload = payload

    def to_dict(self):
        return self._payload


class FakeService:
    """Capture wrapper arguments without invoking production services."""

    def __init__(self):
        self.calls = []

    def test_connection(self, *args, **kwargs):
        self.calls.append(("test_connection", args, kwargs))
        return FakeResponse(
            {
                "success": True,
                "tool": "test_connection",
                "environment": kwargs.get("environment", "DEV"),
                "request_id": "abc",
                "timestamp": "2026-06-28T00:00:00Z",
                "execution_time_ms": 1,
            }
        )

    def health(self, *args, **kwargs):
        self.calls.append(("health", args, kwargs))
        return FakeResponse(
            {
                "success": True,
                "tool": "health",
                "environment": kwargs.get("environment", "DEV"),
                "request_id": "abc",
                "timestamp": "2026-06-28T00:00:00Z",
                "execution_time_ms": 1,
                "status": "healthy",
            }
        )

    def list_databases(self, *args, **kwargs):
        self.calls.append(("list_databases", args, kwargs))
        return FakeResponse(
            {
                "success": True,
                "tool": "list_databases",
                "environment": kwargs.get("environment", "DEV"),
                "request_id": "abc",
                "timestamp": "2026-06-28T00:00:00Z",
                "execution_time_ms": 1,
                "count": 0,
                "databases": [],
            }
        )

    def list_tables(self, *args, **kwargs):
        self.calls.append(("list_tables", args, kwargs))
        return FakeResponse({"success": True, "tool": "list_tables", "environment": kwargs.get("environment", "DEV"), "request_id": "abc", "timestamp": "2026-06-28T00:00:00Z", "execution_time_ms": 1, "count": 0, "tables": []})

    def describe_table(self, *args, **kwargs):
        self.calls.append(("describe_table", args, kwargs))
        return FakeResponse({"success": True, "tool": "describe_table", "environment": kwargs.get("environment", "DEV"), "request_id": "abc", "timestamp": "2026-06-28T00:00:00Z", "execution_time_ms": 1, "column_count": 0, "columns": []})

    def execute_select_query(self, *args, **kwargs):
        self.calls.append(("execute_select_query", args, kwargs))
        return FakeResponse({"success": True, "tool": "execute_select_query", "environment": kwargs.get("environment", "DEV"), "request_id": "abc", "timestamp": "2026-06-28T00:00:00Z", "execution_time_ms": 2, "rows": []})

    def execute_query(self, *args, **kwargs):
        self.calls.append(("execute_query", args, kwargs))
        return FakeResponse({"success": True, "tool": "execute_query", "environment": kwargs.get("environment", "DEV"), "request_id": "abc", "timestamp": "2026-06-28T00:00:00Z", "execution_time_ms": 2, "rows": []})


def test_tool_wrappers_return_structured_payload(monkeypatch):
    fake_service = FakeService()
    monkeypatch.setattr(connection_tools, "query_service", fake_service)
    monkeypatch.setattr(metadata_tools, "query_service", fake_service)
    monkeypatch.setattr(query_tools, "query_service", fake_service)

    test_payload = connection_tools.test_connection(environment="DEV")
    health_payload = connection_tools.health(environment="DEV")
    databases_payload = metadata_tools.list_databases(environment="DEV")
    tables_payload = metadata_tools.list_tables(database="sales", schema="dbo", environment="DEV")
    describe_payload = metadata_tools.describe_table(database="sales", table="orders", schema="dbo", environment="DEV")
    query_payload = query_tools.execute_query(sql="UPDATE items SET active = 1", database="sales", environment="DEV")
    alias_payload = query_tools.execute_select_query(sql="SELECT 1", database="sales", environment="DEV")

    assert test_payload["tool"] == "test_connection"
    assert health_payload["status"] == "healthy"
    assert databases_payload["tool"] == "list_databases"
    assert tables_payload["tool"] == "list_tables"
    assert describe_payload["tool"] == "describe_table"
    assert query_payload["execution_time_ms"] == 2
    assert alias_payload["tool"] == "execute_select_query"


def test_query_tools_do_not_expose_execution_mode():
    import inspect

    assert "execution_mode" not in inspect.signature(query_tools.execute_query).parameters
    assert "execution_mode" not in inspect.signature(query_tools.execute_select_query).parameters


def test_server_query_tools_do_not_expose_execution_mode():
    import ast
    from pathlib import Path

    server_tree = ast.parse((Path(__file__).parents[1] / "server.py").read_text(encoding="utf-8"))
    functions = {
        node.name: [argument.arg for argument in node.args.args]
        for node in server_tree.body
        if isinstance(node, ast.FunctionDef)
    }

    assert "execution_mode" not in functions["tool_execute_query"]
    assert "execution_mode" not in functions["tool_execute_select_query"]
