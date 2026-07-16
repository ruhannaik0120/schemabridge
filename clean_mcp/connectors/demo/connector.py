"""Offline demo connector for MCP execution framework demonstrations.

This connector does not connect to an external database. It returns deterministic
sample metadata and query results so demos remain reliable when credentials or
network access are unavailable.
"""

from __future__ import annotations

import re
from typing import Any

from config import Config, ConnectionConfig
from connectors.base import DatabaseConnector

_DEMO_DATABASES = [
    {"name": "qa_demo"},
    {"name": "sales"},
]

_DEMO_TABLES = {
    "qa_demo": [
        {"TABLE_SCHEMA": "public", "TABLE_NAME": "demo_items", "TABLE_TYPE": "BASE TABLE"},
        {"TABLE_SCHEMA": "public", "TABLE_NAME": "validation_results", "TABLE_TYPE": "BASE TABLE"},
    ],
    "sales": [
        {"TABLE_SCHEMA": "dbo", "TABLE_NAME": "orders", "TABLE_TYPE": "BASE TABLE"},
    ],
}

_DEMO_COLUMNS = {
    ("qa_demo", "demo_items"): [
        {"COLUMN_NAME": "item_id", "DATA_TYPE": "integer", "IS_NULLABLE": "NO"},
        {"COLUMN_NAME": "item_name", "DATA_TYPE": "varchar", "IS_NULLABLE": "NO"},
        {"COLUMN_NAME": "status", "DATA_TYPE": "varchar", "IS_NULLABLE": "YES"},
    ],
    ("qa_demo", "validation_results"): [
        {"COLUMN_NAME": "run_id", "DATA_TYPE": "varchar", "IS_NULLABLE": "NO"},
        {"COLUMN_NAME": "passed", "DATA_TYPE": "boolean", "IS_NULLABLE": "NO"},
    ],
    ("sales", "orders"): [
        {"COLUMN_NAME": "order_id", "DATA_TYPE": "integer", "IS_NULLABLE": "NO"},
        {"COLUMN_NAME": "customer", "DATA_TYPE": "varchar", "IS_NULLABLE": "NO"},
    ],
}

_DEMO_ROWS = {
    "health_check": {"columns": ["health_check"], "rows": [{"health_check": 1}]},
    "current_date": {"columns": ["current_date"], "rows": [{"current_date": "2026-07-04"}]},
    "demo_items": {
        "columns": ["item_id", "item_name", "status"],
        "rows": [
            {"item_id": 1, "item_name": "Sample Widget", "status": "active"},
            {"item_id": 2, "item_name": "Demo Record", "status": "pending"},
        ],
    },
}


class DemoConnector(DatabaseConnector):
    """Deterministic connector used for offline demonstrations only."""

    def _profile(self) -> ConnectionConfig:
        """Return the same neutral configuration used by live connectors."""
        return Config.connection_config()

    def _target_database(self, database: str | None) -> str:
        """Resolve a requested demo database to a deterministic fallback."""
        profile = self._profile()
        return (database or profile.database or "qa_demo").strip() or "qa_demo"

    def connect(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Return simulated connection context without opening a network socket."""
        return {"connector_type": self.__class__.__name__, "database": self._target_database(database), "mode": "demo"}

    def test_connection(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Return a predictable successful connection snapshot for demos."""
        target_database = self._target_database(database)
        return {
            "connector_type": self.__class__.__name__,
            "db_type": "demo",
            "database": target_database,
            "connection_status": "connected",
            "server_information": {
                "server_name": "demo-local",
                "version": "demo-connector-1.0",
                "logged_in_user": "demo_user",
                "utc_time": "2026-07-04T00:00:00+00:00",
            },
        }

    def health_check(self, database: str | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
        """Return deterministic liveness data without external dependencies."""
        snapshot = self.test_connection(database=database, timeout_seconds=timeout_seconds)
        snapshot["mode"] = "offline_demo"
        snapshot["note"] = "Demo connector only. No external database is contacted."
        return snapshot

    def list_databases(self, timeout_seconds: int | None = None) -> dict[str, Any]:
        """List the in-memory sample databases exposed by this connector."""
        return {
            "connector_type": self.__class__.__name__,
            "db_type": "demo",
            "count": len(_DEMO_DATABASES),
            "databases": list(_DEMO_DATABASES),
        }

    def list_tables(
        self,
        database: str | None = None,
        schema: str | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """List deterministic sample tables, optionally filtered by schema."""

        target_database = self._target_database(database)
        tables = _DEMO_TABLES.get(target_database, [])
        if schema:
            tables = [table for table in tables if table.get("TABLE_SCHEMA") == schema]
        return {
            "connector_type": self.__class__.__name__,
            "db_type": "demo",
            "database": target_database,
            "schema": schema or "",
            "count": len(tables),
            "tables": tables,
        }

    def describe_table(
        self,
        database: str | None = None,
        table: str | None = None,
        schema: str | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Return deterministic sample columns for a requested demo table."""

        target_database = self._target_database(database)
        columns = _DEMO_COLUMNS.get((target_database, table or ""), [])
        return {
            "connector_type": self.__class__.__name__,
            "db_type": "demo",
            "database": target_database,
            "schema": schema or "public",
            "table": table or "",
            "column_count": len(columns),
            "columns": columns,
        }

    def execute_query(
        self,
        query: str,
        *,
        database: str | None = None,
        timeout_seconds: int | None = None,
        max_rows: int | None = None,
    ) -> Any:
        """Return predictable rows for a small set of demonstration queries."""

        # Match a small deterministic query set so demonstrations remain useful
        # without pretending that this connector is a SQL execution engine.
        normalized = query.strip().rstrip(";").lower()
        if re.search(r"\bselect\s+1\b", normalized):
            payload = _DEMO_ROWS["health_check"]
        elif "current_date" in normalized:
            payload = _DEMO_ROWS["current_date"]
        elif "demo_items" in normalized:
            payload = _DEMO_ROWS["demo_items"]
        else:
            payload = {"columns": ["message"], "rows": [{"message": "Demo query executed successfully."}]}

        rows = payload["rows"][: max_rows or len(payload["rows"])]
        return {
            "connector_type": self.__class__.__name__,
            "db_type": "demo",
            "database": self._target_database(database),
            "columns": payload["columns"],
            "rows": rows,
        }

    def close(self) -> None:
        """Satisfy the connector lifecycle contract; no resource is open."""
        return None


Connector = DemoConnector
