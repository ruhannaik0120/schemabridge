"""Architecture rules that keep database code behind connector boundaries."""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_DRIVER_MODULES = {"pyodbc", "mysql", "psycopg", "snowflake"}


def test_database_drivers_are_imported_only_by_connectors():
    """Prevent tools and services from bypassing connector boundaries."""

    violations: list[str] = []
    for path in ROOT.rglob("*.py"):
        relative = path.relative_to(ROOT)
        if relative.parts[0] in {"connectors", ".venv"}:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = {alias.name.split(".")[0] for alias in node.names}
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = {node.module.split(".")[0]}
            else:
                continue
            if modules & FORBIDDEN_DRIVER_MODULES:
                violations.append(f"{relative}:{node.lineno}")
    assert not violations, "Database drivers imported outside connectors/: " + ", ".join(violations)
