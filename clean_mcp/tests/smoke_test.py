"""Minimal smoke test for MCP startup validation.

This script exercises configuration loading, startup validation, connector
creation, and an optional lightweight connectivity check.
It is intended to catch environment and startup issues before starting the
MCP server.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config, ConfigError
from connectors.factory import ConnectorFactory


def main() -> int:
    """Validate startup and one optional connector round trip."""

    try:
        print("Loading configuration...")
        Config.load()
        print("Validating configuration...")
        Config.validate()
        print("Configuration validation passed.")

        print("Configuration diagnostics:")
        print(Config.diagnostics())

        if _should_attempt_connection():
            print(f"Attempting lightweight {Config.DB_TYPE} connectivity check...")
            connector = ConnectorFactory.create()
            metadata = connector.test_connection(database=Config.DATABASE)
            print("Connectivity check succeeded.")
            print(metadata)

        print("Smoke test complete.")
        return 0
    except ConfigError as exc:
        print(f"Configuration validation failed: {exc}")
        return 1
    except Exception as exc:
        print(f"Smoke test failed: {exc}")
        return 2


def _should_attempt_connection() -> bool:
    """Attempt a live check only when required connection settings exist."""

    import os

    return os.getenv("DB_SMOKE_TEST_CONNECT", "false").strip().lower() in {"1", "true", "yes", "y", "on"}


if __name__ == "__main__":
    raise SystemExit(main())
