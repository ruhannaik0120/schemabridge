"""Apply the checksum-verified SchemaBridge control-plane migrations."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from persistence.config import ControlPlaneConfig
from persistence.migrations import ControlPlaneMigrationRunner, connect_control_plane


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Apply SchemaBridge control-plane migrations in deterministic order."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="List discovered migration versions without connecting to PostgreSQL.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    runner = ControlPlaneMigrationRunner(lambda: _connect())
    if arguments.check:
        versions = tuple(item[0] for item in runner.discover())
        print("Discovered control-plane migrations: " + ", ".join(map(str, versions)))
        return 0
    try:
        from dotenv import load_dotenv

        load_dotenv(WORKSPACE_ROOT / ".env", override=False)
        load_dotenv(PROJECT_ROOT / ".env", override=False)
        config = ControlPlaneConfig.from_environment()
        if not config.enabled:
            print("SCHEMABRIDGE_CONTROL_PLANE_DSN is required.", file=sys.stderr)
            return 2
        os.environ.setdefault("SCHEMABRIDGE_CONTROL_PLANE_DSN", config.dsn)
        versions = runner.run()
    except Exception:
        print("Control-plane migration failed. Review database availability and configuration.", file=sys.stderr)
        return 1
    print("Control-plane migrations verified: " + ", ".join(map(str, versions)))
    return 0


def _connect():
    dsn = os.getenv("SCHEMABRIDGE_CONTROL_PLANE_DSN", "")
    return connect_control_plane(dsn)


if __name__ == "__main__":
    raise SystemExit(main())
