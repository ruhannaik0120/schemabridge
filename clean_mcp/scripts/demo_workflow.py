"""Create and inspect a safe local SchemaBridge workflow through HTTP."""

from __future__ import annotations

import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _request(base_url: str, method: str, path: str, payload=None, *, key: str | None = None):
    headers = {"Accept": "application/json", "X-Request-ID": f"demo-{key or 'read'}"}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if key is not None:
        headers["Idempotency-Key"] = key
    request = Request(base_url.rstrip("/") + path, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"API returned HTTP {error.code}: {detail}") from None
    except URLError:
        raise RuntimeError("SchemaBridge API is unavailable. Start it before running the demo.") from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create and inspect a SchemaBridge demo workflow.")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--source-profile", default="postgres-source")
    parser.add_argument("--target-profile", default="snowflake-target")
    parser.add_argument("--source-schema", default="public")
    parser.add_argument("--source-table", default="source_people")
    parser.add_argument("--target-database", default="ANALYTICS")
    parser.add_argument("--target-schema", default="PUBLIC")
    parser.add_argument("--target-table", default="PEOPLE")
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Use configured real profiles to discover both relations and generate a mapping proposal.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        health = _request(args.base_url, "GET", "/health/ready")
        created = _request(
            args.base_url,
            "POST",
            "/api/v1/migrations/workflows",
            {
                "display_name": "Placement demo workflow",
                "source_profile_id": args.source_profile,
                "target_profile_id": args.target_profile,
                "source_relation": {
                    "catalog_name": None,
                    "schema_name": args.source_schema,
                    "object_name": args.source_table,
                    "system": "postgresql",
                },
                "target_relation": {
                    "catalog_name": args.target_database,
                    "schema_name": args.target_schema,
                    "object_name": args.target_table,
                    "system": "snowflake",
                },
                "actor_type": "USER",
                "actor_reference": "local-demo",
            },
            key="demo-create",
        )
        workflow_id = created["workflow_id"]
        current = created
        if args.discover:
            source = _request(
                args.base_url,
                "POST",
                f"/api/v1/migrations/workflows/{workflow_id}/discover-source",
                {"expected_version": current["version"], "actor_type": "SERVICE"},
                key="demo-discover-source",
            )
            target = _request(
                args.base_url,
                "POST",
                f"/api/v1/migrations/workflows/{workflow_id}/discover-target",
                {"expected_version": source["workflow"]["version"], "actor_type": "SERVICE"},
                key="demo-discover-target",
            )
            proposal = _request(
                args.base_url,
                "POST",
                f"/api/v1/migrations/workflows/{workflow_id}/mapping-proposals",
                {"expected_version": target["workflow"]["version"], "actor_type": "SERVICE"},
                key="demo-mapping-proposal",
            )
            current = proposal["workflow"]
        retrieved = _request(args.base_url, "GET", f"/api/v1/migrations/workflows/{workflow_id}")
        artifacts = _request(args.base_url, "GET", f"/api/v1/migrations/workflows/{workflow_id}/artifacts")
        audit = _request(args.base_url, "GET", f"/api/v1/migrations/workflows/{workflow_id}/audit-events")
    except (RuntimeError, KeyError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1

    print(json.dumps({"health": health, "workflow": retrieved, "artifacts": artifacts, "audit": audit}, indent=2))
    print("\nThis script did not execute a migration.")
    if args.discover:
        print("A mapping proposal was generated. Review it in Swagger before approval.")
    else:
        print("Use --discover only after configuring real PostgreSQL and Snowflake profiles.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
