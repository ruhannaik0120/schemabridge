"""Exercise one disposable PostgreSQL-to-MySQL workflow through FastAPI.

The proof creates uniquely named source and target tables, drives the durable
workflow API through discovery, approval, staging, execution, validation, and
staging cleanup, then removes the disposable data-plane tables.  Durable
control-plane evidence remains intentionally available for audit inspection.
"""

from __future__ import annotations

import argparse
import os
from uuid import uuid4

from dotenv import load_dotenv
from fastapi.testclient import TestClient

from schemabridge.api.app import create_app
from schemabridge.connectors.factory import ConnectorFactory
from schemabridge.connectors.mysql.connector import MySQLConnector
from schemabridge.connectors.postgresql.connector import PostgreSQLConnector
from schemabridge.models.transport import TransportRelation
from schemabridge.services.profile_registry import ProfileRegistry


BASE = "/api/v1/migrations/workflows"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm-live-write", action="store_true")
    parser.add_argument("--postgresql-profile", required=True)
    parser.add_argument("--mysql-profile", required=True)
    parser.add_argument("--postgresql-schema", default="public")
    return parser.parse_args()


def _post(client: TestClient, path: str, payload: dict, key: str) -> dict:
    response = client.post(
        path,
        json=payload,
        headers={"Idempotency-Key": key, "X-Request-ID": f"live-{key}"},
    )
    if response.status_code != 201:
        raise RuntimeError(f"Workflow API failed at {key}: HTTP {response.status_code}.")
    return response.json()


def _decisions() -> list[dict]:
    return [
        {
            "source_column": "first_name",
            "target_column": "full_name",
            "status": "APPROVED",
            "transformation": {
                "expression_type": "CONCAT",
                "source_columns": ["first_name", "last_name"],
                "separator": " ",
            },
        },
        {"source_column": "last_name", "status": "REJECTED"},
        {
            "source_column": "age",
            "target_column": "age",
            "status": "APPROVED",
            "transformation": {
                "expression_type": "DIRECT_COPY",
                "source_columns": ["age"],
            },
        },
    ]


def main() -> int:
    args = _arguments()
    if not args.confirm_live_write:
        raise SystemExit("Pass --confirm-live-write to create disposable remote tables.")

    load_dotenv()
    registry = ProfileRegistry.from_json(os.getenv("DB_PROFILES_JSON", ""))
    postgresql_profile = registry.resolve(args.postgresql_profile)
    mysql_profile = registry.resolve(args.mysql_profile)
    if postgresql_profile.db_type != "postgresql" or mysql_profile.db_type != "mysql":
        raise SystemExit("The selected profiles must be PostgreSQL source and MySQL target profiles.")
    if mysql_profile.write_enabled is not True:
        raise SystemExit("The MySQL target profile must set write_enabled=true.")

    source = ConnectorFactory.create_for_profile(postgresql_profile)
    target = ConnectorFactory.create_for_profile(mysql_profile)
    if not isinstance(source, PostgreSQLConnector) or not isinstance(target, MySQLConnector):
        raise RuntimeError("Configured profiles did not resolve to the expected connectors.")

    suffix = uuid4().hex
    source_name = f"sb_live_workflow_source_{suffix}"
    target_name = f"sb_live_workflow_target_{suffix}"
    source_relation = TransportRelation(
        catalog_name=postgresql_profile.database,
        schema_name=args.postgresql_schema,
        object_name=source_name,
    )
    target_relation = TransportRelation(
        catalog_name=mysql_profile.database,
        schema_name=mysql_profile.database,
        object_name=target_name,
    )
    source_sql = ".".join(
        source._quote_transport_identifier(value)
        for value in (source_relation.schema_name, source_relation.object_name)
    )
    target_sql = ".".join(
        target._quote_identifier(value)
        for value in (target_relation.schema_name, target_relation.object_name)
    )
    staging_relation = None
    source_created = target_created = False
    try:
        source.execute_query(
            f"CREATE TABLE {source_sql} (first_name TEXT NOT NULL, last_name TEXT NOT NULL, age BIGINT NOT NULL)",
            database=postgresql_profile.database,
            timeout_seconds=30,
        )
        source_created = True
        source.execute_query(
            f"INSERT INTO {source_sql} (first_name, last_name, age) VALUES (%s, %s, %s), (%s, %s, %s), (%s, %s, %s)",
            parameters=("Asha", "Rao", 28, "Rahul", "Das", 31, "Neha", "Shah", 28),
            database=postgresql_profile.database,
            timeout_seconds=30,
        )
        target.execute_query(
            f"CREATE TABLE {target_sql} (full_name TEXT NOT NULL, age BIGINT NOT NULL)",
            database=mysql_profile.database,
            timeout_seconds=30,
        )
        target_created = True

        with TestClient(create_app()) as client:
            created = _post(
                client,
                BASE,
                {
                    "display_name": "Disposable PostgreSQL to MySQL workflow",
                    "source_profile_id": args.postgresql_profile,
                    "target_profile_id": args.mysql_profile,
                    "source_relation": {
                        "catalog_name": postgresql_profile.database,
                        "schema_name": args.postgresql_schema,
                        "object_name": source_name,
                        "system": "postgresql",
                    },
                    "target_relation": {
                        "catalog_name": mysql_profile.database,
                        "schema_name": mysql_profile.database,
                        "object_name": target_name,
                        "system": "mysql",
                    },
                    "actor_type": "SERVICE",
                },
                "create",
            )
            workflow_id = created["workflow_id"]
            discovered_source = _post(
                client, f"{BASE}/{workflow_id}/discover-source",
                {"expected_version": created["version"], "actor_type": "SERVICE"}, "discover-source",
            )
            discovered_target = _post(
                client, f"{BASE}/{workflow_id}/discover-target",
                {"expected_version": discovered_source["workflow"]["version"], "actor_type": "SERVICE"}, "discover-target",
            )
            proposed = _post(
                client, f"{BASE}/{workflow_id}/mapping-proposals",
                {"expected_version": discovered_target["workflow"]["version"], "actor_type": "SERVICE"}, "mapping",
            )
            approved = _post(
                client, f"{BASE}/{workflow_id}/mapping-approvals",
                {
                    "expected_version": proposed["workflow"]["version"],
                    "mapping_artifact_version": proposed["artifact"]["artifact_version"],
                    "decisions": _decisions(),
                    "actor_type": "SERVICE",
                },
                "approve",
            )
            staged = _post(
                client, f"{BASE}/{workflow_id}/load-staging",
                {
                    "expected_version": approved["workflow"]["version"],
                    "source_discovery_artifact_version": discovered_source["artifact"]["artifact_version"],
                    "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                    "source_profile_id": args.postgresql_profile,
                    "target_profile_id": args.mysql_profile,
                    "batch_size": 2,
                    "timeout_seconds": 30,
                    "actor_type": "SERVICE",
                },
                "stage",
            )
            staging_relation = staged["result"]["staging_relation"]
            preview = _post(
                client, f"{BASE}/{workflow_id}/transformation-previews",
                {
                    "expected_version": staged["workflow"]["version"],
                    "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                    "staging_database": staging_relation["catalog_name"],
                    "staging_schema": staging_relation["schema_name"],
                    "staging_table": staging_relation["object_name"],
                    "statement_type": "INSERT_SELECT",
                    "actor_type": "SERVICE",
                },
                "preview",
            )
            executed = _post(
                client, f"{BASE}/{workflow_id}/execute",
                {
                    "expected_version": preview["workflow"]["version"],
                    "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                    "transformation_preview_artifact_version": preview["artifact"]["artifact_version"],
                    "target_profile_id": args.mysql_profile,
                    "timeout_seconds": 30,
                    "actor_type": "SERVICE",
                },
                "execute",
            )
            validated = _post(
                client, f"{BASE}/{workflow_id}/validate",
                {
                    "expected_version": executed["workflow"]["version"],
                    "execution_evidence_artifact_version": executed["artifact"]["artifact_version"],
                    "approved_mapping_artifact_version": approved["artifact"]["artifact_version"],
                    "source_profile_id": args.postgresql_profile,
                    "target_profile_id": args.mysql_profile,
                    "timeout_seconds": 30,
                    "actor_type": "SERVICE",
                },
                "validate",
            )
        if validated["workflow"]["status"] != "VALIDATED":
            raise RuntimeError("The durable workflow did not finish validated.")
        rows = target.execute_query(
            f"SELECT COUNT(*) AS row_count FROM {target_sql}", database=mysql_profile.database,
            timeout_seconds=30, max_rows=1,
        )["rows"][0]["row_count"]
        if rows != 3:
            raise RuntimeError("MySQL target row count did not match the workflow result.")
        print(f"Live FastAPI workflow proof passed: workflow={workflow_id}; 3 rows validated; cleanup follows.")
        return 0
    finally:
        if staging_relation is not None:
            target.drop_staging_table(
                relation=TransportRelation(**staging_relation), timeout_seconds=30
            )
        if target_created:
            target.execute_query(f"DROP TABLE IF EXISTS {target_sql}", database=mysql_profile.database, timeout_seconds=30)
        if source_created:
            source.execute_query(f"DROP TABLE IF EXISTS {source_sql}", database=postgresql_profile.database, timeout_seconds=30)


if __name__ == "__main__":
    raise SystemExit(main())
