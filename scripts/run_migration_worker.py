"""Process at most one queued SchemaBridge migration job and then exit."""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

from schemabridge.models.migration_job import MigrationJobStatus
from schemabridge.persistence.config import ControlPlaneConfig
from schemabridge.persistence.postgresql import PostgreSQLWorkflowRepository
from schemabridge.services.database_service import (
    get_database_service,
    reset_database_services,
)
from schemabridge.services.migration_job_runtime import build_migration_job_worker


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    """Run one local worker cycle with safe console output and cleanup."""

    load_dotenv(PROJECT_ROOT / ".env", override=False)
    config = ControlPlaneConfig.from_environment()
    if not config.enabled:
        print("SCHEMABRIDGE_CONTROL_PLANE_DSN is required.", file=sys.stderr)
        return 2

    repository = PostgreSQLWorkflowRepository(config)
    try:
        worker = build_migration_job_worker(
            repository,
            database_service_factory=get_database_service,
        )
        job = worker.run_once()
    except Exception:
        print(
            "Migration worker failed. Review configuration and durable job state.",
            file=sys.stderr,
        )
        return 1
    finally:
        repository.close()
        reset_database_services()

    if job is None:
        print("No queued migration jobs.")
        return 0

    summary = (
        f"Migration job {job.job_id}: "
        f"status={job.status.value}, stage={job.stage.value}"
    )
    if job.failure_category:
        summary += f", failure_category={job.failure_category}"
    print(summary)
    return 0 if job.status is MigrationJobStatus.SUCCEEDED else 3


if __name__ == "__main__":
    raise SystemExit(main())
