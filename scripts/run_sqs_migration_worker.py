"""Run SchemaBridge's optional SQS-backed migration worker."""

from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path
from types import FrameType
from typing import Sequence

from dotenv import load_dotenv

from schemabridge.models.migration_job import MigrationJobStatus
from schemabridge.persistence.config import ControlPlaneConfig
from schemabridge.persistence.postgresql import PostgreSQLWorkflowRepository
from schemabridge.services.database_service import (
    get_database_service,
    reset_database_services,
)
from schemabridge.services.jobs.config import SqsJobQueueSettings
from schemabridge.services.jobs.runtime import build_migration_job_worker
from schemabridge.services.jobs.sqs import create_sqs_client
from schemabridge.services.jobs.sqs_consumer import SqsMigrationJobReceiver
from schemabridge.services.jobs.sqs_worker import SqsMigrationJobCoordinator


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _build_argument_parser() -> argparse.ArgumentParser:
    """Build the deliberately small local-worker command interface."""

    parser = argparse.ArgumentParser(
        description="Process SchemaBridge migration-job notifications from SQS."
    )
    parser.add_argument(
        "--forever",
        action="store_true",
        help="Keep receiving jobs until interrupted with Ctrl+C.",
    )
    return parser


def _print_job_summary(job) -> int:
    """Print a safe durable-job outcome and return its one-cycle exit code."""

    if job is None:
        print("No claimable SQS migration job.")
        return 0

    summary = (
        f"Migration job {job.job_id}: "
        f"status={job.status.value}, stage={job.stage.value}"
    )
    if job.failure_category:
        summary += f", failure_category={job.failure_category}"
    print(summary)

    return 0 if job.status is MigrationJobStatus.SUCCEEDED else 3


def _request_shutdown(_signum: int, _frame: FrameType | None) -> None:
    """Route Docker's termination signal through the normal cleanup path."""

    raise KeyboardInterrupt


def _run_forever(coordinator) -> int:
    """Long-poll SQS until the local operator interrupts the process."""

    print("SQS migration worker started. Press Ctrl+C to stop.")
    previous_sigterm_handler = signal.signal(signal.SIGTERM, _request_shutdown)
    try:
        while True:
            _print_job_summary(coordinator.run_once())
    except KeyboardInterrupt:
        print("SQS migration worker stopped.")
        return 0
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)


def main(argv: Sequence[str] | None = None) -> int:
    """Run one SQS worker cycle, or keep running when ``--forever`` is set."""

    args = _build_argument_parser().parse_args(() if argv is None else argv)

    load_dotenv(PROJECT_ROOT / ".env", override=False)

    control_plane = ControlPlaneConfig.from_environment()
    if not control_plane.enabled:
        print("SCHEMABRIDGE_CONTROL_PLANE_DSN is required.", file=sys.stderr)
        return 2

    sqs_settings = SqsJobQueueSettings.from_environment()
    if not sqs_settings.enabled:
        print("SQS job-queue settings are required.", file=sys.stderr)
        return 2

    assert sqs_settings.region_name is not None
    assert sqs_settings.queue_url is not None

    repository = PostgreSQLWorkflowRepository(control_plane)
    try:
        worker = build_migration_job_worker(
            repository,
            database_service_factory=get_database_service,
        )
        receiver = SqsMigrationJobReceiver(
            create_sqs_client(region_name=sqs_settings.region_name),
            queue_url=sqs_settings.queue_url,
        )
        coordinator = SqsMigrationJobCoordinator(receiver, worker)
        if args.forever:
            return _run_forever(coordinator)
        job = coordinator.run_once()
    except Exception:
        print(
            "SQS migration worker failed. Review configuration and durable job state.",
            file=sys.stderr,
        )
        return 1
    finally:
        repository.close()
        reset_database_services()

    return _print_job_summary(job)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
