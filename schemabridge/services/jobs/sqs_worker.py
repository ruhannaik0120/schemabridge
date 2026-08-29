"""Coordinate one SQS notification with one exact durable migration job."""

from __future__ import annotations

from typing import Protocol

from schemabridge.models.migration_job import MigrationJob
from schemabridge.services.jobs.sqs_consumer import ReceivedMigrationJob


class MigrationJobReceiver(Protocol):
    """Receive and acknowledge one validated migration-job notification."""

    def receive_one(self) -> ReceivedMigrationJob | None:
        """Return one message delivery, or no message when the queue is empty."""

    def acknowledge(self, receipt_handle: str) -> None:
        """Delete one safely handled message delivery."""


class ExactMigrationJobWorker(Protocol):
    """Process exactly the durable job identified by one queue message."""

    def run(self, job_id) -> MigrationJob | None:
        """Process the named queued job, if it remains available."""


class SqsMigrationJobCoordinator:
    """Receive, process, then acknowledge at most one SQS delivery."""

    def __init__(
        self,
        receiver: MigrationJobReceiver,
        worker: ExactMigrationJobWorker,
    ) -> None:
        if not callable(getattr(receiver, "receive_one", None)) or not callable(
            getattr(receiver, "acknowledge", None)
        ):
            raise TypeError("receiver must provide receive_one() and acknowledge().")
        if not callable(getattr(worker, "run", None)):
            raise TypeError("worker must provide run(job_id).")
        self.receiver = receiver
        self.worker = worker

    def run_once(self) -> MigrationJob | None:
        """Handle one delivery, keeping it queued whenever processing fails."""

        received = self.receiver.receive_one()
        if received is None:
            return None

        result = self.worker.run(received.job_id)
        self.receiver.acknowledge(received.receipt_handle)
        return result


__all__ = [
    "ExactMigrationJobWorker",
    "MigrationJobReceiver",
    "SqsMigrationJobCoordinator",
]
