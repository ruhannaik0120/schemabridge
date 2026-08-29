from uuid import uuid4

import pytest

from schemabridge.services.jobs.sqs_consumer import ReceivedMigrationJob
from schemabridge.services.jobs.sqs_worker import SqsMigrationJobCoordinator


class RecordingReceiver:
    def __init__(self, received: ReceivedMigrationJob | None) -> None:
        self.received = received
        self.acknowledged_receipt_handles: list[str] = []

    def receive_one(self) -> ReceivedMigrationJob | None:
        return self.received

    def acknowledge(self, receipt_handle: str) -> None:
        self.acknowledged_receipt_handles.append(receipt_handle)


class RecordingWorker:
    def __init__(self, result) -> None:
        self.result = result
        self.job_ids = []

    def run(self, job_id):
        self.job_ids.append(job_id)
        return self.result


class FailingWorker:
    def run(self, _job_id):
        raise RuntimeError("migration processing failed")


def test_coordinator_returns_when_no_sqs_message_is_available() -> None:
    receiver = RecordingReceiver(None)
    worker = RecordingWorker(object())
    coordinator = SqsMigrationJobCoordinator(receiver, worker)

    assert coordinator.run_once() is None
    assert worker.job_ids == []
    assert receiver.acknowledged_receipt_handles == []


def test_coordinator_processes_the_received_job_then_acknowledges_it() -> None:
    job_id = uuid4()
    receiver = RecordingReceiver(
        ReceivedMigrationJob(job_id=job_id, receipt_handle="receipt-123")
    )
    result = object()
    worker = RecordingWorker(result)
    coordinator = SqsMigrationJobCoordinator(receiver, worker)

    assert coordinator.run_once() is result
    assert worker.job_ids == [job_id]
    assert receiver.acknowledged_receipt_handles == ["receipt-123"]


def test_coordinator_keeps_the_message_when_processing_fails() -> None:
    receiver = RecordingReceiver(
        ReceivedMigrationJob(job_id=uuid4(), receipt_handle="receipt-123")
    )
    coordinator = SqsMigrationJobCoordinator(receiver, FailingWorker())

    with pytest.raises(RuntimeError, match="processing failed"):
        coordinator.run_once()

    assert receiver.acknowledged_receipt_handles == []
