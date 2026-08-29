"""Receive and validate durable migration-job notifications from Amazon SQS."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID


class SqsReceiveClient(Protocol):
    """Describe the SQS read operation required by the job receiver."""

    def receive_message(
        self,
        *,
        QueueUrl: str,
        MaxNumberOfMessages: int,
        WaitTimeSeconds: int,
    ) -> dict:
        """Receive up to one message from an SQS queue."""

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> object:
        """Delete one successfully handled queue message."""


class MigrationJobReceiveError(RuntimeError):
    """Raised when an SQS job notification cannot be read safely."""


@dataclass(frozen=True, slots=True)
class ReceivedMigrationJob:
    """Hold one validated job ID and its SQS delivery receipt."""

    job_id: UUID
    receipt_handle: str


class SqsMigrationJobReceiver:
    """Read and validate one durable migration-job notification."""

    def __init__(
        self,
        client: SqsReceiveClient,
        *,
        queue_url: str,
        wait_time_seconds: int = 20,
    ) -> None:
        if not callable(getattr(client, "receive_message", None)):
            raise TypeError("SQS client must provide receive_message.")
        if not isinstance(queue_url, str) or not queue_url.strip():
            raise ValueError("SQS queue URL is invalid.")
        if (
            type(wait_time_seconds) is not int
            or not 0 <= wait_time_seconds <= 20
        ):
            raise ValueError("SQS receive wait time is invalid.")

        self.client = client
        self.queue_url = queue_url
        self.wait_time_seconds = wait_time_seconds

    def receive_one(self) -> ReceivedMigrationJob | None:
        """Wait for and validate at most one queued migration-job message."""

        try:
            response = self.client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=self.wait_time_seconds,
            )
        except Exception:
            raise MigrationJobReceiveError(
                "Migration-job queue receive failed."
            ) from None

        if not isinstance(response, dict):
            raise MigrationJobReceiveError(
                "Migration-job queue response is invalid."
            )

        messages = response.get("Messages", [])
        if not isinstance(messages, list):
            raise MigrationJobReceiveError(
                "Migration-job queue response is invalid."
            )
        if not messages:
            return None
        if len(messages) != 1 or not isinstance(messages[0], dict):
            raise MigrationJobReceiveError(
                "Migration-job queue response is invalid."
            )

        message = messages[0]
        receipt_handle = message.get("ReceiptHandle")
        body = message.get("Body")

        try:
            payload = json.loads(body)
            if (
                not isinstance(receipt_handle, str)
                or not receipt_handle
                or not isinstance(payload, dict)
                or set(payload) != {"job_id"}
            ):
                raise ValueError()
            return ReceivedMigrationJob(
                job_id=UUID(payload["job_id"]),
                receipt_handle=receipt_handle,
            )
        except Exception:
            raise MigrationJobReceiveError(
                "Migration-job queue message is invalid."
            ) from None

    def acknowledge(self, receipt_handle: str) -> None:
        """Delete one SQS delivery after its durable job was handled safely."""

        if not isinstance(receipt_handle, str) or not receipt_handle:
            raise ValueError("SQS receipt handle is invalid.")
        if not callable(getattr(self.client, "delete_message", None)):
            raise TypeError("SQS client must provide delete_message.")

        try:
            self.client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )
        except Exception:
            raise MigrationJobReceiveError(
                "Migration-job queue acknowledgement failed."
            ) from None


__all__ = [
    "MigrationJobReceiveError",
    "ReceivedMigrationJob",
    "SqsMigrationJobReceiver",
    "SqsReceiveClient",
]
