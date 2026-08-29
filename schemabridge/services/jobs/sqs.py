"""Publish durable migration-job notifications to Amazon SQS."""

from __future__ import annotations

import json
from typing import Protocol
from uuid import UUID

import boto3


class SqsClient(Protocol):
    """Describe only the AWS client method this publisher needs."""

    def send_message(self, *, QueueUrl: str, MessageBody: str) -> object:
        """Send one message to an SQS queue."""


class MigrationJobPublishError(RuntimeError):
    """Raised when a job notification cannot be sent safely."""


def create_sqs_client(*, region_name: str) -> SqsClient:
    """Create the configured AWS SQS client without sending a message."""

    if not isinstance(region_name, str) or not region_name.strip():
        raise ValueError("SQS region name is invalid.")

    return boto3.client("sqs", region_name=region_name)


class SqsMigrationJobPublisher:
    """Send one durable migration-job ID to a configured SQS queue."""

    def __init__(self, client: SqsClient, *, queue_url: str) -> None:
        if not callable(getattr(client, "send_message", None)):
            raise TypeError("SQS client must provide send_message.")
        if not isinstance(queue_url, str) or not queue_url.strip():
            raise ValueError("SQS queue URL is invalid.")
        self.client = client
        self.queue_url = queue_url

    def publish(self, job_id: UUID) -> None:
        if not isinstance(job_id, UUID):
            raise TypeError("job_id must be a UUID.")

        message_body = json.dumps(
            {"job_id": str(job_id)},
            separators=(",", ":"),
            sort_keys=True,
        )
        try:
            self.client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message_body,
            )
        except Exception:
            raise MigrationJobPublishError(
                "Migration-job queue publish failed."
            ) from None


__all__ = [
    "MigrationJobPublishError",
    "SqsClient",
    "SqsMigrationJobPublisher",
    "create_sqs_client",
]
