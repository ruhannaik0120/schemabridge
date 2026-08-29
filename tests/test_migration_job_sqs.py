import json
from unittest.mock import Mock
from uuid import uuid4

import pytest

from schemabridge.services.jobs.queue import MigrationJobPublisher
from schemabridge.services.jobs.sqs import (
    MigrationJobPublishError,
    SqsMigrationJobPublisher,
    create_sqs_client,
)


class RecordingSqsClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def send_message(self, *, QueueUrl: str, MessageBody: str) -> object:
        self.calls.append(
            {
                "QueueUrl": QueueUrl,
                "MessageBody": MessageBody,
            }
        )
        return {"MessageId": "message-123"}


class FailingSqsClient:
    def send_message(self, *, QueueUrl: str, MessageBody: str) -> object:
        raise RuntimeError("raw AWS failure must not reach callers")


def test_sqs_publisher_sends_only_the_job_id() -> None:
    client = RecordingSqsClient()
    publisher = SqsMigrationJobPublisher(
        client,
        queue_url="https://sqs.example.test/123/schemabridge-jobs",
    )
    job_id = uuid4()

    assert isinstance(publisher, MigrationJobPublisher)

    publisher.publish(job_id)

    assert client.calls[0]["QueueUrl"].endswith("/schemabridge-jobs")
    assert json.loads(client.calls[0]["MessageBody"]) == {
        "job_id": str(job_id)
    }


def test_sqs_publisher_hides_raw_client_failures() -> None:
    publisher = SqsMigrationJobPublisher(
        FailingSqsClient(),
        queue_url="https://sqs.example.test/123/schemabridge-jobs",
    )

    with pytest.raises(MigrationJobPublishError, match="queue publish failed") as error:
        publisher.publish(uuid4())

    assert "raw AWS failure" not in str(error.value)


def test_create_sqs_client_uses_the_configured_region(monkeypatch) -> None:
    expected_client = object()
    client_factory = Mock(return_value=expected_client)
    monkeypatch.setattr(
        "schemabridge.services.jobs.sqs.boto3.client",
        client_factory,
    )

    client = create_sqs_client(region_name="ap-south-1")

    assert client is expected_client
    client_factory.assert_called_once_with("sqs", region_name="ap-south-1")


def test_create_sqs_client_rejects_an_empty_region() -> None:
    with pytest.raises(ValueError, match="region name"):
        create_sqs_client(region_name=" ")
