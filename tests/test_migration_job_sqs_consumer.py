import json
from uuid import uuid4

import pytest

from schemabridge.services.jobs.sqs_consumer import (
    MigrationJobReceiveError,
    SqsMigrationJobReceiver,
)


class RecordingSqsReceiveClient:
    def __init__(self, response: dict) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []
        self.delete_calls: list[dict[str, str]] = []

    def receive_message(
        self,
        *,
        QueueUrl: str,
        MaxNumberOfMessages: int,
        WaitTimeSeconds: int,
    ) -> dict:
        self.calls.append(
            {
                "QueueUrl": QueueUrl,
                "MaxNumberOfMessages": MaxNumberOfMessages,
                "WaitTimeSeconds": WaitTimeSeconds,
            }
        )
        return self.response

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> object:
        self.delete_calls.append(
            {
                "QueueUrl": QueueUrl,
                "ReceiptHandle": ReceiptHandle,
            }
        )
        return {}


class FailingSqsReceiveClient:
    def receive_message(self, **_kwargs) -> dict:
        raise RuntimeError("raw AWS failure must not reach callers")


class FailingSqsAcknowledgementClient(RecordingSqsReceiveClient):
    def delete_message(self, **_kwargs) -> object:
        raise RuntimeError("raw AWS failure must not reach callers")


def test_receiver_reads_one_valid_job_notification() -> None:
    job_id = uuid4()
    client = RecordingSqsReceiveClient(
        {
            "Messages": [
                {
                    "ReceiptHandle": "receipt-123",
                    "Body": json.dumps({"job_id": str(job_id)}),
                }
            ]
        }
    )
    receiver = SqsMigrationJobReceiver(
        client,
        queue_url="https://sqs.example.test/123/schemabridge-jobs",
    )

    received = receiver.receive_one()

    assert received is not None
    assert received.job_id == job_id
    assert received.receipt_handle == "receipt-123"
    assert client.calls == [
        {
            "QueueUrl": "https://sqs.example.test/123/schemabridge-jobs",
            "MaxNumberOfMessages": 1,
            "WaitTimeSeconds": 20,
        }
    ]


def test_receiver_returns_none_when_no_message_is_available() -> None:
    receiver = SqsMigrationJobReceiver(
        RecordingSqsReceiveClient({}),
        queue_url="https://sqs.example.test/123/schemabridge-jobs",
    )

    assert receiver.receive_one() is None


def test_receiver_rejects_invalid_message_payloads() -> None:
    receiver = SqsMigrationJobReceiver(
        RecordingSqsReceiveClient(
            {
                "Messages": [
                    {
                        "ReceiptHandle": "receipt-123",
                        "Body": json.dumps({"unexpected": "value"}),
                    }
                ]
            }
        ),
        queue_url="https://sqs.example.test/123/schemabridge-jobs",
    )

    with pytest.raises(MigrationJobReceiveError, match="message is invalid"):
        receiver.receive_one()


def test_receiver_hides_raw_client_failures() -> None:
    receiver = SqsMigrationJobReceiver(
        FailingSqsReceiveClient(),
        queue_url="https://sqs.example.test/123/schemabridge-jobs",
    )

    with pytest.raises(MigrationJobReceiveError, match="receive failed") as error:
        receiver.receive_one()

    assert "raw AWS failure" not in str(error.value)


def test_receiver_acknowledges_a_successfully_handled_message() -> None:
    client = RecordingSqsReceiveClient({})
    receiver = SqsMigrationJobReceiver(
        client,
        queue_url="https://sqs.example.test/123/schemabridge-jobs",
    )

    receiver.acknowledge("receipt-123")

    assert client.delete_calls == [
        {
            "QueueUrl": "https://sqs.example.test/123/schemabridge-jobs",
            "ReceiptHandle": "receipt-123",
        }
    ]


def test_receiver_hides_raw_acknowledgement_failures() -> None:
    receiver = SqsMigrationJobReceiver(
        FailingSqsAcknowledgementClient({}),
        queue_url="https://sqs.example.test/123/schemabridge-jobs",
    )

    with pytest.raises(MigrationJobReceiveError, match="acknowledgement failed") as error:
        receiver.acknowledge("receipt-123")

    assert "raw AWS failure" not in str(error.value)
