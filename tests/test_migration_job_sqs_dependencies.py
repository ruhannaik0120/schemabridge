from schemabridge.api.dependencies import (
    get_migration_job_submission_service,
)
from schemabridge.services.jobs.sqs import SqsMigrationJobPublisher


class FakeSqsClient:
    def send_message(self, *, QueueUrl: str, MessageBody: str) -> object:
        return {"MessageId": "fake-message-id"}


def test_job_submission_keeps_local_behavior_when_sqs_is_disabled(
    monkeypatch,
) -> None:
    monkeypatch.delenv("SCHEMABRIDGE_AWS_REGION", raising=False)
    monkeypatch.delenv("SCHEMABRIDGE_SQS_QUEUE_URL", raising=False)

    service = get_migration_job_submission_service(persistence=object())

    assert service.job_publisher is None


def test_job_submission_builds_an_sqs_publisher_when_enabled(
    monkeypatch,
) -> None:
    fake_client = FakeSqsClient()
    monkeypatch.setenv("SCHEMABRIDGE_AWS_REGION", "ap-south-1")
    monkeypatch.setenv(
        "SCHEMABRIDGE_SQS_QUEUE_URL",
        "https://sqs.ap-south-1.amazonaws.com/123/schemabridge-migration-jobs",
    )
    monkeypatch.setattr(
        "schemabridge.services.jobs.sqs.create_sqs_client",
        lambda *, region_name: fake_client,
    )

    service = get_migration_job_submission_service(persistence=object())

    assert isinstance(service.job_publisher, SqsMigrationJobPublisher)
    assert service.job_publisher.client is fake_client
    assert service.job_publisher.queue_url.endswith(
        "/schemabridge-migration-jobs"
    )