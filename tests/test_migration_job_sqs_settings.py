import pytest

from schemabridge.services.jobs.config import (
    SqsJobQueueConfigurationError,
    SqsJobQueueSettings,
)


def test_sqs_is_disabled_when_queue_url_is_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("SCHEMABRIDGE_AWS_REGION", raising=False)
    monkeypatch.delenv("SCHEMABRIDGE_SQS_QUEUE_URL", raising=False)

    settings = SqsJobQueueSettings.from_environment()

    assert settings.enabled is False
    assert settings.region_name is None
    assert settings.queue_url is None


def test_sqs_settings_accept_a_region_and_https_queue_url(monkeypatch) -> None:
    monkeypatch.setenv("SCHEMABRIDGE_AWS_REGION", "ap-southeast-2")
    monkeypatch.setenv(
        "SCHEMABRIDGE_SQS_QUEUE_URL",
        "https://sqs.ap-southeast-2.amazonaws.com/123/schemabridge-jobs",
    )

    settings = SqsJobQueueSettings.from_environment()

    assert settings.enabled is True
    assert settings.region_name == "ap-southeast-2"


def test_sqs_queue_url_requires_a_region(monkeypatch) -> None:
    monkeypatch.delenv("SCHEMABRIDGE_AWS_REGION", raising=False)
    monkeypatch.setenv(
        "SCHEMABRIDGE_SQS_QUEUE_URL",
        "https://sqs.ap-southeast-2.amazonaws.com/123/schemabridge-jobs",
    )

    with pytest.raises(SqsJobQueueConfigurationError, match="AWS_REGION"):
        SqsJobQueueSettings.from_environment()