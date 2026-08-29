"""Load optional Amazon SQS job-notification settings."""

from __future__ import annotations

import os
from dataclasses import dataclass


class SqsJobQueueConfigurationError(ValueError):
    """Raised when optional SQS job-queue settings are unsafe or incomplete."""


def _optional_environment_value(name: str) -> str | None:
    """Return a trimmed optional environment value."""

    value = os.getenv(name)
    if value is None:
        return None
    return value.strip() or None


@dataclass(frozen=True, slots=True)
class SqsJobQueueSettings:
    """Describe optional SQS job notification without storing credentials."""

    region_name: str | None = None
    queue_url: str | None = None

    def __post_init__(self) -> None:
        for value, name in (
            (self.region_name, "region_name"),
            (self.queue_url, "queue_url"),
        ):
            if value is not None and (
                not isinstance(value, str)
                or not value.strip()
                or "\x00" in value
            ):
                raise ValueError(f"{name} is invalid.")

        if self.queue_url is not None:
            if self.region_name is None:
                raise SqsJobQueueConfigurationError(
                    "SCHEMABRIDGE_AWS_REGION is required when SQS is enabled."
                )
            if not self.queue_url.startswith("https://"):
                raise SqsJobQueueConfigurationError(
                    "SCHEMABRIDGE_SQS_QUEUE_URL must use HTTPS."
                )

    @property
    def enabled(self) -> bool:
        """Return whether this process should publish SQS notifications."""

        return self.queue_url is not None

    @classmethod
    def from_environment(cls) -> "SqsJobQueueSettings":
        """Read optional SQS settings without reading AWS credentials."""

        return cls(
            region_name=_optional_environment_value("SCHEMABRIDGE_AWS_REGION"),
            queue_url=_optional_environment_value(
                "SCHEMABRIDGE_SQS_QUEUE_URL"
            ),
        )


__all__ = [
    "SqsJobQueueConfigurationError",
    "SqsJobQueueSettings",
]