"""Define the boundary used to notify workers about durable migration jobs."""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class MigrationJobPublisher(Protocol):
    """Notify a worker that one PostgreSQL-backed job is available."""

    def publish(self, job_id: UUID) -> None:
        """Send the durable job ID to a queue."""


__all__ = ["MigrationJobPublisher"]