"""Coordinate one local background-job cycle without owning pipeline logic."""

from __future__ import annotations

from typing import Protocol

from schemabridge.models.migration_job import MigrationJob, MigrationJobStatus
from schemabridge.services.migration_jobs import MigrationJobClaimService


class MigrationJobProcessor(Protocol):
    """Perform one claimed migration job and return its durable final record."""

    def process(self, job: MigrationJob) -> MigrationJob:
        ...


class MigrationJobProcessorContractError(RuntimeError):
    """Signal that a processor returned an invalid or unfinished job result."""


class MigrationJobWorker:
    """Claim at most one queued job and hand it to the configured processor."""

    def __init__(
        self,
        claim_service: MigrationJobClaimService,
        processor: MigrationJobProcessor,
    ) -> None:
        if not callable(getattr(processor, "process", None)):
            raise TypeError("processor must provide process(job).")
        self.claim_service = claim_service
        self.processor = processor

    def run_once(self) -> MigrationJob | None:
        """Process one available job, or return immediately when none is queued."""

        claimed = self.claim_service.claim_next()
        if claimed is None:
            return None
        result = self.processor.process(claimed)
        terminal = {
            MigrationJobStatus.SUCCEEDED,
            MigrationJobStatus.FAILED,
            MigrationJobStatus.REVIEW_REQUIRED,
            MigrationJobStatus.RECOVERY_REQUIRED,
        }
        if (
            not isinstance(result, MigrationJob)
            or result.job_id != claimed.job_id
            or result.status not in terminal
        ):
            raise MigrationJobProcessorContractError(
                "The migration job processor did not return the claimed terminal job."
            )
        return result


__all__ = [
    "MigrationJobProcessor",
    "MigrationJobProcessorContractError",
    "MigrationJobWorker",
]
