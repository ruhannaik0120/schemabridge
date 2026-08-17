"""Verify production assembly for the one-shot local migration-job worker."""

from schemabridge.services.migration_job_pipeline import (
    MigrationJobPipelineProcessor,
)
from schemabridge.services.migration_job_runtime import build_migration_job_worker
from schemabridge.services.migration_job_worker import MigrationJobWorker
from tests.fakes.workflow_repository import InMemoryWorkflowRepository


def test_runtime_assembles_the_real_pipeline_without_connecting() -> None:
    repository = InMemoryWorkflowRepository()

    worker = build_migration_job_worker(
        repository,
        database_service_factory=lambda _profile_id: object(),
    )

    assert isinstance(worker, MigrationJobWorker)
    assert isinstance(worker.processor, MigrationJobPipelineProcessor)
    assert worker.run_once() is None
