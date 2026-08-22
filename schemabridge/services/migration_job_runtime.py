"""Assemble the production services used by the local migration-job worker."""

from __future__ import annotations

from collections.abc import Callable

from schemabridge.services.batch_transport import ProfileBoundBatchTransportService
from schemabridge.services.mapping_approval import MappingApprovalService
from schemabridge.services.migration_execution import (
    ProfileBoundMigrationExecutionService,
)
from schemabridge.services.migration_job_pipeline import (
    MigrationJobExecutionStep,
    MigrationJobPipelineProcessor,
    MigrationJobStagingStep,
    MigrationJobValidationStep,
)
from schemabridge.services.migration_job_worker import MigrationJobWorker
from schemabridge.services.migration_jobs import (
    MigrationJobClaimService,
    MigrationJobCompletionService,
)
from schemabridge.services.schema_mapping import SchemaMappingService
from schemabridge.target_execution import (
    MySqlTargetExecutionAdapter,
    PostgreSqlTargetExecutionAdapter,
    SnowflakeTargetExecutionAdapter,
    TargetExecutionRegistry,
)
from schemabridge.services.validation_execution import (
    MigrationValidationExecutionService,
)
from schemabridge.services.validation_sql import compile_validation_sql
from schemabridge.services.workflow_execution import WorkflowExecutionOrchestrator
from schemabridge.services.workflow_orchestration import WorkflowPlanningOrchestrator
from schemabridge.services.workflow_persistence import WorkflowPersistenceService
from schemabridge.services.workflow_transport import WorkflowTransportOrchestrator
from schemabridge.services.workflow_validation import WorkflowValidationOrchestrator


def build_migration_job_worker(
    repository: object,
    *,
    database_service_factory: Callable[[str], object],
) -> MigrationJobWorker:
    """Connect one durable repository to the existing real migration pipeline."""

    persistence = WorkflowPersistenceService(repository)
    completion = MigrationJobCompletionService(persistence)
    transport_service = ProfileBoundBatchTransportService(database_service_factory)
    execution_service = ProfileBoundMigrationExecutionService(
        database_service_factory
    )
    target_registry = TargetExecutionRegistry(
        (
            SnowflakeTargetExecutionAdapter(),
            PostgreSqlTargetExecutionAdapter(),
            MySqlTargetExecutionAdapter(),
        )
    )

    staging_step = MigrationJobStagingStep(
        persistence,
        WorkflowTransportOrchestrator(
            persistence,
            transport_service=transport_service,
        ),
        completion_service=completion,
    )
    execution_step = MigrationJobExecutionStep(
        persistence,
        WorkflowPlanningOrchestrator(
            persistence,
            discovery_resolver=database_service_factory,
            mapping_service=SchemaMappingService(),
            approval_service=MappingApprovalService(),
            target_registry=target_registry,
        ),
        WorkflowExecutionOrchestrator(
            persistence,
            target_registry=target_registry,
            execution_service=execution_service,
            staging_cleanup_service=transport_service,
        ),
        completion_service=completion,
    )
    validation_step = MigrationJobValidationStep(
        persistence,
        WorkflowValidationOrchestrator(
            persistence,
            validation_compiler=compile_validation_sql,
            validation_execution_service=MigrationValidationExecutionService(
                database_service_factory
            ),
        ),
        completion_service=completion,
    )
    processor = MigrationJobPipelineProcessor(
        staging_step,
        execution_step,
        validation_step,
    )
    return MigrationJobWorker(
        MigrationJobClaimService(persistence),
        processor,
    )


__all__ = ["build_migration_job_worker"]
