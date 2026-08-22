"""Construct request services at the FastAPI dependency boundary.

Dependencies keep routes focused on HTTP translation while delaying imports,
profile resolution, and connector construction until an operation needs them.
The durable repository is application-owned; lightweight orchestration objects
are assembled per request around that shared repository.
"""

from collections.abc import Callable

from fastapi import Depends, Request

from .errors import ApiError

def get_profile_resolver() -> Callable:
    """Return the named-profile parser without reading configuration yet."""

    from schemabridge.services.profile_registry import ProfileRegistry

    return ProfileRegistry.from_json


def get_schema_discovery_service() -> Callable:
    """Return a lazy resolver backed by profile-bound database services."""

    return _resolve_profile_database_service


def _resolve_profile_database_service(profile_id: str):
    """Resolve one profile to the cached, safety-bounded database service."""

    from schemabridge.services.database_service import get_database_service

    return get_database_service(profile_id)


def get_schema_mapping_service():
    """Build the deterministic schema-mapping domain service."""

    from schemabridge.services.schema_mapping import SchemaMappingService

    return SchemaMappingService()


def get_mapping_approval_service():
    """Build the service that applies explicit human mapping decisions."""

    from schemabridge.services.mapping_approval import MappingApprovalService

    return MappingApprovalService()


def get_validation_execution_service():
    """Build the paired source/target aggregate-validation executor."""

    from schemabridge.services.validation_execution import MigrationValidationExecutionService

    return MigrationValidationExecutionService()


def get_validation_execution_service_factory() -> Callable:
    """Delay importing the execution orchestrator until approval is confirmed."""

    return get_validation_execution_service


def get_validation_compiler() -> Callable:
    """Return the pure function that generates paired validation queries."""

    from schemabridge.services.validation_sql import compile_validation_sql

    return compile_validation_sql


def get_database_service_factory():
    """Return the profile-bound database-service resolver."""

    from schemabridge.services.database_service import get_database_service

    return get_database_service


def get_migration_execution_service(
    database_service_factory=Depends(get_database_service_factory),
):
    """Build the write-gated Snowflake execution boundary."""

    from schemabridge.services.migration_execution import ProfileBoundMigrationExecutionService

    return ProfileBoundMigrationExecutionService(database_service_factory)


def get_target_execution_registry():
    """Register the target adapters currently supported by the application."""

    from schemabridge.target_execution import (
        MySqlTargetExecutionAdapter,
        PostgreSqlTargetExecutionAdapter,
        SnowflakeTargetExecutionAdapter,
        TargetExecutionRegistry,
    )

    return TargetExecutionRegistry(
        (
            SnowflakeTargetExecutionAdapter(),
            PostgreSqlTargetExecutionAdapter(),
            MySqlTargetExecutionAdapter(),
        )
    )


def get_batch_transport_service(
    database_service_factory=Depends(get_database_service_factory),
):
    """Build the profile-bound source-to-managed-staging boundary."""

    from schemabridge.services.batch_transport import ProfileBoundBatchTransportService

    return ProfileBoundBatchTransportService(database_service_factory)


def build_workflow_repository(config):
    """Build the app-owned durable repository without opening a connection."""

    from schemabridge.persistence.postgresql import PostgreSQLWorkflowRepository

    return PostgreSQLWorkflowRepository(config)


def get_workflow_repository(request: Request):
    """Return durable persistence or fail clearly when it is not configured."""

    repository = getattr(request.app.state, "workflow_repository", None)
    if repository is None:
        raise ApiError(
            503,
            "CONTROL_PLANE_UNAVAILABLE",
            "Durable workflow persistence is not configured.",
        )
    return repository


def get_workflow_persistence_service(repository=Depends(get_workflow_repository)):
    """Create a request-scoped domain service over the app-owned repository."""

    from schemabridge.services.workflow_persistence import WorkflowPersistenceService

    return WorkflowPersistenceService(repository)


def get_migration_job_submission_service(
    persistence=Depends(get_workflow_persistence_service),
):
    """Build the service that validates and records queued migration jobs."""

    from schemabridge.services.migration_jobs import MigrationJobSubmissionService

    return MigrationJobSubmissionService(persistence)


def get_workflow_planning_orchestrator(
    persistence=Depends(get_workflow_persistence_service),
    discovery_resolver=Depends(get_schema_discovery_service),
    mapping_service=Depends(get_schema_mapping_service),
    approval_service=Depends(get_mapping_approval_service),
    target_registry=Depends(get_target_execution_registry),
):
    """Assemble planning coordination over request-scoped dependencies."""

    from schemabridge.services.workflow_orchestration import WorkflowPlanningOrchestrator

    return WorkflowPlanningOrchestrator(
        persistence,
        discovery_resolver=discovery_resolver,
        mapping_service=mapping_service,
        approval_service=approval_service,
        target_registry=target_registry,
    )


def get_workflow_execution_orchestrator(
    persistence=Depends(get_workflow_persistence_service),
    target_registry=Depends(get_target_execution_registry),
    execution_service=Depends(get_migration_execution_service),
    staging_cleanup_service=Depends(get_batch_transport_service),
):
    """Assemble approval-gated execution coordination."""

    from schemabridge.services.workflow_execution import WorkflowExecutionOrchestrator

    return WorkflowExecutionOrchestrator(
        persistence,
        target_registry=target_registry,
        execution_service=execution_service,
        staging_cleanup_service=staging_cleanup_service,
    )


def get_workflow_transport_orchestrator(
    persistence=Depends(get_workflow_persistence_service),
    transport_service=Depends(get_batch_transport_service),
):
    """Assemble the durable source-to-managed-staging coordinator."""

    from schemabridge.services.workflow_transport import WorkflowTransportOrchestrator

    return WorkflowTransportOrchestrator(
        persistence,
        transport_service=transport_service,
    )


def get_workflow_validation_orchestrator(
    persistence=Depends(get_workflow_persistence_service),
    validation_compiler=Depends(get_validation_compiler),
    validation_execution_service=Depends(get_validation_execution_service),
):
    """Assemble durable validation and reconciliation coordination."""

    from schemabridge.services.workflow_validation import WorkflowValidationOrchestrator

    return WorkflowValidationOrchestrator(
        persistence,
        validation_compiler=validation_compiler,
        validation_execution_service=validation_execution_service,
    )


REQUIRED_DEPENDENCY_HOOKS = (
    get_profile_resolver,
    get_schema_discovery_service,
    get_schema_mapping_service,
    get_mapping_approval_service,
    get_validation_compiler,
    get_validation_execution_service,
    get_validation_execution_service_factory,
    get_database_service_factory,
    get_migration_execution_service,
    get_target_execution_registry,
    get_batch_transport_service,
    build_workflow_repository,
    get_workflow_repository,
    get_workflow_persistence_service,
    get_migration_job_submission_service,
    get_workflow_planning_orchestrator,
    get_workflow_execution_orchestrator,
    get_workflow_transport_orchestrator,
    get_workflow_validation_orchestrator,
)

__all__ = [hook.__name__ for hook in REQUIRED_DEPENDENCY_HOOKS] + ["REQUIRED_DEPENDENCY_HOOKS"]
