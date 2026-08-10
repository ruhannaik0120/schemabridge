"""Lazy production dependency hooks for API routes."""

from collections.abc import Callable

from fastapi import Depends, Request

from .errors import ApiError

def get_profile_resolver() -> Callable:
    from schemabridge.services.profile_registry import ProfileRegistry

    return ProfileRegistry.from_json


def get_schema_discovery_service() -> Callable:
    """Return a lazy resolver backed by profile-bound database services."""

    return _resolve_profile_database_service


def _resolve_profile_database_service(profile_id: str):
    from schemabridge.services.database_service import get_database_service

    return get_database_service(profile_id)


def get_schema_mapping_service():
    from schemabridge.services.schema_mapping import SchemaMappingService

    return SchemaMappingService()


def get_mapping_approval_service():
    from schemabridge.services.mapping_approval import MappingApprovalService

    return MappingApprovalService()


def get_validation_execution_service():
    from schemabridge.services.validation_execution import MigrationValidationExecutionService

    return MigrationValidationExecutionService()


def get_validation_execution_service_factory() -> Callable:
    """Delay importing the execution orchestrator until approval is confirmed."""

    return get_validation_execution_service


def get_transformation_compiler():
    from schemabridge.services.transformation_sql import SnowflakeTransformationSqlCompiler

    return SnowflakeTransformationSqlCompiler()


def get_validation_compiler() -> Callable:
    from schemabridge.services.validation_sql import compile_validation_sql

    return compile_validation_sql


def get_database_service_factory():
    from schemabridge.services.database_service import get_database_service

    return get_database_service


def get_migration_execution_service(
    database_service_factory=Depends(get_database_service_factory),
):
    from schemabridge.services.migration_execution import ProfileBoundMigrationExecutionService

    return ProfileBoundMigrationExecutionService(database_service_factory)


def build_workflow_repository(config):
    """Build the app-owned durable repository without opening a connection."""

    from schemabridge.persistence.postgresql import PostgreSQLWorkflowRepository

    return PostgreSQLWorkflowRepository(config)


def get_workflow_repository(request: Request):
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


def get_workflow_planning_orchestrator(
    persistence=Depends(get_workflow_persistence_service),
    discovery_resolver=Depends(get_schema_discovery_service),
    mapping_service=Depends(get_schema_mapping_service),
    approval_service=Depends(get_mapping_approval_service),
    transformation_compiler=Depends(get_transformation_compiler),
):
    from schemabridge.services.workflow_orchestration import WorkflowPlanningOrchestrator

    return WorkflowPlanningOrchestrator(
        persistence,
        discovery_resolver=discovery_resolver,
        mapping_service=mapping_service,
        approval_service=approval_service,
        transformation_compiler=transformation_compiler,
    )


def get_workflow_execution_orchestrator(
    persistence=Depends(get_workflow_persistence_service),
    transformation_compiler=Depends(get_transformation_compiler),
    execution_service=Depends(get_migration_execution_service),
):
    from schemabridge.services.workflow_execution import WorkflowExecutionOrchestrator

    return WorkflowExecutionOrchestrator(
        persistence,
        transformation_compiler=transformation_compiler,
        execution_service=execution_service,
    )


def get_workflow_validation_orchestrator(
    persistence=Depends(get_workflow_persistence_service),
    validation_compiler=Depends(get_validation_compiler),
    validation_execution_service=Depends(get_validation_execution_service),
):
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
    get_transformation_compiler,
    get_validation_compiler,
    get_validation_execution_service,
    get_validation_execution_service_factory,
    get_database_service_factory,
    get_migration_execution_service,
    build_workflow_repository,
    get_workflow_repository,
    get_workflow_persistence_service,
    get_workflow_planning_orchestrator,
    get_workflow_execution_orchestrator,
    get_workflow_validation_orchestrator,
)

__all__ = [hook.__name__ for hook in REQUIRED_DEPENDENCY_HOOKS] + ["REQUIRED_DEPENDENCY_HOOKS"]
