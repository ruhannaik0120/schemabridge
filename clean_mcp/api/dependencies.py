"""Lazy production dependency hooks for future workflow routes."""

import sys
from collections.abc import Callable
from pathlib import Path


def _prepare_project_imports() -> None:
    """Expose the repository's established top-level modules for the root ASGI target."""

    project_directory = str(Path(__file__).resolve().parents[1])
    if project_directory not in sys.path:
        sys.path.insert(0, project_directory)


def get_profile_resolver() -> Callable:
    _prepare_project_imports()
    from services.profile_registry import ProfileRegistry

    return ProfileRegistry.from_json


def get_schema_discovery_service() -> Callable:
    """Return a lazy resolver backed by the existing profile-bound service cache."""

    return _resolve_profile_discovery_connector


def _resolve_profile_discovery_connector(profile_id: str):
    _prepare_project_imports()
    from services.query_service import get_query_service

    return get_query_service(profile_id).connector


def get_schema_mapping_service():
    _prepare_project_imports()
    from services.schema_mapping import SchemaMappingService

    return SchemaMappingService()


def get_mapping_approval_service():
    _prepare_project_imports()
    from services.mapping_approval import MappingApprovalService

    return MappingApprovalService()


def get_validation_execution_service():
    _prepare_project_imports()
    from services.validation_execution import MigrationValidationExecutionService

    return MigrationValidationExecutionService()


def get_validation_execution_service_factory() -> Callable:
    """Delay importing the execution orchestrator until approval is confirmed."""

    return get_validation_execution_service


def get_transformation_compiler():
    _prepare_project_imports()
    from services.transformation_sql import SnowflakeTransformationSqlCompiler

    return SnowflakeTransformationSqlCompiler()


def get_validation_compiler() -> Callable:
    _prepare_project_imports()
    from services.validation_sql import compile_validation_sql

    return compile_validation_sql


def get_query_service_factory():
    _prepare_project_imports()
    from services.query_service import get_query_service

    return get_query_service


REQUIRED_DEPENDENCY_HOOKS = (
    get_profile_resolver,
    get_schema_discovery_service,
    get_schema_mapping_service,
    get_mapping_approval_service,
    get_transformation_compiler,
    get_validation_compiler,
    get_validation_execution_service,
    get_validation_execution_service_factory,
    get_query_service_factory,
)

__all__ = [hook.__name__ for hook in REQUIRED_DEPENDENCY_HOOKS] + ["REQUIRED_DEPENDENCY_HOOKS"]
