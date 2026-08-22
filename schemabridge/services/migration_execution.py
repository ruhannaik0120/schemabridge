"""Prepare write-enabled target profiles for registered execution adapters.

Profile preparation is database-neutral: the selected profile must match the
workflow's registered target system and be explicitly write enabled. Target
adapters own dialect validation, statement execution, and outcome sanitation.
"""

from __future__ import annotations

from typing import Callable

from schemabridge.persistence.errors import (
    WorkflowTargetProfileNotWriteCapableError,
    WorkflowTargetProfileUnavailableError,
    WorkflowUnsupportedExecutionConnectorError,
)
from schemabridge.target_execution.base import (
    PreparedMigrationTarget,
    TargetExecutionDisposition,
    TargetExecutionResult,
)


class ProfileBoundMigrationExecutionService:
    """Resolve a write-enabled target profile and discard unsafe driver output."""

    def __init__(self, database_service_factory: Callable[[str], object]) -> None:
        """Accept a profile resolver so preparation remains testable and lazy."""

        self.database_service_factory = database_service_factory

    def prepare(
        self,
        profile_id: str,
        *,
        target_database: str | None,
        target_system: str,
        timeout_seconds: int | None,
    ) -> PreparedMigrationTarget:
        """Resolve a write-enabled profile matching the workflow target system.

        Raises a workflow-safe domain error when the profile is missing,
        mismatched, read-only, or otherwise malformed.
        """

        try:
            service = self.database_service_factory(profile_id)
            context = service.migration_execution_context(timeout_seconds)
        except Exception:
            raise WorkflowTargetProfileUnavailableError() from None
        if context.get("profile_id") != profile_id:
            raise WorkflowTargetProfileUnavailableError()
        # Write authorization is operator-controlled profile state, never a
        # property a client can grant in an execution request.
        if context.get("write_enabled") is not True:
            raise WorkflowTargetProfileNotWriteCapableError()
        profile_system = context.get("db_type")
        if (
            not isinstance(profile_system, str)
            or not profile_system.strip()
            or not isinstance(target_system, str)
            or not target_system.strip()
            or profile_system.strip().casefold()
            != target_system.strip().casefold()
        ):
            raise WorkflowUnsupportedExecutionConnectorError()
        configured_database = context.get("database")
        if (
            not isinstance(configured_database, str)
            or target_database is None
            or configured_database != target_database
        ):
            raise WorkflowTargetProfileUnavailableError()
        effective_timeout = context.get("timeout_seconds")
        if (
            isinstance(effective_timeout, bool)
            or not isinstance(effective_timeout, int)
            or effective_timeout <= 0
        ):
            raise WorkflowTargetProfileUnavailableError()
        connector_type = context.get("connector_type")
        if not isinstance(connector_type, str) or not connector_type:
            raise WorkflowUnsupportedExecutionConnectorError()
        return PreparedMigrationTarget(
            profile_id=profile_id,
            database=configured_database,
            connector_type=connector_type,
            timeout_seconds=effective_timeout,
            service=service,
        )

__all__ = [
    "PreparedMigrationTarget",
    "ProfileBoundMigrationExecutionService",
    "TargetExecutionDisposition",
    "TargetExecutionResult",
]
