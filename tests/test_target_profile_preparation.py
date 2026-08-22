"""Verify target profile preparation matches roles rather than vendor names."""

import pytest

from schemabridge.persistence.errors import (
    WorkflowTargetProfileNotWriteCapableError,
    WorkflowUnsupportedExecutionConnectorError,
)
from schemabridge.services.migration_execution import (
    ProfileBoundMigrationExecutionService,
)


class ProfileService:
    def __init__(
        self,
        *,
        profile_id="target-profile",
        database_type="postgresql",
        database="target_database",
        write_enabled=True,
    ):
        self.context = {
            "profile_id": profile_id,
            "db_type": database_type,
            "database": database,
            "timeout_seconds": 20,
            "write_enabled": write_enabled,
            "connector_type": database_type,
        }

    def migration_execution_context(self, _timeout_seconds):
        return self.context


@pytest.mark.parametrize("database_type", ["postgresql", "mysql", "snowflake"])
def test_preparation_accepts_any_matching_write_enabled_target(database_type) -> None:
    service = ProfileService(database_type=database_type)
    preparer = ProfileBoundMigrationExecutionService(lambda _profile_id: service)

    prepared = preparer.prepare(
        "target-profile",
        target_database="target_database",
        target_system=database_type.upper(),
        timeout_seconds=30,
    )

    assert prepared.profile_id == "target-profile"
    assert prepared.database == "target_database"
    assert prepared.connector_type == database_type
    assert prepared.timeout_seconds == 20
    assert prepared.service is service


def test_preparation_rejects_profile_and_workflow_system_mismatch() -> None:
    preparer = ProfileBoundMigrationExecutionService(
        lambda _profile_id: ProfileService(database_type="postgresql")
    )

    with pytest.raises(WorkflowUnsupportedExecutionConnectorError):
        preparer.prepare(
            "target-profile",
            target_database="target_database",
            target_system="mysql",
            timeout_seconds=30,
        )


@pytest.mark.parametrize("target_system", ["", "   ", None, 7])
def test_preparation_rejects_invalid_target_system_safely(target_system) -> None:
    preparer = ProfileBoundMigrationExecutionService(
        lambda _profile_id: ProfileService(database_type="postgresql")
    )

    with pytest.raises(WorkflowUnsupportedExecutionConnectorError):
        preparer.prepare(
            "target-profile",
            target_database="target_database",
            target_system=target_system,
            timeout_seconds=30,
        )


def test_preparation_still_requires_operator_write_permission() -> None:
    preparer = ProfileBoundMigrationExecutionService(
        lambda _profile_id: ProfileService(write_enabled=False)
    )

    with pytest.raises(WorkflowTargetProfileNotWriteCapableError):
        preparer.prepare(
            "target-profile",
            target_database="target_database",
            target_system="postgresql",
            timeout_seconds=30,
        )
