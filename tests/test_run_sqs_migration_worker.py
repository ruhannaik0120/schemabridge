from datetime import datetime, timezone
from uuid import UUID

import pytest

from schemabridge.models.migration_job import (
    MigrationJob,
    MigrationJobStage,
    MigrationJobStatus,
)
from schemabridge.models.workflow import AuditActorType
from schemabridge.persistence.config import ControlPlaneConfig
from schemabridge.services.jobs.config import SqsJobQueueSettings
from scripts import run_sqs_migration_worker


JOB_ID = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
NOW = datetime(2026, 8, 17, tzinfo=timezone.utc)


class FakeRepository:
    def __init__(self, _config) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeCoordinator:
    def __init__(self, result) -> None:
        self.result = result

    def run_once(self):
        return self.result


class InterruptingCoordinator:
    def __init__(self) -> None:
        self.calls = 0

    def run_once(self):
        self.calls += 1
        if self.calls == 1:
            return None
        raise KeyboardInterrupt


def _job(status=MigrationJobStatus.SUCCEEDED) -> MigrationJob:
    return MigrationJob(
        job_id=JOB_ID,
        workflow_id=UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        expected_workflow_version=5,
        source_discovery_artifact_version=1,
        approved_mapping_artifact_version=4,
        source_profile_id="source",
        target_profile_id="target",
        batch_size=100,
        timeout_seconds=30,
        job_fingerprint="a" * 64,
        status=status,
        stage=(
            MigrationJobStage.COMPLETED
            if status is MigrationJobStatus.SUCCEEDED
            else MigrationJobStage.VALIDATING
        ),
        queued_at=NOW,
        actor_type=AuditActorType.SERVICE,
        idempotency_key="job-1",
        started_at=NOW,
        completed_at=NOW,
        duration_ms=0,
        failure_category=(
            None if status is MigrationJobStatus.SUCCEEDED else "VALIDATION_MISMATCH"
        ),
    )


def _configure(monkeypatch, result) -> FakeRepository:
    repository = FakeRepository(None)

    monkeypatch.setattr(
        run_sqs_migration_worker,
        "load_dotenv",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        run_sqs_migration_worker.ControlPlaneConfig,
        "from_environment",
        lambda: ControlPlaneConfig("postgresql://configured"),
    )
    monkeypatch.setattr(
        run_sqs_migration_worker.SqsJobQueueSettings,
        "from_environment",
        lambda: SqsJobQueueSettings(
            region_name="ap-south-1",
            queue_url="https://sqs.example.test/123/schemabridge-jobs",
        ),
    )
    monkeypatch.setattr(
        run_sqs_migration_worker,
        "PostgreSQLWorkflowRepository",
        lambda _config: repository,
    )
    monkeypatch.setattr(
        run_sqs_migration_worker,
        "build_migration_job_worker",
        lambda *_a, **_k: object(),
    )
    monkeypatch.setattr(
        run_sqs_migration_worker,
        "create_sqs_client",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        run_sqs_migration_worker,
        "SqsMigrationJobReceiver",
        lambda *_a, **_k: object(),
    )
    monkeypatch.setattr(
        run_sqs_migration_worker,
        "SqsMigrationJobCoordinator",
        lambda *_a, **_k: FakeCoordinator(result),
    )
    monkeypatch.setattr(
        run_sqs_migration_worker,
        "reset_database_services",
        lambda: None,
    )

    return repository


def test_command_reports_no_claimable_sqs_job_and_closes_resources(
    monkeypatch,
    capsys,
) -> None:
    repository = _configure(monkeypatch, None)

    assert run_sqs_migration_worker.main() == 0
    assert capsys.readouterr().out.strip() == "No claimable SQS migration job."
    assert repository.closed is True


def test_command_reports_a_successful_sqs_job_without_secrets(
    monkeypatch,
    capsys,
) -> None:
    repository = _configure(monkeypatch, _job())

    assert run_sqs_migration_worker.main() == 0
    output = capsys.readouterr().out
    assert str(JOB_ID) in output
    assert "status=SUCCEEDED, stage=COMPLETED" in output
    assert "postgresql://configured" not in output
    assert repository.closed is True


def test_command_requires_sqs_settings(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        run_sqs_migration_worker,
        "load_dotenv",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        run_sqs_migration_worker.ControlPlaneConfig,
        "from_environment",
        lambda: ControlPlaneConfig("postgresql://configured"),
    )
    monkeypatch.setattr(
        run_sqs_migration_worker.SqsJobQueueSettings,
        "from_environment",
        lambda: SqsJobQueueSettings(),
    )

    assert run_sqs_migration_worker.main() == 2
    assert "SQS job-queue settings are required" in capsys.readouterr().err


def test_forever_command_stops_cleanly_after_keyboard_interrupt(
    monkeypatch,
    capsys,
) -> None:
    repository = _configure(monkeypatch, None)
    coordinator = InterruptingCoordinator()
    monkeypatch.setattr(
        run_sqs_migration_worker,
        "SqsMigrationJobCoordinator",
        lambda *_a, **_k: coordinator,
    )

    assert run_sqs_migration_worker.main(["--forever"]) == 0
    output = capsys.readouterr().out
    assert "SQS migration worker started" in output
    assert "No claimable SQS migration job." in output
    assert "SQS migration worker stopped." in output
    assert coordinator.calls == 2
    assert repository.closed is True


def test_termination_signal_uses_the_normal_shutdown_path() -> None:
    with pytest.raises(KeyboardInterrupt):
        run_sqs_migration_worker._request_shutdown(15, None)
