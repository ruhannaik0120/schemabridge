"""Verify the safe console boundary of the one-shot local worker command."""

from datetime import datetime, timezone
from uuid import UUID

from schemabridge.models.migration_job import (
    MigrationJob,
    MigrationJobStage,
    MigrationJobStatus,
)
from schemabridge.models.workflow import AuditActorType
from schemabridge.persistence.config import ControlPlaneConfig
from scripts import run_migration_worker


JOB_ID = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
NOW = datetime(2026, 8, 17, tzinfo=timezone.utc)


class FakeRepository:
    def __init__(self, _config) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeWorker:
    def __init__(self, result) -> None:
        self.result = result

    def run_once(self):
        return self.result


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
    monkeypatch.setattr(run_migration_worker, "load_dotenv", lambda *_a, **_k: None)
    monkeypatch.setattr(
        run_migration_worker.ControlPlaneConfig,
        "from_environment",
        lambda: ControlPlaneConfig("postgresql://configured"),
    )
    monkeypatch.setattr(
        run_migration_worker,
        "PostgreSQLWorkflowRepository",
        lambda _config: repository,
    )
    monkeypatch.setattr(
        run_migration_worker,
        "build_migration_job_worker",
        lambda *_a, **_k: FakeWorker(result),
    )
    monkeypatch.setattr(run_migration_worker, "reset_database_services", lambda: None)
    return repository


def test_command_reports_an_empty_queue_and_closes_resources(monkeypatch, capsys) -> None:
    repository = _configure(monkeypatch, None)

    assert run_migration_worker.main() == 0
    assert capsys.readouterr().out.strip() == "No queued migration jobs."
    assert repository.closed is True


def test_command_reports_a_successful_job_without_secrets(monkeypatch, capsys) -> None:
    repository = _configure(monkeypatch, _job())

    assert run_migration_worker.main() == 0
    output = capsys.readouterr().out
    assert str(JOB_ID) in output
    assert "status=SUCCEEDED, stage=COMPLETED" in output
    assert "postgresql://configured" not in output
    assert repository.closed is True


def test_command_returns_distinct_code_for_a_review_job(monkeypatch, capsys) -> None:
    _configure(monkeypatch, _job(MigrationJobStatus.REVIEW_REQUIRED))

    assert run_migration_worker.main() == 3
    output = capsys.readouterr().out
    assert "status=REVIEW_REQUIRED, stage=VALIDATING" in output
    assert "failure_category=VALIDATION_MISMATCH" in output


def test_command_requires_the_control_plane(monkeypatch, capsys) -> None:
    monkeypatch.setattr(run_migration_worker, "load_dotenv", lambda *_a, **_k: None)
    monkeypatch.setattr(
        run_migration_worker.ControlPlaneConfig,
        "from_environment",
        lambda: ControlPlaneConfig("", enabled=False),
    )

    assert run_migration_worker.main() == 2
    assert "SCHEMABRIDGE_CONTROL_PLANE_DSN is required" in capsys.readouterr().err
