"""Verify the repository contract for creating and reading migration jobs."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from schemabridge.models.migration_job import (
    MigrationJob,
    MigrationJobStage,
    MigrationJobStatus,
)
from schemabridge.models.transport import BatchTransportProgress
from schemabridge.models.workflow import (
    AuditActorType,
    MigrationWorkflow,
    MigrationWorkflowStatus,
    WorkflowRelation,
)
from schemabridge.persistence.errors import (
    MigrationJobAlreadyActiveError,
    MigrationJobNotFoundError,
    MigrationJobTransitionError,
    WorkflowConflictError,
    WorkflowIdempotencyConflictError,
    WorkflowOperationUnavailableError,
)
from schemabridge.persistence.config import ControlPlaneConfig
from schemabridge.persistence.postgresql import PostgreSQLWorkflowRepository
from tests.fakes.workflow_repository import InMemoryWorkflowRepository


NOW = datetime(2026, 8, 16, tzinfo=timezone.utc)
WORKFLOW_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
JOB_ID = UUID("11111111-2222-3333-4444-555555555555")


def _workflow(*, status=MigrationWorkflowStatus.MAPPING_APPROVED, version=5):
    return MigrationWorkflow(
        workflow_id=WORKFLOW_ID,
        display_name="Background migration",
        source_profile_id="mysql-source",
        target_profile_id="snowflake-target",
        source_relation=WorkflowRelation(
            catalog_name="source", schema_name="source", object_name="customers", system="mysql"
        ),
        target_relation=WorkflowRelation(
            catalog_name="target", schema_name="public", object_name="customers", system="snowflake"
        ),
        status=status,
        version=version,
        created_at=NOW,
        updated_at=NOW,
        latest_artifact_version=4,
    )


def _job(**overrides):
    values = {
        "job_id": JOB_ID,
        "workflow_id": WORKFLOW_ID,
        "expected_workflow_version": 5,
        "source_discovery_artifact_version": 1,
        "approved_mapping_artifact_version": 4,
        "source_profile_id": "mysql-source",
        "target_profile_id": "snowflake-target",
        "batch_size": 500,
        "timeout_seconds": 30,
        "job_fingerprint": "a" * 64,
        "status": MigrationJobStatus.QUEUED,
        "stage": MigrationJobStage.QUEUED,
        "queued_at": NOW,
        "actor_type": AuditActorType.USER,
        "idempotency_key": "create-job-1",
    }
    values.update(overrides)
    return MigrationJob(**values)


def _repository(workflow=None):
    repository = InMemoryWorkflowRepository()
    value = workflow or _workflow()
    repository._workflows[value.workflow_id] = value
    return repository


def test_create_and_get_job_and_exact_replay() -> None:
    repository = _repository()
    job = _job()

    created, was_created = repository.create_migration_job(job)
    replayed, replay_created = repository.create_migration_job(job)

    assert created == replayed == repository.get_migration_job(JOB_ID)
    assert was_created is True
    assert replay_created is False


def test_same_idempotency_key_with_changed_inputs_conflicts() -> None:
    repository = _repository()
    repository.create_migration_job(_job())

    with pytest.raises(WorkflowIdempotencyConflictError):
        repository.create_migration_job(
            _job(job_id=UUID(int=2), batch_size=100, job_fingerprint="b" * 64)
        )


def test_missing_job_has_a_job_specific_safe_error() -> None:
    with pytest.raises(MigrationJobNotFoundError, match="migration job is unavailable"):
        _repository().get_migration_job(UUID(int=99))


def test_stale_or_unapproved_workflow_cannot_create_job() -> None:
    with pytest.raises(WorkflowConflictError):
        _repository().create_migration_job(_job(expected_workflow_version=4))
    with pytest.raises(WorkflowOperationUnavailableError):
        _repository(_workflow(status=MigrationWorkflowStatus.MAPPING_PROPOSED)).create_migration_job(_job())


def test_second_active_job_for_workflow_is_rejected() -> None:
    repository = _repository()
    repository.create_migration_job(_job())

    with pytest.raises(MigrationJobAlreadyActiveError):
        repository.create_migration_job(
            _job(
                job_id=UUID(int=2),
                idempotency_key="create-job-2",
                job_fingerprint="b" * 64,
            )
        )


def test_failed_job_allows_a_new_safe_retry() -> None:
    repository = _repository()
    failed = replace(
        _job(),
        status=MigrationJobStatus.FAILED,
        stage=MigrationJobStage.STAGING,
        started_at=NOW,
        completed_at=NOW,
        duration_ms=0,
        failure_category="STAGING_FAILED",
    )
    repository._jobs[failed.job_id] = failed

    retry = _job(
        job_id=UUID(int=2),
        idempotency_key="create-job-retry",
        job_fingerprint="b" * 64,
    )
    assert repository.create_migration_job(retry) == (retry, True)


def test_review_required_job_blocks_a_new_full_migration() -> None:
    repository = _repository()
    review = replace(
        _job(),
        status=MigrationJobStatus.REVIEW_REQUIRED,
        stage=MigrationJobStage.VALIDATING,
        started_at=NOW,
        completed_at=NOW,
        duration_ms=0,
        failure_category="VALIDATION_MISMATCH",
    )
    repository._jobs[review.job_id] = review

    with pytest.raises(MigrationJobAlreadyActiveError):
        repository.create_migration_job(
            _job(
                job_id=UUID(int=2),
                idempotency_key="unsafe-review-retry",
                job_fingerprint="b" * 64,
            )
        )


def test_claim_returns_oldest_job_once_and_marks_it_running() -> None:
    repository = _repository()
    newer = _job()
    older = _job(
        job_id=UUID(int=2),
        workflow_id=UUID(int=2),
        queued_at=NOW - timedelta(seconds=1),
        idempotency_key="older-job",
        job_fingerprint="b" * 64,
    )
    repository._jobs[newer.job_id] = newer
    repository._jobs[older.job_id] = older
    started_at = NOW + timedelta(seconds=1)

    claimed = repository.claim_next_migration_job(started_at)
    next_claim = repository.claim_next_migration_job(started_at)

    assert claimed.job_id == older.job_id
    assert claimed.status is MigrationJobStatus.RUNNING
    assert claimed.stage is MigrationJobStage.PREPARING
    assert claimed.started_at == started_at
    assert next_claim.job_id == newer.job_id
    assert repository.claim_next_migration_job(started_at) is None


def test_stage_progress_moves_once_and_rejects_stale_or_skipped_updates() -> None:
    repository = _repository()
    running = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.PREPARING,
        started_at=NOW,
    )
    repository._jobs[running.job_id] = running

    staging = repository.update_migration_job_stage(
        JOB_ID,
        MigrationJobStage.PREPARING,
        MigrationJobStage.STAGING,
    )

    assert staging.stage is MigrationJobStage.STAGING
    with pytest.raises(MigrationJobTransitionError):
        repository.update_migration_job_stage(
            JOB_ID,
            MigrationJobStage.PREPARING,
            MigrationJobStage.STAGING,
        )
    with pytest.raises(MigrationJobTransitionError):
        repository.update_migration_job_stage(
            JOB_ID,
            MigrationJobStage.STAGING,
            MigrationJobStage.EXECUTING,
        )


def test_progress_cannot_mark_completion_or_advance_an_unclaimed_job() -> None:
    repository = _repository()
    repository._jobs[JOB_ID] = _job()

    with pytest.raises(MigrationJobTransitionError):
        repository.update_migration_job_stage(
            JOB_ID,
            MigrationJobStage.QUEUED,
            MigrationJobStage.PREPARING,
        )

    validating = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.VALIDATING,
        started_at=NOW,
    )
    repository._jobs[JOB_ID] = validating
    with pytest.raises(MigrationJobTransitionError):
        repository.update_migration_job_stage(
            JOB_ID,
            MigrationJobStage.VALIDATING,
            MigrationJobStage.COMPLETED,
        )


def _progress(batches=1, rows=500, estimate=1_200):
    return BatchTransportProgress(
        batches_completed=batches,
        rows_read=rows,
        rows_written=rows,
        total_rows_estimate=estimate,
    )


def test_batch_progress_is_durable_and_strictly_monotonic() -> None:
    repository = _repository()
    repository._jobs[JOB_ID] = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.STAGING,
        started_at=NOW,
    )

    first = repository.update_migration_job_progress(
        JOB_ID, _progress(), NOW + timedelta(seconds=1)
    )
    second = repository.update_migration_job_progress(
        JOB_ID, _progress(batches=2, rows=1_000), NOW + timedelta(seconds=2)
    )

    assert first.batch_progress == _progress()
    assert second == repository.get_migration_job(JOB_ID)
    assert second.batch_progress == _progress(batches=2, rows=1_000)
    assert second.progress_updated_at == NOW + timedelta(seconds=2)


@pytest.mark.parametrize(
    "progress,updated_at",
    [
        (_progress(), NOW + timedelta(seconds=2)),
        (_progress(batches=2, rows=1_000), NOW),
        (_progress(batches=2, rows=1_000, estimate=1_300), NOW + timedelta(seconds=2)),
    ],
)
def test_batch_progress_rejects_replay_old_time_and_changed_estimate(progress, updated_at) -> None:
    repository = _repository()
    repository._jobs[JOB_ID] = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.STAGING,
        started_at=NOW,
        batch_progress=_progress(),
        progress_updated_at=NOW + timedelta(seconds=1),
    )

    with pytest.raises(MigrationJobTransitionError):
        repository.update_migration_job_progress(JOB_ID, progress, updated_at)


def test_batch_progress_requires_a_running_staging_job() -> None:
    repository = _repository()
    repository._jobs[JOB_ID] = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.PREPARING,
        started_at=NOW,
    )

    with pytest.raises(MigrationJobTransitionError):
        repository.update_migration_job_progress(
            JOB_ID, _progress(), NOW + timedelta(seconds=1)
        )


def test_successful_finish_is_atomic_timed_and_exactly_replayable() -> None:
    repository = _repository()
    running = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.VALIDATING,
        started_at=NOW,
    )
    repository._jobs[JOB_ID] = running
    completed_at = NOW + timedelta(seconds=5)

    finished = repository.finish_migration_job(
        JOB_ID,
        MigrationJobStage.VALIDATING,
        MigrationJobStatus.SUCCEEDED,
        completed_at,
        None,
    )
    replay = repository.finish_migration_job(
        JOB_ID,
        MigrationJobStage.VALIDATING,
        MigrationJobStatus.SUCCEEDED,
        completed_at + timedelta(seconds=1),
        None,
    )

    assert finished.status is MigrationJobStatus.SUCCEEDED
    assert finished.stage is MigrationJobStage.COMPLETED
    assert finished.completed_at == completed_at
    assert finished.duration_ms == 5_000
    assert replay == finished


@pytest.mark.parametrize(
    ("outcome", "category"),
    [
        (MigrationJobStatus.FAILED, "STAGING_FAILED"),
        (MigrationJobStatus.RECOVERY_REQUIRED, "STAGING_OUTCOME_UNCERTAIN"),
    ],
)
def test_unsuccessful_finish_preserves_failure_stage(outcome, category) -> None:
    repository = _repository()
    running = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.STAGING,
        started_at=NOW,
    )
    repository._jobs[JOB_ID] = running

    finished = repository.finish_migration_job(
        JOB_ID,
        MigrationJobStage.STAGING,
        outcome,
        NOW + timedelta(seconds=2),
        category,
    )

    assert finished.status is outcome
    assert finished.stage is MigrationJobStage.STAGING
    assert finished.duration_ms == 2_000
    assert finished.failure_category == category


def test_finish_rejects_success_before_validation_and_unsanitized_failure() -> None:
    repository = _repository()
    running = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.STAGING,
        started_at=NOW,
    )
    repository._jobs[JOB_ID] = running

    with pytest.raises(MigrationJobTransitionError):
        repository.finish_migration_job(
            JOB_ID,
            MigrationJobStage.STAGING,
            MigrationJobStatus.SUCCEEDED,
            NOW + timedelta(seconds=1),
            None,
        )
    with pytest.raises(MigrationJobTransitionError):
        repository.finish_migration_job(
            JOB_ID,
            MigrationJobStage.STAGING,
            MigrationJobStatus.FAILED,
            NOW + timedelta(seconds=1),
            "raw database error: password=secret",
        )


class ReadCursor:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, parameters=None):
        self.calls.append((sql, parameters))

    def fetchone(self):
        return self.row


class ReadConnection:
    def __init__(self, cursor):
        self.value = cursor
        self.closed = False

    def cursor(self):
        return self.value

    def close(self):
        self.closed = True


def _job_row(job=None):
    job = job or _job()
    return (
        job.job_id,
        job.workflow_id,
        job.expected_workflow_version,
        job.source_discovery_artifact_version,
        job.approved_mapping_artifact_version,
        job.source_profile_id,
        job.target_profile_id,
        job.batch_size,
        job.timeout_seconds,
        job.job_fingerprint,
        job.status.value,
        job.stage.value,
        job.queued_at,
        job.actor_type.value,
        job.idempotency_key,
        job.actor_reference,
        job.started_at,
        job.completed_at,
        job.duration_ms,
        job.failure_category,
        job.batch_progress.batches_completed if job.batch_progress else 0,
        job.batch_progress.rows_read if job.batch_progress else 0,
        job.batch_progress.rows_written if job.batch_progress else 0,
        job.batch_progress.total_rows_estimate if job.batch_progress else None,
        job.progress_updated_at,
    )


def test_postgresql_get_job_is_parameterized_rehydrates_and_closes() -> None:
    cursor = ReadCursor(_job_row())
    connection = ReadConnection(cursor)
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"), connect=lambda _dsn: connection
    )

    result = repository.get_migration_job(JOB_ID)

    assert result == _job()
    assert cursor.calls[0][1] == (JOB_ID,)
    assert "job_id=%s" in cursor.calls[0][0]
    assert connection.closed is True


def test_postgresql_get_missing_job_uses_safe_error_and_closes() -> None:
    cursor = ReadCursor(None)
    connection = ReadConnection(cursor)
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"), connect=lambda _dsn: connection
    )

    with pytest.raises(MigrationJobNotFoundError, match="migration job is unavailable"):
        repository.get_migration_job(JOB_ID)

    assert connection.closed is True


def _workflow_row(workflow=None):
    value = workflow or _workflow()
    return (
        value.workflow_id,
        value.display_name,
        value.source_profile_id,
        value.target_profile_id,
        {
            "catalog_name": value.source_relation.catalog_name,
            "schema_name": value.source_relation.schema_name,
            "object_name": value.source_relation.object_name,
            "system": value.source_relation.system,
        },
        {
            "catalog_name": value.target_relation.catalog_name,
            "schema_name": value.target_relation.schema_name,
            "object_name": value.target_relation.object_name,
            "system": value.target_relation.system,
        },
        value.status.value,
        value.version,
        value.created_at,
        value.updated_at,
        value.latest_artifact_version,
        value.last_error_code,
        list(value.warnings),
    )


class Transaction:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class CreateCursor(ReadCursor):
    def __init__(self, *, replay=None, workflow=None, active=False):
        super().__init__(None)
        self.replay = replay
        self.workflow = workflow or _workflow_row()
        self.active = active
        self.current = None

    def execute(self, sql, parameters=None):
        super().execute(sql, parameters)
        if "FROM migration_idempotency" in sql:
            self.current = self.replay
        elif "FROM migration_workflows" in sql:
            self.current = self.workflow
        elif "SELECT 1 FROM migration_jobs" in sql:
            self.current = (1,) if self.active else None
        elif "FROM migration_jobs WHERE job_id" in sql:
            self.current = _job_row()
        else:
            self.current = None

    def fetchone(self):
        return self.current


class CreateConnection(ReadConnection):
    def transaction(self):
        return Transaction()


class ClaimCursor(ReadCursor):
    def __init__(self, row):
        super().__init__(row)
        self.current = row
        self.rowcount = 0

    def execute(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        if sql.startswith("SELECT"):
            self.current = self.row
        elif sql.startswith("UPDATE"):
            self.rowcount = 1
            self.current = None

    def fetchone(self):
        return self.current


def test_postgresql_create_job_inserts_job_and_idempotency_atomically() -> None:
    cursor = CreateCursor()
    connection = CreateConnection(cursor)
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"), connect=lambda _dsn: connection
    )

    result = repository.create_migration_job(_job())

    assert result == (_job(), True)
    statements = [sql for sql, _parameters in cursor.calls]
    assert any("pg_advisory_xact_lock" in sql for sql in statements)
    assert any("FROM migration_workflows" in sql and "FOR UPDATE" in sql for sql in statements)
    assert any("INSERT INTO migration_jobs" in sql for sql in statements)
    assert any("INSERT INTO migration_idempotency" in sql for sql in statements)
    assert connection.closed is True


def test_postgresql_create_job_exact_replay_does_not_insert_again() -> None:
    cursor = CreateCursor(replay=("a" * 64, WORKFLOW_ID, JOB_ID))
    connection = CreateConnection(cursor)
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"), connect=lambda _dsn: connection
    )

    result = repository.create_migration_job(_job())

    assert result == (_job(), False)
    assert not any("INSERT INTO migration_jobs" in sql for sql, _parameters in cursor.calls)


def test_postgresql_create_job_rejects_stale_unapproved_and_active_work() -> None:
    stale = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret"),
        connect=lambda _dsn: CreateConnection(CreateCursor()),
    )
    with pytest.raises(WorkflowConflictError):
        stale.create_migration_job(_job(expected_workflow_version=4))

    unapproved = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret"),
        connect=lambda _dsn: CreateConnection(
            CreateCursor(workflow=_workflow_row(_workflow(status=MigrationWorkflowStatus.MAPPING_PROPOSED)))
        ),
    )
    with pytest.raises(WorkflowOperationUnavailableError):
        unapproved.create_migration_job(_job())

    active = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret"),
        connect=lambda _dsn: CreateConnection(CreateCursor(active=True)),
    )
    with pytest.raises(MigrationJobAlreadyActiveError):
        active.create_migration_job(_job())


def test_postgresql_claim_uses_skip_locked_and_updates_atomically() -> None:
    cursor = ClaimCursor(_job_row())
    connection = CreateConnection(cursor)
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"), connect=lambda _dsn: connection
    )
    started_at = NOW + timedelta(seconds=1)

    claimed = repository.claim_next_migration_job(started_at)

    assert claimed.status is MigrationJobStatus.RUNNING
    assert claimed.stage is MigrationJobStage.PREPARING
    assert claimed.started_at == started_at
    select_sql, select_parameters = cursor.calls[0]
    assert "FOR UPDATE SKIP LOCKED" in select_sql
    assert "ORDER BY queued_at,job_id" in select_sql
    assert select_parameters is None
    update_sql, update_parameters = cursor.calls[1]
    assert "status='QUEUED'" in update_sql
    assert update_parameters == (started_at, JOB_ID)
    assert connection.closed is True


def test_postgresql_claim_returns_none_when_queue_is_empty() -> None:
    cursor = ClaimCursor(None)
    connection = CreateConnection(cursor)
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"), connect=lambda _dsn: connection
    )

    assert repository.claim_next_migration_job(NOW) is None
    assert len(cursor.calls) == 1
    assert connection.closed is True


def test_postgresql_stage_progress_locks_checks_and_updates() -> None:
    running = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.PREPARING,
        started_at=NOW,
    )
    cursor = ClaimCursor(_job_row(running))
    connection = CreateConnection(cursor)
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"), connect=lambda _dsn: connection
    )

    updated = repository.update_migration_job_stage(
        JOB_ID,
        MigrationJobStage.PREPARING,
        MigrationJobStage.STAGING,
    )

    assert updated.stage is MigrationJobStage.STAGING
    assert "WHERE job_id=%s FOR UPDATE" in cursor.calls[0][0]
    assert cursor.calls[0][1] == (JOB_ID,)
    assert cursor.calls[1][1] == ("STAGING", JOB_ID, "PREPARING")
    assert connection.closed is True


def test_postgresql_stage_progress_rejects_a_skipped_stage() -> None:
    running = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.PREPARING,
        started_at=NOW,
    )
    cursor = ClaimCursor(_job_row(running))
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"),
        connect=lambda _dsn: CreateConnection(cursor),
    )

    with pytest.raises(MigrationJobTransitionError):
        repository.update_migration_job_stage(
            JOB_ID,
            MigrationJobStage.PREPARING,
            MigrationJobStage.EXECUTING,
        )

    assert len(cursor.calls) == 1


def test_postgresql_batch_progress_locks_and_updates_all_metrics() -> None:
    running = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.STAGING,
        started_at=NOW,
    )
    cursor = ClaimCursor(_job_row(running))
    connection = CreateConnection(cursor)
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"), connect=lambda _dsn: connection
    )
    updated_at = NOW + timedelta(seconds=1)

    updated = repository.update_migration_job_progress(JOB_ID, _progress(), updated_at)

    assert updated.batch_progress == _progress()
    assert updated.progress_updated_at == updated_at
    assert "FOR UPDATE" in cursor.calls[0][0]
    assert cursor.calls[1][1] == (1, 500, 500, 1_200, updated_at, JOB_ID)
    assert connection.closed is True


def test_postgresql_batch_progress_rejects_a_duplicate_snapshot_without_update() -> None:
    running = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.STAGING,
        started_at=NOW,
        batch_progress=_progress(),
        progress_updated_at=NOW + timedelta(seconds=1),
    )
    cursor = ClaimCursor(_job_row(running))
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"),
        connect=lambda _dsn: CreateConnection(cursor),
    )

    with pytest.raises(MigrationJobTransitionError):
        repository.update_migration_job_progress(
            JOB_ID, _progress(), NOW + timedelta(seconds=2)
        )

    assert len(cursor.calls) == 1


def test_postgresql_successful_finish_locks_and_writes_terminal_fields() -> None:
    running = replace(
        _job(),
        status=MigrationJobStatus.RUNNING,
        stage=MigrationJobStage.VALIDATING,
        started_at=NOW,
    )
    cursor = ClaimCursor(_job_row(running))
    connection = CreateConnection(cursor)
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"), connect=lambda _dsn: connection
    )
    completed_at = NOW + timedelta(seconds=3)

    finished = repository.finish_migration_job(
        JOB_ID,
        MigrationJobStage.VALIDATING,
        MigrationJobStatus.SUCCEEDED,
        completed_at,
        None,
    )

    assert finished.status is MigrationJobStatus.SUCCEEDED
    assert finished.stage is MigrationJobStage.COMPLETED
    assert finished.duration_ms == 3_000
    assert "FOR UPDATE" in cursor.calls[0][0]
    assert cursor.calls[1][1] == (
        "SUCCEEDED",
        "COMPLETED",
        completed_at,
        3_000,
        None,
        JOB_ID,
        "VALIDATING",
    )
    assert connection.closed is True


def test_postgresql_exact_terminal_replay_does_not_update_again() -> None:
    succeeded = replace(
        _job(),
        status=MigrationJobStatus.SUCCEEDED,
        stage=MigrationJobStage.COMPLETED,
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=3),
        duration_ms=3_000,
    )
    cursor = ClaimCursor(_job_row(succeeded))
    repository = PostgreSQLWorkflowRepository(
        ControlPlaneConfig(dsn="secret-dsn"),
        connect=lambda _dsn: CreateConnection(cursor),
    )

    replay = repository.finish_migration_job(
        JOB_ID,
        MigrationJobStage.VALIDATING,
        MigrationJobStatus.SUCCEEDED,
        NOW + timedelta(seconds=10),
        None,
    )

    assert replay == succeeded
    assert len(cursor.calls) == 1
