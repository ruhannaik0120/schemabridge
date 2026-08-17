ALTER TABLE migration_idempotency
    DROP CONSTRAINT IF EXISTS migration_idempotency_command_type_check;
ALTER TABLE migration_idempotency
    ADD CONSTRAINT migration_idempotency_command_type_check CHECK (
        command_type IN (
            'CREATE_WORKFLOW','TRANSITION_STATUS','APPEND_ARTIFACT',
            'LOAD_STAGING','EXECUTE_WORKFLOW','VALIDATE_WORKFLOW',
            'CREATE_MIGRATION_JOB'
        )
    );

CREATE TABLE IF NOT EXISTS migration_jobs (
    job_id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES migration_workflows(workflow_id),
    expected_workflow_version BIGINT NOT NULL CHECK (expected_workflow_version > 0),
    source_discovery_artifact_version BIGINT NOT NULL
        CHECK (source_discovery_artifact_version > 0),
    approved_mapping_artifact_version BIGINT NOT NULL
        CHECK (approved_mapping_artifact_version > 0),
    source_profile_id VARCHAR(256) NOT NULL,
    target_profile_id VARCHAR(256) NOT NULL,
    batch_size INTEGER NOT NULL CHECK (batch_size > 0 AND batch_size <= 10000),
    timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds > 0),
    job_fingerprint CHAR(64) NOT NULL
        CHECK (job_fingerprint ~ '^[0-9a-f]{64}$'),
    status VARCHAR(32) NOT NULL CHECK (status IN (
        'QUEUED','RUNNING','SUCCEEDED','FAILED','RECOVERY_REQUIRED'
    )),
    stage VARCHAR(32) NOT NULL CHECK (stage IN (
        'QUEUED','PREPARING','STAGING','TRANSFORMING',
        'EXECUTING','CLEANING_UP','VALIDATING','COMPLETED'
    )),
    queued_at TIMESTAMPTZ NOT NULL,
    actor_type VARCHAR(16) NOT NULL CHECK (actor_type IN ('SYSTEM','USER','SERVICE')),
    idempotency_key VARCHAR(128) NOT NULL,
    actor_reference VARCHAR(256),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    duration_ms BIGINT CHECK (duration_ms IS NULL OR duration_ms >= 0),
    failure_category VARCHAR(64)
        CHECK (failure_category IS NULL OR failure_category ~ '^[A-Z][A-Z0-9_]{0,63}$'),
    UNIQUE(workflow_id, idempotency_key),
    CHECK (started_at IS NULL OR started_at >= queued_at),
    CHECK (completed_at IS NULL OR (started_at IS NOT NULL AND completed_at >= started_at)),
    CHECK (
        (
            status = 'QUEUED'
            AND stage = 'QUEUED'
            AND started_at IS NULL
            AND completed_at IS NULL
            AND duration_ms IS NULL
            AND failure_category IS NULL
        )
        OR (
            status = 'RUNNING'
            AND stage IN (
                'PREPARING','STAGING','TRANSFORMING','EXECUTING','CLEANING_UP','VALIDATING'
            )
            AND started_at IS NOT NULL
            AND completed_at IS NULL
            AND duration_ms IS NULL
            AND failure_category IS NULL
        )
        OR (
            status = 'SUCCEEDED'
            AND stage = 'COMPLETED'
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND duration_ms IS NOT NULL
            AND failure_category IS NULL
        )
        OR (
            status IN ('FAILED','RECOVERY_REQUIRED')
            AND stage IN (
                'PREPARING','STAGING','TRANSFORMING','EXECUTING','CLEANING_UP','VALIDATING'
            )
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND duration_ms IS NOT NULL
            AND failure_category IS NOT NULL
        )
    )
);

COMMENT ON TABLE migration_jobs IS
    'Durable background migration jobs bound to approved workflow inputs.';

CREATE INDEX IF NOT EXISTS ix_migration_jobs_workflow
    ON migration_jobs(workflow_id, queued_at);

CREATE INDEX IF NOT EXISTS ix_migration_jobs_status_queue
    ON migration_jobs(status, queued_at);

CREATE UNIQUE INDEX IF NOT EXISTS ux_migration_jobs_active_fingerprint
    ON migration_jobs(workflow_id, job_fingerprint)
    WHERE status IN ('QUEUED','RUNNING','SUCCEEDED','RECOVERY_REQUIRED');
