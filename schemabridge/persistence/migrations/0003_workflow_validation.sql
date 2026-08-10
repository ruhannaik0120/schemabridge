ALTER TABLE migration_workflows
    DROP CONSTRAINT IF EXISTS migration_workflows_status_check;
ALTER TABLE migration_workflows
    ADD CONSTRAINT migration_workflows_status_check CHECK (status IN (
        'DRAFT','DISCOVERED','MAPPING_PROPOSED','MAPPING_APPROVED',
        'EXECUTION_READY','EXECUTING','EXECUTED','EXECUTION_RECOVERY_REQUIRED',
        'VALIDATION_READY','VALIDATING','VALIDATED','VALIDATION_REVIEW_REQUIRED',
        'VALIDATION_RECOVERY_REQUIRED','FAILED','CANCELLED'
    ));

ALTER TABLE migration_audit_events
    DROP CONSTRAINT IF EXISTS migration_audit_events_previous_status_check;
ALTER TABLE migration_audit_events
    ADD CONSTRAINT migration_audit_events_previous_status_check CHECK (
        previous_status IS NULL OR previous_status IN (
            'DRAFT','DISCOVERED','MAPPING_PROPOSED','MAPPING_APPROVED',
            'EXECUTION_READY','EXECUTING','EXECUTED','EXECUTION_RECOVERY_REQUIRED',
            'VALIDATION_READY','VALIDATING','VALIDATED','VALIDATION_REVIEW_REQUIRED',
            'VALIDATION_RECOVERY_REQUIRED','FAILED','CANCELLED'
        )
    );
ALTER TABLE migration_audit_events
    DROP CONSTRAINT IF EXISTS migration_audit_events_new_status_check;
ALTER TABLE migration_audit_events
    ADD CONSTRAINT migration_audit_events_new_status_check CHECK (
        new_status IS NULL OR new_status IN (
            'DRAFT','DISCOVERED','MAPPING_PROPOSED','MAPPING_APPROVED',
            'EXECUTION_READY','EXECUTING','EXECUTED','EXECUTION_RECOVERY_REQUIRED',
            'VALIDATION_READY','VALIDATING','VALIDATED','VALIDATION_REVIEW_REQUIRED',
            'VALIDATION_RECOVERY_REQUIRED','FAILED','CANCELLED'
        )
    );

ALTER TABLE migration_idempotency
    DROP CONSTRAINT IF EXISTS migration_idempotency_command_type_check;
ALTER TABLE migration_idempotency
    ADD CONSTRAINT migration_idempotency_command_type_check CHECK (
        command_type IN (
            'CREATE_WORKFLOW','TRANSITION_STATUS','APPEND_ARTIFACT',
            'EXECUTE_WORKFLOW','VALIDATE_WORKFLOW'
        )
    );

CREATE TABLE IF NOT EXISTS migration_validation_runs (
    run_id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES migration_workflows(workflow_id),
    execution_attempt_id UUID NOT NULL REFERENCES migration_execution_attempts(attempt_id),
    execution_evidence_artifact_version BIGINT NOT NULL CHECK (execution_evidence_artifact_version > 0),
    approved_mapping_artifact_version BIGINT NOT NULL CHECK (approved_mapping_artifact_version > 0),
    validation_preview_artifact_version BIGINT NOT NULL CHECK (validation_preview_artifact_version > 0),
    source_profile_id VARCHAR(256) NOT NULL,
    target_profile_id VARCHAR(256) NOT NULL,
    validation_fingerprint CHAR(64) NOT NULL CHECK (validation_fingerprint ~ '^[0-9a-f]{64}$'),
    status VARCHAR(32) NOT NULL CHECK (status IN ('CLAIMED','RUNNING','SUCCEEDED','REVIEW_REQUIRED','OUTCOME_UNCERTAIN')),
    timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds > 0),
    claimed_at TIMESTAMPTZ NOT NULL,
    actor_type VARCHAR(16) NOT NULL CHECK (actor_type IN ('SYSTEM','USER','SERVICE')),
    idempotency_key VARCHAR(128) NOT NULL,
    actor_reference VARCHAR(256),
    running_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    duration_ms BIGINT CHECK (duration_ms >= 0),
    evidence_artifact_id UUID,
    failure_category VARCHAR(64),
    UNIQUE(workflow_id, run_id),
    FOREIGN KEY(workflow_id, evidence_artifact_id)
        REFERENCES migration_workflow_artifacts(workflow_id, artifact_id),
    CHECK (running_at IS NULL OR running_at >= claimed_at),
    CHECK (completed_at IS NULL OR (running_at IS NOT NULL AND completed_at >= running_at))
);
COMMENT ON TABLE migration_validation_runs IS
    'Durable read-only validation claims and sanitized terminal outcomes.';
CREATE INDEX IF NOT EXISTS ix_migration_validation_runs_workflow
    ON migration_validation_runs(workflow_id, claimed_at);
CREATE UNIQUE INDEX IF NOT EXISTS ux_migration_validation_terminal
    ON migration_validation_runs(workflow_id)
    WHERE status IN ('CLAIMED','RUNNING','SUCCEEDED','REVIEW_REQUIRED','OUTCOME_UNCERTAIN');
