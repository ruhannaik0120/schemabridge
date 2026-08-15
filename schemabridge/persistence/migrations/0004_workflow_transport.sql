ALTER TABLE migration_workflows
    DROP CONSTRAINT IF EXISTS migration_workflows_status_check;
ALTER TABLE migration_workflows
    ADD CONSTRAINT migration_workflows_status_check CHECK (status IN (
        'DRAFT','DISCOVERED','MAPPING_PROPOSED','MAPPING_APPROVED',
        'STAGING','STAGED','STAGING_RECOVERY_REQUIRED',
        'EXECUTION_READY','EXECUTING','EXECUTED','EXECUTION_RECOVERY_REQUIRED',
        'VALIDATION_READY','VALIDATING','VALIDATED','VALIDATION_REVIEW_REQUIRED',
        'VALIDATION_RECOVERY_REQUIRED','FAILED','CANCELLED'
    ));

ALTER TABLE migration_workflow_artifacts
    DROP CONSTRAINT IF EXISTS migration_workflow_artifacts_artifact_type_check;
ALTER TABLE migration_workflow_artifacts
    ADD CONSTRAINT migration_workflow_artifacts_artifact_type_check CHECK (
        artifact_type IN (
            'SOURCE_DISCOVERY','TARGET_DISCOVERY','MAPPING_PLAN',
            'APPROVED_MAPPING_PLAN','STAGING_LOAD_EVIDENCE',
            'TRANSFORMATION_PREVIEW','EXECUTION_EVIDENCE',
            'VALIDATION_PREVIEW','VALIDATION_EXECUTION_REPORT'
        )
    );

ALTER TABLE migration_audit_events
    DROP CONSTRAINT IF EXISTS migration_audit_events_previous_status_check;
ALTER TABLE migration_audit_events
    ADD CONSTRAINT migration_audit_events_previous_status_check CHECK (
        previous_status IS NULL OR previous_status IN (
            'DRAFT','DISCOVERED','MAPPING_PROPOSED','MAPPING_APPROVED',
            'STAGING','STAGED','STAGING_RECOVERY_REQUIRED',
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
            'STAGING','STAGED','STAGING_RECOVERY_REQUIRED',
            'EXECUTION_READY','EXECUTING','EXECUTED','EXECUTION_RECOVERY_REQUIRED',
            'VALIDATION_READY','VALIDATING','VALIDATED','VALIDATION_REVIEW_REQUIRED',
            'VALIDATION_RECOVERY_REQUIRED','FAILED','CANCELLED'
        )
    );
ALTER TABLE migration_audit_events
    DROP CONSTRAINT IF EXISTS migration_audit_events_artifact_type_check;
ALTER TABLE migration_audit_events
    ADD CONSTRAINT migration_audit_events_artifact_type_check CHECK (
        artifact_type IS NULL OR artifact_type IN (
            'SOURCE_DISCOVERY','TARGET_DISCOVERY','MAPPING_PLAN',
            'APPROVED_MAPPING_PLAN','STAGING_LOAD_EVIDENCE',
            'TRANSFORMATION_PREVIEW','EXECUTION_EVIDENCE',
            'VALIDATION_PREVIEW','VALIDATION_EXECUTION_REPORT'
        )
    );

ALTER TABLE migration_idempotency
    DROP CONSTRAINT IF EXISTS migration_idempotency_command_type_check;
ALTER TABLE migration_idempotency
    ADD CONSTRAINT migration_idempotency_command_type_check CHECK (
        command_type IN (
            'CREATE_WORKFLOW','TRANSITION_STATUS','APPEND_ARTIFACT',
            'LOAD_STAGING','EXECUTE_WORKFLOW','VALIDATE_WORKFLOW'
        )
    );

CREATE TABLE IF NOT EXISTS migration_transport_attempts (
    attempt_id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES migration_workflows(workflow_id),
    source_discovery_artifact_version BIGINT NOT NULL CHECK (source_discovery_artifact_version > 0),
    approved_mapping_artifact_version BIGINT NOT NULL CHECK (approved_mapping_artifact_version > 0),
    source_profile_id VARCHAR(256) NOT NULL,
    target_profile_id VARCHAR(256) NOT NULL,
    staging_relation JSONB NOT NULL,
    batch_size INTEGER NOT NULL CHECK (batch_size > 0 AND batch_size <= 10000),
    timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds > 0),
    transport_fingerprint CHAR(64) NOT NULL CHECK (transport_fingerprint ~ '^[0-9a-f]{64}$'),
    status VARCHAR(32) NOT NULL CHECK (status IN (
        'CLAIMED','RUNNING','SUCCEEDED','FAILED_CLEANED_UP','OUTCOME_UNCERTAIN'
    )),
    claimed_at TIMESTAMPTZ NOT NULL,
    actor_type VARCHAR(16) NOT NULL CHECK (actor_type IN ('SYSTEM','USER','SERVICE')),
    idempotency_key VARCHAR(128) NOT NULL,
    actor_reference VARCHAR(256),
    running_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    evidence_artifact_id UUID,
    failure_category VARCHAR(64),
    UNIQUE(workflow_id, attempt_id),
    FOREIGN KEY(workflow_id, evidence_artifact_id)
        REFERENCES migration_workflow_artifacts(workflow_id, artifact_id),
    CHECK (running_at IS NULL OR running_at >= claimed_at),
    CHECK (completed_at IS NULL OR (running_at IS NOT NULL AND completed_at >= running_at))
);
COMMENT ON TABLE migration_transport_attempts IS
    'Durable source-to-staging claims and row-free terminal outcomes.';
CREATE INDEX IF NOT EXISTS ix_migration_transport_attempts_workflow
    ON migration_transport_attempts(workflow_id, claimed_at);
CREATE UNIQUE INDEX IF NOT EXISTS ux_migration_transport_active_fingerprint
    ON migration_transport_attempts(workflow_id, transport_fingerprint)
    WHERE status IN ('CLAIMED','RUNNING','SUCCEEDED','OUTCOME_UNCERTAIN');
