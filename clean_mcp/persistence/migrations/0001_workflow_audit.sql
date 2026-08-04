CREATE TABLE IF NOT EXISTS migration_workflows (
    workflow_id UUID PRIMARY KEY,
    display_name VARCHAR(200) NOT NULL CHECK (length(btrim(display_name)) > 0),
    source_profile_id VARCHAR(256) NOT NULL,
    target_profile_id VARCHAR(256) NOT NULL,
    source_relation JSONB NOT NULL CHECK (jsonb_typeof(source_relation) = 'object'),
    target_relation JSONB NOT NULL CHECK (jsonb_typeof(target_relation) = 'object'),
    status VARCHAR(32) NOT NULL CHECK (status IN ('DRAFT','DISCOVERED','MAPPING_PROPOSED','MAPPING_APPROVED','VALIDATION_READY','VALIDATING','VALIDATED','FAILED','CANCELLED')),
    version BIGINT NOT NULL CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    latest_artifact_version BIGINT NOT NULL DEFAULT 0 CHECK (latest_artifact_version >= 0),
    last_error_code VARCHAR(64), warnings JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(warnings) = 'array'),
    CHECK (updated_at >= created_at)
);
CREATE INDEX IF NOT EXISTS ix_migration_workflows_status ON migration_workflows(status);
CREATE INDEX IF NOT EXISTS ix_migration_workflows_updated_at ON migration_workflows(updated_at);

CREATE TABLE IF NOT EXISTS migration_workflow_artifacts (
    artifact_id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES migration_workflows(workflow_id),
    artifact_type VARCHAR(40) NOT NULL CHECK (artifact_type IN ('SOURCE_DISCOVERY','TARGET_DISCOVERY','MAPPING_PLAN','APPROVED_MAPPING_PLAN','TRANSFORMATION_PREVIEW','VALIDATION_PREVIEW','VALIDATION_EXECUTION_REPORT')),
    artifact_version BIGINT NOT NULL CHECK (artifact_version > 0),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) IN ('object','array')),
    payload_sha256 CHAR(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'), created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(workflow_id, artifact_version),
    UNIQUE(workflow_id, artifact_id)
);
COMMENT ON TABLE migration_workflow_artifacts IS 'Append-only immutable versioned workflow artifacts.';
CREATE INDEX IF NOT EXISTS ix_migration_artifacts_workflow_version ON migration_workflow_artifacts(workflow_id, artifact_version);

CREATE TABLE IF NOT EXISTS migration_audit_events (
    workflow_id UUID NOT NULL REFERENCES migration_workflows(workflow_id), sequence_number BIGINT NOT NULL CHECK (sequence_number > 0),
    event_id UUID NOT NULL UNIQUE, event_type VARCHAR(32) NOT NULL CHECK (event_type IN ('WORKFLOW_CREATED','STATUS_CHANGED','ARTIFACT_APPENDED','WORKFLOW_FAILED','WORKFLOW_CANCELLED')),
    previous_status VARCHAR(32) CHECK (previous_status IS NULL OR previous_status IN ('DRAFT','DISCOVERED','MAPPING_PROPOSED','MAPPING_APPROVED','VALIDATION_READY','VALIDATING','VALIDATED','FAILED','CANCELLED')),
    new_status VARCHAR(32) CHECK (new_status IS NULL OR new_status IN ('DRAFT','DISCOVERED','MAPPING_PROPOSED','MAPPING_APPROVED','VALIDATION_READY','VALIDATING','VALIDATED','FAILED','CANCELLED')),
    workflow_version BIGINT NOT NULL CHECK (workflow_version > 0),
    artifact_id UUID,
    artifact_type VARCHAR(40) CHECK (artifact_type IS NULL OR artifact_type IN ('SOURCE_DISCOVERY','TARGET_DISCOVERY','MAPPING_PLAN','APPROVED_MAPPING_PLAN','TRANSFORMATION_PREVIEW','VALIDATION_PREVIEW','VALIDATION_EXECUTION_REPORT')),
    actor_type VARCHAR(16) NOT NULL CHECK (actor_type IN ('SYSTEM','USER','SERVICE')),
    actor_reference VARCHAR(256), request_id VARCHAR(64), idempotency_key VARCHAR(128) NOT NULL, occurred_at TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    PRIMARY KEY(workflow_id, sequence_number),
    FOREIGN KEY(workflow_id, artifact_id) REFERENCES migration_workflow_artifacts(workflow_id, artifact_id)
);
COMMENT ON TABLE migration_audit_events IS 'Append-only audit history; application repository exposes no update or delete API.';
CREATE INDEX IF NOT EXISTS ix_migration_audit_workflow_sequence ON migration_audit_events(workflow_id, sequence_number);

CREATE TABLE IF NOT EXISTS migration_idempotency (
    command_scope VARCHAR(128) NOT NULL, idempotency_key VARCHAR(128) NOT NULL,
    command_type VARCHAR(40) NOT NULL CHECK (command_type IN ('CREATE_WORKFLOW','TRANSITION_STATUS','APPEND_ARTIFACT')),
    request_sha256 CHAR(64) NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'), workflow_id UUID REFERENCES migration_workflows(workflow_id),
    result_reference UUID, created_at TIMESTAMPTZ NOT NULL, PRIMARY KEY(command_scope, idempotency_key)
);
CREATE INDEX IF NOT EXISTS ix_migration_idempotency_workflow ON migration_idempotency(workflow_id);
