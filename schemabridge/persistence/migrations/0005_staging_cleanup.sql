ALTER TABLE migration_workflow_artifacts
    DROP CONSTRAINT IF EXISTS migration_workflow_artifacts_artifact_type_check;
ALTER TABLE migration_workflow_artifacts
    ADD CONSTRAINT migration_workflow_artifacts_artifact_type_check CHECK (
        artifact_type IN (
            'SOURCE_DISCOVERY','TARGET_DISCOVERY','MAPPING_PLAN',
            'APPROVED_MAPPING_PLAN','STAGING_LOAD_EVIDENCE',
            'STAGING_CLEANUP_EVIDENCE','TRANSFORMATION_PREVIEW',
            'EXECUTION_EVIDENCE','VALIDATION_PREVIEW',
            'VALIDATION_EXECUTION_REPORT'
        )
    );

ALTER TABLE migration_audit_events
    DROP CONSTRAINT IF EXISTS migration_audit_events_artifact_type_check;
ALTER TABLE migration_audit_events
    ADD CONSTRAINT migration_audit_events_artifact_type_check CHECK (
        artifact_type IS NULL OR artifact_type IN (
            'SOURCE_DISCOVERY','TARGET_DISCOVERY','MAPPING_PLAN',
            'APPROVED_MAPPING_PLAN','STAGING_LOAD_EVIDENCE',
            'STAGING_CLEANUP_EVIDENCE','TRANSFORMATION_PREVIEW',
            'EXECUTION_EVIDENCE','VALIDATION_PREVIEW',
            'VALIDATION_EXECUTION_REPORT'
        )
    );
