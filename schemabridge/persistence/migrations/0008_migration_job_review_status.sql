ALTER TABLE migration_jobs
    DROP CONSTRAINT migration_jobs_status_check;
ALTER TABLE migration_jobs
    ADD CONSTRAINT migration_jobs_status_check CHECK (status IN (
        'QUEUED','RUNNING','SUCCEEDED','FAILED','REVIEW_REQUIRED','RECOVERY_REQUIRED'
    ));

ALTER TABLE migration_jobs
    DROP CONSTRAINT migration_jobs_check2;
ALTER TABLE migration_jobs
    ADD CONSTRAINT migration_jobs_check2 CHECK (
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
            status IN ('FAILED','REVIEW_REQUIRED','RECOVERY_REQUIRED')
            AND stage IN (
                'PREPARING','STAGING','TRANSFORMING','EXECUTING','CLEANING_UP','VALIDATING'
            )
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND duration_ms IS NOT NULL
            AND failure_category IS NOT NULL
        )
    );

DROP INDEX ux_migration_jobs_active_fingerprint;
CREATE UNIQUE INDEX ux_migration_jobs_active_fingerprint
    ON migration_jobs(workflow_id, job_fingerprint)
    WHERE status IN (
        'QUEUED','RUNNING','SUCCEEDED','REVIEW_REQUIRED','RECOVERY_REQUIRED'
    );

DROP INDEX ux_migration_jobs_one_active_workflow;
CREATE UNIQUE INDEX ux_migration_jobs_one_active_workflow
    ON migration_jobs(workflow_id)
    WHERE status IN (
        'QUEUED','RUNNING','SUCCEEDED','REVIEW_REQUIRED','RECOVERY_REQUIRED'
    );
