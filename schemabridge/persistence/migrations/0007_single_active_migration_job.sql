CREATE UNIQUE INDEX IF NOT EXISTS ux_migration_jobs_one_active_workflow
    ON migration_jobs(workflow_id)
    WHERE status IN ('QUEUED','RUNNING','SUCCEEDED','RECOVERY_REQUIRED');
