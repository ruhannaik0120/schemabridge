ALTER TABLE migration_jobs
    ADD COLUMN batches_completed BIGINT NOT NULL DEFAULT 0
        CHECK (batches_completed >= 0),
    ADD COLUMN rows_read BIGINT NOT NULL DEFAULT 0
        CHECK (rows_read >= 0),
    ADD COLUMN rows_written BIGINT NOT NULL DEFAULT 0
        CHECK (rows_written >= 0),
    ADD COLUMN total_rows_estimate BIGINT
        CHECK (total_rows_estimate IS NULL OR total_rows_estimate >= 0),
    ADD COLUMN progress_updated_at TIMESTAMPTZ;

ALTER TABLE migration_jobs
    ADD CONSTRAINT migration_jobs_batch_progress_check CHECK (
        (
            progress_updated_at IS NULL
            AND batches_completed = 0
            AND rows_read = 0
            AND rows_written = 0
            AND total_rows_estimate IS NULL
        )
        OR (
            progress_updated_at IS NOT NULL
            AND started_at IS NOT NULL
            AND progress_updated_at >= started_at
            AND (completed_at IS NULL OR progress_updated_at <= completed_at)
            AND rows_read = rows_written
            AND (
                (batches_completed = 0 AND rows_read = 0)
                OR (batches_completed > 0 AND rows_read > 0)
            )
            AND stage IN (
                'STAGING','TRANSFORMING','EXECUTING',
                'CLEANING_UP','VALIDATING','COMPLETED'
            )
        )
    );

COMMENT ON COLUMN migration_jobs.progress_updated_at IS
    'Time of the latest cumulative, connector-neutral completed-batch snapshot.';
