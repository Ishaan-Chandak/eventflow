ALTER TABLE delivery_jobs ADD COLUMN cycle_attempt_count INT NOT NULL DEFAULT 0 CHECK (cycle_attempt_count >= 0);
CREATE INDEX delivery_attempts_job_started_idx ON delivery_attempts(delivery_job_id, started_at);
