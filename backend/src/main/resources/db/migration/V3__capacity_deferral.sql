ALTER TABLE delivery_jobs ADD COLUMN capacity_deferred_until TIMESTAMPTZ;
CREATE INDEX delivery_jobs_capacity_deferred_idx ON delivery_jobs(capacity_deferred_until) WHERE status = 'QUEUED';
