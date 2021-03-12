BEGIN;

ALTER TABLE read_queue_groups ADD last_accessed TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL;

-- SELECT cron.schedule('30 6 * * *',$$
-- DELETE FROM read_queue_groups WHERE last_accessed < now() - interval '1 day';
-- $$);

COMMIT;