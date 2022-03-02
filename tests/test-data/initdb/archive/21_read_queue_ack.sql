ALTER TABLE
    read_queue
ADD
    issued TIMESTAMP WITH TIME ZONE;

SELECT cron.schedule('*/5 * * * *',$$
UPDATE read_queue SET issued = NULL WHERE now() - issued > interval '10 minutes';
$$);