
ALTER TABLE read_queue_groups
ADD COLUMN chunk_size INTEGER,
ADD COLUMN error BOOLEAN
;