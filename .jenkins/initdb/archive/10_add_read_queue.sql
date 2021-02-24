
BEGIN;

CREATE TABLE read_queue_groups (
  group_id SERIAL NOT NULL, 
  group_name TEXT NOT NULL UNIQUE,
  PRIMARY KEY (group_id), 
  UNIQUE (group_name)
);

CREATE TABLE read_queue (
  item_id SERIAL NOT NULL,
  group_id INTEGER NOT NULL,
  alert_ids INTEGER[] NOT NULL,
  PRIMARY KEY (item_id), 
  FOREIGN KEY(group_id) REFERENCES read_queue_groups (group_id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- GRANT SELECT, INSERT, UPDATE, DELETE on read_queue_groups TO GROUP readers;
-- GRANT SELECT, INSERT, UPDATE, DELETE on read_queue TO GROUP readers;

DROP INDEX alert_playback;
CREATE INDEX alert_playback ON alert (jd);

COMMIT;