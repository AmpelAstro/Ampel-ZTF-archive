BEGIN;

CREATE TABLE topic_groups (
  topic_id SERIAL NOT NULL, 
  topic_name TEXT NOT NULL UNIQUE,
  topic_description TEXT,
  PRIMARY KEY (topic_id), 
  UNIQUE (topic_name)
);

CREATE TABLE topic (
  item_id SERIAL NOT NULL,
  topic_id INTEGER NOT NULL,
  alert_ids INTEGER[] NOT NULL,
  PRIMARY KEY (item_id), 
  FOREIGN KEY(topic_id) REFERENCES topic_groups (topic_id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- GRANT SELECT, INSERT, UPDATE, DELETE on topic_groups TO GROUP readers;
-- GRANT SELECT, INSERT, UPDATE, DELETE on topic TO GROUP readers;

COMMIT;