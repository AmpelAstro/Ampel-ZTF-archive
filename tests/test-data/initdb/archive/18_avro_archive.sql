BEGIN;

CREATE TABLE avro_archive (
	avro_archive_id SERIAL NOT NULL, 
	uri TEXT NOT NULL,
	PRIMARY KEY (avro_archive_id)
);

ALTER TABLE alert ADD avro_archive_id INTEGER;
ALTER TABLE alert ADD avro_archive_start INTEGER;
ALTER TABLE alert ADD avro_archive_end INTEGER;

COMMIT;