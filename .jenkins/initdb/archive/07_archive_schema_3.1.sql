BEGIN;

/* 2018-09-20 UT: add fields to candidate */
ALTER TABLE candidate ADD exptime FLOAT;

INSERT INTO versions (alert_version) VALUES (3.1);

COMMIT;
