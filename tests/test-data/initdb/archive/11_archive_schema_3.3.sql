BEGIN;

/* 2019-06-19 UT: add deep-learning RB score to candidate */
ALTER TABLE candidate ADD drb FLOAT;
ALTER TABLE candidate ADD drbversion TEXT;

INSERT INTO versions (alert_version) VALUES (3.3);

COMMIT;
