BEGIN;

ALTER TABLE
    candidate
DROP
    COLUMN _hpx_64;

ALTER TABLE
    test_candidate
ADD
    COLUMN _hpx bigint;

CREATE INDEX candidate_jd_hpx on candidate using brin (jd, _hpx) WITH (pages_per_range=1);

COMMIT;