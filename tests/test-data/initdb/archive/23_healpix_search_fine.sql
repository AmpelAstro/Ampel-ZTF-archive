BEGIN;

ALTER TABLE
    candidate
DROP
    COLUMN _hpx_64;

ALTER TABLE
    candidate
ADD
    COLUMN _hpx bigint;

DROP INDEX IF EXISTS candidate_jd_healpix_64;
DROP INDEX IF EXISTS candidate_jd_hpx_64;
DROP INDEX IF EXISTS cone_search;

CREATE INDEX ix_candidate_jd_hpx on candidate (jd, _hpx) include (alert_id);
CREATE INDEX ix_candidate_hpx on candidate (_hpx) include (alert_id);

COMMIT;