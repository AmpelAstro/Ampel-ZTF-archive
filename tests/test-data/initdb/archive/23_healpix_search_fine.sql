BEGIN;

ALTER TABLE
    candidate
DROP
    COLUMN _hpx_64;

ALTER TABLE
    candidate
ADD
    COLUMN _hpx bigint;

DROP INDEX candidate_jd_healpix_64;
DROP INDEX cone_search;
CREATE INDEX CONCURRENTLY candidate_jd_hpx on candidate using brin (jd, _hpx) WITH (pages_per_range=1);

COMMIT;