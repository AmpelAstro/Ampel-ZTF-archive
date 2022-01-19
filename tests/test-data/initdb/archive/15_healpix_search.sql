
BEGIN;

CREATE EXTENSION pg_healpix;

CREATE INDEX
    candidate_jd_healpix_64
ON
    candidate (jd, healpix_ang2ipix_nest(64, ra, dec))
;

ALTER TABLE
    candidate
ADD COLUMN
    _hpx_64 integer
GENERATED ALWAYS AS (healpix_ang2ipix_nest(64, ra, dec)) STORED
;

CREATE INDEX
    candidate_jd_hpx_64
ON
    candidate (jd, _hpx_64)
;

COMMIT;