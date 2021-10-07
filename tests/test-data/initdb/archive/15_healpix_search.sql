
BEGIN;

CREATE EXTENSION pg_healpix;

CREATE INDEX
    candidate_jd_healpix_64
ON
    candidate (jd, healpix_ang2ipix_nest(64, ra, dec))
;

COMMIT;