
CREATE EXTENSION cube;
CREATE EXTENSION earthdistance;

/* redefine earth radius such that great circle distances are in degrees */
CREATE OR REPLACE FUNCTION earth() RETURNS float8
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
AS 'SELECT 180/pi()';

/* schema-qualify definition of ll_to_earth() so that it can used for autoanalyze */
CREATE OR REPLACE FUNCTION ll_to_earth(float8, float8)
RETURNS earth
LANGUAGE SQL
IMMUTABLE STRICT
PARALLEL SAFE
AS 'SELECT public.cube(public.cube(public.cube(public.earth()*cos(radians($1))*cos(radians($2))),public.earth()*cos(radians($1))*sin(radians($2))),public.earth()*sin(radians($1)))::public.earth';

CREATE INDEX cone_search on candidate USING gist (ll_to_earth(dec, ra));

