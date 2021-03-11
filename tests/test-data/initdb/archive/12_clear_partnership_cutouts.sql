
/*
Drop cutout images from the ZTF private survey that are older than 3 months
*/

BEGIN;

-- SELECT cron.schedule('30 23 * * *',$$
-- DELETE FROM cutout WHERE alert_id in (
--   SELECT
--     cutout.alert_id
--   FROM
--     alert INNER JOIN cutout ON alert.alert_id=cutout.alert_id
--   WHERE
--     jd < (extract(epoch from now() - interval '3 months')/86400.0 + 2440587.5)
--     AND programid=2
--   );
-- $$);

COMMIT;

