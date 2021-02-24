
/*
Renumber a sparsely-populated primary key so that all the slots are
used, and reset the primary key sequence to the first available slot.

With 3.3M rows in upper_limit and 33M rows (2.1B array elements) in
alert_upper_limit_pivot, this update takes approximately 45 minutes. With 89k
new rows in upper_limit and 1.3M new rows (184M array elements), it takes 4.5.

*/

BEGIN;

CREATE OR REPLACE FUNCTION compactify_pivot_table(name text)
RETURNS void
AS $$
DECLARE rows integer;
DECLARE min_prv_id integer;
DECLARE min_alert_id integer;
BEGIN

EXECUTE 'LOCK TABLE '||name||';';
EXECUTE 'LOCK TABLE alert_'||name||'_pivot;';

EXECUTE 'CREATE TABLE IF NOT EXISTS '||name||'_compacitification_history (
  '||name||'_compacitification_history_id SERIAL NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  alert_id INTEGER NOT NULL, 
  '||name||'_id INTEGER NOT NULL);';
EXECUTE 'CREATE TEMPORARY VIEW previous AS
  SELECT
    alert_id, '||name||'_id
  FROM
    '||name||'_compacitification_history
  ORDER BY
    '||name||'_compacitification_history_id DESC
  ;';
EXECUTE 'CREATE TEMPORARY TABLE id_map (new_id SERIAL PRIMARY KEY, old_id INT);';
EXECUTE 'SELECT count(alert_id) from '||name||'_compacitification_history;' INTO rows;
IF rows = 0 THEN
  EXECUTE 'INSERT INTO '||name||'_compacitification_history(alert_id,'||name||'_id) VALUES (0,0);';
ELSE
  EXECUTE 'SELECT setval(''id_map_new_id_seq'', (SELECT '||name||'_id FROM previous LIMIT 1));';
END IF;
EXECUTE '
INSERT INTO
  id_map (old_id)
SELECT
  '||name||'_id
FROM
  '||name||'
WHERE
  '||name||'_id > (SELECT '||name||'_id FROM previous LIMIT 1)
ORDER BY
  '||name||'_id ASC;';
EXECUTE 'CREATE UNIQUE INDEX id_map_reverse_index ON id_map (old_id);';

/*
NB: since the following UPDATEs can execute in any order, an updated id may take
on a value that is still occupied, violating the primary key constraint. When
the statement finishes, however, all ids will be unique again. Since we can't
defer primary key constraints, we do the next best thing and drop the
constraint while the statement is running. Duplicate ids after the update will
cause entire transaction to roll back.
*/
EXECUTE 'ALTER TABLE '||name||' DROP CONSTRAINT '||name||'_pkey;';
EXECUTE 'SELECT '||name||'_id FROM previous LIMIT 1;' INTO min_prv_id;
EXECUTE 'SELECT alert_id FROM previous LIMIT 1;' INTO min_alert_id;

EXECUTE '
UPDATE
  '||name||'
SET
  '||name||'_id = (
    SELECT
      new_id
    FROM
      id_map
    WHERE
      id_map.old_id='||name||'_id
  )
WHERE
  '||name||'_id > $1;' USING min_prv_id;
EXECUTE '
UPDATE
  alert_'||name||'_pivot
SET
  '||name||'_id = subq.new_id
FROM
  (SELECT
    alert_id,
    array_agg(
      (
        CASE
          WHEN id_map.new_id IS NULL THEN unnested.old_id
          ELSE id_map.new_id
        END
      )
      ORDER BY num
    ) AS new_id
  FROM
    (SELECT alert_id, old_id, num
      FROM
        alert_'||name||'_pivot,
        unnest('||name||'_id) WITH ORDINALITY AS T (old_id, num)
      WHERE
        alert_id >= $1
    ) AS unnested
    LEFT JOIN id_map
      ON id_map.old_id=unnested.old_id
    GROUP BY unnested.alert_id
  ) AS subq
WHERE
  alert_'||name||'_pivot.alert_id >= $1
    AND
  alert_'||name||'_pivot.alert_id = subq.alert_id;' USING min_alert_id;

EXECUTE 'ALTER TABLE '||name||' ADD PRIMARY KEY ('||name||'_id);';

/*
Reset the auto-increment sequence. Note that we use setval(),
because ALTER SEQUENCE can not use subqueries
*/
EXECUTE 'SELECT MAX('||name||'_id) FROM '||name||' LIMIT 1;' INTO min_prv_id;
EXECUTE 'SELECT MAX(alert_id) FROM alert LIMIT 1;' INTO min_alert_id;
PERFORM setval(pg_get_serial_sequence(name, name||'_id'), min_alert_id+1, false);

EXECUTE 'INSERT INTO '||name||'_compacitification_history (alert_id,'||name||'_id) VALUES ($1,$2);' USING min_alert_id, min_prv_id;

EXECUTE 'DROP TABLE id_map;';
END;
$$ LANGUAGE plpgsql;

COMMIT;

BEGIN;
/* Run compactify at midnight CET */
-- CREATE EXTENSION pg_cron;
-- SELECT cron.schedule('0 23 * * *', $$SELECT compactify_pivot_table('prv_candidate');$$);
-- SELECT cron.schedule('0 23 * * *', $$SELECT compactify_pivot_table('upper_limit');$$);
-- SELECT cron.schedule('0 0 * * *', $$VACUUM ANALYZE;$$);
COMMIT;

