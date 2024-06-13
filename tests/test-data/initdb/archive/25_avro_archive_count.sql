BEGIN;

-- number of alerts in the blob
ALTER TABLE avro_archive ADD count INTEGER;
-- number of rows in the alert table that reference this blob
ALTER TABLE avro_archive ADD refcount INTEGER DEFAULT 0;

-- update reference count when alerts are added or updated
-- adapted from https://stackoverflow.com/a/15582484
CREATE OR REPLACE FUNCTION f_trg_update_avro_archive_refcount() 
  RETURNS trigger
  LANGUAGE plpgsql AS
$func$
BEGIN
   UPDATE avro_archive
   SET refcount = refcount + 1
   WHERE avro_archive_id = NEW.avro_archive_id;
   UPDATE avro_archive
   SET refcount = refcount - 1
   WHERE avro_archive_id = OLD.avro_archive_id;
   RETURN NULL;
END
$func$;

CREATE TRIGGER trg_update_avro_archive_refcount
AFTER INSERT OR DELETE OR UPDATE OF avro_archive_id
ON alert
FOR EACH ROW EXECUTE PROCEDURE f_trg_update_avro_archive_refcount();

COMMIT;