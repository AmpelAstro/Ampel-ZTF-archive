BEGIN;

/* 2018-11-13 UT: add fields to prv_candidate */
ALTER TABLE prv_candidate ADD magzpsci FLOAT;
ALTER TABLE prv_candidate ADD magzpsciunc FLOAT;
ALTER TABLE prv_candidate ADD magzpscirms FLOAT;
ALTER TABLE prv_candidate ADD clrcoeff FLOAT;
ALTER TABLE prv_candidate ADD clrcounc FLOAT;

/* From Frank Masci, 2018-10-25:
Furthermore, the existing "nid", "rcid", and “field” identifiers will be
populated for non-detections as well in the prv_candidate records.

This will enable more precise “DC” lightcurves to be generated using the
photometric histories in individual packets, as well as help identify the image
origin of upper limit measurements on a filter / CCD-quad / fieldID basis —
which can differ from that on which the alert was triggered.
*/
ALTER TABLE upper_limit ADD nid INTEGER;
ALTER TABLE upper_limit ADD rcid INTEGER;
ALTER TABLE upper_limit ADD field INTEGER;

INSERT INTO versions (alert_version) VALUES (3.2);

COMMIT;
