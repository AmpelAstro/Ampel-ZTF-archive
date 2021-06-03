BEGIN;

CREATE EXTENSION pgcrypto;

CREATE TABLE access_token (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  created TIMESTAMP DEFAULT now(),
  owner TEXT NOT NULL,
  PRIMARY KEY (id)
);

-- GRANT SELECT, INSERT, UPDATE, DELETE on access_token TO GROUP readers;
-- GRANT SELECT, USAGE on SEQUENCE access_token_token_id_seq TO GROUP readers;

COMMIT;