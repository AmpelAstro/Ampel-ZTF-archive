BEGIN;

CREATE EXTENSION pgcrypto;

CREATE TABLE access_token (
  token_id SERIAL NOT NULL,
  owner TEXT NOT NULL,
  created TIMESTAMP NOT NULL DEFAULT now(),
  token uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  PRIMARY KEY (token_id)
);

-- GRANT SELECT, INSERT, UPDATE, DELETE on access_token TO GROUP readers;
-- GRANT SELECT, USAGE on SEQUENCE access_token_token_id_seq TO GROUP readers;

COMMIT;