-- Runs once, on first start of the Postgres container (docker-entrypoint-initdb.d).
-- The application database ("hospital") is created by POSTGRES_DB; Airflow needs its own.
-- Owner: Member A (platform). Files run in alphabetical order: 00_, 01_, then init.sql (Member C).
CREATE DATABASE airflow;
