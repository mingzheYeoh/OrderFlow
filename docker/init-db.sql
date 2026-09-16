-- Airflow keeps its metadata in the default `airflow` database; the warehouse
-- gets its own role and database so a metadata restore cannot take the
-- analytics tables with it.
CREATE USER orderflow WITH PASSWORD 'orderflow';
CREATE DATABASE orderflow OWNER orderflow;
