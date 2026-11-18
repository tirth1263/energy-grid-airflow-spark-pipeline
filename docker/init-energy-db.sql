-- Separate database for pipeline output, so analytics tables don't mix with Airflow's metadata.
CREATE DATABASE energy OWNER airflow;
