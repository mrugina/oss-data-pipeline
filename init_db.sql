CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

CREATE USER superset WITH PASSWORD 'superset';
CREATE DATABASE superset;
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;

CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE TABLE IF NOT EXISTS raw_data.flights (
    id SERIAL PRIMARY KEY,
    data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.opensky_active_flights (
    id SERIAL PRIMARY KEY,
    snapshot_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    airborne_flights INT NOT NULL,
    total_states INT NOT NULL,
    source TEXT NOT NULL DEFAULT 'opensky'
);

