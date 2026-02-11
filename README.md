# Airflow Tutorial

## Project Status (as of February 7, 2026)

This project currently implements a **Bronze -> Silver** data pipeline for NYC Yellow Taxi data using **Airflow + PySpark + Delta Lake**.

## What Is Completed

- Airflow environment is running with Docker Compose (`airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, `postgres`).
- Custom Airflow image is set up with Java for Spark jobs (`Dockerfile`).
- A reusable Spark session helper with Delta Lake support is implemented in `dags/scripts/spark/spark_session.py`.
- Bronze layer pipeline is implemented in `dags/scripts/bronze/bronze_taxi.py`.
- Silver layer pipeline is implemented in `dags/scripts/silver/silver_taxi.py`.
- Airflow DAG orchestration is implemented in `dags/dag_1.py` with task order:
  - `bronze` -> `silver`
- Input data file is available at `data/raw/yellow_tripdata_2025-11.parquet`.

## Current Data Flow

1. **Bronze step**
   - Reads raw parquet from: `/opt/airflow/data/raw/yellow_tripdata_2025-11.parquet`
   - Adds ingestion metadata:
     - `_ingested_date`
     - `_data_source`
     - `_load_timestamp`
   - Writes Delta output to: `/opt/airflow/data/bronze/trips_data`

2. **Silver step**
   - Reads Delta from Bronze: `/opt/airflow/data/bronze/trips_data`
   - Applies schema standardization and column renaming
   - Removes duplicates
   - Applies data quality filters (valid pickup/dropoff, positive distance, non-negative fares)
   - Fills selected nullable numeric columns with defaults
   - Adds derived columns:
     - `trip_duration` (minutes)
     - `trip_date`
   - Writes Delta output to: `/opt/airflow/data/silver/trips_data`

## How to Run

1. Start Airflow services:
   ```bash
   docker compose up airflow-init
   docker compose up -d
   ```
2. Open Airflow UI: `http://localhost:8080`
3. Login (defaults from compose file):
   - Username: `airflow`
   - Password: `airflow`
4. Trigger DAG `taxi_pipeline` manually.

Notes:
- DAG is currently configured with `schedule=None` and `catchup=False`.
- `start_date` is set to `2026-02-07`.

## Key Paths

- DAG: `dags/dag_1.py`
- Bronze job: `dags/scripts/bronze/bronze_taxi.py`
- Silver job: `dags/scripts/silver/silver_taxi.py`
- Spark session helper: `dags/scripts/spark/spark_session.py`
- Raw data: `data/raw/yellow_tripdata_2025-11.parquet`
- Bronze data: `data/bronze/trips_data`
- Silver data: `data/silver/trips_data`

## Next Steps

The following items are intentionally **not completed yet** and are planned next:

1. Build the **Gold layer** transformations and outputs.
2. Connect the pipeline to **Snowflake**.
3. Load curated Gold data into Snowflake tables.
4. Add scheduling/monitoring and production hardening after Snowflake load is in place.
