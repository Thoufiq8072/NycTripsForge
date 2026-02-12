# NycTripsForge

NycTripsForge is a local data engineering project that builds a Medallion-style pipeline for NYC taxi data using Airflow, PySpark, and Delta Lake.

## Stack

- Apache Airflow 3.1.6 (Docker Compose)
- PySpark 3.5.1
- Delta Lake (`delta-spark` 3.1.0)
- PostgreSQL 16 (Airflow metadata DB)

## What is implemented

- Separate ingestion pipelines for trip and location datasets.
- Bronze and Silver layers for both datasets.
- Gold layer with star-schema style outputs:
  - `dim_location`
  - `fact_trip`
- Containerized Airflow runtime with mounted DAGs/config/plugins and persistent data volume.

## Project structure

```text
.
|- dags/
|  |- trips_pipeline_dag.py
|  |- location_pipeline_dag.py
|  |- gold_dag.py
|  `- scripts/
|     |- bronze/
|     |- silver/
|     |- gold/
|     `- spark/
|- data/raw/
|  |- trip/yellow_tripdata_2025-11.parquet
|  `- location/taxi_zone_lookup.csv
|- docker-compose.yaml
`- Dockerfile
```

## DAGs

All DAGs are manual (`schedule=None`, `catchup=False`).

| DAG | Purpose | Task flow |
| --- | --- | --- |
| `taxi_pipeline` | Build Bronze/Silver trip data | `seed_raw_data -> bronze_trip -> silver_trip` |
| `location_pipeline` | Build Bronze/Silver location data | `seed_raw_data -> bronze_location -> silver_location` |
| `gold_dag` | Build Gold dimensional tables | `dim_location -> fact_trip` |

`gold_dag` depends on Silver outputs from both `taxi_pipeline` and `location_pipeline`.

## Data paths

### Raw inputs (host)

- `data/raw/trip/yellow_tripdata_2025-11.parquet`
- `data/raw/location/taxi_zone_lookup.csv`

### Runtime paths (inside Airflow containers)

`/opt/airflow/data` is a named Docker volume (`airflow-data-volume`).

- Raw (copied by seed tasks)
  - `/opt/airflow/data/raw/trip/yellow_tripdata_2025-11.parquet`
  - `/opt/airflow/data/raw/location/taxi_zone_lookup.csv`
- Bronze (Delta)
  - `/opt/airflow/data/bronze/trips_data`
  - `/opt/airflow/data/bronze/location_data`
- Silver (Delta)
  - `/opt/airflow/data/silver/trips_data`
  - `/opt/airflow/data/silver/location_data`
- Gold (Delta)
  - `/opt/airflow/data/gold/dim_location`
  - `/opt/airflow/data/gold/fact_trip`

## Transformation summary

### Trip pipeline

- Bronze (`bronze_taxi.py`)
  - Reads raw parquet trip file.
  - Adds `_ingested_date`, `_data_source`, `_load_timestamp`.
  - Writes Delta to Bronze.
- Silver (`silver_taxi.py`)
  - Enforces schema and normalized column names.
  - Deduplicates trips.
  - Filters invalid records (null keys, negative amounts, invalid pickup/dropoff ordering, non-positive distance).
  - Fills selected nullable numeric columns with defaults.
  - Adds `trip_duration` (minutes) and `trip_date`.
  - Writes Delta to Silver.

### Location pipeline

- Bronze (`bronze_location.py`)
  - Reads taxi zone CSV.
  - Adds audit metadata columns.
  - Writes Delta to Bronze.
- Silver (`silver_location.py`)
  - Enforces schema for location fields.
  - Deduplicates by `location_id`.
  - Filters null critical columns.
  - Fills `service_zone` defaults.
  - Writes Delta to Silver.

### Gold pipeline

- `dim_location.py`
  - Reads Silver location data.
  - Drops audit columns.
  - Builds `location_sk` surrogate key (`sha2` on `location_id`).
  - Writes `dim_location` Delta table.
- `fact_trip.py`
  - Reads Silver trip data.
  - Joins `dim_location` twice to map pickup/dropoff surrogate keys.
  - Builds `trip_sk` surrogate key (`sha2` on vendor + pickup/dropoff timestamps).
  - Writes `fact_trip` Delta table.

## Run locally

### 1) Prepare env file

```powershell
Copy-Item .env.example .env
```

### 2) Initialize Airflow metadata DB and admin user

```bash
docker compose up airflow-init
```

### 3) Start services

```bash
docker compose up -d
```

### 4) Open Airflow UI

- URL: `http://localhost:8080`
- Default credentials: `airflow` / `airflow`

### 5) Trigger DAGs in order

1. `location_pipeline`
2. `taxi_pipeline`
3. `gold_dag`

## Useful commands

Check container status:

```bash
docker compose ps
```

Tail scheduler logs:

```bash
docker compose logs -f airflow-scheduler
```

List generated data folders inside Airflow container:

```bash
docker compose exec airflow-apiserver bash -lc "find /opt/airflow/data -maxdepth 3 -type d | sort"
```

Stop services:

```bash
docker compose down
```

Reset everything including volumes (deletes DB + generated data):

```bash
docker compose down -v
```
