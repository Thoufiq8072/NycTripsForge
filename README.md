# NycTripsForge

**Version 1.0 - Complete** ✓

NycTripsForge is a data engineering project that builds a Medallion Architecture pipeline for NYC taxi data using Airflow, PySpark, and Delta Lake. The project implements a full-stack ELT/ETL workflow with bronze, silver, and gold layers orchestrated through Apache Airflow.

## Stack

- **Apache Airflow** 3.1.6 (Docker Compose)
- **PySpark** 3.5.1
- **Delta Lake** (`delta-spark` 3.1.0)
- **PostgreSQL** 16 (Airflow metadata DB)

## Version 1.0 - Completed Features

✅ **Multi-dataset Bronze layer** - Parallel ingestion of trip and location data with audit metadata  
✅ **Silver layer transformations** - Data quality, deduplication, schema enforcement, and enrichment  
✅ **Gold layer star schema** - Dimensional and fact tables for analytics  
✅ **Orchestration DAG** - Master DAG that triggers all pipelines with proper dependency management  
✅ **Containerized runtime** - Docker Compose setup with persistent volumes and easy local development  
✅ **Spark integration** - PySpark jobs for scalable data processing  

## Project Structure

```text
.
├── dags/
│   ├── nyc_taxi_pipeline_dag.py    # Master orchestration DAG (NEW)
│   ├── trips_pipeline_dag.py        # Taxi pipeline
│   ├── location_pipeline_dag.py     # Location pipeline
│   ├── gold_dag.py                  # Gold layer transformations
│   └── scripts/
│       ├── bronze/                  # Bronze layer transformations
│       ├── silver/                  # Silver layer transformations
│       ├── gold/                    # Gold layer transformations
│       └── spark/                   # Spark session utilities
├── data/raw/
│   ├── trip/yellow_tripdata_2025-11.parquet
│   └── location/taxi_zone_lookup.csv
├── docker-compose.yaml
├── Dockerfile
└── requirements.txt
```

## DAGs

All DAGs are manual (`schedule=None`, `catchup=False`).

| DAG | Purpose | Task Flow |
| --- | --- | --- |
| **`nyc_taxi_pipeline`** ⭐ | **Master orchestration** | `run_taxi_pipeline` → `run_location_pipeline` → `run_gold_pipeline` |
| `taxi_pipeline` | Build Bronze/Silver trip data | `seed_raw_data` → `bronze_trip` → `silver_trip` |
| `location_pipeline` | Build Bronze/Silver location data | `seed_raw_data` → `bronze_location` → `silver_location` |
| `gold_dag` | Build Gold dimensional/fact tables | `dim_location` → `fact_trip` |

**→ Start with `nyc_taxi_pipeline` for full workflow execution**

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

## Quick Start

### 1. Prepare environment file

```bash
cp .env.example .env
```

### 2. Initialize Airflow metadata DB and create admin user

```bash
docker compose up airflow-init
```

### 3. Start all services

```bash
docker compose up -d
```

### 4. Open Airflow UI

- **URL:** `http://localhost:8080`
- **Default credentials:** `airflow` / `airflow`

### 5. Trigger the master DAG

1. Open Airflow UI dashboard
2. Find and click on `nyc_taxi_pipeline`
3. Click **Trigger DAG**
4. The orchestration will automatically execute all dependent pipelines in the correct order:
   - `taxi_pipeline` and `location_pipeline` run in parallel
   - Once both complete, `gold_dag` runs automatically

That's it! The entire data pipeline will execute end-to-end.

## Development & Troubleshooting

### Check container status

```bash
docker compose ps
```

### View Airflow scheduler logs

```bash
docker compose logs -f airflow-scheduler
```

### Inspect generated data (inside Airflow container)

```bash
docker compose exec airflow-apiserver bash -lc "find /opt/airflow/data -maxdepth 3 -type d | sort"
```

### Stop services

```bash
docker compose down
```

### Reset everything (includes DB and generated data)

```bash
docker compose down -v
```

## System Architecture

### Data Flow

```
Raw Data
  ├─→ bronze_location → silver_location ─┐
  └─→ bronze_trip     → silver_trip     ─→ dim_location + fact_trip (Gold)
```

### Layer Responsibilities

**Bronze Layer:** Ingest, audit metadata  
**Silver Layer:** Clean, deduplicate, validate, enrich  
**Gold Layer:** Dimensional modeling, star schema  
