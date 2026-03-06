# Airflow + Spark NYC Taxi Medallion Pipeline
### Version 2.0 

Local data engineering project that orchestrates a medallion-style NYC taxi pipeline with Apache Airflow and Apache Spark, using Delta Lake tables across Bronze, Silver, and Gold layers.

## What This Project Does

- Runs Airflow in Docker (scheduler, api-server, triggerer, dag-processor) with a Postgres metadata DB.
- Runs a standalone Spark master/worker container for batch transformations.
- Loads raw NYC taxi and taxi zone lookup files from `warehouse/raw`.
- Builds Delta Lake tables through medallion layers:
  - Bronze: raw ingestion + audit columns
  - Silver: schema enforcement, deduplication, quality filters
  - Gold: dimensional model (`dim_location`, `fact_trip`)
- Uses Airflow Datasets to chain DAGs:
  - `bronze_pipeline` -> `silver_pipeline` -> `gold_pipeline`

## Repository Layout

```text
.
├── dags/
│   ├── bronze_dag.py
│   ├── silver_dag.py
│   ├── gold_dag.py
│   └── datasets.py
├── spark/jobs/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── utils/spark_session.py
├── warehouse/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker-compose.yaml
├── dockerfile
└── .env.example
```

## Prerequisites

- Docker + Docker Compose
- At least 4 GB RAM allocated to Docker
- At least 2 CPU cores recommended

## Configuration

1. Copy environment file:

```bash
cp .env.example .env
```

2. Default `.env` values:

```env
AIRFLOW_UID=1000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
```

3. Create the Folders for spark writes

```bash 
cd warehouse && mkdir -p bronze/location_data bronze/trips_data silver/location_data silver/trips_data gold/location_data gold/trips_data && cd ..
```

3. Ensure write permissions for `warehouse` on Linux if needed:

```bash
sudo chown -R 1000:1000 warehouse
sudo chmod -R 777 warehouse
```

## Start The Stack

```bash
docker compose up airflow-init
docker compose up -d
```

Services:

- Airflow API/UI: `http://localhost:8080`
- Spark master endpoint: `spark://spark:7077`
- Spark UI: `http://localhost:8081`
- Warehouse Postgres (optional): `localhost:5433`

## Create Airflow Spark Connection

DAGs use `conn_id="spark_default"`. If not already configured, add it:

```bash
docker compose exec airflow-scheduler airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark \
  --conn-port 7077 \
  --conn-extra '{"queue":"default","deploy-mode":"client","spark-binary":"/opt/spark/bin/spark-submit"}'
```

## Run The Pipeline

1. Trigger `bronze_pipeline` from Airflow UI.
2. `silver_pipeline` starts automatically when Bronze datasets are updated.
3. `gold_pipeline` starts automatically when Silver datasets are updated.

You can also trigger from CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger bronze_pipeline
```

## Input And Output Data Paths

Raw input files expected:

- `warehouse/raw/trip/yellow_tripdata_2025-11.parquet`
- `warehouse/raw/location/taxi_zone_lookup.csv`

Produced Delta tables:

- Bronze:
  - `warehouse/bronze/trips_data`
  - `warehouse/bronze/location_data`
- Silver:
  - `warehouse/silver/trips_data`
  - `warehouse/silver/location_data`
- Gold:
  - `warehouse/gold/dim_location`
  - `warehouse/gold/fact_trip`

## DAG Overview

- `bronze_pipeline`
  - `bronze_trip`: ingests trip parquet to Delta Bronze
  - `bronze_location`: ingests location CSV to Delta Bronze
- `silver_pipeline`
  - `silver_trip`: enforces schema, deduplicates, quality checks, derives `trip_duration` and `trip_date`
  - `silver_location`: cleans and standardizes location dimension source
- `gold_pipeline`
  - `dim_location`: creates location dimension with surrogate key `location_sk`
  - `fact_trip`: creates trip fact with surrogate keys (`trip_sk`, pickup/dropoff location SKs)


## Common Commands

```bash
# Stop all services
docker compose down

# Stop and remove volumes (resets Airflow metadata + warehouse-db)
docker compose down -v

# View scheduler logs
docker compose logs -f airflow-scheduler

# Open shell in scheduler container
docker compose exec airflow-scheduler bash
```
