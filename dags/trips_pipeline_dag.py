from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2026, 2, 8), 
    schedule=None, 
    catchup=False
)
def taxi_pipeline():

    @task.bash
    def seed_raw_data():
        return "mkdir -p /opt/airflow/data/raw && cp -r /seed/raw/trip /opt/airflow/data/raw || true"

    @task.bash
    def bronze_trip():
        return "cd /opt/airflow/dags && python -m scripts.bronze.bronze_taxi"
    
    
    @task.bash
    def silver_trip():
        return "cd /opt/airflow/dags && python -m scripts.silver.silver_taxi"
  
    bronze_trip_task = bronze_trip()
    silver_trip_task = silver_trip()

    seed_raw_data() >> bronze_trip_task >> silver_trip_task
    
taxi_pipeline()