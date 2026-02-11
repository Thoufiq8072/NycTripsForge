from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2026, 2, 10), 
    schedule=None, 
    catchup=False
)
def location_pipeline():

    @task.bash
    def seed_raw_data():
        return """
        rsync -av --remove-source-files /seed/raw/location /opt/airflow/data/raw/location &&
        rm -rf /seed/raw/location || true
        """
    
    @task.bash
    def bronze_location():
        return "cd /opt/airflow/dags && python -m scripts.bronze.bronze_location"
    
    @task.bash
    def silver_location():
        return "cd /opt/airflow/dags && python -m scripts.silver.silver_location"
    
    bronze_location_task = bronze_location()
    silver_location_task = silver_location()

    seed_raw_data() >> bronze_location_task >> silver_location_task

location_pipeline()
