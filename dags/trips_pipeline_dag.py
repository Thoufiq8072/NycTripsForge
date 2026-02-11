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
        return """
        rsync -av --remove-source-files /seed/raw/trip /opt/airflow/data/raw/trip &&
        rm -rf /seed/raw/trip || true
        """

    @task.bash
    def bronze():
        return "cd /opt/airflow/dags && python -m scripts.bronze.bronze_taxi"
    
    
    @task.bash
    def silver():
        return "cd /opt/airflow/dags && python -m scripts.silver.silver_taxi"
  
    bronze_task = bronze()
    silver_task = silver()

    seed_raw_data() >> bronze_task >> silver_task
    
taxi_pipeline()