from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime


@dag(
    start_date=datetime(2026, 2, 19),
    schedule=None,
    catchup=False,
    tags=["bronze-silver", "v2"]
)
def location_pipeline():

    # @task.bash
    # def seed_raw_data():
    #     return "mkdir -p /opt/airflow/data/raw && cp -r /seed/raw/location /opt/airflow/data/raw || true"

    # seed_raw_data = SparkSubmitOperator(
    #     task_id="build_seed_raw_data",
    #     application="/opt/spark-apps/gold/dim_location.py",
    #     packages="io.delta:delta-spark_2.12:3.1.0",
    # )
    
    # @task.bash
    # def bronze_location():
    #     return "cd /opt/airflow/dags && python -m scripts.bronze.bronze_location"
        
    bronze_location = SparkSubmitOperator(
        task_id="build_bronze_location",
        application="/opt/spark-apps/bronze/bronze_location.py",
        packages="io.delta:delta-spark_2.12:3.1.0",
    )
    
    # @task.bash
    # def silver_location():
    #     return "cd /opt/airflow/dags && python -m scripts.silver.silver_location"

    silver_location = SparkSubmitOperator(
        task_id="build_silver_location",
        application="/opt/spark-apps/silver/silver_location.py",
        packages="io.delta:delta-spark_2.12:3.1.0",
    )
    
    # bronze_location_task = bronze_location()
    # silver_location_task = silver_location()

    # seed_raw_data() >> bronze_location_task >> silver_location_task

    bronze_location >> silver_location

location_pipeline()
