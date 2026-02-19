from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime


@dag(
    start_date=datetime(2026, 2, 19),
    schedule=None,
    catchup=False,
    tags=["bronze-silver", "v2"]
)
def taxi_pipeline():

    # @task.bash
    # def seed_raw_data():
    #     return "mkdir -p /opt/airflow/data/raw && cp -r /seed/raw/trip /opt/airflow/data/raw || true"

    # @task.bash
    # def bronze_trip():
    #     return "cd /opt/airflow/dags && python -m scripts.bronze.bronze_taxi"

    bronze_trip = SparkSubmitOperator(
        task_id="build_bronze_trip",
        application="/opt/spark-apps/bronze/bronze_taxi.py",
        packages="io.delta:delta-spark_2.12:3.1.0",
    )

    
    # @task.bash
    # def silver_trip():
    #     return "cd /opt/airflow/dags && python -m scripts.silver.silver_taxi"

    silver_trip = SparkSubmitOperator(
        task_id="build_silver_trip",
        application="/opt/spark-apps/silver/silver_taxi.py",
        packages="io.delta:delta-spark_2.12:3.1.0",
    )
  
    # bronze_trip_task = bronze_trip()
    # silver_trip_task = silver_trip()

    # seed_raw_data() >> bronze_trip_task >> silver_trip_task
    bronze_trip >> silver_trip
    
taxi_pipeline()