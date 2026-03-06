from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime
from datasets import bronze_trips_dataset, bronze_location_dataset


SPARK_CONF = {
    "spark.sql.extensions":"io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":"org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.ui.enabled": "false",
}

@dag(
    dag_id="bronze_pipeline",
    schedule=None,
    start_date=datetime(2024,1,1),
    catchup=False
)   
def bronze_pipeline():

    bronze_trip = SparkSubmitOperator(
        task_id="bronze_trip",
        application="/opt/spark/jobs/bronze/bronze_trip.py",
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit", 
        verbose=True,
        # packages="io.delta:delta-spark_2.12:3.2.0",
        conf=SPARK_CONF,
        outlets=[bronze_trips_dataset],
        name="bronze_trip_task"
    )

    bronze_location = SparkSubmitOperator(
        task_id="bronze_location",
        application="/opt/spark/jobs/bronze/bronze_location.py",
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit", 
        verbose=True,
        # packages="io.delta:delta-spark_2.12:3.2.0",
        conf=SPARK_CONF,
        outlets=[bronze_location_dataset],
        name="bronze_location_task"
    )

    bronze_trip, bronze_location
    
bronze_pipeline()