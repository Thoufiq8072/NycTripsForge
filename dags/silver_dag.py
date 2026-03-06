from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime
from datasets import silver_trips_dataset, silver_location_dataset, bronze_trips_dataset, bronze_location_dataset


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
    dag_id="silver_pipeline",
    schedule=[bronze_trips_dataset, bronze_location_dataset],
    start_date=datetime(2024,1,1),
    catchup=False
)
def silver_pipeline():
    silver_trip = SparkSubmitOperator(
        task_id="silver_trip",
        application="/opt/spark/jobs/silver/silver_trip.py",
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit", 
        verbose=True,
        # packages="io.delta:delta-spark_2.12:3.2.0",
        conf=SPARK_CONF,
        outlets=[silver_trips_dataset],
        name="silver_trip_task"
    )

    silver_location = SparkSubmitOperator(
        task_id="silver_location",
        application="/opt/spark/jobs/silver/silver_location.py",
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit", 
        verbose=True,
        # packages="io.delta:delta-spark_2.12:3.2.0",
        conf=SPARK_CONF,
        outlets=[silver_location_dataset],
        name="silver_location_task"
    )
    silver_trip, silver_location

silver_pipeline()