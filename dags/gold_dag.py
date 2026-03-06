from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime
from datasets import silver_trips_dataset, silver_location_dataset


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
    dag_id="gold_pipeline",
    schedule=[silver_trips_dataset, silver_location_dataset],
    start_date=datetime(2024,1,1),
    catchup=False
)  
def gold_pipeline():
    dim_location = SparkSubmitOperator(
        task_id="dim_location", 
        application="/opt/spark/jobs/gold/dim_location.py",
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit", 
        verbose=True,
        # packages="io.delta:delta-spark_2.12:3.2.0",
        conf=SPARK_CONF,
        name="dim_location_task"
    )
    
    fact_trip = SparkSubmitOperator(
        task_id="fact_trip",
        application="/opt/spark/jobs/gold/fact_trip.py",
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit", 
        verbose=True,
        # packages="io.delta:delta-spark_2.12:3.2.0",
        conf=SPARK_CONF,
        name="fact_trip_task"
    )

    dim_location >> fact_trip
    
gold_pipeline()