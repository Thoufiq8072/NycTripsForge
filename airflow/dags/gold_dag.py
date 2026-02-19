from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime


@dag(
    start_date=datetime(2026, 2, 19),
    schedule=None,
    catchup=False,
    tags=["gold", "v2"]
)
def gold_dag():

    run_dim_location = SparkSubmitOperator(
        task_id="build_dim_location",
        application="/opt/spark-apps/gold/dim_location.py",
        packages="io.delta:delta-spark_2.12:3.1.0",
    )

    run_fact_trip = SparkSubmitOperator(
        task_id="build_fact_trip",
        application="/opt/spark-apps/gold/fact_trip.py",
        packages="io.delta:delta-spark_2.12:3.1.0",
    )

    run_dim_location >> run_fact_trip


gold_dag()