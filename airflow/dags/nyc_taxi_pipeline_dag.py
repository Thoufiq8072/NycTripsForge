from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime


@dag(
    start_date=datetime(2026, 2, 13),
    schedule=None,
    catchup=False,
    doc_md="Orchestrates all NYC Taxi data pipelines: taxi, location, and gold layer transformations"
)
def nyc_taxi_pipeline():
    """
    Main orchestration DAG that triggers all dependent pipelines:
    - taxi_pipeline: Processes taxi raw data through bronze and silver layers
    - location_pipeline: Processes location raw data through bronze and silver layers
    - gold_dag: Creates dimensional and fact tables from silver layer data
    """

    # Trigger taxi pipeline
    run_taxi = TriggerDagRunOperator(
        task_id="run_taxi_pipeline",
        trigger_dag_id="taxi_pipeline",
        wait_for_completion=True
    )

    # Trigger location pipeline
    run_location = TriggerDagRunOperator(
        task_id="run_location_pipeline",
        trigger_dag_id="location_pipeline",
        wait_for_completion=True
    )

    # Trigger gold pipeline (depends on taxi and location completion)
    run_gold = TriggerDagRunOperator(
        task_id="run_gold_pipeline",
        trigger_dag_id="gold_dag",
        wait_for_completion=True
    )

    # Set task dependencies
    # Run taxi and location pipelines in parallel
    # Then run gold pipeline after both complete
    [run_taxi, run_location] >> run_gold


# Instantiate the DAG
nyc_taxi_pipeline()