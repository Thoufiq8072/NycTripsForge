from airflow.sdk import dag, task
from pendulum import datetime

@dag(
  start_date=datetime(2026, 2, 12), 
  schedule=None, 
  catchup=False
)
def gold_dag():

    @task.bash
    def dim_location():
      return "cd /opt/airflow/dags && python -m scripts.gold.dim_location"
  
    @task.bash
    def fact_trip():
      return "cd /opt/airflow/dags && python -m scripts.gold.fact_trip"
    
    dim_location_task = dim_location()
    fact_trip_task = fact_trip()
  
    dim_location_task >> fact_trip_task
    
gold_dag()