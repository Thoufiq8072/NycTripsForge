from airflow.sdk import dag, task
from pendulum import datetime

@dag(
  start_date=datetime(2026, 2, 8), 
  schedule=None, 
  catchup=False
)
def gold_dag():

    @task.bash
    def gold():
      return "cd /opt/airflow/dags && python -m scripts.gold.gold_taxi"
  
    gold_task = gold()
  
    gold_task
    
gold_dag()