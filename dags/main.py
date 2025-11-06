from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

def hello_airflow(**context):
    logging.getLogger("airflow.task").info("Hello from Airflow 3.0.2")
    return "done"

with DAG("hello_airflow_3",
         start_date=datetime(2024,1,1),
         schedule=None,      # manual trigger for now
         catchup=False,
         tags=["example"]) as dag:

    PythonOperator(
        task_id="public_repo_example",
        python_callable=hello_airflow,
        retries=0,
    )
