from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

def oh_hello_airflow(**context):
    logging.getLogger("airflow.task").info("Oh hello from Airflow 3.0.2")
    return "done"

with DAG("oh_hello_airflow_dag",
         start_date=datetime(2024,1,1),
         schedule=None,
         catchup=False,
         tags=["example"]) as dag:

    PythonOperator(
        task_id="oh_hello_airflow",
        python_callable=oh_hello_airflow,
        retries=0,
    )
