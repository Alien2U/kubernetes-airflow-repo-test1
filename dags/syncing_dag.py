from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

def syncing_dag_airflow(**context):
    logging.getLogger("airflow.task").info("Syncing dag from Airflow 3.0.2")
    return "done"

with DAG("syncing_dag_airflow_dag",
         start_date=datetime(2024,1,1),
         schedule=None,
         catchup=False,
         tags=["example"]) as dag:

    PythonOperator(
        task_id="syncing_dag_airflow",
        python_callable=syncing_dag_airflow,
        retries=0,
    )
