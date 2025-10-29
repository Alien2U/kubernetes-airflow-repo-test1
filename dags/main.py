from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function to run
def hello_airflow():
    print ("Hello from Airflow 3.0.2")

# Define the DAG
with DAG(
    dag_id="hello-airflow_3",
    start_date=datetime(2025, 10, 29),
    schedule="@daily",
    catchup=False,
    tags=["example tag"],
) as dag:
    
    t1 = PythonOperator(
        task_id="public_repo_example",
        python_callable=hello_airflow
    )

    t1