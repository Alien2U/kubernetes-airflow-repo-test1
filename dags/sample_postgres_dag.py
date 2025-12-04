from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Change this to your actual Airflow connection ID
CONN_ID = "salesforce_demo_db"

with DAG(
    dag_id="sample_postgres_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # or "@daily", "0 2 * * *", etc.
    catchup=False,
    tags=["github", "logging", "postgres"],
) as dag:

    @task
    def log_names_from_rke_Account():
        """
        Connects to Postgres using an Airflow connection,
        reads all rows from rke_Account, and logs the 'name' column.
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)

        # Get a raw DB-API connection and cursor
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT \"Name\" FROM \"rke_Account\";")
        rows = cursor.fetchall()

        logger = logging.getLogger("airflow.task")

        if not rows:
            logger.info("No rows found in rke_Account.")
        else:
            logger.info("Names found in rke_Account:")
            for (name,) in rows:
                logger.info(" - %s", name)

        cursor.close()
        conn.close()

    log_names_from_rke_Account()
