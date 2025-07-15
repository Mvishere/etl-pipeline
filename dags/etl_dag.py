from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Adjust path to your project root
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))

from etl_pipeline import extract, transform, load  # Reuse your functions

default_args = {
    'owner': 'manas',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_pipeline_dag',
    default_args=default_args,
    description='A simple ETL pipeline with psycopg2 and pandas',
    start_date=datetime(2025, 7, 15),
    schedule_interval='@daily',  # Can be cron too
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=lambda: transform(extract())  # Chain functions
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=lambda: load(transform(extract()))
    )

    extract_task >> transform_task >> load_task  # Task order
