from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion.world_bank_birth_rate import fetch_birth_rate, save_data_locally
import logging

def run_ingestion():
    logging.info("Starting birth rate ingestion for India...")
    df = fetch_birth_rate("IND")
    save_data_locally(df, "data/raw/india_birth_rate.csv")
    logging.info("Uploaded data to S3 successfully.")

with DAG(
    dag_id = 'birth_rate_ingestion',
    start_date = datetime(2025, 1, 1),
    schedule= "@monthly",
    catchup = False,
    tags=["social-stats", "birth-rate"],
) as dag:
    
    ingest_task = PythonOperator(
        task_id = 'fetch_and_upload_birth_rate',
        python_callable = run_ingestion

    )

    ingest_task
