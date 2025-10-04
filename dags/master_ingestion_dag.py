from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

# Import your ingestion functions
from ingestion.world_bank_birth_rate import fetch_birth_rate, save_data_locally as save_birth
from ingestion.who_smoking_ingestion import fetch_smoking_prevalence, clean_smoking_data, save_data_locally as save_smoking

# Define ingestion functions for Airflow
def run_birthrate_ingestion():
    logging.info("Starting birth rate ingestion for India...")
    df = fetch_birth_rate("IND")
    save_birth(df, "/usr/local/airflow/include/data/raw/india_birth_rate.csv")
    logging.info("Birth rate ingestion completed.")
    

def run_smoking_ingestion():
    logging.info("Starting smoking prevalence ingestion for India...")
    df = fetch_smoking_prevalence("IND")
    df_clean = clean_smoking_data(df)
    save_smoking(df_clean, "/usr/local/airflow/include/data/raw/india_smoking.csv")
    logging.info("Smoking prevalence ingestion completed.")

# Define DAG
with DAG(
    dag_id='who_health_indicators_ingestion',
    start_date=datetime(2025, 1, 1),
    schedule='@monthly',  # or @daily depending on your preference
    catchup=False,
    tags=["social-stats", "who", "health-indicators"],
) as dag:

    # Tasks
    ingest_birthrate = PythonOperator(
        task_id='ingest_birth_rate',
        python_callable=run_birthrate_ingestion
    )

    ingest_smoking = PythonOperator(
        task_id='ingest_smoking_prevalence',
        python_callable=run_smoking_ingestion
    )

    # Define execution order
    [ingest_birthrate,ingest_smoking]  # birthrate first, then smoking
