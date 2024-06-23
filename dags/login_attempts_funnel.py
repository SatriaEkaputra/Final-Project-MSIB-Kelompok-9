from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from sqlalchemy import create_engine
import glob
import os

# Function to concatenate login attempts JSON files
def login_attempts_concat(directory_path):
    return pd.concat((pd.read_json(file) for file in glob.glob(os.path.join(directory_path, 'login_attempts_*.json'))), ignore_index=True)

# Concatenate login attempts files and save to a temporary JSON file
login_attempts_concat("data").to_json("/tmp/login_attempts_data.json", index=False)

# Function to ingest concatenated login attempts data into PostgreSQL
def login_attempts_funnel():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Read the concatenated login attempts data and load into PostgreSQL table
    pd.read_json("/tmp/login_attempts_data.json").to_sql("login attempts", engine, if_exists="replace", index=False)

# Define the default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define the DAG
dag = DAG(
    "ingest_login_attempts",
    default_args=default_args,
    description="Login Attempts Data Ingestion",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the PythonOperator to execute the function
task_load_login_attempts = PythonOperator(
     task_id="ingest_login_attempts",
     python_callable=login_attempts_funnel,
     dag=dag,
)

task_load_login_attempts
