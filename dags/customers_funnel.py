from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from sqlalchemy import create_engine
import glob
import os

# Function to concatenate customer data CSV files and ingest into PostgreSQL
def customers_funnel():
    # Concatenate all CSV files in the data directory and save to a temporary file
    pd.concat((pd.read_csv(file) for file in glob.glob(os.path.join("data", '*.csv'))), ignore_index=True).to_csv("/tmp/customer_data.csv", index=False)

    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Read the concatenated customer data and load into PostgreSQL table
    pd.read_csv("/tmp/customer_data.csv").to_sql("customers", engine, if_exists="replace", index=False)

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
    "ingest_customer",
    default_args=default_args,
    description="Customer Data Ingestion",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the PythonOperator to execute the function
task_load_customer = PythonOperator(
     task_id="ingest_customer",
     python_callable=customers_funnel,
     dag=dag,
)

task_load_customer
