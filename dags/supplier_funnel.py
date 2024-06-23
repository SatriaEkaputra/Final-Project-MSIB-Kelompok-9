from datetime import datetime
import pandas as pd
import openpyxl
from airflow import DAG
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Define the function to ingest supplier data into PostgreSQL
def supplier_funnel():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Read supplier data from Excel file and ingest into "supplier" table
    pd.read_excel("data/supplier.xls").to_sql("supplier", engine, if_exists="replace", index=False)

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define the DAG
dag = DAG(
    "ingest_supplier",
    default_args=default_args,
    description="Supplier Data Ingestion",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the PythonOperator to execute the supplier_funnel function
task_load_supplier = PythonOperator(
     task_id="ingest_supplier",
     python_callable=supplier_funnel,
     dag=dag,
)

task_load_supplier  # Return the task object
