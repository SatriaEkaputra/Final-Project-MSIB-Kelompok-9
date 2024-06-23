from datetime import datetime
import pandas as pd
import openpyxl
from airflow import DAG
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Function to ingest product category data from Excel file into PostgreSQL
def product_category_funnel():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Read the Excel file and load into PostgreSQL table
    pd.read_excel("data/product_category.xls").to_sql("product category", engine, if_exists="replace", index=False)

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
    "ingest_product_category",
    default_args=default_args,
    description="Product Category Data Ingestion",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the PythonOperator to execute the function
task_load_product_category = PythonOperator(
     task_id="ingest_product_category",
     python_callable=product_category_funnel,
     dag=dag,
)

task_load_product_category
