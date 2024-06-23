from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import fastavro
import struct
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Function to ingest order item data from Avro file into PostgreSQL
def order_item_funnel():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Read the Avro file using fastavro and load into PostgreSQL table
    with open("data/order_item.avro", 'rb') as f:
        reader = fastavro.reader(f)
        pd.DataFrame(list(reader)).to_sql("order item", engine, if_exists="replace", index=False)

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
    "ingest_order_item",
    default_args=default_args,
    description="Order Item Data Ingestion",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the PythonOperator to execute the function
task_load_order_item = PythonOperator(
     task_id="load_order_item",
     python_callable=order_item_funnel,
     dag=dag,
)

task_load_order_item
