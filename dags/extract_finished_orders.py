from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'extract_finished_orders',
    default_args=default_args,
    description='Extract rows with status "FINISHED" from order table and store in finished_orders table',
    schedule_interval=None,
)

def extract_and_reingest_finished_orders():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Extract rows with status "FINISHED" from the order table
    query = "SELECT * FROM order WHERE status LIKE '%FINISHED%';"
    df_finished_orders = pd.read_sql(query, engine)

    # Re-ingest into a new table named finished_orders
    df_finished_orders.to_sql("finished orders", engine, if_exists='replace', index=False)

extract_and_reingest_task = PythonOperator(
    task_id='extract_and_reingest_finished_orders',
    python_callable=extract_and_reingest_finished_orders,
    dag=dag,
)

extract_and_reingest_task
