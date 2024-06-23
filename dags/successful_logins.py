from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'successful_logins',
    default_args=default_args,
    description='Extract successful logins and re-ingest into a new table',
    schedule_interval=None,
)

# Function to extract successful logins and re-ingest into PostgreSQL
def extract_and_reingest():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Extract rows with login_successful = 't' and re-ingest into "successful logins" table
    query = 'SELECT * FROM "login attempts" WHERE login_successful = \'t\';'
    pd.read_sql(query, engine).to_sql("successful logins", engine, if_exists='replace', index=False)

# Define the PythonOperator to execute the function
extract_and_reingest_task = PythonOperator(
    task_id='successful_logins',
    python_callable=extract_and_reingest,
    dag=dag,
)

extract_and_reingest_task
