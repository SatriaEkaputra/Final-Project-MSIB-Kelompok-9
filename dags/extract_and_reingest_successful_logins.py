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
    'extract_and_reingest_successful_logins',
    default_args=default_args,
    description='Extract successful logins and re-ingest into a new table',
    schedule_interval=None,
)

def extract_and_reingest():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Extract rows with login_successful = 't'
    query = "SELECT * FROM login_attempt WHERE login_successful = 't';"
    df = pd.read_sql(query, engine)

    # Re-ingest into a new table
    df.to_sql('successful logins', engine, if_exists='replace', index=False)

extract_and_reingest_task = PythonOperator(
    task_id='extract_and_reingest_successful_logins',
    python_callable=extract_and_reingest,
    dag=dag,
)

extract_and_reingest_task
