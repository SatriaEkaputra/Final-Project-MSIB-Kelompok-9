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
    'customer_login_count',
    default_args=default_args,
    description='Count successful logins per customer and store in a new table',
    schedule_interval=None,
)

def extract_and_count_logins():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Extract data from the successful_logins table
    query = 'SELECT customer_id FROM "successful_logins";'
    df = pd.read_sql(query, engine)
    
    # Count the occurrences of each customer_id
    login_counts = df['customer_id'].value_counts().reset_index()
    login_counts.columns = ['customer_id', 'login_count']
    
    # Re-ingest into a new table named customer_login_count
    login_counts.to_sql('customer login count', engine, if_exists='replace', index=False)

extract_and_count_task = PythonOperator(
    task_id='extract_and_count_logins',
    python_callable=extract_and_count_logins,
    dag=dag,
)

extract_and_count_task
