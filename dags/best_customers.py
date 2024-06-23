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
    'create_best_customers',
    default_args=default_args,
    description='Create best_customers table from customer login count and finished order value',
    schedule_interval=None,
)

def extract_and_create_best_customers():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Query to extract customer login count
    login_count_query = 'SELECT customer_id, login_count FROM "customer login count";'
    login_count_df = pd.read_sql(login_count_query, engine)

    # Query to extract customer details
    customer_details_query = 'SELECT id AS customer_id, first_name, last_name FROM customers;'
    customer_details_df = pd.read_sql(customer_details_query, engine)

    # Query to extract finished order value
    finished_order_value_query = """
    SELECT o.customer_id, SUM(fov.final_value) AS total_purchase
    FROM "finished order value" fov
    JOIN "order" o ON fov.id = o.id
    GROUP BY o.customer_id;
    """
    finished_order_value_df = pd.read_sql(finished_order_value_query, engine)

    # Merge dataframes to get best customers
    merged_df = pd.merge(login_count_df, customer_details_df, on='customer_id')
    merged_df = pd.merge(merged_df, finished_order_value_df, on='customer_id', how='left').fillna(0)

    # Re-ingest into a new table named best_customers
    merged_df.to_sql('best customers', engine, if_exists='replace', index=False)

extract_and_create_task = PythonOperator(
    task_id='extract_and_create_best_customers',
    python_callable=extract_and_create_best_customers,
    dag=dag,
)

extract_and_create_task
