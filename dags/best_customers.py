from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'create_best_customers',
    default_args=default_args,
    description='Create best_customers table from customer login count and finished order value',
    schedule_interval=None,
)

# Function to extract data and create the best_customers table
def extract_and_create_best_customers():
    # Initialize the Postgres hook and get SQLAlchemy engine
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
    JOIN "order" o ON fov.order_id = o.id
    GROUP BY o.customer_id;
    """
    finished_order_value_df = pd.read_sql(finished_order_value_query, engine)

    # Merge dataframes to get best customers
    merged_df = pd.merge(login_count_df, customer_details_df, on='customer_id')
    merged_df = pd.merge(merged_df, finished_order_value_df, on='customer_id', how='left').fillna(0)

    # Reorder columns to place login_count between last_name and total_purchase
    merged_df = merged_df[['customer_id', 'first_name', 'last_name', 'login_count', 'total_purchase']]

    # Calculate rank based on login_count and total_purchase
    merged_df['rank'] = merged_df.sort_values(['login_count', 'total_purchase'], ascending=[False, False])\
                                 .reset_index(drop=True).index + 1

    # Re-ingest into a new table named best_customers
    merged_df.to_sql('best customers', engine, if_exists='replace', index=False)

# Define the PythonOperator to execute the function
extract_and_create_task = PythonOperator(
    task_id='extract_and_create_best_customers',
    python_callable=extract_and_create_best_customers,
    dag=dag,
)

extract_and_create_task
