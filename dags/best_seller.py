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
    'create_best_seller',
    default_args=default_args,
    description='Create best_seller table from product and order item data',
    schedule_interval=None,
)

# Function to extract data and create the best_seller table
def extract_and_create_best_seller():
    # Initialize the Postgres hook and get SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Query to extract product details
    product_details_query = 'SELECT id AS product_id, name AS product_name FROM product;'
    product_details_df = pd.read_sql(product_details_query, engine)

    # Query to extract order items that are in finished order value
    order_item_query = """
    SELECT oi.product_id, SUM(oi.amount) AS product_purchased
    FROM "order item" oi
    JOIN "finished order value" fov ON oi.order_id = fov.order_id
    GROUP BY oi.product_id;
    """
    order_item_df = pd.read_sql(order_item_query, engine)

    # Merge product details with order item details
    best_seller_df = pd.merge(product_details_df, order_item_df, on='product_id')

    # Re-ingest into a new table named best_seller
    best_seller_df.to_sql('best sellers', engine, if_exists='replace', index=False)

# Define the PythonOperator to execute the function
extract_and_create_task = PythonOperator(
    task_id='extract_and_create_best_seller',
    python_callable=extract_and_create_best_seller,
    dag=dag,
)

extract_and_create_task
