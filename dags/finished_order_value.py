from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'create_finished_order_value',
    default_args=default_args,
    description='Create finished_order_value table from order_item and product tables',
    schedule_interval=None,
)

def extract_and_create_finished_order_value():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Extract data from order_item table
    order_item_query = 'SELECT order_id, product_id, amount FROM "order_item";'
    order_item_df = pd.read_sql(order_item_query, engine)
    
    # Extract data from product table
    product_query = 'SELECT id AS product_id, name AS product_name, price FROM "product";'
    product_df = pd.read_sql(product_query, engine)

    # Merge order_item with product to get product names and prices
    merged_df = pd.merge(order_item_df, product_df, on='product_id', how='inner')

    # Calculate total value for each order item
    merged_df['total_value'] = merged_df['amount'] * merged_df['price']

    # Create product and quantity strings in the format Name(Amount)
    merged_df['product_quantity'] = merged_df.apply(lambda row: f"{row['product_name']}({row['amount']})", axis=1)

    # Group by order_id and aggregate data
    final_df = merged_df.groupby('order_id').agg({
        'product_quantity': lambda x: ', '.join(x),
        'total_value': 'sum'
    }).reset_index()

    # Rename columns to match the requirements
    final_df.columns = ['order_id', 'products_ordered', 'total_value']

    # Re-ingest into a new table named finished_order_value
    final_df.to_sql('finished_order_value', engine, if_exists='replace', index=False)

extract_and_create_task = PythonOperator(
    task_id='extract_and_create_finished_order_value',
    python_callable=extract_and_create_finished_order_value,
    dag=dag,
)

extract_and_create_task
