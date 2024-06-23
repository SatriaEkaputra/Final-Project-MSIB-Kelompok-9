from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
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
    'finished_order_value',
    default_args=default_args,
    description='Create finished_order_value table from order_item, product, finished_order, and coupons tables',
    schedule_interval=None,
)

# Function to extract data and create the finished_order_value table with discounts
def extract_and_create_finished_order_value_with_discounts():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Query to extract necessary data from multiple tables
    query = """
    SELECT 
        oi.order_id, 
        oi.product_id, 
        oi.amount, 
        p.name AS product_name, 
        p.price,
        oi.coupon_id,
        c.discount_percent
    FROM "order item" oi
    JOIN "finished orders" fo ON oi.order_id = fo.id
    JOIN product p ON oi.product_id = p.id
    LEFT JOIN coupons c ON oi.coupon_id = c.id
    WHERE fo.status = 'FINISHED';
    """
    df = pd.read_sql(query, engine)

    # Add a column for product quantity with name and amount
    df['product_quantity'] = df.apply(lambda row: f"{row['product_name']}({row['amount']})", axis=1)
    
    # Calculate the total value of each order item
    df['total_value'] = df['amount'] * df['price']
    
    # Calculate the discount based on the coupon applied
    df['discount'] = df.apply(lambda row: (row['discount_percent'] * 0.01 * row['amount'] * row['price']) 
                              if pd.notnull(row['coupon_id']) and 1 <= row['coupon_id'] <= 10 else 0, axis=1)
    
    # Calculate the final value after discount
    df['final_value'] = df['total_value'] - df['discount']
    
    # Aggregate data by order_id and calculate the total values
    final_df = df.groupby('order_id').agg({
        'product_quantity': ', '.join,
        'total_value': 'sum',
        'discount': 'sum',
        'final_value': 'sum'
    }).reset_index()
    
    # Rename the columns for clarity
    final_df.columns = ['order_id', 'products_ordered', 'total_value', 'discount', 'final_value']
    
    # Re-ingest the final DataFrame into a new table named finished_order_value
    final_df.to_sql('finished order value', engine, if_exists='replace', index=False)

# Define the PythonOperator to execute the function
PythonOperator(
    task_id='extract_and_create_finished_order_value_with_discounts',
    python_callable=extract_and_create_finished_order_value_with_discounts,
    dag=dag,
)
