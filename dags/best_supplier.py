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
    'create_best_suppliers',
    default_args=default_args,
    description='Create best_suppliers table from supplier, product, product category, and best seller data',
    schedule_interval=None,
)

# Function to extract data and create the best_suppliers table
def extract_and_create_best_suppliers():
    # Initialize the Postgres hook and get SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Query to extract supplier details
    supplier_details_query = 'SELECT id AS supplier_id, name AS supplier_name, country AS supplier_country FROM supplier;'
    supplier_details_df = pd.read_sql(supplier_details_query, engine)

    # Query to extract product details
    product_query = 'SELECT id AS product_id, supplier_id, category_id FROM product;'
    product_df = pd.read_sql(product_query, engine)

    # Query to extract product category details
    product_category_query = 'SELECT id AS category_id, name AS category_name FROM "product category";'
    product_category_df = pd.read_sql(product_category_query, engine)

    # Query to extract best seller details
    best_seller_query = 'SELECT product_id, product_purchased FROM "best sellers";'
    best_seller_df = pd.read_sql(best_seller_query, engine)

    # Merge product details with category details to get category names
    product_with_category_df = pd.merge(product_df, product_category_df, on='category_id')

    # Find the most common category for each supplier
    supplier_forte_df = product_with_category_df.groupby(['supplier_id', 'category_id', 'category_name'])\
        .size()\
        .reset_index(name='count')\
        .sort_values(['supplier_id', 'count'], ascending=[True, False])

    # Extract the category with the highest count per supplier
    supplier_forte_df = supplier_forte_df.groupby('supplier_id').first().reset_index()

    # Merge supplier details with the most common category details
    best_suppliers_df = pd.merge(supplier_details_df, supplier_forte_df[['supplier_id', 'category_name']], on='supplier_id')
    best_suppliers_df = best_suppliers_df.rename(columns={'category_name': 'forte'})

    # Calculate the sum of product_purchased for each supplier
    product_supplier_df = pd.merge(product_df, best_seller_df, on='product_id')
    supplier_sales_df = product_supplier_df.groupby('supplier_id').agg({'product_purchased': 'sum'}).reset_index()
    supplier_sales_df = supplier_sales_df.rename(columns={'product_purchased': 'total_product_purchased'})

    # Merge the sales data into best suppliers data
    best_suppliers_df = pd.merge(best_suppliers_df, supplier_sales_df, on='supplier_id', how='left').fillna(0)

    # Re-ingest into a new table named best_suppliers
    best_suppliers_df.to_sql('best suppliers', engine, if_exists='replace', index=False)

# Define the PythonOperator to execute the function
extract_and_create_task = PythonOperator(
    task_id='extract_and_create_best_suppliers',
    python_callable=extract_and_create_best_suppliers,
    dag=dag,
)

extract_and_create_task
