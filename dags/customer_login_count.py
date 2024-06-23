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
    'customer_login_count_with_details',
    default_args=default_args,
    description='Count successful logins per customer and store in a new table with customer details',
    schedule_interval=None,
)

# Function to extract data, count logins, and re-ingest with customer details
def extract_and_reingest():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Extract data from the successful_logins table
    query_successful_logins = 'SELECT customer_id FROM "successful logins";'
    df_logins = pd.read_sql(query_successful_logins, engine)
    
    # Count the occurrences of each customer_id
    login_counts = df_logins['customer_id'].value_counts().reset_index()
    login_counts.columns = ['customer_id', 'login_count']

    # Extract customer details from the customers table
    customer_ids = tuple(login_counts['customer_id'].unique())
    query_customers = f"""
    SELECT id AS customer_id, first_name, last_name, gender, address, zip_code
    FROM customers
    WHERE id IN {customer_ids};
    """
    df_customers = pd.read_sql(query_customers, engine)

    # Merge the login counts with customer details
    result_df = pd.merge(login_counts, df_customers, left_on='customer_id', right_on='customer_id', how='inner')
    
    # Re-ingest into a new table named customer_login_count
    result_df.to_sql("customer login count", engine, if_exists='replace', index=False)

# Define the PythonOperator to execute the function
extract_and_reingest_task = PythonOperator(
    task_id='extract_and_reingest',
    python_callable=extract_and_reingest,
    dag=dag,
)

extract_and_reingest_task
