from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scripts.main import extract_data, transform_data, load_to_duckdb, query_duckdb
import duckdb

# Define the DAG
dag = DAG(
    "etl_pipeline_dag",
    description="ETL Pipeline DAG",
    schedule_interval="@daily",
    start_date=datetime(2023, 10, 1),
    catchup=False,
)

# Define the tasks
def run_etl():
    # Extract data
    data = extract_data("data/sample_data.csv")
    
    # Transform data
    transformed_data = transform_data(data)
    
    # Load data into DuckDB
    conn = duckdb.connect(":memory:")
    load_to_duckdb(conn, transformed_data, "sample_table")
    
    # Query data from DuckDB
    query_result = query_duckdb(conn, "SELECT * FROM sample_table")
    print(query_result)

# Create the tasks
etl_task = PythonOperator(
    task_id="run_etl",
    python_callable=run_etl,
    dag=dag,
)

# Set task dependencies
etl_task