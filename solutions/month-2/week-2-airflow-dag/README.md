# Week 2 Project: Airflow DAG for ETL Pipeline with DuckDB

## Problem Statement
- Build an **Airflow DAG** to automate the ETL pipeline from Week 3.
- Schedule the pipeline to run daily.
- Use **DuckDB** as the database for storing and querying processed data.

## Requirements
1. **Design the ETL Pipeline**:
   - Extract data from a source (e.g., CSV, API, or cloud storage).
   - Transform the data (e.g., clean, filter, aggregate).
   - Load the transformed data into a **DuckDB** database.

2. **Build the Airflow DAG**:
   - Define tasks for each step of the ETL pipeline (e.g., extract, transform, load).
   - Schedule the DAG to run daily.
   - Ensure proper error handling and retries for each task.

3. **Use DuckDB for Data Storage**:
   - Create a DuckDB database and table schema to store the processed data.
   - Load the transformed data into DuckDB.
   - Ensure data integrity and handle errors (e.g., duplicate entries, schema mismatches).

4. **Query and Analyze Data**:
   - Use DuckDB to query the processed data (e.g., filter, aggregate, join).
   - Generate insights or reports from the data.

5. **Logging and Monitoring**:
   - Implement logging to track the ETL pipeline (e.g., success, failures).
   - Use Airflow's built-in monitoring tools to track DAG runs and task statuses.

6. **Testing**:
   - Write unit tests to validate the ETL pipeline and Airflow tasks.
   - Test data extraction, transformation, loading, and DuckDB operations.

## Resources
- [Apache Airflow Documentation](https://airflow.apache.org/docs/): Official documentation for Apache Airflow.
- [DuckDB Documentation](https://duckdb.org/docs/): Official documentation for DuckDB.
- [Pytest Documentation](https://docs.pytest.org/en/stable/): Official documentation for writing tests using `pytest`.
- [Python Logging Documentation](https://docs.python.org/3/library/logging.html): Documentation for implementing logging in Python.
- [Sample Dataset](https://www.kaggle.com/datasets): A resource for finding sample datasets for testing.

## Setup Instructions
1. **Install Dependencies**:
   ```bash
   pip install apache-airflow duckdb pytest pandas
2. **Initialize Airflow**:
```
    airflow db init
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
    airflow webserver --port 8080
    airflow scheduler
```