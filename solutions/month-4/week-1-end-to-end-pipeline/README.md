# Week 1 Project: End-to-End Data Pipeline with DuckDB

## Problem Statement
- Build a complete data pipeline:
  - Ingest data from multiple sources (e.g., CSV, API, cloud storage).
  - Process and transform the data (e.g., clean, aggregate, join).
  - Load the processed data into a **DuckDB** database.
  - Visualize the data using a BI tool (e.g., Tableau).

## Requirements
1. **Data Ingestion**:
   - Ingest data from multiple sources (e.g., CSV files, APIs, cloud storage).
   - Handle different data formats (e.g., JSON, CSV, Parquet).

2. **Data Processing and Transformation**:
   - Clean the data (e.g., handle missing values, remove duplicates).
   - Perform transformations (e.g., filtering, aggregation, joins).
   - Ensure data consistency and quality.

3. **Data Loading**:
   - Load the processed data into a **DuckDB** database.
   - Create a table schema to store the processed data.
   - Ensure data integrity and handle errors (e.g., duplicate entries, schema mismatches).

4. **Data Visualization**:
   - Connect DuckDB to a BI tool (e.g., Tableau) for visualization.
   - Create dashboards and reports to visualize the data.

5. **Logging and Monitoring**:
   - Implement logging to track the data pipeline (e.g., success, failures).
   - Monitor pipeline performance (e.g., execution time, memory usage).

6. **Testing**:
   - Write unit tests to validate the data pipeline.
   - Test data ingestion, processing, loading, and visualization.

## Resources
- [DuckDB Documentation](https://duckdb.org/docs/): Official documentation for DuckDB.
- [Tableau Documentation](https://help.tableau.com/current/pro/desktop/en-us/default.htm): Official documentation for Tableau.
- [Pytest Documentation](https://docs.pytest.org/en/stable/): Official documentation for writing tests using `pytest`.
- [Python Logging Documentation](https://docs.python.org/3/library/logging.html): Documentation for implementing logging in Python.
- [Sample Dataset](https://www.kaggle.com/datasets): A resource for finding sample datasets for testing.

## Setup Instructions
1. **Install Dependencies**:
   ```bash
   pip install duckdb pytest pandas requests
   ```