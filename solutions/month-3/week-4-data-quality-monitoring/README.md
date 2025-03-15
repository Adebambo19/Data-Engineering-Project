# Week 4 Project: Data Quality Monitoring with DuckDB

## Problem Statement
- Implement **data quality checks** (e.g., null values, duplicates) in a data pipeline.
- Set up **monitoring and alerts** to notify stakeholders of data quality issues.
- Use **DuckDB** as the database for storing and querying data.

## Requirements
1. **Implement Data Quality Checks**:
   - Check for null values in critical columns.
   - Identify and handle duplicate records.
   - Validate data against predefined rules (e.g., data types, ranges).

2. **Set Up Monitoring and Alerts**:
   - Log data quality issues (e.g., null values, duplicates) for monitoring.
   - Send alerts (e.g., email, Slack) when data quality issues are detected.

3. **Store and Query Data in DuckDB**:
   - Load data into a **DuckDB** database.
   - Create a table schema to store the data and quality check results.
   - Ensure data integrity and handle errors (e.g., duplicate entries, schema mismatches).

4. **Query and Analyze Data**:
   - Use DuckDB to query the data and quality check results (e.g., filter, aggregate, join).
   - Generate insights or reports from the data.

5. **Logging and Monitoring**:
   - Implement logging to track data quality issues and monitoring processes.
   - Monitor data quality metrics (e.g., null count, duplicate count).

6. **Testing**:
   - Write unit tests to validate the data quality checks and monitoring processes.
   - Test data quality checks, monitoring, and DuckDB operations.

## Resources
- [DuckDB Documentation](https://duckdb.org/docs/): Official documentation for DuckDB.
- [Pytest Documentation](https://docs.pytest.org/en/stable/): Official documentation for writing tests using `pytest`.
- [Python Logging Documentation](https://docs.python.org/3/library/logging.html): Documentation for implementing logging in Python.
- [Sample Dataset](https://www.kaggle.com/datasets): A resource for finding sample datasets for testing.

## Setup Instructions
1. **Install Dependencies**:
   ```bash
   pip install duckdb pytest pandas
   ```
