# Week 2 Project: ETL Pipeline Optimization with DuckDB

## Problem Statement
- Optimize an existing ETL pipeline for **performance** and **cost**.
- Use **partitioning**, **indexing**, and **caching** techniques to improve efficiency.
- Store the processed data in a **DuckDB** database for further analysis.

## Requirements
1. **Optimize Data Extraction**:
   - Use efficient data extraction techniques (e.g., batch processing, parallel downloads).
   - Handle large datasets by partitioning data during extraction.

2. **Optimize Data Transformation**:
   - Use indexing to speed up data transformations (e.g., filtering, joins).
   - Implement caching for frequently accessed data.

3. **Optimize Data Loading**:
   - Use DuckDB's built-in features (e.g., partitioning, indexing) to optimize data loading.
   - Ensure data integrity and handle errors (e.g., duplicate entries, schema mismatches).

4. **Query and Analyze Data**:
   - Use DuckDB to query the processed data (e.g., filter, aggregate, join).
   - Generate insights or reports from the data.

5. **Logging and Monitoring**:
   - Implement logging to track the ETL pipeline (e.g., success, failures).
   - Monitor performance metrics (e.g., execution time, memory usage).

6. **Testing**:
   - Write unit tests to validate the optimized ETL pipeline.
   - Test data extraction, transformation, loading, and DuckDB operations.

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

