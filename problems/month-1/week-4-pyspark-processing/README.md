# Week 4 Project: pyspark-processing

## Problem Statement
- This project involves using PySpark to process a large dataset (e.g., log files), perform aggregations and transformations, and store the results in a DuckDB database. The goal is to create a scalable data processing pipeline.

## Requirements
1. Load and Process Data with PySpark:

    - Load a large dataset (e.g., log files) into a PySpark DataFrame.

    - Perform data cleaning (e.g., handle missing values, remove duplicates).

    - Perform aggregations (e.g., count, sum, average) and transformations (e.g., filtering, grouping).

2. Store Processed Data in DuckDB:

    - Create a DuckDB database and table schema to store the processed data.

    - Load the transformed PySpark DataFrame into DuckDB.
    - Ensure data integrity and handle errors (e.g., duplicate entries, schema mismatches).

3. Query and Analyze Data:

    - Use DuckDB to query the processed data (e.g., filter, aggregate, join).

    - Generate insights or reports from the data.

4. Logging and Error Handling:

    - Implement logging to track the data processing pipeline (e.g., success, failures).

    - Handle exceptions gracefully (e.g., retry failed operations, skip invalid data).

## Resources
- PySpark Documentation: Official documentation for PySpark.

- DuckDB Documentation: Official documentation for DuckDB.

- Pytest Documentation: Official documentation for writing tests using pytest.

- Python Logging Documentation: Documentation for implementing logging in Python.

- Sample Log Dataset: A resource for finding sample log datasets for testing.
