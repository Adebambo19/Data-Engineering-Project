# Week 1 Project: Hadoop MapReduce with Docker and DuckDB

## Problem Statement
- Set up a **Hadoop cluster** using Docker.
- Use **MapReduce** to process a large dataset (e.g., word count on text data).
- Store the processed data in a **DuckDB** database for further analysis.

## Requirements
1. **Set Up Hadoop Cluster**:
   - Use Docker to set up a Hadoop cluster with HDFS (Hadoop Distributed File System) and YARN.
   - Ensure the cluster is accessible for data processing.

2. **Process Data with MapReduce**:
   - Write a MapReduce job to process a large dataset (e.g., word count on text data).
   - Run the MapReduce job on the Hadoop cluster.

3. **Store Processed Data in DuckDB**:
   - Load the processed data into a **DuckDB** database.
   - Create a table schema to store the processed data.
   - Ensure data integrity and handle errors (e.g., duplicate entries, schema mismatches).

4. **Query and Analyze Data**:
   - Use DuckDB to query the processed data (e.g., filter, aggregate, join).
   - Generate insights or reports from the data.

5. **Logging and Monitoring**:
   - Implement logging to track the data processing pipeline.
   - Use Hadoop's built-in monitoring tools to track MapReduce jobs.

6. **Testing**:
   - Write unit tests to validate the data processing pipeline.
   - Test Hadoop cluster setup, MapReduce job execution, and DuckDB operations.

## Resources
- [Hadoop Documentation](https://hadoop.apache.org/docs/): Official documentation for Hadoop.
- [DuckDB Documentation](https://duckdb.org/docs/): Official documentation for DuckDB.
- [Pytest Documentation](https://docs.pytest.org/en/stable/): Official documentation for writing tests using `pytest`.
- [Python Logging Documentation](https://docs.python.org/3/library/logging.html): Documentation for implementing logging in Python.
- [Sample Text Dataset](https://www.kaggle.com/datasets): A resource for finding sample text datasets for testing.

## Setup Instructions
1. **Install Dependencies**:
   ```bash
   pip install duckdb pytest pandas

   ```
2. Set Up Hadoop Cluster Using Docker ` docker-compose up -d `