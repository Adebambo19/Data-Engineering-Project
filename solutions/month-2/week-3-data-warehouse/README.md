# Week 3 Project: Data Warehouse with DuckDB

## Problem Statement
- Design a **star schema** for a sample dataset (e.g., sales data).
- Load data into a **DuckDB** database.
- Run analytical queries to generate insights.

## Requirements
1. **Design the Star Schema**:
   - Identify the fact table and dimension tables for the dataset.
   - Define the relationships between the fact table and dimension tables.
   - Ensure the schema is normalized and optimized for analytical queries.

2. **Load Data into DuckDB**:
   - Extract data from the source (e.g., CSV, API, or cloud storage).
   - Transform the data to fit the star schema (e.g., clean, aggregate, join).
   - Load the transformed data into DuckDB tables.

3. **Run Analytical Queries**:
   - Write SQL queries to analyze the data (e.g., aggregations, filtering, joins).
   - Generate insights or reports from the data.

4. **Logging and Error Handling**:
   - Implement logging to track the data loading and querying process.
   - Handle exceptions gracefully (e.g., invalid data, connection errors).

5. **Testing**:
   - Write unit tests to validate the data loading and querying process.
   - Test the star schema design, data transformations, and analytical queries.

## Resources
- [DuckDB Documentation](https://duckdb.org/docs/): Official documentation for DuckDB.
- [Star Schema Design](https://en.wikipedia.org/wiki/Star_schema): Overview of star schema design.
- [Pytest Documentation](https://docs.pytest.org/en/stable/): Official documentation for writing tests using `pytest`.
- [Python Logging Documentation](https://docs.python.org/3/library/logging.html): Documentation for implementing logging in Python.
- [Sample Sales Dataset](https://www.kaggle.com/datasets): A resource for finding sample sales datasets for testing.

## Setup Instructions
1. **Install Dependencies**:
   ```bash
   pip install duckdb pytest pandas