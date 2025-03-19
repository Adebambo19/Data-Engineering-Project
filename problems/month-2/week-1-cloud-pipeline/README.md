# Week 1 Project: Cloud Data Pipeline with DuckDB

## Problem Statement
- Set up a cloud storage bucket (e.g., AWS S3) to store raw data.
- Use a serverless function (e.g., AWS Lambda) to process the data.
- Load the processed data into a **DuckDB** database for analysis and querying.

## Requirements
1. **Cloud Storage Setup**:
   - Create a cloud storage bucket (e.g., AWS S3) and upload raw data files (e.g., CSV, JSON).
   - Ensure proper access permissions and configurations for the bucket.

2. **Data Processing with Serverless Function**:
   - Write a serverless function (e.g., AWS Lambda) to:
     - Download data from the cloud storage bucket.
     - Perform data cleaning and transformations (e.g., handle missing values, filter rows).
     - Upload the processed data back to the cloud storage bucket.

3. **Load Data into DuckDB**:
   - Download the processed data from the cloud storage bucket.
   - Load the data into a **DuckDB** database.
   - Create a table schema to store the processed data.

4. **Query and Analyze Data**:
   - Use DuckDB to query the processed data (e.g., filter, aggregate, join).
   - Generate insights or reports from the data.

5. **Logging and Error Handling**:
   - Implement logging to track the data processing pipeline (e.g., success, failures).
   - Handle exceptions gracefully (e.g., retry failed operations, skip invalid data).

6. **Testing**:
   - Write unit tests to validate the data processing pipeline.
   - Test cloud storage operations, serverless function logic, and DuckDB operations.

## Resources
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/): Official documentation for AWS S3.
- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/): Official documentation for AWS Lambda.
- [DuckDB Documentation](https://duckdb.org/docs/): Official documentation for DuckDB.
- [Pytest Documentation](https://docs.pytest.org/en/stable/): Official documentation for writing tests using `pytest`.
- [Python Logging Documentation](https://docs.python.org/3/library/logging.html): Documentation for implementing logging in Python.
- [Sample Dataset](https://www.kaggle.com/datasets): A resource for finding sample datasets for testing.

## Setup Instructions
1. **Install Dependencies**:
   ```bash
   pip install boto3 duckdb pytest
   ```
1. **Export your AWS credentials**:
   ```bash
   export AWS_ACCESS_KEY_ID="<your AWS access key>"
   export AWS_SECRET_ACCESS_KEY="<your AWS secret key>"
   ```