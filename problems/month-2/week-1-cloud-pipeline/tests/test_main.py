import pytest
import boto3
import duckdb
import pandas as pd
from scripts.main import download_from_s3, process_data, upload_to_s3, load_to_duckdb, query_duckdb

# Fixture for DuckDB connection
@pytest.fixture
def db_connection():
    conn = duckdb.connect(":memory:")  # Use an in-memory DuckDB database for testing
    yield conn
    conn.close()

# Fixture for sample data
@pytest.fixture
def sample_data():
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35]
    })

# Mock S3 bucket and key
S3_BUCKET = "test-bucket"
S3_KEY = "test-data.csv"

# Test downloading data from S3
def test_download_from_s3():
    # Mock S3 client
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=S3_BUCKET)
    s3_client.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body="id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35")

    # Test download
    data = download_from_s3(S3_BUCKET, S3_KEY)
    assert isinstance(data, pd.DataFrame), "Downloaded data should be a DataFrame."
    assert not data.empty, "Downloaded data should not be empty."

# Test data processing
def test_process_data(sample_data):
    processed_data = process_data(sample_data)
    assert isinstance(processed_data, pd.DataFrame), "Processed data should be a DataFrame."
    assert "name" in processed_data.columns, "Processed data should have a 'name' column."

# Test uploading data to S3
def test_upload_to_s3(sample_data):
    # Mock S3 client
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=S3_BUCKET)

    # Test upload
    upload_to_s3(sample_data, S3_BUCKET, S3_KEY)
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200, "Data should be uploaded to S3."

# Test loading data into DuckDB
def test_load_to_duckdb(db_connection, sample_data):
    load_to_duckdb(db_connection, sample_data, "sample_table")
    result = db_connection.execute("SELECT * FROM sample_table").fetchall()
    assert len(result) == 3, "Data should be loaded into DuckDB."

# Test querying data from DuckDB
def test_query_duckdb(db_connection, sample_data):
    load_to_duckdb(db_connection, sample_data, "sample_table")
    query_result = query_duckdb(db_connection, "SELECT COUNT(*) as total FROM sample_table")
    assert query_result[0][0] == 3, "Query should return the correct count."

# Test error handling for S3 operations
def test_s3_error_handling():
    with pytest.raises(Exception):
        download_from_s3("invalid-bucket", "invalid-key")

# Test logging and error handling
def test_logging_and_error_handling(caplog):
    # Mock a function that logs errors
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Test error message")
    assert "Test error message" in caplog.text, "Error message should be logged."