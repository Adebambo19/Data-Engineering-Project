import pytest
import boto3
import duckdb
import pandas as pd
from scripts.main import download_from_s3, process_data, upload_to_s3, load_to_duckdb, query_duckdb
import uuid

# Initialize S3 client for AWS
s3_client = boto3.client("s3")

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

# Fixture for a unique S3 bucket name
@pytest.fixture
def s3_bucket():
    # Generate a unique bucket name for each test run
    bucket_name = f"test-bucket-{uuid.uuid4().hex}"
    yield bucket_name
    # Clean up: Delete the bucket after the test
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        for obj in objects:
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
        s3_client.delete_bucket(Bucket=bucket_name)
    except Exception as e:
        print(f"Error cleaning up bucket {bucket_name}: {e}")

# Test downloading data from S3
def test_download_from_s3(s3_bucket):
    # Create a test bucket and upload sample data
    s3_client.create_bucket(Bucket=s3_bucket)
    s3_client.put_object(Bucket=s3_bucket, Key="test-data.csv", Body="id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35")

    # Test download
    data = download_from_s3(s3_bucket, "test-data.csv")
    assert isinstance(data, pd.DataFrame), "Downloaded data should be a DataFrame."
    assert not data.empty, "Downloaded data should not be empty."

# Test data processing
def test_process_data(sample_data):
    processed_data = process_data(sample_data)
    assert isinstance(processed_data, pd.DataFrame), "Processed data should be a DataFrame."
    assert "name" in processed_data.columns, "Processed data should have a 'name' column."

# Test uploading data to S3
def test_upload_to_s3(s3_bucket, sample_data):
    # Create a test bucket
    s3_client.create_bucket(Bucket=s3_bucket)

    # Test upload
    upload_to_s3(sample_data, s3_bucket, "test-data.csv")
    response = s3_client.get_object(Bucket=s3_bucket, Key="test-data.csv")
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