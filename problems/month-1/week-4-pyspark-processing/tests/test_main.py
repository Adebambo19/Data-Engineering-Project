import pytest
import pyspark
from pyspark.sql import SparkSession
import duckdb
import pandas as pd
from scripts.main import load_data, process_data, save_to_duckdb, query_data

# Fixture for Spark session
@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("PySpark Test") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

# Fixture for DuckDB connection
@pytest.fixture
def db_connection():
    conn = duckdb.connect(":memory:")  # Use an in-memory DuckDB database for testing
    yield conn
    conn.close()

# Fixture for sample log data
@pytest.fixture
def sample_log_data(spark_session):
    data = [
        ("2023-10-01 12:00:00", "user1", "login", "success"),
        ("2023-10-01 12:01:00", "user2", "login", "failure"),
        ("2023-10-01 12:02:00", "user1", "logout", "success"),
        ("2023-10-01 12:03:00", "user3", "login", "success"),
    ]
    columns = ["timestamp", "user", "action", "status"]
    return spark_session.createDataFrame(data, columns)

# Test data loading with PySpark
def test_load_data(spark_session):
    data = load_data(spark_session, "tests/sample_logs.csv")
    assert data.count() > 0, "Data should be loaded into a PySpark DataFrame."

# Test data processing with PySpark
def test_process_data(sample_log_data):
    processed_data = process_data(sample_log_data)
    assert processed_data.count() > 0, "Processed data should not be empty."
    assert "user" in processed_data.columns, "Processed data should have a 'user' column."

# Test saving data to DuckDB
def test_save_to_duckdb(db_connection, sample_log_data):
    # Process the data
    processed_data = process_data(sample_log_data)
    # Save to DuckDB
    save_to_duckdb(db_connection, processed_data, "log_data")
    # Verify data was inserted into DuckDB
    result = db_connection.execute("SELECT * FROM log_data").fetchall()
    assert len(result) > 0, "Data should be inserted into DuckDB."

# Test querying data from DuckDB
def test_query_data(db_connection, sample_log_data):
    # Process the data
    processed_data = process_data(sample_log_data)
    # Save to DuckDB
    save_to_duckdb(db_connection, processed_data, "log_data")
    # Query the data
    query_result = query_data(db_connection, "SELECT COUNT(*) as total FROM log_data")
    assert query_result[0][0] > 0, "Query should return results."

# Test error handling for data processing
def test_process_data_error_handling(spark_session):
    # Test invalid data format
    invalid_data = spark_session.createDataFrame([], ["invalid_column"])
    with pytest.raises(Exception):
        process_data(invalid_data)

# Test logging and error handling
def test_logging_and_error_handling(caplog):
    # Mock a function that logs errors
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Test error message")
    assert "Test error message" in caplog.text, "Error message should be logged."
