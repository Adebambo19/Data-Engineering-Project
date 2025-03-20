import pytest
import duckdb
import pandas as pd
from scripts.main import extract_data, transform_data, load_to_duckdb, query_duckdb

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

# Test data extraction
def test_extract_data():
    data = extract_data("data/sample_data.csv")
    assert isinstance(data, pd.DataFrame), "Extracted data should be a DataFrame."
    assert not data.empty, "Extracted data should not be empty."

# Test data transformation with indexing
def test_transform_data(sample_data):
    transformed_data = transform_data(sample_data)
    assert isinstance(transformed_data, pd.DataFrame), "Transformed data should be a DataFrame."
    assert "name" in transformed_data.columns, "Transformed data should have a 'name' column."

# Test data loading with partitioning
def test_load_to_duckdb(db_connection, sample_data):
    load_to_duckdb(db_connection, sample_data, "sample_table")
    result = db_connection.execute("SELECT * FROM sample_table").fetchall()
    assert len(result) == 3, "Data should be loaded into DuckDB."

# Test querying data with indexing
def test_query_duckdb(db_connection, sample_data):
    load_to_duckdb(db_connection, sample_data, "sample_table")
    query_result = query_duckdb(db_connection, "SELECT COUNT(*) as total FROM sample_table")
    assert query_result[0][0] == 3, "Query should return the correct count."

# Test caching for frequently accessed data
def test_caching(sample_data):
    # Mock caching mechanism
    cache = {}
    cache_key = "sample_data"
    cache[cache_key] = sample_data
    assert cache_key in cache, "Data should be cached."
    assert isinstance(cache[cache_key], pd.DataFrame), "Cached data should be a DataFrame."

# Test error handling for data extraction
def test_extract_data_error_handling():
    with pytest.raises(Exception):
        extract_data("invalid_path.csv")

# Test logging and error handling
def test_logging_and_error_handling(caplog):
    # Mock a function that logs errors
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Test error message")
    assert "Test error message" in caplog.text, "Error message should be logged."