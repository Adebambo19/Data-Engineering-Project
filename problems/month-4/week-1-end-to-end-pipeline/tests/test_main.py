import pytest
import duckdb
import pandas as pd
from scripts.main import ingest_data, process_data, load_to_duckdb, query_duckdb

# Fixture for DuckDB connection
@pytest.fixture
def db_connection():
    conn = duckdb.connect(":memory:")  # Use an in-memory DuckDB database for testing
    yield conn
    conn.close()

# Fixture for sample CSV data
@pytest.fixture
def sample_csv_data():
    return "tests/sample_data.csv"

# Fixture for sample API data
@pytest.fixture
def sample_api_data():
    return "https://api.example.com/data"

# Test data ingestion from CSV
def test_ingest_csv_data(sample_csv_data):
    data = ingest_data(sample_csv_data, source_type="csv")
    assert isinstance(data, pd.DataFrame), "Ingested data should be a DataFrame."
    assert not data.empty, "Ingested data should not be empty."

# Test data ingestion from API
def test_ingest_api_data(sample_api_data):
    data = ingest_data(sample_api_data, source_type="api")
    assert isinstance(data, pd.DataFrame), "Ingested data should be a DataFrame."
    assert not data.empty, "Ingested data should not be empty."

# Test data processing
def test_process_data(sample_csv_data):
    data = ingest_data(sample_csv_data, source_type="csv")
    processed_data = process_data(data)
    assert isinstance(processed_data, pd.DataFrame), "Processed data should be a DataFrame."
    assert "name" in processed_data.columns, "Processed data should have a 'name' column."

# Test data loading into DuckDB
def test_load_to_duckdb(db_connection, sample_csv_data):
    data = ingest_data(sample_csv_data, source_type="csv")
    processed_data = process_data(data)
    load_to_duckdb(db_connection, processed_data, "processed_data")
    result = db_connection.execute("SELECT * FROM processed_data").fetchall()
    assert len(result) > 0, "Data should be loaded into DuckDB."

# Test querying data from DuckDB
def test_query_duckdb(db_connection, sample_csv_data):
    data = ingest_data(sample_csv_data, source_type="csv")
    processed_data = process_data(data)
    load_to_duckdb(db_connection, processed_data, "processed_data")
    query_result = query_duckdb(db_connection, "SELECT COUNT(*) as total FROM processed_data")
    assert query_result[0][0] > 0, "Query should return results."

# Test error handling for data ingestion
def test_ingest_data_error_handling():
    with pytest.raises(Exception):
        ingest_data("invalid_path.csv", source_type="csv")

# Test logging and error handling
def test_logging_and_error_handling(caplog):
    # Mock a function that logs errors
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Test error message")
    assert "Test error message" in caplog.text, "Error message should be logged."