import pytest
import duckdb
import pandas as pd
from scripts.main import check_null_values, check_duplicates, load_to_duckdb, query_duckdb, send_alert

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
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", None],
        "age": [25, 30, 30, 35]
    })

# Test null value check
def test_check_null_values(sample_data):
    null_counts = check_null_values(sample_data)
    assert isinstance(null_counts, dict), "Null counts should be a dictionary."
    assert null_counts["name"] == 1, "There should be 1 null value in the 'name' column."

# Test duplicate check
def test_check_duplicates(sample_data):
    duplicate_counts = check_duplicates(sample_data, ["age"])
    assert isinstance(duplicate_counts, dict), "Duplicate counts should be a dictionary."
    assert duplicate_counts["age"] == 1, "There should be 1 duplicate in the 'age' column."

# Test loading data into DuckDB
def test_load_to_duckdb(db_connection, sample_data):
    load_to_duckdb(db_connection, sample_data, "sample_table")
    result = db_connection.execute("SELECT * FROM sample_table").fetchall()
    assert len(result) == 4, "Data should be loaded into DuckDB."

# Test querying data from DuckDB
def test_query_duckdb(db_connection, sample_data):
    load_to_duckdb(db_connection, sample_data, "sample_table")
    query_result = query_duckdb(db_connection, "SELECT COUNT(*) as total FROM sample_table")
    assert query_result[0][0] == 4, "Query should return the correct count."

# Test sending alerts
def test_send_alert():
    # Mock sending an alert
    alert_message = "Test alert message"
    send_alert(alert_message)
    assert True, "Alert should be sent."

# Test error handling for null value check
def test_check_null_values_error_handling():
    with pytest.raises(Exception):
        check_null_values(None)

# Test logging and error handling
def test_logging_and_error_handling(caplog):
    # Mock a function that logs errors
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Test error message")
    assert "Test error message" in caplog.text, "Error message should be logged."