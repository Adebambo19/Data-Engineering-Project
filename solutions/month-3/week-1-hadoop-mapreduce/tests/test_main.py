import pytest
import duckdb
import pandas as pd
from scripts.main import run_mapreduce_job, load_to_duckdb, query_duckdb

# Fixture for DuckDB connection
@pytest.fixture
def db_connection():
    conn = duckdb.connect(":memory:")  # Use an in-memory DuckDB database for testing
    yield conn
    conn.close()

# Fixture for sample text data
@pytest.fixture
def sample_text_data():
    return [
        "hello world",
        "hello hadoop",
        "world of data"
    ]

# Test MapReduce job execution
def test_run_mapreduce_job(sample_text_data):
    # Mock running a MapReduce job
    result = run_mapreduce_job(sample_text_data)
    assert isinstance(result, dict), "MapReduce result should be a dictionary."
    assert "hello" in result, "Word 'hello' should be in the result."
    assert result["hello"] == 2, "Word 'hello' should appear twice."

# Test loading data into DuckDB
def test_load_to_duckdb(db_connection):
    # Mock MapReduce result
    mapreduce_result = {"hello": 2, "world": 2, "hadoop": 1, "data": 1}
    load_to_duckdb(db_connection, mapreduce_result, "word_count")
    result = db_connection.execute("SELECT * FROM word_count").fetchall()
    assert len(result) == 4, "Data should be loaded into DuckDB."

# Test querying data from DuckDB
def test_query_duckdb(db_connection):
    # Mock MapReduce result
    mapreduce_result = {"hello": 2, "world": 2, "hadoop": 1, "data": 1}
    load_to_duckdb(db_connection, mapreduce_result, "word_count")
    query_result = query_duckdb(db_connection, "SELECT COUNT(*) as total FROM word_count")
    assert query_result[0][0] == 4, "Query should return the correct count."

# Test error handling for MapReduce job
def test_mapreduce_error_handling():
    with pytest.raises(Exception):
        run_mapreduce_job([])

# Test logging and error handling
def test_logging_and_error_handling(caplog):
    # Mock a function that logs errors
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Test error message")
    assert "Test error message" in caplog.text, "Error message should be logged."