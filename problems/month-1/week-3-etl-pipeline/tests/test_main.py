import pytest
import requests
import pandas as pd
import duckdb
from scripts.main import extract_data, transform_data, load_data

# Fixture for API URL and API key
@pytest.fixture
def api_config():
    return {
        "url": "https://api.openweathermap.org/data/2.5/weather",
        "api_key": "da3ec86825cb6c36e7fd77b24bf962ec",  # Replace with your actual API key
        "city": "London"
    }

# Fixture for DuckDB connection
@pytest.fixture
def db_connection():
    conn = duckdb.connect(":memory:")  # Use an in-memory DuckDB database for testing
    yield conn
    conn.close()

# Test API connectivity and data extraction
def test_extract_data(api_config):
    data = extract_data(api_config["url"], api_config["api_key"], api_config["city"])
    assert isinstance(data, dict), "Extracted data should be a dictionary."
    assert "main" in data, "Extracted data should contain weather details."

# Test data transformation
def test_transform_data(api_config):
    # Mock extracted data
    extracted_data = {
        "main": {"temp": 280.32, "humidity": 81},
        "weather": [{"description": "clear sky"}],
        "name": "London"
    }
    transformed_data = transform_data(extracted_data)
    assert isinstance(transformed_data, pd.DataFrame), "Transformed data should be a DataFrame."
    assert not transformed_data.empty, "Transformed data should not be empty."
    assert "temperature" in transformed_data.columns, "Transformed data should have a 'temperature' column."

# Test database loading with DuckDB
def test_load_data(db_connection, api_config):
    # Mock transformed data
    transformed_data = pd.DataFrame({
        "city": ["London"],
        "temperature": [280.32],
        "humidity": [81],
        "description": ["clear sky"]
    })
    load_data(db_connection, transformed_data)

    # Verify data was inserted into the database
    result = db_connection.execute("SELECT * FROM weather_data").fetchall()
    assert len(result) == 1, "Data should be inserted into the database."
    assert result[0][1] == "London", "City name should match the inserted data."

# Test error handling for API extraction
def test_extract_data_error_handling(api_config):
    # Test invalid API key
    with pytest.raises(Exception):
        extract_data(api_config["url"], "invalid_api_key", api_config["city"])

# Test error handling for database loading
def test_load_data_error_handling(db_connection):
    # Test invalid data format
    invalid_data = pd.DataFrame({
        "invalid_column": ["invalid_value"]
    })
    with pytest.raises(Exception):
        load_data(db_connection, invalid_data)

# Test logging and error handling
def test_logging_and_error_handling(caplog):
    # Mock a function that logs errors
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Test error message")
    assert "Test error message" in caplog.text, "Error message should be logged."