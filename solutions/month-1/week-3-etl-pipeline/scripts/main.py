import requests
import pandas as pd
import duckdb
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_data(url, api_key, city):
    """
    Extract weather data from the OpenWeatherMap API.
    """
    try:
        params = {
            "q": city,
            "appid": api_key,
            "units": "metric"
        }
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error extracting data: {e}")
        raise

def transform_data(data):
    """
    Transform the extracted weather data into a structured format.
    """
    try:
        transformed_data = {
            "city": [data["name"]],
            "temperature": [data["main"]["temp"]],
            "humidity": [data["main"]["humidity"]],
            "description": [data["weather"][0]["description"]]
        }
        return pd.DataFrame(transformed_data)
    except KeyError as e:
        logger.error(f"Error transforming data: Missing key {e}")
        raise
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def load_data(db_connection, data):
    """
    Load the transformed data into a DuckDB database.
    """
    try:
        # Create a table if it doesn't exist
        db_connection.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                city VARCHAR,
                temperature FLOAT,
                humidity INT,
                description VARCHAR
            )
        """)
        
        # Insert data into the table
        db_connection.execute("INSERT INTO weather_data SELECT * FROM data")
        logger.info("Data loaded successfully into DuckDB.")
    except Exception as e:
        logger.error(f"Error loading data into DuckDB: {e}")
        raise