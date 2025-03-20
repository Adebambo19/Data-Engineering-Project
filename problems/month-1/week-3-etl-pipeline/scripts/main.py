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
    pass

def transform_data(data):
    """
    Transform the extracted weather data into a structured format.
    """
    pass

def load_data(db_connection, data):
    """
    Load the transformed data into a DuckDB database.
    """
    pass
