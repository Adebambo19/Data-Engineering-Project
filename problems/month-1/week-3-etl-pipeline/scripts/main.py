import requests
import pandas as pd
import duckdb
api_config = {'api_key': 'da3ec86825cb6c36e7fd77b24bf962ec', 'city': 'London', 'url': 'https://api.openweathermap.org/data/2.5/weather'}
def db_connection():
    conn = duckdb.connect(":memory:")  # Use an in-memory DuckDB database for testing
    yield conn
    conn.close()

# Extract data
def extract_data(url, api_key, city):
    # write your code here
    url = api_config["url"]
    api_key = api_config['api_key']
    city = api_config['city']
    data = requests.get(url = url, params = city, headers = api_key,)   
    

# Load data
def load_data():

    # write your code here
    pass

# Transform data
def transform_data():
    # write your code here
    pass


