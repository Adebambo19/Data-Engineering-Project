import pandas as pd
import duckdb
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_data(file_path):
    """
    Extracts data from a CSV file.
    """
    try:
        data = pd.read_csv(file_path)
        logger.info(f"Data extracted successfully from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

def transform_data(data):
    """
    Transforms the data (e.g., cleaning, filtering).
    """
    try:
        # Example transformation: Convert names to uppercase
        data["name"] = data["name"].str.upper()
        logger.info("Data transformed successfully.")
        return data
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def load_to_duckdb(conn, data, table_name):
    """
    Loads data into a DuckDB table.
    """
    try:
        conn.register(table_name, data)
        logger.info(f"Data loaded successfully into table {table_name}")
    except Exception as e:
        logger.error(f"Error loading data into DuckDB: {e}")
        raise

def query_duckdb(conn, query):
    """
    Queries data from a DuckDB table.
    """
    try:
        result = conn.execute(query).fetchall()
        logger.info(f"Query executed successfully: {query}")
        return result
    except Exception as e:
        logger.error(f"Error querying DuckDB: {e}")
        raise