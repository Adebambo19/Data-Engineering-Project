import pandas as pd
import duckdb
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_data(file_path):
    """
    Extract data from a CSV file.
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
    Transform the data (e.g., clean, filter, add columns).
    """
    try:
        # Example transformation: Convert names to uppercase
        data["name"] = data["name"].str.upper()
        logger.info("Data transformed successfully")
        return data
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def load_to_duckdb(conn, data, table_name):
    """
    Load data into a DuckDB table.
    """
    try:
        conn.register("temp_df", data)
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
        logger.info(f"Data loaded successfully into table {table_name}")
    except Exception as e:
        logger.error(f"Error loading data into DuckDB: {e}")
        raise

def query_duckdb(conn, query):
    """
    Query data from a DuckDB table.
    """
    try:
        result = conn.execute(query).fetchall()
        logger.info(f"Query executed successfully: {query}")
        return result
    except Exception as e:
        logger.error(f"Error querying data from DuckDB: {e}")
        raise