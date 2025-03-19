import pandas as pd
import duckdb
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_null_values(data: pd.DataFrame) -> dict:
    """
    Check for null values in each column of the DataFrame.

    Args:
        data (pd.DataFrame): The input DataFrame.

    Returns:
        dict: A dictionary with column names as keys and null counts as values.
    """
    if data is None:
        raise ValueError("Input data cannot be None.")
    
    null_counts = data.isnull().sum().to_dict()
    logger.info(f"Null value counts: {null_counts}")
    return null_counts

def check_duplicates(data: pd.DataFrame, columns: list) -> dict:
    """
    Check for duplicate rows based on specified columns.

    Args:
        data (pd.DataFrame): The input DataFrame.
        columns (list): List of columns to check for duplicates.

    Returns:
        dict: A dictionary with column names as keys and duplicate counts as values.
    """
    if data is None or not columns:
        raise ValueError("Input data and columns cannot be None or empty.")
    
    # Ensure the specified columns exist in the DataFrame
    for column in columns:
        if column not in data.columns:
            raise KeyError(f"Column '{column}' not found in DataFrame.")
    
    # Count duplicates based on the specified columns
    duplicate_counts = data.duplicated(subset=columns).sum()
    logger.info(f"Duplicate counts for columns {columns}: {duplicate_counts}")
    return {"duplicates": duplicate_counts}

def load_to_duckdb(conn: duckdb.DuckDBPyConnection, data: pd.DataFrame, table_name: str):
    """
    Load a DataFrame into a DuckDB table.

    Args:
        conn (duckdb.DuckDBPyConnection): The DuckDB connection.
        data (pd.DataFrame): The DataFrame to load.
        table_name (str): The name of the table to create.
    """
    if conn is None or data is None or not table_name:
        raise ValueError("Connection, data, and table name cannot be None or empty.")
    
    conn.register(table_name, data)
    logger.info(f"Data loaded into DuckDB table: {table_name}")

def query_duckdb(conn: duckdb.DuckDBPyConnection, query: str):
    """
    Execute a query on a DuckDB database.

    Args:
        conn (duckdb.DuckDBPyConnection): The DuckDB connection.
        query (str): The SQL query to execute.

    Returns:
        list: The result of the query.
    """
    if conn is None or not query:
        raise ValueError("Connection and query cannot be None or empty.")
    
    result = conn.execute(query).fetchall()
    logger.info(f"Query executed: {query}")
    return result

def send_alert(message: str):
    """
    Send an alert message (mock implementation).

    Args:
        message (str): The alert message to send.
    """
    if not message:
        raise ValueError("Alert message cannot be empty.")
    
    logger.info(f"Alert sent: {message}")
    # In a real implementation, this could send an email, Slack message, etc.