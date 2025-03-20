import pandas as pd
import duckdb
import requests
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_data(source, source_type="csv"):
    """
    Ingest data from a CSV file or an API.
    
    Args:
        source (str): Path to the CSV file or API endpoint.
        source_type (str): Type of source ("csv" or "api").
    
    Returns:
        pd.DataFrame: Ingested data as a DataFrame.
    
    Raises:
        Exception: If the source type is invalid or data ingestion fails.
    """
    try:
        if source_type == "csv":
            # Read data from a CSV file
            data = pd.read_csv(source)
        elif source_type == "api":
            # Fetch data from an API
            response = requests.get(source)
            response.raise_for_status()
            data = pd.DataFrame(response.json())
        else:
            raise ValueError(f"Invalid source type: {source_type}")
        
        logger.info(f"Successfully ingested data from {source}")
        return data
    except Exception as e:
        logger.error(f"Failed to ingest data: {e}")
        raise

def process_data(data):
    """
    Process the ingested data (e.g., clean, transform).
    
    Args:
        data (pd.DataFrame): Input data as a DataFrame.
    
    Returns:
        pd.DataFrame: Processed data as a DataFrame.
    """
    try:
        # Example: Clean data by dropping rows with missing values
        processed_data = data.dropna()
        
        # Example: Transform data by renaming columns
        if "name" in processed_data.columns:
            processed_data["name"] = processed_data["name"].str.strip().str.title()
        
        logger.info("Data processing completed successfully.")
        return processed_data
    except Exception as e:
        logger.error(f"Failed to process data: {e}")
        raise

def load_to_duckdb(conn, data, table_name):
    """
    Load processed data into a DuckDB table.
    
    Args:
        conn (duckdb.Connection): DuckDB connection object.
        data (pd.DataFrame): Processed data as a DataFrame.
        table_name (str): Name of the table to create/insert into.
    """
    try:
        # Create or replace the table with the processed data
        conn.register(table_name, data)
        logger.info(f"Data loaded into DuckDB table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to load data into DuckDB: {e}")
        raise

def query_duckdb(conn, query):
    """
    Query data from a DuckDB table.
    
    Args:
        conn (duckdb.Connection): DuckDB connection object.
        query (str): SQL query to execute.
    
    Returns:
        list: Query results as a list of tuples.
    """
    try:
        result = conn.execute(query).fetchall()
        logger.info(f"Query executed successfully: {query}")
        return result
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise
