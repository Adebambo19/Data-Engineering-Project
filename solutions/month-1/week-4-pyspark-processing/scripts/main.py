import pyspark
from pyspark.sql import SparkSession, DataFrame
import duckdb
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data(spark: SparkSession, file_location: str) -> DataFrame:
    """
    Load data from a file location into a PySpark DataFrame.
    """
    try:
        logger.info(f"Loading data from {file_location}")
        df = spark.read.csv(file_location, header=True, inferSchema=True)
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def process_data(df: DataFrame) -> DataFrame:
    """
    Process the data by performing transformations.
    """
    try:
        logger.info("Processing data")
        # Example transformation: Filter rows where status is 'success'
        processed_df = df.filter(df["status"] == "success")
        return processed_df
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

def save_to_duckdb(conn: duckdb.DuckDBPyConnection, df: DataFrame, table_name: str):
    """
    Save a PySpark DataFrame to a DuckDB table.
    """
    try:
        logger.info(f"Saving data to DuckDB table {table_name}")
        # Convert PySpark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()
        # Save to DuckDB
        conn.register(table_name, pandas_df)
    except Exception as e:
        logger.error(f"Error saving data to DuckDB: {e}")
        raise

def query_data(conn: duckdb.DuckDBPyConnection, query: str):
    """
    Query data from DuckDB.
    """
    try:
        logger.info(f"Executing query: {query}")
        result = conn.execute(query).fetchall()
        return result
    except Exception as e:
        logger.error(f"Error querying data: {e}")
        raise