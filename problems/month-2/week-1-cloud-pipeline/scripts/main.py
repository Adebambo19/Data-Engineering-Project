import boto3
import pandas as pd
import duckdb
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_from_s3(bucket, key):
    """
    Download data from an S3 bucket and return it as a Pandas DataFrame.
    """
    try:
        s3_client = boto3.client("s3")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        data = pd.read_csv(obj["Body"])
        logger.info(f"Successfully downloaded data from S3: {bucket}/{key}")
        return data
    except Exception as e:
        logger.error(f"Error downloading data from S3: {e}")
        raise

def process_data(data):
    """
    Process the data (e.g., clean, transform).
    """
    try:
        # Example transformation: Convert names to uppercase
        data["name"] = data["name"].str.upper()
        logger.info("Data processing completed successfully.")
        return data
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

def upload_to_s3(data, bucket, key):
    """
    Upload data to an S3 bucket.
    """
    try:
        s3_client = boto3.client("s3")
        csv_buffer = data.to_csv(index=False).encode()
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer)
        logger.info(f"Successfully uploaded data to S3: {bucket}/{key}")
    except Exception as e:
        logger.error(f"Error uploading data to S3: {e}")
        raise

def load_to_duckdb(conn, data, table_name):
    """
    Load data into a DuckDB table.
    """
    try:
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM data")
        logger.info(f"Successfully loaded data into DuckDB table: {table_name}")
    except Exception as e:
        logger.error(f"Error loading data into DuckDB: {e}")
        raise

def query_duckdb(conn, query):
    """
    Query data from a DuckDB table.
    """
    try:
        result = conn.execute(query).fetchall()
        logger.info(f"Successfully executed query: {query}")
        return result
    except Exception as e:
        logger.error(f"Error querying data from DuckDB: {e}")
        raise
