import duckdb
import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_mapreduce_job(text_data):
    """
    Runs a MapReduce job to count word frequencies in the given text data.
    
    Args:
        text_data (list): A list of strings (text data).
    
    Returns:
        dict: A dictionary where keys are words and values are their frequencies.
    
    Raises:
        Exception: If the input text_data is empty.
    """
    if not text_data:
        logger.error("Input text data is empty.")
        raise Exception("Input text data is empty.")

    word_count = {}
    for line in text_data:
        words = line.split()
        for word in words:
            word_count[word] = word_count.get(word, 0) + 1

    logger.info("MapReduce job completed successfully.")
    return word_count

def load_to_duckdb(connection, data, table_name):
    """
    Loads data into a DuckDB table.
    
    Args:
        connection: A DuckDB connection object.
        data (dict): A dictionary containing the data to be loaded.
        table_name (str): The name of the table to create.
    """
    # Convert the dictionary to a DataFrame
    df = pd.DataFrame(list(data.items()), columns=["word", "count"])

    # Create a table and load the data
    connection.execute(f"CREATE TABLE {table_name} (word VARCHAR, count INTEGER)")
    connection.register("df_temp", df)
    connection.execute(f"INSERT INTO {table_name} SELECT * FROM df_temp")

    logger.info(f"Data loaded into DuckDB table '{table_name}'.")

def query_duckdb(connection, query):
    """
    Executes a SQL query on a DuckDB database.
    
    Args:
        connection: A DuckDB connection object.
        query (str): The SQL query to execute.
    
    Returns:
        list: The result of the query as a list of tuples.
    """
    result = connection.execute(query).fetchall()
    logger.info(f"Query executed: {query}")
    return result
