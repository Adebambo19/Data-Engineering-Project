import duckdb
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data(db_connection, data, table_name):
    """
    Load data into DuckDB.
    Raises an Exception if the data is invalid or the table_name is invalid.
    """
    try:
        # Validate input data
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input data must be a Pandas DataFrame.")
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Table name must be a non-empty string.")
        if data.empty:
            raise ValueError("Input data cannot be empty.")

        # Register the DataFrame as a table in DuckDB
        db_connection.register(table_name, data)
        logger.info(f"Data loaded into table: {table_name}")
    except Exception as e:
        logger.error(f"Error loading data into table {table_name}: {e}")
        raise  # Re-raise the exception to fail the test

def transform_data(sales_data, customer_data, product_data):
    """
    Transform data by joining sales, customer, and product data.
    """
    try:
        # Merge sales data with customer and product data
        merged_data = sales_data.merge(customer_data, on="customer_id", how="left")
        merged_data = merged_data.merge(product_data, on="product_id", how="left")
        logger.info("Data transformation completed.")
        return merged_data
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def create_star_schema(db_connection):
    """
    Create a star schema in DuckDB.
    """
    try:
        # Create fact table
        db_connection.execute("""
            CREATE TABLE sales_fact AS
            SELECT 
                s.order_id,
                s.customer_id,
                s.product_id,
                s.quantity,
                s.price,
                s.order_date,
                c.customer_name,
                c.city,
                p.product_name,
                p.category
            FROM sales s
            JOIN customers c ON s.customer_id = c.customer_id
            JOIN products p ON s.product_id = p.product_id
        """)
        logger.info("Star schema created: sales_fact table.")
    except Exception as e:
        logger.error(f"Error creating star schema: {e}")
        raise

def run_analytical_query(db_connection, query):
    """
    Run an analytical query on the DuckDB database.
    """
    try:
        result = db_connection.execute(query).fetchall()
        logger.info("Analytical query executed successfully.")
        return result
    except Exception as e:
        logger.error(f"Error running analytical query: {e}")
        raise
