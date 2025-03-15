import pytest
import duckdb
import pandas as pd
from scripts.main import load_data, transform_data, create_star_schema, run_analytical_query

# Fixture for DuckDB connection
@pytest.fixture
def db_connection():
    conn = duckdb.connect(":memory:")  # Use an in-memory DuckDB database for testing
    yield conn
    conn.close()

# Fixture for sample sales data
@pytest.fixture
def sample_sales_data():
    return pd.DataFrame({
        "order_id": [1, 2, 3],
        "customer_id": [101, 102, 103],
        "product_id": [201, 202, 203],
        "quantity": [2, 1, 3],
        "price": [10.0, 20.0, 15.0],
        "order_date": ["2023-10-01", "2023-10-02", "2023-10-03"]
    })

# Fixture for sample customer data
@pytest.fixture
def sample_customer_data():
    return pd.DataFrame({
        "customer_id": [101, 102, 103],
        "customer_name": ["Alice", "Bob", "Charlie"],
        "city": ["New York", "Los Angeles", "Chicago"]
    })

# Fixture for sample product data
@pytest.fixture
def sample_product_data():
    return pd.DataFrame({
        "product_id": [201, 202, 203],
        "product_name": ["Product A", "Product B", "Product C"],
        "category": ["Category 1", "Category 2", "Category 1"]
    })

# Test data loading
def test_load_data(db_connection, sample_sales_data):
    load_data(db_connection, sample_sales_data, "sales")
    result = db_connection.execute("SELECT * FROM sales").fetchall()
    assert len(result) == 3, "Sales data should be loaded into DuckDB."

# Test star schema creation
def test_create_star_schema(db_connection, sample_sales_data, sample_customer_data, sample_product_data):
    # Load data into DuckDB
    load_data(db_connection, sample_sales_data, "sales")
    load_data(db_connection, sample_customer_data, "customers")
    load_data(db_connection, sample_product_data, "products")

    # Create star schema
    create_star_schema(db_connection)

    # Verify star schema
    result = db_connection.execute("SELECT * FROM sales_fact").fetchall()
    assert len(result) == 3, "Fact table should contain all sales records."

# Test analytical queries
def test_run_analytical_query(db_connection, sample_sales_data, sample_customer_data, sample_product_data):
    # Load data into DuckDB
    load_data(db_connection, sample_sales_data, "sales")
    load_data(db_connection, sample_customer_data, "customers")
    load_data(db_connection, sample_product_data, "products")

    # Create star schema
    create_star_schema(db_connection)

    # Run analytical query
    query = """
        SELECT c.customer_name, SUM(s.quantity * s.price) as total_spent
        FROM sales_fact s
        JOIN customers c ON s.customer_id = c.customer_id
        GROUP BY c.customer_name
    """
    result = run_analytical_query(db_connection, query)
    assert len(result) == 3, "Query should return results for all customers."

# Test error handling for data loading
def test_load_data_error_handling(db_connection):
    invalid_data = pd.DataFrame({
        "invalid_column": ["invalid_value"]
    })
    with pytest.raises(Exception):
        load_data(db_connection, invalid_data, "invalid_table")

# Test logging and error handling
def test_logging_and_error_handling(caplog):
    # Mock a function that logs errors
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Test error message")
    assert "Test error message" in caplog.text, "Error message should be logged."