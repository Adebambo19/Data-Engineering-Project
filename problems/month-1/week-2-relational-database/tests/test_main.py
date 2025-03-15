import pytest
from scripts.customer import Customer
from scripts.product import Product
from scripts.order import Order
import os

# Fixture to initialize a sample customer
@pytest.fixture
def sample_customer():
    return Customer(id=1, name="John Doe", email="john.doe@example.com")

# Fixture to initialize a sample product
@pytest.fixture
def sample_product():
    return Product(id=101, name="Laptop", price=999.99)

# Fixture to initialize a sample order
@pytest.fixture
def sample_order(sample_customer, sample_product):
    return Order(id=1001, customer=sample_customer, products=[sample_product])

# Test data loading
def test_load_data():
    # Ensure the sample data file exists
    data_file = "data/sample_data.csv"
    assert os.path.exists(data_file), f"Data file {data_file} does not exist."

    # Load data (assuming a method exists to load data from CSV)
    customers, products, orders = load_data(data_file)
    assert len(customers) > 0, "No customers loaded."
    assert len(products) > 0, "No products loaded."
    assert len(orders) > 0, "No orders loaded."

# Test Customer model
def test_customer_creation(sample_customer):
    assert sample_customer.id == 1
    assert sample_customer.name == "John Doe"
    assert sample_customer.email == "john.doe@example.com"

def test_customer_update(sample_customer):
    sample_customer.update(name="Jane Doe", email="jane.doe@example.com")
    assert sample_customer.name == "Jane Doe"
    assert sample_customer.email == "jane.doe@example.com"

def test_customer_delete(sample_customer):
    sample_customer.delete()
    assert sample_customer.id is None
    assert sample_customer.name is None
    assert sample_customer.email is None

# Test Product model
def test_product_creation(sample_product):
    assert sample_product.id == 101
    assert sample_product.name == "Laptop"
    assert sample_product.price == 999.99

def test_product_update(sample_product):
    sample_product.update(name="Gaming Laptop", price=1299.99)
    assert sample_product.name == "Gaming Laptop"
    assert sample_product.price == 1299.99

def test_product_delete(sample_product):
    sample_product.delete()
    assert sample_product.id is None
    assert sample_product.name is None
    assert sample_product.price is None

# Test Order model
def test_order_creation(sample_order, sample_customer, sample_product):
    assert sample_order.id == 1001
    assert sample_order.customer == sample_customer
    assert sample_order.products == [sample_product]

def test_order_update(sample_order, sample_product):
    new_product = Product(id=102, name="Mouse", price=29.99)
    sample_order.update(products=[sample_product, new_product])
    assert len(sample_order.products) == 2
    assert new_product in sample_order.products

def test_order_delete(sample_order):
    sample_order.delete()
    assert sample_order.id is None
    assert sample_order.customer is None
    assert sample_order.products == []

# Test CRUD operations
def test_crud_operations(sample_customer, sample_product, sample_order):
    # Create
    assert sample_customer.id is not None
    assert sample_product.id is not None
    assert sample_order.id is not None

    # Read
    assert sample_customer.name == "John Doe"
    assert sample_product.name == "Laptop"
    assert sample_order.customer == sample_customer

    # Update
    sample_customer.update(name="Jane Doe")
    sample_product.update(name="Gaming Laptop")
    new_product = Product(id=102, name="Mouse", price=29.99)
    sample_order.update(products=[sample_product, new_product])

    assert sample_customer.name == "Jane Doe"
    assert sample_product.name == "Gaming Laptop"
    assert len(sample_order.products) == 2

    # Delete
    sample_customer.delete()
    sample_product.delete()
    sample_order.delete()

    assert sample_customer.id is None
    assert sample_product.id is None
    assert sample_order.id is None

# Helper function to load data (mock implementation)
def load_data(data_file):
    # Mock implementation for loading data from CSV
    customers = [Customer(id=1, name="John Doe", email="john.doe@example.com")]
    products = [Product(id=101, name="Laptop", price=999.99)]
    orders = [Order(id=1001, customer=customers[0], products=[products[0]])]
    return customers, products, orders