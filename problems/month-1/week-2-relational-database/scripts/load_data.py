from scripts.customer import Customer
from scripts.product import Product
from scripts.order import Order

def load_data(data_file):
    # Mock implementation for loading data from CSV
    customers = [Customer(id=1, name="John Doe", email="john.doe@example.com")]
    products = [Product(id=101, name="Laptop", price=999.99)]
    orders = [Order(id=1001, customer=customers[0], products=[products[0]])]
    return customers, products, orders