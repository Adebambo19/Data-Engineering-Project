from .customer import Customer
from .product import Product
from typing import List
class Order:
    def __init__(self, id: int, customer: Customer, products: List[Product]):
        self.id = id
        self.customer = customer
        self.products = products
    def update (self, products):
        self.products.append(products)
    def delete (self):
        self.customer = None
        self.products = None
        self.id = None