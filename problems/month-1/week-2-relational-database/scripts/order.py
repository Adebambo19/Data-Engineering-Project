class Order:
    def __init__(self, id, customer, products):
        self.id = id
        self.customer = customer
        self.products = products

    def update(self, products=None):
        if products:
            self.products = products

    def delete(self):
        self.id = None
        self.customer = None
        self.products = []
