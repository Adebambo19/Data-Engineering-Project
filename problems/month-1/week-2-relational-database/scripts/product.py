class Product:
    def __init__(self, id, name, price):
        self.id = id
        self.name = name
        self.price = price

    def update(self, name=None, price=None):
        if name:
            self.name = name
        if price:
            self.price = price

    def delete(self):
        self.id = None
        self.name = None
        self.price = None