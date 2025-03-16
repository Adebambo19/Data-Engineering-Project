
class Product:
    def __init__(self, id, name, price):
        self.id = id
        self.name = name
        self.price = price
    def update (self, name, price):
        self.name = name
        self.price = price
    def delete (self):
        self.name = None
        self.price = None
        self.id = None