
class Customer:
    def __init__(self, id, name, email):
        self.id = id
        self.name = name
        self.email = email
    def update (self, name, email):
        self.name = name
        self.email = email
    def delete (self):
        self.name = None
        self.email = None
        self.id = None
