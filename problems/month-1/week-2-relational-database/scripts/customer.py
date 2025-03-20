class Customer:
    def __init__(self, id, name, email):
        self.id = id
        self.name = name
        self.email = email

    def update(self, name=None, email=None):
        if name:
            self.name = name
        if email:
            self.email = email

    def delete(self):
        self.id = None
        self.name = None
        self.email = None
