import duckdb
duckdb.connect("Week_2")
def create_Product(productname, unit_price, quantity):
    duckdb.sql("CREATE TABLE if not exist Products (Id bigserial primary key, productname varchar, unit_price decimal, 
     quantity int")"
     "INSERT INTO Products (productname, unit_price, quantity)"