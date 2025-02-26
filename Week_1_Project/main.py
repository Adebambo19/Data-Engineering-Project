import duckdb


dataframe = duckdb.read_csv('./train.csv')
#print(dataframe.show())

sortbyage = duckdb.sql('Select * from dataframe where Age > 30 order by Age Asc')
print (f"sort by age table = {sortbyage.show()}")

sortbygender = duckdb.sql("Select * from dataframe where Sex = 'male' order by Fare Desc")
print (f"Sort by gender = {sortbygender.show()}")
