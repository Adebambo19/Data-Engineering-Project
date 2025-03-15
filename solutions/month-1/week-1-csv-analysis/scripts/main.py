import pandas as pd
import duckdb
import kagglehub
import os 
import shutil



def load_data():
    # kagglehub authentication
    # download data into the data folder
    # create a copy of the train.csv to 
    data = kagglehub.competition_download("titanic")
    shutil.move(src=data, dst="./data")


    shutil.rmtree("./data/titanic")

    # check if week-1-project.csv doesnot exist, then create it else: 
    if os.path.exists("./data/week-1-project.csv"):
        pass
    else: 
        shutil.copy(src="./data/train.csv", dst="./data/week-1-project.csv")
    return "./data/week-1-project.csv"


def process_data(data):
    # 2. Get the mean age of the passenger and also get the number of each gender on board 
    # 3. Get the 2nd highest purchases ticket


    relation = duckdb.read_csv(data)

    # Calculate mean age
    mean_age = duckdb.sql("SELECT avg(age) FROM relation").fetchone()[0]

    # Calculate gender statistics
    gender_stats = duckdb.sql("SELECT Sex, COUNT(*) as count FROM relation GROUP BY Sex").fetchall()

    # Find the second most purchased ticket
    second_ticket = duckdb.sql("""
        SELECT Ticket
        FROM (
            SELECT Ticket, COUNT(*) as count,
                   ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
            FROM relation
            GROUP BY Ticket
        )
        WHERE rank = 2
    """).fetchone()[0]

    return mean_age, gender_stats, second_ticket


print(process_data(load_data()))




