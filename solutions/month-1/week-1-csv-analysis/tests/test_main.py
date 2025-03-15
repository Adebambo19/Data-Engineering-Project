import pandas as pd
import duckdb
import sys
import os
from scripts.main import process_data, load_data


def test_load_data():

    # assert data folder is not empty
    # assert the data folder contain a file name, week-1-project.csv
    data_folder = "data" 
    load_data()

    # 1. Assert data folder is not empty
    assert os.path.exists(data_folder), f"Data folder '{data_folder}' does not exist."
    assert os.listdir(data_folder), f"Data folder '{data_folder}' is empty."

    # 2. Assert the data folder contains a file named 'week-1-project.csv'
    expected_file = "week-1-project.csv"
    assert expected_file in os.listdir(data_folder), f"File '{expected_file}' not found in data folder."


def test_process_data():
    # assert the return values of the process data are (mean_age, gender_stats = [("Male", #), ("Female", ##)] , second_most_purchased_ticket)
    # Assuming process_data takes the path to the CSV file as input
    csv_file_path = os.path.join("data", "week-1-project.csv")

    # Call the process_data function
    mean_age, gender_stats, most_purchased_ticket = process_data(csv_file_path)

    # 3. Assert the return values of the process_data function
    assert isinstance(mean_age, (int, float)), "mean_age should be a number."
    assert isinstance(gender_stats, list), "gender_stats should be a list."
    assert all(isinstance(item, tuple) and len(item) == 2 for item in gender_stats), "gender_stats should be a list of tuples with 2 elements each."
    assert isinstance(most_purchased_ticket, str), "most_purchased_ticket should be a string."

    # Example of more specific assertions (adjust based on your expected values)
    assert mean_age > 0, "mean_age should be greater than 0."
    assert len(gender_stats) == 2, "gender_stats should contain exactly 2 tuples."
