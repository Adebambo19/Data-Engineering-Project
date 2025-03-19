import os
import pandas as pd
import duckdb

def load_data():
    """
    Load data from the 'data' folder.
    Assumes the data folder contains a file named 'week-1-project.csv'.
    """
    data_folder = "data"
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
    
    # Check if the file exists
    csv_file_path = os.path.join(data_folder, "week-1-project.csv")
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"File '{csv_file_path}' not found in the data folder.")
    
    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv(csv_file_path)
    return df


def process_data(csv_file_path):
    """
    Process the data from the CSV file.
    Returns:
        - mean_age: The mean age of the dataset.
        - gender_stats: A list of tuples containing gender counts, e.g., [("Male", 10), ("Female", 15)].
        - most_purchased_ticket: The most purchased ticket type.
    """
    # Load the data into a Pandas DataFrame
    df = pd.read_csv(csv_file_path)

    # 1. Calculate mean age
    mean_age = df["age"].mean()

    # 2. Calculate gender statistics
    gender_stats = df["gender"].value_counts().items()
    gender_stats = [(gender, count) for gender, count in gender_stats]

    # 3. Find the most purchased ticket type
    most_purchased_ticket = df["ticket_type"].mode()[0]

    return mean_age, gender_stats, most_purchased_ticket