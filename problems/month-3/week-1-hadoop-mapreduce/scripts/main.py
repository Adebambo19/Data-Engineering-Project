import pandas as pd
import duckdb

# Load data
data = pd.read_csv("data/hadoop-mapreduce.csv")

# Process data
# Add your code here

# Save results
data.to_csv("data/processed_hadoop-mapreduce.csv", index=False)
