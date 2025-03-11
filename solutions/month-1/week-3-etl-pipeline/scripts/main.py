import pandas as pd
import duckdb

# Load data
data = pd.read_csv("data/etl-pipeline.csv")

# Process data
# Add your code here

# Save results
data.to_csv("data/processed_etl-pipeline.csv", index=False)
