import pandas as pd
import duckdb

# Load data
data = pd.read_csv("data/cloud-pipeline.csv")

# Process data
# Add your code here

# Save results
data.to_csv("data/processed_cloud-pipeline.csv", index=False)
