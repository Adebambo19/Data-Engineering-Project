import pandas as pd
import duckdb

# Load data
data = pd.read_csv("data/end-to-end-pipeline.csv")

# Process data
# Add your code here

# Save results
data.to_csv("data/processed_end-to-end-pipeline.csv", index=False)
