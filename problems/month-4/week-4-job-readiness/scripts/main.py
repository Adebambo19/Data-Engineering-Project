import pandas as pd
import duckdb

# Load data
data = pd.read_csv("data/job-readiness.csv")

# Process data
# Add your code here

# Save results
data.to_csv("data/processed_job-readiness.csv", index=False)
