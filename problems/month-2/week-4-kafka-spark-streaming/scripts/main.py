import pandas as pd
import duckdb

# Load data
data = pd.read_csv("data/kafka-spark-streaming.csv")

# Process data
# Add your code here

# Save results
data.to_csv("data/processed_kafka-spark-streaming.csv", index=False)
