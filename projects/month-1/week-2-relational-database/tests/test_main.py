import pandas as pd
import duckdb
from scripts.main import process_data

def test_process_data():
    # Test data
    data = pd.DataFrame({"column1": [1, 2, 3], "column2": [4, 5, 6]})
    result = process_data(data)
    assert result.shape == (3, 2), "Data processing failed"
