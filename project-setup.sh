#!/bin/bash

# Initialize Git repository
git init
echo "data-engineering-projects/" > .gitignore
git add .gitignore
git commit -m "Initial commit: Set up project structure"

# Function to create problem and solution folders
create_project() {
    local month=$1
    local week=$2
    local project_name=$3
    local problem_path="problems/month-$month/week-$week-$project_name"
    local solution_path="solutions/month-$month/week-$week-$project_name"

    # Create problem directories
    mkdir -p "$problem_path"/{data,scripts,tests}

    # Create problem README.md
    cat <<EOF > "$problem_path/README.md"
# Week $week Project: $project_name

## Problem Statement
- Describe the problem to be solved here.

## Requirements
1. Requirement 1
2. Requirement 2
3. Requirement 3

## Resources
- [Resource 1](https://example.com)
- [Resource 2](https://example.com)
EOF

    # Create problem requirements.txt
    echo "pandas==1.5.3" > "$problem_path/requirements.txt"
    echo "numpy==1.23.5" >> "$problem_path/requirements.txt"
    echo "duckdb==0.8.0" >> "$problem_path/requirements.txt"
    echo "pytest==7.4.0" >> "$problem_path/requirements.txt"

    # Create problem boilerplate script
    cat <<EOF > "$problem_path/scripts/main.py"
import pandas as pd
import duckdb

# Load data
data = pd.read_csv("data/$project_name.csv")

# Process data
# Add your code here

# Save results
data.to_csv("data/processed_$project_name.csv", index=False)
EOF

    # Create problem test file
    cat <<EOF > "$problem_path/tests/test_main.py"
import pandas as pd
import duckdb
from scripts.main import process_data

def test_process_data():
    # Test data
    data = pd.DataFrame({"column1": [1, 2, 3], "column2": [4, 5, 6]})
    result = process_data(data)
    assert result.shape == (3, 2), "Data processing failed"
EOF

    # Add sample data file to problem
    touch "$problem_path/data/$project_name.csv"

    # Create solution directories
    mkdir -p "$solution_path"/{data,scripts,tests}

    # Create solution README.md
    cat <<EOF > "$solution_path/README.md"
# Week $week Project: $project_name

## Solution
- Describe the solution here.

## Steps
1. Step 1
2. Step 2
3. Step 3

## Resources
- [Resource 1](https://example.com)
- [Resource 2](https://example.com)
EOF

    # Copy problem files to solution (as a starting point)
    cp -r "$problem_path"/* "$solution_path/"

    # Add to Git
    git add "$problem_path" "$solution_path"
    git commit -m "Set up project: $project_name"
}

# Create projects for each week
create_project 1 1 "csv-analysis"
create_project 1 2 "relational-database"
create_project 1 3 "etl-pipeline"
create_project 1 4 "pyspark-processing"
create_project 2 1 "cloud-pipeline"
create_project 2 2 "airflow-dag"
create_project 2 3 "data-warehouse"
create_project 2 4 "kafka-spark-streaming"
create_project 3 1 "hadoop-mapreduce"
create_project 3 2 "pipeline-optimization"
create_project 3 3 "data-security"
create_project 3 4 "data-quality-monitoring"
create_project 4 1 "end-to-end-pipeline"
create_project 4 2 "real-time-analytics"
create_project 4 3 "portfolio-documentation"
create_project 4 4 "job-readiness"

# Finalize Git setup
git remote add origin <repository-url>  # Replace with your Git repository URL
git push -u origin main

echo "Project setup complete!"