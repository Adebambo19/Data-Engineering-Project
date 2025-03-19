# Week 3 Project: etl-pipeline

## Problem Statement
- This project involves extracting data from an API (e.g., weather data), transforming it (cleaning, aggregating), and loading it into a database. The goal is to create a robust ETL (Extract, Transform, Load) pipeline using Python.

## Requirements
1. Extract Data from an API:
    - Connect to a weather API (e.g., OpenWeatherMap).
    - Fetch weather data for a specific location or set of locations.
    - Handle API authentication (e.g., API keys).
    - Handle errors (e.g., API rate limits, invalid responses)
2. Transform Data:

    - Clean the extracted data (e.g., remove missing values, handle duplicates).
    - Aggregate data (e.g., calculate average temperature, max/min values).
    - Convert data into a structured format (e.g., JSON, CSV, or database schema).

3. Load Data into a Database:

    - Connect to a database (e.g., SQLite, PostgreSQL).

    - Create a table schema to store the transformed data.

    - Insert the transformed data into the database.

    - Handle database errors (e.g., duplicate entries, connection issues).

4. Logging and Error Handling:

    - Implement logging to track the ETL process (e.g., success, failures).

    - Handle exceptions gracefully (e.g., retry failed API calls, skip invalid data).

## Resources
- OpenWeatherMap API Documentation: Official documentation for the OpenWeatherMap API.


- Pytest Documentation: Official documentation for writing tests using pytest.

- Python Requests Library: Documentation for making HTTP requests in Python.

- Pandas Documentation: Documentation for data manipulation using Pandas.
