# Week 4 Project: Real-Time Data Streaming with Kafka, Spark Streaming, and DuckDB

## Problem Statement
- Use **Kafka** to stream data (e.g., sensor data).
- Use **Spark Streaming** to process and analyze the data in real-time.
- Store the processed data in a **DuckDB** database for further analysis.

## Requirements
1. **Set Up Kafka**:
   - Create a Kafka producer to stream data (e.g., sensor readings).
   - Create a Kafka consumer to read the streamed data.

2. **Process Data with Spark Streaming**:
   - Use Spark Streaming to consume data from Kafka.
   - Perform real-time processing (e.g., filtering, aggregation, windowing).
   - Handle late or out-of-order data.

3. **Store Processed Data in DuckDB**:
   - Load the processed data into a **DuckDB** database.
   - Create a table schema to store the processed data.
   - Ensure data integrity and handle errors (e.g., duplicate entries, schema mismatches).

4. **Query and Analyze Data**:
   - Use DuckDB to query the processed data (e.g., filter, aggregate, join).
   - Generate insights or reports from the data.

5. **Logging and Monitoring**:
   - Implement logging to track the data streaming and processing pipeline.
   - Use Spark's built-in monitoring tools to track streaming jobs.

6. **Testing**:
   - Write unit tests to validate the data streaming and processing pipeline.
   - Test Kafka producer/consumer, Spark Streaming transformations, and DuckDB operations.

## Resources
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/): Official documentation for Apache Kafka.
- [Apache Spark Streaming Documentation](https://spark.apache.org/docs/latest/streaming-programming-guide.html): Official documentation for Spark Streaming.
- [DuckDB Documentation](https://duckdb.org/docs/): Official documentation for DuckDB.
- [Pytest Documentation](https://docs.pytest.org/en/stable/): Official documentation for writing tests using `pytest`.
- [Python Logging Documentation](https://docs.python.org/3/library/logging.html): Documentation for implementing logging in Python.
- [Sample Sensor Dataset](https://www.kaggle.com/datasets): A resource for finding sample sensor datasets for testing.

## Setup Instructions
1. **Install Dependencies**:
   ```bash
   pip install kafka-python pyspark duckdb pytest pandas
    ```
2. start kafka and zookeeper containers ` docker-compose up -d `
3. Create a Kafka topic for sensor data:
```bash
docker exec -it kafka kafka-topics --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 
```