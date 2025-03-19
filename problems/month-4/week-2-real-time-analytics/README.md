# Week 2 Project: Real-Time Analytics with Kafka, Spark Streaming, and DuckDB

## Problem Statement
- Build a real-time analytics system:
  - Use **Kafka** to stream data (e.g., sensor data, logs).
  - Use **Spark Streaming** to process the data in real-time.
  - Store the processed results in a **DuckDB** database for real-time visualization.

## Requirements
1. **Set Up Kafka for Data Streaming**:
   - Create a Kafka producer to stream data.
   - Create a Kafka consumer to read the streamed data.
   - Handle large volumes of data and ensure low-latency processing.

2. **Process Data with Spark Streaming**:
   - Use Spark Streaming to consume data from Kafka.
   - Perform real-time processing (e.g., filtering, aggregation, windowing).
   - Handle late or out-of-order data.

3. **Store Processed Data in DuckDB**:
   - Load the processed data into a **DuckDB** database.
   - Create a table schema to store the processed data.
   - Ensure data integrity and handle errors (e.g., duplicate entries, schema mismatches).

4. **Real-Time Visualization**:
   - Connect DuckDB to a visualization tool (e.g., Tableau, Grafana).
   - Create real-time dashboards to visualize the processed data.

5. **Logging and Monitoring**:
   - Implement logging to track the real-time analytics pipeline.
   - Monitor pipeline performance (e.g., latency, throughput).

6. **Testing**:
   - Write unit tests to validate the real-time analytics pipeline.
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
2. Set Up Kafka and Spark Using Docker. Start the Docker containers: ` docker-compose up -d`
3. Create a Kafka topic for sensor data:  
```bash
docker exec -it kafka kafka-topics --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
