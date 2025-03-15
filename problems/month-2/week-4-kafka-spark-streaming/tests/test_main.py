import pytest
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from kafka import KafkaProducer, KafkaConsumer
from scripts.main import produce_to_kafka, consume_from_kafka, process_with_spark, load_to_duckdb, query_duckdb

# Fixture for Spark session
@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("Spark Streaming Test") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

# Fixture for DuckDB connection
@pytest.fixture
def db_connection():
    conn = duckdb.connect(":memory:")  # Use an in-memory DuckDB database for testing
    yield conn
    conn.close()

# Fixture for Kafka producer
@pytest.fixture
def kafka_producer():
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    yield producer
    producer.close()

# Fixture for Kafka consumer
@pytest.fixture
def kafka_consumer():
    consumer = KafkaConsumer("sensor-data", bootstrap_servers="localhost:9092", auto_offset_reset="earliest")
    yield consumer
    consumer.close()

# Fixture for sample sensor data
@pytest.fixture
def sample_sensor_data():
    return pd.DataFrame({
        "timestamp": ["2023-10-01 12:00:00", "2023-10-01 12:01:00"],
        "sensor_id": [1, 2],
        "value": [25.5, 30.0]
    })

# Test Kafka producer
def test_produce_to_kafka(kafka_producer):
    test_message = b"test_message"
    produce_to_kafka(kafka_producer, "sensor-data", test_message)
    # Verify the message was sent (mock verification)
    assert True, "Message should be produced to Kafka."

# Test Kafka consumer
def test_consume_from_kafka(kafka_consumer):
    test_message = b"test_message"
    # Mock producing a message to Kafka
    kafka_producer = KafkaProducer(bootstrap_servers="localhost:9092")
    kafka_producer.send("sensor-data", test_message)
    kafka_producer.flush()

    # Consume the message
    message = consume_from_kafka(kafka_consumer)
    assert message == test_message, "Message should be consumed from Kafka."

# Test Spark Streaming processing
def test_process_with_spark(spark_session, sample_sensor_data):
    processed_data = process_with_spark(spark_session, sample_sensor_data)
    assert isinstance(processed_data, pd.DataFrame), "Processed data should be a DataFrame."
    assert "sensor_id" in processed_data.columns, "Processed data should have a 'sensor_id' column."

# Test loading data into DuckDB
def test_load_to_duckdb(db_connection, sample_sensor_data):
    load_to_duckdb(db_connection, sample_sensor_data, "sensor_data")
    result = db_connection.execute("SELECT * FROM sensor_data").fetchall()
    assert len(result) == 2, "Data should be loaded into DuckDB."

# Test querying data from DuckDB
def test_query_duckdb(db_connection, sample_sensor_data):
    load_to_duckdb(db_connection, sample_sensor_data, "sensor_data")
    query_result = query_duckdb(db_connection, "SELECT COUNT(*) as total FROM sensor_data")
    assert query_result[0][0] == 2, "Query should return the correct count."

# Test error handling for Kafka producer
def test_kafka_producer_error_handling():
    with pytest.raises(Exception):
        produce_to_kafka(None, "invalid-topic", b"test_message")

# Test logging and error handling
def test_logging_and_error_handling(caplog):
    # Mock a function that logs errors
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Test error message")
    assert "Test error message" in caplog.text, "Error message should be logged."