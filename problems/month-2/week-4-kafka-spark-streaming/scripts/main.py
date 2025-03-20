from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import SparkSession
import pandas as pd
import duckdb
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def produce_to_kafka(producer, topic, message):
    """
    Produce a message to a Kafka topic.
    """
    if not producer or not topic or not message:
        raise ValueError("Invalid producer, topic, or message.")
    try:
        producer.send(topic, message)
        producer.flush()
        logger.info(f"Message sent to Kafka topic: {topic}")
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        raise

def consume_from_kafka(consumer):
    """
    Consume a message from a Kafka topic.
    """
    if not consumer:
        raise ValueError("Invalid Kafka consumer.")
    try:
        for message in consumer:
            return message.value
    except Exception as e:
        logger.error(f"Failed to consume message from Kafka: {e}")
        raise

def process_with_spark(spark, data):
    """
    Process data using Spark Streaming.
    """
    if not spark or data.empty:
        raise ValueError("Invalid Spark session or empty data.")
    try:
        # Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(data)
        # Perform some processing (e.g., filtering, aggregation)
        processed_df = spark_df.select("timestamp", "sensor_id", "value")
        # Convert back to Pandas DataFrame for simplicity
        return processed_df.toPandas()
    except Exception as e:
        logger.error(f"Failed to process data with Spark: {e}")
        raise

def load_to_duckdb(connection, data, table_name):
    """
    Load data into a DuckDB table.
    """
    if not connection or data.empty or not table_name:
        raise ValueError("Invalid connection, data, or table name.")
    try:
        connection.register(table_name, data)
        logger.info(f"Data loaded into DuckDB table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to load data into DuckDB: {e}")
        raise

def query_duckdb(connection, query):
    """
    Query data from a DuckDB table.
    """
    if not connection or not query:
        raise ValueError("Invalid connection or query.")
    try:
        result = connection.execute(query).fetchall()
        logger.info(f"Query executed successfully: {query}")
        return result
    except Exception as e:
        logger.error(f"Failed to query data from DuckDB: {e}")
        raise
