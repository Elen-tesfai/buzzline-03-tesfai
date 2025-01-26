"""
csv_consumer_tesfai.py

Consume CSV data from a Kafka topic and perform real-time analytics.

Example CSV message:
timestamp, temperature
2025-01-01 15:00:00, 70.4

Example deserialized message from Kafka:
{"timestamp": "2025-01-01 15:00:00", "temperature": 70.4}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import json

# Import external packages
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Import functions from local modules
from utils.utils_consumer import process_message
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "smoker_csv_topic")  # Modify to your topic name
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("BUZZ_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Kafka Consumer Setup
#####################################

def create_kafka_consumer():
    """Create a Kafka consumer."""
    topic = get_kafka_topic()
    consumer = KafkaConsumer(
        topic,
        group_id="my-group",  # Define consumer group
        bootstrap_servers="localhost:9092",  # Kafka server address
        auto_offset_reset="earliest",  # Start consuming from the beginning
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    logger.info(f"Consumer connected to Kafka topic: {topic}")
    return consumer

#####################################
# Main Function
#####################################

def main():
    """
    Main entry point for the consumer.

    - Consumes messages from Kafka topic.
    - Calls the `process_message` function for real-time analytics or logging.
    """

    logger.info("START consumer.")
    
    consumer = create_kafka_consumer()

    try:
        for message in consumer:
            logger.info(f"Received message: {message.value}")
            process_message(message.value)  # You can add your custom processing logic here
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message consumption: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

    logger.info("END consumer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
    