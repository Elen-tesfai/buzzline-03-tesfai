# json_consumer_tesfai.py

"""
Consume JSON data from a Kafka topic.

Example received JSON message:
{"message": "I love Python!", "author": "Eve"}

Example serialized from Kafka message:
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import json
import pathlib

# Import external packages
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException, KafkaError

# Import functions from local modules
from utils.utils_logger import logger
from utils.utils_producer import verify_services

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "smoker_topic")  # Same topic as producer
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_group() -> str:
    """Fetch Kafka consumer group from environment or use default."""
    group = os.getenv("KAFKA_GROUP", "smoker_consumer_group")  # Modify as needed
    logger.info(f"Kafka group: {group}")
    return group

#####################################
# Kafka Consumer Setup
#####################################

def create_kafka_consumer():
    """Create and return a Kafka consumer instance."""
    group = get_kafka_group()
    topic = get_kafka_topic()

    # Consumer configuration
    conf = {
        'bootstrap.servers': os.getenv("KAFKA_BROKER", "localhost:9092"),
        'group.id': group,
        'auto.offset.reset': 'earliest',  # Start from the beginning
    }

    # Create consumer instance
    consumer = Consumer(conf)

    try:
        consumer.subscribe([topic])
        logger.info(f"Subscribed to topic: {topic}")
    except KafkaException as e:
        logger.error(f"Error subscribing to Kafka topic: {e}")
        sys.exit(1)

    return consumer

#####################################
# Message Consumer
#####################################

def consume_messages():
    """Consume messages from Kafka topic."""
    consumer = create_kafka_consumer()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Polling for new messages

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached {msg.partition}, offset {msg.offset}")
                else:
                    raise KafkaException(msg.error())
            else:
                logger.info(f"Consumed message: {msg.value().decode('utf-8')}")
                try:
                    # Deserialize JSON message
                    message_dict = json.loads(msg.value().decode('utf-8'))
                    logger.debug(f"Processed message: {message_dict}")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode JSON: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error while processing message: {e}")

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    logger.info("START consumer.")
    verify_services()
    consume_messages()
    logger.info("END consumer.")
    