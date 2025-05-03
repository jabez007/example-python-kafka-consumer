import logging

from src.consumer.kafka import KafkaConsumer
from src.handlers.topic1 import Topic1Handler


def main():
    try:
        # Create handlers
        topic1_handler = Topic1Handler()
    
        # Create consumer
        consumer = KafkaConsumer()

        # Register handlers
        consumer.register_handler("topic1", topic1_handler)
    
        # Start consumer
        consumer.start()
    except Exception as e:
        logging.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()

