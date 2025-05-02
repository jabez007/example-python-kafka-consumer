from .consumer.kafka import KafkaConsumer
from .handlers.topic1 import Topic1Handler


def main():
    # Create handlers
    topic1_handler = Topic1Handler()
    
    # Create consumer
    consumer = KafkaConsumer()

    # Register handlers
    consumer.register_handler("topic1", topic1_handler)
    
    # Start consumer
    consumer.start()

if __name__ == "__main__":
    main()

