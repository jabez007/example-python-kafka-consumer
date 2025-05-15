import logging
import signal



from src.consumer.kafka import KafkaConsumer
from src.handlers.topic1 import Topic1Handler

SHUTDOWN_SIGNALS = [signal.SIGTERM, signal.SIGINT]

logging.basicConfig(
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Creating handler(s)")
        topic1_handler = Topic1Handler()
    
        logger.info("Creating consumer")
        consumer = KafkaConsumer()

        logger.info("Registering topic handlers with consumer")
        consumer.register_handler("topic1", topic1_handler)

        logger.info("Registering shutdown signals")
        for sig in SHUTDOWN_SIGNALS:
            signal.signal(sig, consumer._handle_shutdown)

        logger.info("Starting consumer")
        consumer.start()
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()

