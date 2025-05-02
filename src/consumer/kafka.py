"""
Kafka consumer implementation with dependency injection for handlers.
"""
import datetime
import json
import logging
import signal
from typing import Callable, Dict, Optional


from confluent_kafka import Consumer, KafkaError, Producer



from ..handlers.base import BaseHandler
from .config import ConsumerConfig

"""

"""

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """
    Kafka consumer that routes messages to topic-specific handlers and
    manages offset commits based on handler success.
    """

    def __init__(self, config: Optional[ConsumerConfig] = None):
        """
        Initialize the Kafka consumer with configuration.
        
        Args:
            config: Consumer configuration
        """
        self.config = config if config is not None else ConsumerConfig() 
        self.handlers: Dict[str, BaseHandler] = {}
        self.running = False
        
        """
        
        """
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        
        
        # Configure Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': config.bootstrap_servers,
            'group.id': config.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        
        # Configure dead letter queue producer
        self.dlq_producer = Producer({
            'bootstrap.servers': config.bootstrap_servers,
        })
        

    def register_handler(self, topic: str, handler: BaseHandler) -> None:
        """
        Register a handler for a specific topic.
        
        Args:
            topic: Kafka topic name
            handler: Handler instance for processing messages
        """
        self.handlers[topic] = handler
        logger.info(f"Registered handler {handler.__class__.__name__} for topic {topic}")

    def _handle_shutdown(self, signum, frame) -> None:
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    
    def start(self) -> None:
        """Start consuming messages from Kafka."""
        if not self.handlers:
            logger.error("No handlers registered. Exiting.")
            return
        
        # Subscribe to topics
        topics = list(self.handlers.keys())
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {', '.join(topics)}")
        
        self.running = True
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                topic = msg.topic()
                """
                
                """

                try:
                    handler = self.handlers.get(topic)
                    if not handler:
                        logger.warning(f"No handler registered for topic {topic}")
                        continue
                    
                    # Parse message
                    try:
                        message_data = json.loads(msg.value().decode('utf-8'))
                    except json.JSONDecodeError:
                        logger.error(f"Failed to decode message as JSON from topic {topic}")
                        self._send_to_dlq(topic, msg.value(), "Invalid JSON format")
                        self.consumer.commit(msg)
                        """
                        
                        """
                        continue
                    
                    # Process message
                    success = handler.handle(message_data, self._get_dlq_callback(topic, msg.value()))
                    
                    if success:
                        self.consumer.commit(msg)
                        """
                        
                        """
                    else:
                        logger.warning(f"Handler returned False for message in topic {topic}")
                        """
                        
                        """
                    
                except Exception as e:
                    logger.exception(f"Error processing message from {topic}: {e}")
                    self._send_to_dlq(topic, msg.value(), str(e))
                    self.consumer.commit(msg)
                    """
                    
                    """
                """
                
                """
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            logger.info("Closing consumer")
            self.consumer.close()

    def _send_to_dlq(self, original_topic: str, message: bytes, reason: str) -> None:
        """
        Send a message to the dead letter queue.
        
        Args:
            original_topic: Original topic the message came from
            message: Original message bytes
            reason: Reason for sending to DLQ
        """
        dlq_topic = f"{original_topic}.dlq"
        
        try:
            # Create a wrapper that includes the original message and metadata
            dlq_message = {
                "original_message": message.decode('utf-8', errors='replace'),
                "error_reason": reason,
                "original_topic": original_topic,
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            self.dlq_producer.produce(
                dlq_topic,
                json.dumps(dlq_message).encode('utf-8'),
                callback=self._delivery_report
            )
            self.dlq_producer.flush()
            
            logger.info(f"Message sent to DLQ topic {dlq_topic}")
            """
            
            """
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ {dlq_topic}: {e}")

    def _delivery_report(self, err, msg) -> None:
        """Callback for producer to report delivery success/failure."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def _get_dlq_callback(self, topic: str, original_message: bytes) -> Callable[[str], None]:
        """
        Return a callback function for handlers to send messages to DLQ.
        
        Args:
            topic: Original topic
            original_message: Original message bytes
        
        Returns:
            Callable function that sends to DLQ with given reason
        """
        def send_to_dlq(reason: str) -> None:
            self._send_to_dlq(topic, original_message, reason)
        return send_to_dlq

    
