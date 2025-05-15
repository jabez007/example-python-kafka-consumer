"""
Kafka consumer implementation with dependency injection for handlers.
"""
import datetime
import json
import logging
import random
from copy import deepcopy
from typing import Callable, Dict, Optional


from time import sleep

from confluent_kafka import Consumer, KafkaError, Message, Producer



from src.consumer.config import ConsumerConfig
from src.handlers.base import BaseHandler
from src.models.envelope import MessageEnvelope

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
        
        
        # Configure Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': self.config.bootstrap_servers,
            'group.id': self.config.group_id,
            'auto.offset.reset': self.config.auto_offset_reset,
            'enable.auto.commit': False,
        })

        # Configure retry producer
        self.retry_producer = Producer({
            'bootstrap.servers': self.config.bootstrap_servers,
            'message.send.max.retries': 3,
            'retry.backoff.ms': 500,
            'delivery.timeout.ms': 10000,
        })
        # Configure dead letter queue producer
        self.dlq_producer = Producer({
            'bootstrap.servers': self.config.bootstrap_servers,
            'message.send.max.retries': 3,
            'retry.backoff.ms': 500,
            'delivery.timeout.ms': 10000,
        })
        

        logger.debug("KafkaConsumer initialized with config: %s", self.config)

    def register_handler(self, topic: str, handler: BaseHandler) -> None:
        """
        Register a handler for a specific topic.
        
        Args:
            topic: Kafka topic name
            handler: Handler instance for processing messages
        """
        self.handlers[topic] = handler
        logger.info(f"Registered handler {handler.__class__.__name__} for topic {topic}")

    
    def start(self) -> None:
        """Start consuming messages from Kafka."""
        if not self.handlers:
            logger.error("No handlers registered. Exiting.")
            return
        
        # Subscribe to topics
        topics = list(self.handlers.keys())
        logger.info("Topics to subscribe: %s", topics)
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {', '.join(topics)}")
        
        self.running = True
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0) # 1 second
                
                if msg is None:
                    continue
    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                logger.info("Received message offset=%d from %s:%d", msg.offset(), msg.topic(), msg.partition())

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
                    except (UnicodeError, json.JSONDecodeError):
                        logger.error(f"Failed to decode message ({msg.offset()}) as JSON from topic {topic}")
                        self._send_to_dlq(topic, msg.value(), "Invalid JSON format")
                        self.consumer.commit(msg)
                        """
                        
                        """
                        continue
                    
                    # Process message
                    logger.debug("Processing message offset=%d from topic=%s", msg.offset(), topic)
                    success = handler.handle(message_data, self._get_retry_callback(topic), self._get_dlq_callback(topic, msg.value()))
                    
                    if success:
                        logger.info("Successfully processed message offset=%d on topic=%s", msg.offset(), topic)
                        self.consumer.commit(msg)
                        """
                        
                        """
                    else:
                        logger.warning(f"Handler returned False for message ({msg.offset()}) on topic {topic}")
                        sleep(random.uniform(1, 3)) # avoid hammering both the broker and our logs. 
                        """
                        
                        """
                    
                except Exception as e:
                    logger.exception(f"Error processing message ({msg.offset()}) from {topic}: {e}")
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
            # Flush any pending messages and close producers
            logger.info("Flushing and closing producers")
            self.retry_producer.flush()
            self.dlq_producer.flush()

    def _retry_message(self, original_topic: str, failed_message: MessageEnvelope, reason: str) -> None:
        """
        Send a message to the retry queue.
        
        Args:
            original_topic: Original topic the message came from
            failed_message: Envelope for failed message
            reason: Reason for retrying message
        """
        retry_topic = f'{original_topic}.retry'

        # Extract retry count if present
        header = failed_message.header or {} # ensure not None
        try:
            retry_count = int(header.get("retryCount", 0))
        except (ValueError, TypeError):
            logger.warning(f"Invalid retryCount value: {header.get('retryCount')}, using 0")
            retry_count = 0

        envelope_copy = MessageEnvelope.from_dict(failed_message.to_dict())

        # Increment retry count for next attempt
        envelope_copy.header = deepcopy(header)  # work on an isolated copy
        envelope_copy.header["retryCount"] = str(retry_count + 1)

        # Include metadata about original topic
        envelope_copy.header["originalTopic"] = original_topic
        envelope_copy.header["retryReason"] = reason

        try:
            # Produce new message with updated headers
            self.retry_producer.produce(
                retry_topic,
                json.dumps(envelope_copy.to_dict()).encode("utf-8"),
                callback=self._delivery_report
            )
            # allow delivery callback processing without blocking
            self.retry_producer.poll(0)
    
            logger.info(f"Message sent to retry topic {retry_topic}, attempt {retry_count + 1}")
            """
            
            """

        except Exception as e:
            logger.error(f"Failed to send message to retry {retry_topic}: {e}")


    def _send_to_dlq(self, original_topic: str, message: bytes, reason: str) -> None:
        """
        Send a message to the dead letter queue.
        
        Args:
            original_topic: Original topic the message came from
            message: Original message bytes
            reason: Reason for sending to DLQ
        """
        dlq_topic = f'{original_topic}.dlq'
        
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
            # allow delivery callback processing without blocking
            self.dlq_producer.poll(0)
            
            logger.info(f"Message sent to DLQ topic {dlq_topic}")
            """
            
            """
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ {dlq_topic}: {e}")

    def _delivery_report(self, err: Optional[Exception], msg: Message) -> None:
        """Callback for producer to report delivery success/failure."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def _get_retry_callback(self, topic: str) -> Callable[[MessageEnvelope, str], None]:
        """
        Return a callback function for handlers to send messages to retry topic.
        
        Args:
            topic: Original topic
        
        Returns:
            Callable function that sends to retry topic
        """
        def retry_message(failed_message: MessageEnvelope, reason: str) -> None:
            self._retry_message(topic, failed_message, reason)
        return retry_message

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

    def _handle_shutdown(self, signum, frame) -> None:
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    
