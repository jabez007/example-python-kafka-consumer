"""
Kafka consumer implementation with dependency injection for handlers.
"""
import datetime
import json
import logging
import random
import signal
from typing import Callable, Dict, Optional


import asyncio
from typing import Awaitable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition



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
        
        # Set up signal handlers for graceful shutdown
        
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._handle_shutdown(s)))
            except NotImplementedError:
                # Fallback for Windows / non-main thread
                def _sync_shutdown_handler(_sig, _frame):
                    loop = asyncio.get_event_loop()
                    loop.call_soon_threadsafe(asyncio.create_task, self._handle_shutdown(_sig))

                signal.signal(sig, _sync_shutdown_handler)
        

        
        # These will be initialized in start() for aiokafka
        self.consumer = None
        self.retry_producer = None
        self.dlq_producer = None
        

    def register_handler(self, topic: str, handler: BaseHandler) -> None:
        """
        Register a handler for a specific topic.
        
        Args:
            topic: Kafka topic name
            handler: Handler instance for processing messages
        """
        self.handlers[topic] = handler
        logger.info(f"Registered handler {handler.__class__.__name__} for topic {topic}")

    
    async def start(self) -> None:
        """Start consuming messages from Kafka."""
        if not self.handlers:
            logger.error("No handlers registered. Exiting.")
            return
        
        # Subscribe to topics
        topics = list(self.handlers.keys())
        
        # Initialize consumer and producer
        self.consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.config.group_id,
            auto_offset_reset=self.config.auto_offset_reset,
            enable_auto_commit=False,
        )
        
        self.retry_producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
        )

        self.dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
        )
        
        await self.consumer.start()
        await self.retry_producer.start()
        await self.dlq_producer.start()
        
        logger.info(f"Subscribed to topics: {', '.join(topics)}")
        
        self.running = True
        
        try:
            while self.running:
                try:
                    async for msg in self.consumer:
                        topic = msg.topic
                        """
                        
                        """

                        try:
                            handler = self.handlers.get(topic)
                            if not handler:
                                logger.warning(f"No handler registered for topic {topic}")
                                continue
                            
                            # Parse message
                            try:
                                message_data = json.loads(msg.value.decode('utf-8'))
                            except (UnicodeError, json.JSONDecodeError):
                                logger.error(f"Failed to decode message as JSON from topic {topic}")
                                await self._send_to_dlq(topic, msg.value, "Invalid JSON format")
                                tp = TopicPartition(msg.topic, msg.partition)
                                await self.consumer.commit({tp: msg.offset + 1})
                                """
                                
                                """
                                continue
                            
                            # Process message
                            retry_callback = self._get_retry_callback(topic)
                            dlq_callback = self._get_dlq_callback(topic, msg.value)
                            
                            success = await handler.handle(message_data, retry_callback, dlq_callback)
                            
                            if success:
                                tp = TopicPartition(msg.topic, msg.partition)
                                await self.consumer.commit({tp: msg.offset + 1})
                                """
                                
                                """
                            else:
                                logger.warning(f"Handler returned False for message in topic {topic}")
                                await asyncio.sleep(random.uniform(1, 3)) # avoid hammering both the broker and our logs. 
                                """
                                
                                """
                            
                        except Exception as e:
                            logger.exception(f"Error processing message from {topic}: {e}")
                            await self._send_to_dlq(topic, msg.value, str(e))
                            tp = TopicPartition(msg.topic, msg.partition)
                            await self.consumer.commit({tp: msg.offset + 1})
                            """
                            
                            """
                        """
                        
                        """
                
                except Exception as e:
                    logger.exception(f"Consumer error: {e}")
                    if self.running:
                        await asyncio.sleep(1)
                    
        finally:
            logger.info("Closing consumer and producers")
            await self.consumer.stop()
            await self.retry_producer.stop()
            await self.dlq_producer.stop()

    async def _retry_message(self, original_topic: str, failed_message: MessageEnvelope, reason: str) -> None:
        """
        Send a message to the retry queue.
        
        Args:
            original_topic: Original topic the message came from
            failed_message: Envelope of failed message
            reason: Reason for retrying message
        """
        retry_topic = f'{original_topic}.retry'
        
        # Extract retry count if present
        try:
            retry_count = int(failed_message.header.get("retryCount", 0))
        except ValueError:
            logger.warning(f"Invalid retryCount value: {failed_message.header.get('retryCount')}, using 0")
            retry_count = 0

        envelope_copy = MessageEnvelope.from_dict(failed_message.to_dict())

        # Increment retry count for next attempt
        envelope_copy.header["retryCount"] = retry_count + 1

        # Include metadata about original topic
        envelope_copy.header["originalTopic"] = original_topic
        envelope_copy.header["retryReason"] = reason

        try:
            await self.retry_producer.send_and_wait(
                retry_topic,
                json.dumps(envelope_copy.to_dict()).encode('utf-8')
            )
            
            logger.info(f"Message sent to retry topic {retry_topic}, attempt {retry_count}")
            """
            
            """

        except Exception as e:
            logger.error(f"Failed to send message to retry {retry_topic}: {e}")

    async def _send_to_dlq(self, original_topic: str, message: bytes, reason: str) -> None:
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
            
            await self.dlq_producer.send_and_wait(
                dlq_topic,
                json.dumps(dlq_message).encode('utf-8')
            )
            
            logger.info(f"Message sent to DLQ topic {dlq_topic}")
            """
            
            """

        except Exception as e:
            logger.error(f"Failed to send message to DLQ {dlq_topic}: {e}")

    def _get_retry_callback(self, topic: str) -> Callable[[MessageEnvelope, str], Awaitable[None]]:
        """
        Return a callback function for handlers to send messages to retry topic.
        
        Args:
            topic: Original topic
        
        Returns:
            Callable function that sends to retry topic with given reason
        """
        async def retry_message(original_message: MessageEnvelope, reason: str) -> None:
            await self._retry_message(topic, original_message, reason)
        return retry_message

    def _get_dlq_callback(self, topic: str, original_message: bytes) -> Callable[[str], Awaitable[None]]:
        """
        Return a callback function for handlers to send messages to DLQ.
        
        Args:
            topic: Original topic
            original_message: Original message bytes
        
        Returns:
            Callable function that sends to DLQ with given reason
        """
        async def send_to_dlq(reason: str) -> None:
            await self._send_to_dlq(topic, original_message, reason)
        return send_to_dlq

    async def _handle_shutdown(self, signum) -> None:
        """Handle shutdown signals gracefully."""
        logger.info("Received signal %s, shutting down…", signum)
        self.running = False
        if self.consumer is not None:
            await self.consumer.stop()
        if self.retry_producer is not None:
            await self.retry_producer.stop()
        if self.dlq_producer is not None:
            await self.dlq_producer.stop()
    
