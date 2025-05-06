"""
Base handler interface for processing Kafka messages.
"""
import abc
import logging
from typing import Any, Callable, Dict


from typing import Awaitable





from src.models.envelope import MessageEnvelope

logger = logging.getLogger(__name__)


class BaseHandler(abc.ABC):
    """
    Base handler interface that all topic-specific handlers must implement.
    """
    
    def __init__(self, max_retries: int = 3):
        """
        Initialize handler.
        
        Args:
            max_retries: Maximum number of retry attempts before sending to DLQ
        """
        self.max_retries = max_retries
    
    
    async def handle(self, message_data: Dict[str, Any], retry_message: Callable[[MessageEnvelope, str], Awaitable[None]], send_to_dlq: Callable[[str], Awaitable[None]]) -> bool:
        """
        Process a message from Kafka.
        
        Args:
            message_data: The JSON-decoded message data
            retry_message: Callback function to retry processing a message
            send_to_dlq: Callback function to send message to DLQ
            
        Returns:
            bool: True if message was processed successfully, False otherwise
        """
        try:
            # Parse the envelope structure
            envelope = MessageEnvelope.from_dict(message_data)
            
            # Validate the message format
            if not self._validate_message(envelope):
                await send_to_dlq("Invalid message format")
                return True
            
            # Extract retry count if present
            try:
                retry_count = int(envelope.header.get("retryCount", 0))
            except ValueError:
                logger.warning(f"Invalid retryCount value: {envelope.header.get('retryCount')}, using 0")
                retry_count = 0

            try:
                # Process the message
                return await self._process_message(envelope)
                
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                
                # Check if we should retry
                if retry_count < self.max_retries:
                    logger.info(f"Retrying message, attempt {retry_count + 1} of {self.max_retries}")
                    await retry_message(envelope, str(e))
                    return True
                else:
                    logger.warning(f"Max retries ({self.max_retries}) reached, sending to DLQ")
                    await send_to_dlq(f"Max retries reached: {str(e)}")
                    return True
                    
        except Exception as e:
            logger.exception(f"Error parsing message envelope: {e}")
            await send_to_dlq(f"Error parsing message envelope: {str(e)}")
            return True
    
    
    def _validate_message(self, envelope: MessageEnvelope) -> bool:
        """
        Validate message format.
        
        Args:
            envelope: Message envelope to validate
            
        Returns:
            bool: True if message is valid, False otherwise
        """
        # Validate required header fields
        required_fields = ["messageType", "timestamp", "producer"]
        for field in required_fields:
            if field not in envelope.header:
                logger.error(f"Missing required header field: {field}")
                return False
        
        
        
        return True
    
    
    
    
    @abc.abstractmethod
    async def _process_message(self, envelope: MessageEnvelope) -> bool:
        """
        Process the message. Must be implemented by subclasses.
        
        Args:
            envelope: Message envelope containing header and body
        """
        pass
    
