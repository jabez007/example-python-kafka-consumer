"""
Base handler interface for processing Kafka messages.
"""
import abc
import logging
from typing import Any, Callable, Dict


from typing import Awaitable



from src.models.envelope import MessageEnvelope

logger = logging.getLogger(__name__)

class ProcessingError(Exception):
    """Base exception for all message processing errors"""
    pass

class RetryableError(ProcessingError):
    """Error indicating message should be retried"""
    pass

class NonRetryableError(ProcessingError):
    """Error indicating message should go to DLQ"""
    pass

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
    
    
    async def handle(
            self,
            message_data: Dict[str, Any],
            retry_message: Callable[[MessageEnvelope, str], Awaitable[None]],
            send_to_dlq: Callable[[str], Awaitable[None]]) -> bool:
        """
        Process a message from Kafka.
        
        Args:
            message_data: The JSON-decoded message data
            retry_message: Callback function to retry processing a message
            send_to_dlq: Callback function to send message to DLQ
            
        Returns:
            bool: True if the message was handled (successfully processed, sent to DLQ, or scheduled for retry),
                  False if handling failed and the message should be reprocessed
        """
        # Parse the envelope structure
        try:
            envelope = MessageEnvelope.from_dict(message_data)
        except (ValueError, TypeError) as e:
            logger.exception(f"Error parsing message envelope: {e}")
            await send_to_dlq(f"Error parsing message envelope: {str(e)}")
            return True

        try:
            retry_count = int(envelope.header.get("retryCount", 0))
        except (ValueError, TypeError):
            logger.warning(f"Invalid retryCount value: {envelope.header.get('retryCount')}, using 0")
            retry_count = 0

        try:
            # Process the message
            return await self._process_message(envelope)

        except NonRetryableError as e:
            logger.exception(f"Unrecoverable error processing message, sending to DLQ: {e}")
            await send_to_dlq(f"Error processing message: {str(e)}")
            return True

        except RetryableError as e:
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
    
    
    
    @abc.abstractmethod
    async def _process_message(self, envelope: MessageEnvelope) -> bool:
        """
        Process the message. Must be implemented by subclasses.
        
        Args:
            envelope: Message envelope containing header and body

        Returns:
            bool: True if the message was successfully processed and should be committed,
                  False if processing failed and the message should not be committed
        """
        pass
    
