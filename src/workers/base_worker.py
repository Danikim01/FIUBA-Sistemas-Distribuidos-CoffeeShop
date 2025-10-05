"""Base worker class providing common functionality for all workers."""

import logging
import signal
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from handle_eof import EOFHandler
from middleware_config import MiddlewareConfig
from message_utils import (
    is_eof_message,
    extract_client_metadata,
    create_message_with_metadata,
)

logger = logging.getLogger(__name__)

class BaseWorker(ABC):
    """Base class for all workers providing common functionality.
    
    Handles middleware setup, signal handling, and message processing patterns.
    """
    
    def __init__(self):
        """Initialize base worker.
        """
        # Configure SIGTERM handling
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.shutdown_requested = False
        self.current_client_id = ''  # Track current client being processed

        self.middleware_config = MiddlewareConfig()
        self.eof_handler = EOFHandler(self.middleware_config)

        logger.info(
            "%s initialized - Input: %s, Output: %s",
            self.__class__.__name__,
            self.middleware_config.get_input_target(),
            self.middleware_config.get_output_target()
        )
    
    def _handle_sigterm(self, signum, frame):
        """Handle SIGTERM signal for graceful shutdown.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        logger.info("SIGTERM received, initiating graceful shutdown...", signum, frame)
        self.middleware_config.input_middleware.stop_consuming()
        self.shutdown_requested = True
    
    def send_message(self, data: Any, client_id: str = '', **metadata):
        """Send a message to the output with client metadata.
        
        Args:
            data: The actual data to send
            client_id: Client identifier
            **metadata: Additional metadata fields
        """
        if client_id == '':
            client_id = self.current_client_id
        message = create_message_with_metadata(client_id, data, **metadata)
        self.middleware_config.output_middleware.send(message)
    
    @abstractmethod
    def process_message(self, message: dict):
        """Process a single message. Must be implemented by subclasses.
        
        Args:
            message: Message to process
        """
        pass
    
    @abstractmethod
    def process_batch(self, batch: List[dict]):
        """Process a batch of messages. Must be implemented by subclasses.
        
        Args:
            batch: List of messages to process
        """
        pass

    # overwritten by top worker
    def handle_eof(self, message: Dict[str, Any]):
        """Handle EOF message. Can be overridden by subclasses.
        
        Args:
            message: EOF message dictionary
        """
        self.eof_handler.handle_eof(message)

    def start_consuming(self):
        """Start consuming messages from the input queue."""
        try:
            def on_message(message):
                """Callback for processing received messages.
                
                Args:
                    message: Received message
                """
                try:
                    if self.shutdown_requested:
                        logger.info("Shutdown requested, stopping message processing")
                        return
                    
                    if is_eof_message(message):
                        self.handle_eof(message)
                        return
                    
                    client_id, actual_data = extract_client_metadata(message)
                    self.current_client_id = client_id

                    logger.info(f"Processing message for client {client_id}")
                    
                    if isinstance(actual_data, list):
                        self.process_batch(actual_data)
                    else:
                        self.process_message(actual_data)
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

            self.middleware_config.input_middleware.start_consuming(on_message)

        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        except Exception as e:
            logger.error(f"Error starting consumption: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        try:
            self.eof_handler.cleanup()
            self.middleware_config.cleanup()
            logger.info("Resources cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up resources: {e}")
