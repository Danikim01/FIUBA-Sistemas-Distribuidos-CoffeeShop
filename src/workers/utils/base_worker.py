"""Base worker class providing common functionality for all workers."""

import logging
import signal
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue, RabbitMQMiddlewareExchange
from worker_config import WorkerConfig
from message_utils import (
    is_eof_message,
    extract_client_metadata,
    create_message_with_metadata,
    extract_eof_metadata
)

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    """Base class for all workers providing common functionality.
    
    Handles middleware setup, signal handling, and message processing patterns.
    """
    
    def __init__(self):
        """Initialize base worker.
        """
        config = WorkerConfig()

        self.config = config
        self.shutdown_requested = False
        self.current_client_id = ''  # Track current client being processed
        
        # Configure SIGTERM handling
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        
        # Setup input middleware
        self.input_middleware = RabbitMQMiddlewareQueue(
            host=config.rabbitmq_host,
            queue_name=config.input_queue,
            port=config.rabbitmq_port,
            prefetch_count=config.prefetch_count
        )
        
        # Setup output middleware (fanout exchange or direct queue)
        if config.has_output_exchange():
            self.output_middleware = RabbitMQMiddlewareExchange(
                host=config.rabbitmq_host,
                exchange_name=config.output_exchange,
                route_keys=[],  # Not needed for fanout
                exchange_type='fanout',
                port=config.rabbitmq_port
            )
        elif config.has_output_queue():
            self.output_middleware = RabbitMQMiddlewareQueue(
                host=config.rabbitmq_host,
                queue_name=config.output_queue,  # type: ignore
                port=config.rabbitmq_port
            )
        else:
            self.output_middleware = None
        
        logger.info(
            "%s initialized - Input: %s, Output: %s",
            self.__class__.__name__,
            config.input_queue,
            config.get_output_target()
        )
    
    def _handle_sigterm(self, signum, frame):
        """Handle SIGTERM signal for graceful shutdown.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        logger.info("SIGTERM received, initiating graceful shutdown...", signum, frame)
        self.input_middleware.stop_consuming()
        self.shutdown_requested = True
    
    def send_message(self, data: Any, client_id: str = '', **metadata):
        """Send a message to the output with client metadata.
        
        Args:
            data: The actual data to send
            client_id: Client identifier
            **metadata: Additional metadata fields
        """
        if self.output_middleware is None:
            raise Exception("Output middleware is not configured")
        if client_id == '':
            client_id = self.current_client_id
        message = create_message_with_metadata(client_id, data, **metadata)
        self.output_middleware.send(message)
    
    def send_eof(self, client_id: str = '', additional_data: Optional[Dict[str, Any]] = None):
        """Send EOF message to output with client metadata.
        
        Args:
            client_id: Client identifier
            additional_data: Additional data to include in EOF message
        """
        if client_id == '':
            client_id = self.current_client_id
        eof_data = additional_data or {}
        self.send_message(eof_data, client_id=client_id, type='EOF')
    
    @abstractmethod
    def process_message(self, message: Any):
        """Process a single message. Must be implemented by subclasses.
        
        Args:
            message: Message to process
        """
        pass
    
    @abstractmethod
    def process_batch(self, batch: List[Any]):
        """Process a batch of messages. Must be implemented by subclasses.
        
        Args:
            batch: List of messages to process
        """
        pass
    
    def handle_eof(self, message: Dict[str, Any]):
        """Handle EOF message. Can be overridden by subclasses.
        
        Args:
            message: EOF message dictionary
        """
        # Extract client_id from EOF message
        client_id = message.get('client_id', self.current_client_id)
        data_type = message.get('data_type', '')
        
        logger.info(f"Received EOF for client {client_id}, data_type: {data_type}")
        
        # Forward EOF with client metadata
        additional_data = extract_eof_metadata(message)
        self.send_eof(client_id=client_id, additional_data=additional_data)
        self.input_middleware.stop_consuming()
    
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
                    
                    # Extract client metadata
                    client_id, actual_data = extract_client_metadata(message)
                    
                    # Store client_id for use in response methods
                    self.current_client_id = client_id

                    logger.info(f"Processing message for client {client_id}")
                    
                    if isinstance(actual_data, list):
                        self.process_batch(actual_data)
                    else:
                        self.process_message(actual_data)
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
            self.input_middleware.start_consuming(on_message)
            
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        except Exception as e:
            logger.error(f"Error starting consumption: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        try:
            if self.input_middleware:
                self.input_middleware.close()
            if self.output_middleware:
                self.output_middleware.close()
            logger.info("Resources cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up resources: {e}")