"""Base worker class providing common functionality for all workers."""

import logging
import signal
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from contextlib import contextmanager
from handle_eof import EOFHandler # pyright: ignore[reportMissingImports]
from middleware_config import MiddlewareConfig # pyright: ignore[reportMissingImports]
from message_utils import ( # pyright: ignore[reportMissingImports]
    ClientId,
    extract_data_and_client_id,
    extract_sequence_id,
    is_eof_message,
    create_message_with_metadata,
)

from healthcheck.service import HealthcheckService

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

        self.middleware_config = MiddlewareConfig()
        self.eof_handler = EOFHandler(self.middleware_config)
        self._current_message_metadata: Dict[str, Any] | None = None
        self._inflight_condition = threading.Condition()
        self._inflight_messages = 0
        self._pause_requests = 0
        self._pause_consumption = False

        # Initialize healthcheck service
        self.healthcheck_service = None
        try:
            self.healthcheck_service = HealthcheckService()
            self.healthcheck_service.start()
            logger.info("Worker %s started healthcheck service", self.__class__.__name__)
        except Exception as e:
            logger.error(f"Failed to start healthcheck service: {e}")

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
        logger.info("SIGTERM received, initiating graceful shutdown...")
        self.shutdown_requested = True
        
        # Stop consuming immediately to prevent new messages
        try:
            if hasattr(self.middleware_config, 'input_middleware'):
                self.middleware_config.input_middleware.stop_consuming()
        except Exception as e:
            logger.warning(f"Error stopping consumption: {e}")
        
        # Clean up resources
        self.cleanup()
    
    def send_message(self, client_id: ClientId, data: Any, routing_key: str | None = None ,  **metadata):
        """Send a message to the output with client metadata.
        
        Automatically propagates sequence_id from the current message being processed
        if it exists and is not explicitly provided in metadata.
        
        Args:
            client_id: Client identifier
            data: The actual data to send
            routing_key: Optional routing key for exchange-based middleware
            **metadata: Additional metadata fields (including routing_key for exchanges)
        """
        # Propagate sequence_id from current message if not explicitly provided
        if 'sequence_id' not in metadata:
            current_metadata = self._get_current_message_metadata()
            if current_metadata:
                sequence_id = extract_sequence_id(current_metadata)
                if sequence_id:
                    metadata['sequence_id'] = sequence_id
                    logger.debug(f"[WORKER {self.__class__.__name__}] Propagating sequence_id: {sequence_id}")
        
        message = create_message_with_metadata(client_id, data, **metadata)
        
        # Send message with routing_key if it's an exchange middleware
        if routing_key:
            logger.info(f"[WORKER {self.__class__.__name__}] Sending message to routing key: {routing_key}")
            self.middleware_config.output_middleware.send(message, routing_key=routing_key)
        else:
            self.middleware_config.output_middleware.send(message)
    
    @abstractmethod
    def process_message(self, message: dict, client_id: ClientId):
        """Process a single message. Must be implemented by subclasses.
        
        Args:
            message: Message to process
        """
        pass
    
    @abstractmethod
    def process_batch(self, batch: List[dict], client_id: ClientId):
        """Process a batch of messages. Must be implemented by subclasses.
        
        Args:
            batch: List of messages to process
        """
        pass

    # overwritten by top worker
    def handle_eof(self, message: Dict[str, Any], client_id: ClientId):
        with self._pause_message_processing():
            self.eof_handler.handle_eof(message, client_id)

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
                        logger.info("Shutdown requested, rejecting message to requeue")
                        raise InterruptedError("Shutdown requested, message will be requeued")

                    self._current_message_metadata = message

                    client_id, actual_data = extract_data_and_client_id(message)
                    
                    if is_eof_message(message):
                        return self.handle_eof(message, client_id)

                    # Validate actual_data before processing
                    if not isinstance(actual_data, (dict, list)):
                        logger.error(
                            f"Invalid message data type for client {client_id}: "
                            f"expected dict or list, got {type(actual_data).__name__} "
                            f"with value: {actual_data}"
                        )
                        #return

                    self._increment_inflight()
                    try:
                        logger.debug(f"Processing message for client {client_id}, data: {actual_data}")
                        if isinstance(actual_data, list):
                            logger.info(f"[BASE-WORKER] [BATCH-START] Processing batch of {len(actual_data)} messages for client {client_id}, message id: {message.get('message_uuid')}")
                            self.process_batch(actual_data, client_id)
                            logger.info(f"[BASE-WORKER] [BATCH-END] Batch processing completed for client {client_id}, message id: {message.get('message_uuid')}. About to return from on_message callback.")
                        else:
                            logger.info(f"Processing single message for client {client_id}, message id: {message.get('message_uuid')}")
                            self.process_message(actual_data, client_id)
                    finally:
                        self._decrement_inflight()

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Re-raise the exception so the middleware can NACK
                    raise
                finally:
                    self._current_message_metadata = None

            #self.eof_handler.start_consuming(on_message)
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
            # Stop healthcheck service
            if self.healthcheck_service:
                try:
                    self.healthcheck_service.stop()
                except Exception as e:
                    logger.warning(f"Error stopping healthcheck service: {e}")
            
            self.eof_handler.cleanup()
            self.middleware_config.cleanup()
            logger.info("Resources cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up resources: {e}")

    def _get_current_message_metadata(self) -> Dict[str, Any] | None:
        """Return the metadata of the message currently being processed."""
        return self._current_message_metadata

    def _increment_inflight(self) -> None:
        with self._inflight_condition:
            while self._pause_consumption:
                self._inflight_condition.wait()
            self._inflight_messages += 1

    def _decrement_inflight(self) -> None:
        with self._inflight_condition:
            if self._inflight_messages > 0:
                self._inflight_messages -= 1
            if self._inflight_messages == 0:
                self._inflight_condition.notify_all()

    def _begin_pause(self) -> None:
        with self._inflight_condition:
            self._pause_requests += 1
            self._pause_consumption = True
            while self._inflight_messages > 0:
                self._inflight_condition.wait()

    def _end_pause(self) -> None:
        with self._inflight_condition:
            if self._pause_requests > 0:
                self._pause_requests -= 1
            if self._pause_requests == 0:
                self._pause_consumption = False
                self._inflight_condition.notify_all()

    @contextmanager
    def _pause_message_processing(self):
        self._begin_pause()
        try:
            yield
        finally:
            self._end_pause()
