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
    extract_message_uuid,
    is_eof_message,
    is_client_reset_message,
    is_reset_all_clients_message,
    create_message_with_metadata,
)
from typing import Optional

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
        # This must be done before cleanup to ensure consumption stops cleanly
        try:
            if hasattr(self.middleware_config, 'input_middleware') and self.middleware_config.input_middleware:
                self.middleware_config.input_middleware.stop_consuming()
        except Exception as e:
            logger.warning(f"Error stopping consumption: {e}")
        
        # Clean up resources
        # This will close all connections gracefully
        self.cleanup()
    
    def send_message(self, client_id: ClientId, data: Any, routing_key: str | None = None ,  **metadata):
        """Send a message to the output with client metadata.
        
        Automatically propagates sequence_id and message_uuid from the current message being processed
        if they exist and are not explicitly provided in metadata.
        
        Args:
            client_id: Client identifier
            data: The actual data to send
            routing_key: Optional routing key for exchange-based middleware
            **metadata: Additional metadata fields (including routing_key for exchanges)
        """
        current_metadata = self._get_current_message_metadata()
        if current_metadata:
            # Propagate sequence_id from current message if not explicitly provided
            if 'sequence_id' not in metadata:
                sequence_id = extract_sequence_id(current_metadata)
                if sequence_id:
                    metadata['sequence_id'] = sequence_id
                    logger.debug(f"[WORKER {self.__class__.__name__}] Propagating sequence_id: {sequence_id}")
            
            # Propagate message_uuid from current message if not explicitly provided
            # This ensures the same UUID is maintained throughout the entire pipeline
            if 'message_uuid' not in metadata:
                message_uuid = extract_message_uuid(current_metadata)
                if message_uuid:
                    metadata['message_uuid'] = message_uuid
                    logger.debug(f"[WORKER {self.__class__.__name__}] Propagating message_uuid: {message_uuid}")
        
        message = create_message_with_metadata(client_id, data, **metadata)
        
        # Send message with routing_key if it's an exchange middleware
        if routing_key:
            #logger.info(f"[WORKER {self.__class__.__name__}] Sending message to routing key: {routing_key}")
            self.middleware_config.output_middleware.send(message, routing_key=routing_key)
        else:
            self.middleware_config.output_middleware.send(message)
    
    
    @abstractmethod
    def process_batch(self, batch: List[dict], client_id: ClientId):
        """Process a batch of messages. Must be implemented by subclasses.
        
        Args:
            batch: List of messages to process
        """
        pass

    # overwritten by top worker
    def handle_eof(self, message: Dict[str, Any], client_id: ClientId, message_uuid: Optional[str] = None):
        """Handle EOF message.
        
        Args:
            message: EOF message dictionary
            client_id: Client identifier
            message_uuid: Optional message UUID from the original message
        """
        with self._pause_message_processing():
            self.eof_handler.handle_eof(message, client_id, message_uuid)

    def start_consuming(self):
        """Start consuming messages from the input queue with automatic reconnection."""
        import time
        
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

                client_id, actual_data, message_uuid = extract_data_and_client_id(message)
                
                if is_reset_all_clients_message(message):
                    logger.info("[CONTROL] Global reset control message received by %s", self.__class__.__name__)
                    return self.handle_reset_all_clients()

                if is_client_reset_message(message):
                    logger.info("[CONTROL] Client reset message received for %s on %s", client_id, self.__class__.__name__)
                    return self.handle_client_reset(client_id)

                if is_eof_message(message):
                    logger.info(
                        f"\033[36m[BASE-WORKER] EOF message detected for client {client_id}, "
                        f"calling handle_eof (worker type: {self.__class__.__name__})\033[0m"
                    )
                    return self.handle_eof(message, client_id, message_uuid)

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
                    #logger.debug(f"Processing message for client {client_id}, data: {actual_data}")
                    if isinstance(actual_data, list):
                        #logger.info(f"[BASE-WORKER] [BATCH-START] Processing batch of {len(actual_data)} messages for client {client_id}, message id: {message.get('message_uuid')}")
                        self.process_batch(actual_data, client_id)
                        #logger.info(f"[BASE-WORKER] [BATCH-END] Batch processing completed for client {client_id}, message id: {message.get('message_uuid')}. About to return from on_message callback.")
                    else:
                        logger.info(f"Processing single message for client {client_id}, message id: {message.get('message_uuid')}")
       
                finally:
                    self._decrement_inflight()

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Re-raise the exception so the middleware can NACK
                raise
            finally:
                self._current_message_metadata = None

        # Main consumption loop with automatic reconnection
        retry_count = 0
        max_retry_delay = 60  # Maximum delay between retries (60 seconds)
        base_retry_delay = 2  # Base delay in seconds
        
        while not self.shutdown_requested:
            try:
                #self.eof_handler.start_consuming(on_message)
                if retry_count > 0:
                    logger.info(f"[BASE-WORKER] Starting message consumption (reconnection attempt {retry_count})")
                else:
                    logger.info("[BASE-WORKER] Starting message consumption")
                self.middleware_config.input_middleware.start_consuming(on_message)
                
                # If we get here, consumption stopped without error
                # This could be due to a graceful shutdown or connection loss
                if self.shutdown_requested:
                    logger.info("Consumption stopped due to shutdown request")
                    break
                
                # Consumption stopped unexpectedly, log and retry
                logger.warning("Consumption stopped unexpectedly, will attempt to reconnect...")
                retry_count += 1
                wait_time = min(max_retry_delay, base_retry_delay * (2 ** min(retry_count - 1, 5)))
                logger.info(f"Waiting {wait_time} seconds before reconnecting...")
                time.sleep(wait_time)
                
            except KeyboardInterrupt:
                logger.info("Worker interrupted by user")
                break
            except Exception as e:
                if self.shutdown_requested:
                    logger.info("Shutdown requested during error handling")
                    break
                logger.error(f"Error in consumption loop: {e}", exc_info=True)
                retry_count += 1
                wait_time = min(max_retry_delay, base_retry_delay * (2 ** min(retry_count - 1, 5)))
                logger.info(f"Error occurred, waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                # Reset retry count on successful connection
                if retry_count > 0:
                    logger.info("Successfully reconnected, resetting retry count")
                    retry_count = 0
        
        # Cleanup after loop exits
        logger.info("Exiting consumption loop, cleaning up...")
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

    # ------------------------------------------------------------------
    # Control message handlers
    # ------------------------------------------------------------------
    def handle_client_reset(self, client_id: ClientId) -> None:
        """Handle a control message that requests per-client cleanup."""
        logger.info("[CONTROL] %s received client reset for %s (default handler)", self.__class__.__name__, client_id)

    def handle_reset_all_clients(self) -> None:
        """Handle a control message that requests global cleanup."""
        logger.info("[CONTROL] %s received global reset (default handler)", self.__class__.__name__)
