import logging
import os
import threading
from contextlib import suppress
from typing import Any, Dict
from message_utils import ClientId, create_message_with_metadata, extract_data_and_client_id, extract_eof_metadata
from middleware_config import MiddlewareConfig
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

logger = logging.getLogger(__name__)

Counter = Dict[str, int]  # [worker_id, count]

class EOFHandler:
    # Callback fn type: (message: Dict[str, Any]) -> None
    def __init__(self, middleware_config: MiddlewareConfig):
        self.worker_id: str = str(os.getenv('WORKER_ID', '0'))
        self.replica_count: int = int(os.getenv('REPLICA_COUNT', '1'))
        # self.max_retries: int = int(os.getenv('MAX_EOF_RETRIES', '100')) * self.replica_count
        
        self.middleware_config = middleware_config
        self.eof_consumer: RabbitMQMiddlewareQueue = middleware_config.create_eof_requeue()
        self.consuming_thread = None

        self._thread_local = threading.local()
        self._publishers_lock = threading.Lock()
        self._output_publishers: list = []
        self._requeue_publishers: list[RabbitMQMiddlewareQueue] = []

    def handle_eof(
        self,
        message: Dict[str, Any],
        client_id: ClientId,
    ) -> None:
        """Handle EOF message. Can be overridden by subclasses.
        
        Args:
            message: EOF message dictionary
            callback: Optional callback to execute before outputting EOF
        """
        _, message = extract_data_and_client_id(message)

        counter = self.get_counter(message)

        if self.should_output(counter):
            logger.info(f"Worker {self.worker_id}: Outputting EOF for client {client_id} with counter {counter}")
            self.output_eof(client_id=client_id)
        else:
            logger.info(f"Worker {self.worker_id}: Requeuing EOF for client {client_id} with counter {counter}")
            self.requeue_eof(client_id=client_id, counter=counter)

    def handle_eof_with_routing_key(self, message: Dict[str, Any], client_id: ClientId, routing_key: str = "", exchange: str = ""):
        """Handle EOF message with specific routing key.
        
        Args:
            message: EOF message dictionary
            client_id: Client identifier
            routing_key: Routing key for the message
        """
        _, message = extract_data_and_client_id(message)

        counter = self.get_counter(message)

        if self.should_output(counter):
            logger.info(f"Worker {self.worker_id}: Outputting EOF for client {client_id} with counter {counter}")
            self.output_eof_with_routing_key(client_id=client_id, routing_key=routing_key, exchange=exchange)
        else:
            logger.info(f"Worker {self.worker_id}: Requeuing EOF for client {client_id} with counter {counter}")
            self.requeue_eof(client_id=client_id, counter=counter)

    def get_counter(self, message: Dict[str, Any]) -> Counter:
        """Extract the counter from the EOF message.

        Args:
            message: EOF message dictionary
        Returns:
            Counter dictionary
        """
        additional_data: Dict[str, Any] = extract_eof_metadata(message)
        logger.info(f"EOF metadata fields: {additional_data}")
        counter: Dict[str, int] = additional_data.get('counter', {})
        counter[self.worker_id] = counter.get(self.worker_id, 0) + 1
        return counter

    def should_output(self, counter: Counter) -> bool:
        """Determine if EOF should be output based on counter.

        Args:
            counter: Dictionary tracking how many workers have processed the EOF
        Returns:
            True if EOF should be output, False otherwise
        """
        if len(counter) >= self.replica_count:
            return True
        # if any(count >= self.max_retries for count in counter.values()):
        #     return True
        return False

    def output_eof(self, client_id: ClientId):
        """Send EOF message to output with client metadata.
        
        Args:
            client_id: Client identifier
            additional_data: Additional data to include in EOF message
        """
        message = create_message_with_metadata(client_id, data=None, message_type='EOF')
        publisher = self._get_output_publisher()
        publisher.send(message)
    
    def output_eof_with_routing_key(self, client_id: ClientId, routing_key: str = "", exchange: str = ""):
        """Send EOF message to output with specific routing key.
        
        Args:
            client_id: Client identifier
            routing_key: Routing key for the message
        """
        message = create_message_with_metadata(client_id, data=None, message_type='EOF')
        publisher = self._get_output_publisher()
        
        logger.info(f"Sending eof message to routing key {routing_key} with exchange {exchange}")
        logger.info(f"EOF message: {message}")
        
        try:
            publisher.send(message, routing_key=routing_key, exchange=exchange)
            logger.info(f"[MESSAGE UTILS] Additional metadata: {message.get('counter', {})}")
        except Exception as exc:
            logger.error(f"Failed to send EOF with routing key {routing_key}: {exc}")
            raise

    def requeue_eof(self, client_id: ClientId, counter: Counter):
        """Requeue an EOF message back to the input middleware.
        
        Args:
            message: EOF message dictionary
        """
        message = create_message_with_metadata(
            client_id,
            data=None,
            message_type='EOF',
            counter=dict(counter),
        )
        publisher = self._get_requeue_publisher()
        logger.info(f"Worker {self.worker_id}: Requeuing EOF to queue {publisher.queue_name} for client {client_id}")
        publisher.send(message)

    def _get_output_publisher(self):
        publisher = getattr(self._thread_local, "output_publisher", None)
        if publisher is None:
            publisher = self.middleware_config._create_output_middleware()
            setattr(self._thread_local, "output_publisher", publisher)
            with self._publishers_lock:
                self._output_publishers.append(publisher)
        return publisher

    def _get_requeue_publisher(self) -> RabbitMQMiddlewareQueue:
        publisher = getattr(self._thread_local, "requeue_publisher", None)
        if publisher is None:
            publisher = self.middleware_config.create_eof_requeue()
            setattr(self._thread_local, "requeue_publisher", publisher)
            with self._publishers_lock:
                self._requeue_publishers.append(publisher)
        return publisher

    def start_consuming(self, on_message):
        """Start the consuming thread."""
        def _start_consuming():
            try:
                logger.info(f"[DEBUG] Worker {self.worker_id} starting EOF consumer for queue: {self.eof_consumer.queue_name}")
                self.eof_consumer.start_consuming(on_message)
            except Exception as exc:  # noqa: BLE001
                logger.error("Error consuming EOF messages: %s", exc)
            finally:
                self.consuming_thread = None
        if not self.consuming_thread or not self.consuming_thread.is_alive():
            logger.info(f"[DEBUG] Starting EOF handler consuming thread for worker {self.worker_id}")
            self.consuming_thread = threading.Thread(target=_start_consuming, daemon=True)
            self.consuming_thread.start()

    def cleanup(self) -> None:
        try:
            self.eof_consumer.close()
            with self._publishers_lock:
                publishers = list(self._output_publishers) + list(self._requeue_publishers)
                self._output_publishers.clear()
                self._requeue_publishers.clear()

            for publisher in publishers:
                with suppress(Exception):
                    publisher.close()

            if self.consuming_thread and self.consuming_thread.is_alive():
                self.consuming_thread.join(timeout=10.0)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Error closing EOF Handler: %s", exc)
