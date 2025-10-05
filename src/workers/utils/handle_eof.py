import logging
import os
from typing import Any, Dict, cast
from message_utils import ClientId, create_message_with_metadata, extract_eof_metadata
from middleware_config import MiddlewareConfig
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

logger = logging.getLogger(__name__)

Counter = Dict[int, int]  # [worker_id, count]

class EOFHandler:
    def __init__(self, middleware_config: MiddlewareConfig):
        self.worker_id: int = int(os.getenv('WORKER_ID', '0'))
        self.replica_count: int = int(os.getenv('REPLICA_COUNT', '1'))
        # self.max_retries: int = int(os.getenv('MAX_EOF_RETRIES', '5'))
        self.max_retries: int = self.replica_count * 2
        
        self.middleware_config = middleware_config
        self._queue_requeue_middleware: RabbitMQMiddlewareQueue = self.get_input_queue()

    def handle_eof(
        self,
        message: Dict[str, Any],
    ) -> None:
        """Handle EOF message. Can be overridden by subclasses.
        
        Args:
            message: EOF message dictionary
            callback: Optional callback to execute before outputting EOF
        """
        client_id = message.get('client_id', '')
        if client_id == '':
            logger.error("No client_id found in EOF message")
            return

        counter = self.get_counter(message)

        if self.should_output(counter):
            self.output_eof(client_id=client_id)
        else:
            self.requeue_eof(client_id=client_id, counter=counter)

    def get_counter(self, message: Dict[str, Any]) -> Counter:
        """Extract the counter from the EOF message.

        Args:
            message: EOF message dictionary
        Returns:
            Counter dictionary
        """
        additional_data: Dict[str, Any] = extract_eof_metadata(message)
        counter: Dict[int, int] = additional_data.get('counter', {})
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
        if any(count >= self.max_retries for count in counter.values()):
            return True
        return False

    def output_eof(self, client_id: ClientId):
        """Send EOF message to output with client metadata.
        
        Args:
            client_id: Client identifier
            additional_data: Additional data to include in EOF message
        """
        message = create_message_with_metadata(client_id, data=None, message_type='EOF')
        self.middleware_config.output_middleware.send(message)

    def get_input_queue(self) -> RabbitMQMiddlewareQueue:
        """Get the final input queue name, if applicable."""
        if self.middleware_config.has_input_exchange() and self.middleware_config.input_queue:
            return self.middleware_config.create_queue(self.middleware_config.input_queue)
        return cast(RabbitMQMiddlewareQueue, self.middleware_config.input_middleware)

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
        self._queue_requeue_middleware.send(message)

    def cleanup(self) -> None:
        try:
            self._queue_requeue_middleware.close()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Error closing requeue middleware: %s", exc)
