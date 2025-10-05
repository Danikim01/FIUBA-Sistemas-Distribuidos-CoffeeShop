from abc import ABC, abstractmethod
import logging
import threading
from typing import Any
from message_utils import ClientId, is_eof_message
from middleware.rabbitmq_middleware import RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue
from workers.aggregator.extra_source.done import Done

logger = logging.getLogger(__name__)

class ExtraSource(ABC):
    def __init__(self, name: str, middleware: RabbitMQMiddlewareQueue | RabbitMQMiddlewareExchange ):
        """Initialize an extra source for the worker.
        
        Args:
            name: Name of the extra source
            queue: Queue name for the extra source (optional)
        """
        self.name = name
        self.middleware = middleware
        self.clients_done = Done()
        self.consuming_thread = threading.Thread(target=self._start_consuming, daemon=True)
        
    def close(self):
        """Close the middleware connection."""
        self.middleware.close()
        if self.consuming_thread.is_alive():
            self.consuming_thread.join(timeout=10.0)

    def _start_consuming(self):
        """Start consuming messages from the extra source."""

        def on_message(message):
            client_id = message.get('client_id')
            if client_id is None or client_id == '':
                logger.warning(f"Message without client_id received from extra source {self.name}, ignoring: {message}")
                return

            if self.clients_done.is_client_done(client_id):
                logger.info(f"Extra source {self.name} already done, ignoring message")
                return
            
            if is_eof_message(message):
                logger.info(f"EOF received from extra source {self.name}")
                self.clients_done.set_done(client_id)
                return

            self.save_message(message)

        try:
            self.middleware.start_consuming(on_message)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Error consuming from {self.name}: {exc}")

    def start_consuming(self):
        """Start the consuming thread."""
        if not self.consuming_thread.is_alive():
            self.consuming_thread.start()

    @abstractmethod
    def save_message(self, message):
        """Handle and persist a message from the extra source.
        
        Args:
            message: The message to handle
        """
        raise NotImplementedError
    
    @abstractmethod
    def _get_item(self, client_id: ClientId, item_id: str) -> str:
        raise NotImplementedError

    def get_item_when_done(
        self,
        client_id: ClientId,
        item_id: str,
    ) -> str:
        if self.clients_done.is_client_done(client_id, block=True, timeout=10.0):
            return self._get_item(client_id, item_id)
        logger.warning(
            "Timed out waiting for extra source %s to finish for client %s before retrieving %s",
            self.name,
            client_id,
            item_id,
        )
        return self._get_item(client_id, item_id)
