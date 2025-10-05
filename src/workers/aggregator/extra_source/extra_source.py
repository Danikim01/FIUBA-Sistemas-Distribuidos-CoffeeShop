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
        self.client_done: dict[ClientId, Done] = {}

    def is_done(self, client_id: ClientId, block: bool = False, timeout: float | None = None) -> bool:
        """Check or wait for the extra source to finish processing for a specific client.
        
        Args:
            client_id: The client ID to check
            block: If True, block until done (or until timeout if provided).
            timeout: Optional maximum seconds to wait when block=True.
        Returns:
            True if done (or became done within the timeout), otherwise False.
        """
        if client_id not in self.client_done:
            self.client_done[client_id] = Done()
        return self.client_done[client_id]._is_done(block=block, timeout=timeout)

    def _set_done(self, client_id: ClientId):
        if client_id not in self.client_done:
            self.client_done[client_id] = Done()
        self.client_done[client_id]._set_done()
        
    def close(self):
        """Close the middleware connection."""
        self.middleware.close()

    def start_consuming(self):
        """Start consuming messages from the extra source."""

        def on_message(message):
            client_id = message.get('client_id')
            if client_id is None or client_id == '':
                logger.warning(f"Message without client_id received from extra source {self.name}, ignoring: {message}")
                return
            
            if client_id not in self.client_done:
                self.client_done[client_id] = Done()

            done = self.client_done[client_id]

            if done._is_done():
                logger.info(f"Extra source {self.name} already done, ignoring message")
                return
            
            if is_eof_message(message):
                logger.info(f"EOF received from extra source {self.name}")
                done._set_done()
                return

            self.save_message(message)

        try:
            self.middleware.start_consuming(on_message)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Error consuming from {self.name}: {exc}")

    @abstractmethod
    def save_message(self, message):
        """Handle and persist a message from the extra source.
        
        Args:
            message: The message to handle
        """
        pass

    @abstractmethod
    def get_item(self, client_id: ClientId, item_id: str) -> Any:
        pass