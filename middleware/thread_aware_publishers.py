"""Thread-aware RabbitMQ publisher wrappers for safe multi-client usage."""

from __future__ import annotations

import threading
from contextlib import suppress
from typing import Callable, Set

from .rabbitmq_middleware import (
    MessageMiddlewareDisconnectedError,
    RabbitMQMiddlewareExchange,
    RabbitMQMiddlewareQueue,
)

QueueFactory = Callable[[], RabbitMQMiddlewareQueue]
ExchangeFactory = Callable[[], RabbitMQMiddlewareExchange]


class ThreadAwareQueuePublisher:
    """Provide a thread-local view over a queue publisher.

    Each thread obtains its own RabbitMQ connection/channel pair while sharing
    the same high-level publisher instance. This avoids sharing Pika blocking
    connections across threads, which would otherwise break when multiple
    clients send data concurrently.
    """

    def __init__(self, factory: QueueFactory):
        self._factory = factory
        self._local = threading.local()
        self._instances: Set[RabbitMQMiddlewareQueue] = set()
        self._instances_lock = threading.Lock()

    def send(self, message: object) -> None:
        """Send a message using the thread-local queue instance."""
        queue = self._get_instance()
        try:
            queue.send(message)
        except MessageMiddlewareDisconnectedError:
            # Connection dropped; recreate and retry once.
            self._reset_instance(queue)
            self._get_instance().send(message)

    def close_all(self) -> None:
        """Close every underlying queue instance."""
        with self._instances_lock:
            instances = list(self._instances)
            self._instances.clear()

        for queue in instances:
            with suppress(Exception):
                queue.close()

    def _get_instance(self) -> RabbitMQMiddlewareQueue:
        queue = getattr(self._local, 'instance', None)
        if queue is None:
            queue = self._factory()
            setattr(self._local, 'instance', queue)
            with self._instances_lock:
                self._instances.add(queue)
        return queue

    def _reset_instance(self, queue: RabbitMQMiddlewareQueue) -> None:
        with suppress(Exception):
            queue.close()

        if getattr(self._local, 'instance', None) is queue:
            delattr(self._local, 'instance')

        with self._instances_lock:
            self._instances.discard(queue)


class ThreadAwareExchangePublisher:
    """Thread-local wrapper for RabbitMQ exchange publishers."""

    def __init__(self, factory: ExchangeFactory):
        self._factory = factory
        self._local = threading.local()
        self._instances: Set[RabbitMQMiddlewareExchange] = set()
        self._instances_lock = threading.Lock()

    def send(self, message: object, routing_key: str = '') -> None:
        exchange = self._get_instance()
        try:
            exchange.send(message, routing_key=routing_key)
        except MessageMiddlewareDisconnectedError:
            self._reset_instance(exchange)
            self._get_instance().send(message, routing_key=routing_key)

    def close_all(self) -> None:
        with self._instances_lock:
            instances = list(self._instances)
            self._instances.clear()

        for exchange in instances:
            with suppress(Exception):
                exchange.close()

    def _get_instance(self) -> RabbitMQMiddlewareExchange:
        exchange = getattr(self._local, 'instance', None)
        if exchange is None:
            exchange = self._factory()
            setattr(self._local, 'instance', exchange)
            with self._instances_lock:
                self._instances.add(exchange)
        return exchange

    def _reset_instance(self, exchange: RabbitMQMiddlewareExchange) -> None:
        with suppress(Exception):
            exchange.close()

        if getattr(self._local, 'instance', None) is exchange:
            delattr(self._local, 'instance')

        with self._instances_lock:
            self._instances.discard(exchange)
