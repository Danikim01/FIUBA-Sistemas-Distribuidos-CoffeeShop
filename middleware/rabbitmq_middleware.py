#!/usr/bin/env python3

"""Robust RabbitMQ middleware helpers built on pika's BlockingConnection."""

from __future__ import annotations

import json
import logging
import threading
import time
from contextlib import suppress
from typing import Any, Callable, List, Optional, Tuple

import pika  # type: ignore
import pika.exceptions  # type: ignore

from .connection_manager import RobustRabbitMQConnection
from .middleware_interface import (
    MessageMiddleware,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)

logger = logging.getLogger(__name__)


def serialize_message(message: Any) -> str:
    """Serialize a message to JSON."""
    try:
        return json.dumps(message, ensure_ascii=False)
    except Exception as exc:  # noqa: BLE001
        raise MessageMiddlewareMessageError(f"Error serializando mensaje: {exc}") from exc


def deserialize_message(serialized_message: str) -> Any:
    """Deserialize JSON payloads coming from RabbitMQ."""
    try:
        return json.loads(serialized_message)
    except Exception as exc:  # noqa: BLE001
        raise MessageMiddlewareMessageError(f"Error deserializando mensaje: {exc}") from exc


class _BaseRabbitMQMiddleware(MessageMiddleware):
    """Shared helpers for queue and exchange middleware implementations."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        prefetch_count: int,
        heartbeat: int = 600,
        blocked_connection_timeout: int = 300,
        max_reconnect_attempts: int = 5,
        base_retry_delay: float = 1.0,
        max_retry_delay: float = 30.0,
    ):
        self.host = host
        self.port = port
        self.prefetch_count = prefetch_count
        self.consuming = False

        self._connection_manager = RobustRabbitMQConnection(
            host=host,
            port=port,
            heartbeat=heartbeat,
            blocked_connection_timeout=blocked_connection_timeout,
        )

        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._active_channel: Optional[pika.channel.Channel] = None
        self._active_connection: Optional[pika.BlockingConnection] = None
        
        # Thread-local storage for persistent send channels (one channel per thread)
        # Pika channels are NOT thread-safe, so each thread must have its own channel
        # This ensures message ordering per thread while maintaining thread safety
        self._send_channel_local = threading.local()

        self._max_reconnect_attempts = max_reconnect_attempts
        self._base_retry_delay = base_retry_delay
        self._max_retry_delay = max_retry_delay

    # ------------------------------------------------------------------
    # Helpers shared by subclasses
    # ------------------------------------------------------------------
    def _wait_before_retry(self, attempt: int) -> None:
        delay = min(self._base_retry_delay * (2**attempt), self._max_retry_delay)
        logger.info("Reintentando conexión a RabbitMQ en %.1f segundos...", delay)
        time.sleep(delay)

    def _set_active_channel(
        self,
        connection: pika.BlockingConnection,
        channel: pika.channel.Channel,
    ) -> None:
        with self._lock:
            self._active_connection = connection
            self._active_channel = channel

    def _clear_active_channel(self) -> None:
        with self._lock:
            self._active_channel = None
            self._active_connection = None
    
    def _get_or_create_send_channel(self) -> pika.channel.Channel:
        """
        Get or create a persistent channel for sending messages (thread-local).
        Each thread gets its own channel to maintain message ordering per thread.
        Thread-safe: uses thread-local storage (Pika channels are NOT thread-safe).
        """
        # Get thread-local channel
        if not hasattr(self._send_channel_local, 'channel'):
            # Create new channel for this thread
            try:
                connection = self._connection_manager.get_connection()
                channel = connection.channel()
                # Enable publisher confirms for reliability
                with suppress(Exception):
                    channel.confirm_delivery()
                
                self._send_channel_local.channel = channel
                self._send_channel_local.connection = connection
                logger.debug(f"Created new persistent send channel for thread {threading.current_thread().name}")
            except Exception as e:
                logger.error(f"Failed to create send channel: {e}")
                raise
        
        # Check if existing channel is still valid
        channel = self._send_channel_local.channel
        connection = self._send_channel_local.connection
        
        if channel and channel.is_open and connection:
            try:
                # Test channel with a simple operation
                connection.process_data_events(time_limit=0)
                return channel
            except Exception as e:
                logger.warning(
                    f"Send channel failed health check in thread {threading.current_thread().name}: {e}, recreating..."
                )
                self._close_send_channel()
                # Recursively call to create new channel
                return self._get_or_create_send_channel()
        
        # Channel is invalid, create new one
        return self._get_or_create_send_channel()
    
    def _close_send_channel(self) -> None:
        """Close the persistent send channel for current thread (thread-safe)."""
        if hasattr(self._send_channel_local, 'channel'):
            channel = self._send_channel_local.channel
            if channel and channel.is_open:
                try:
                    channel.close()
                except Exception:
                    pass
            delattr(self._send_channel_local, 'channel')
            if hasattr(self._send_channel_local, 'connection'):
                delattr(self._send_channel_local, 'connection')

    def _stop_consuming_threadsafe(self) -> None:
        with self._lock:
            channel = self._active_channel
            connection = self._active_connection

        if not channel:
            return

        def _stop() -> None:
            with suppress(Exception):
                if channel and not channel.is_closed:
                    channel.stop_consuming()

        if connection and not connection.is_closed:
            try:
                connection.add_callback_threadsafe(_stop)
                return
            except Exception:  # noqa: BLE001
                pass

        _stop()

    # ------------------------------------------------------------------
    # Public API required by MessageMiddleware
    # ------------------------------------------------------------------
    def stop_consuming(self) -> None:
        self._stop_event.set()
        self._stop_consuming_threadsafe()

    def close(self) -> None:
        self._stop_event.set()
        self._stop_consuming_threadsafe()

        # Clear active channel before closing connection
        self._clear_active_channel()
        # Close send channel
        self._close_send_channel()
        self.consuming = False

        try:
            self._connection_manager.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Error cerrando conexión: {exc}")
            # Don't raise exception during shutdown


class RabbitMQMiddlewareQueue(_BaseRabbitMQMiddleware):
    """Reliable queue middleware with automatic reconnection and retry logic."""

    def __init__(
        self,
        host: str,
        queue_name: str,
        port: int = 5672,
        prefetch_count: int = 10,
    ):
        super().__init__(host, port, prefetch_count=prefetch_count)
        self.queue_name = queue_name
        self._publish_retry_attempts = 5

        logger.info(
            "Inicializando RabbitMQ Queue Middleware (robusto): %s:%s/%s",
            host,
            port,
            queue_name,
        )

    # ------------------------------------------------------------------
    # Queue-specific helpers
    # ------------------------------------------------------------------
    def _declare_queue(self, channel: pika.channel.Channel) -> None:
        channel.queue_declare(queue=self.queue_name, durable=False)

    def _handle_queue_message(
        self,
        user_callback: Callable[[Any], None],
        ch: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        body: bytes,
    ) -> None:
        if self._stop_event.is_set():
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        try:
            message = deserialize_message(body.decode("utf-8"))
        except MessageMiddlewareMessageError as exc:
            logger.error("Descartando mensaje inválido en '%s': %s", self.queue_name, exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error deserializando mensaje en '%s': %s", self.queue_name, exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        try:
            user_callback(message)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error procesando mensaje en '%s': %s", self.queue_name, exc)
            logger.info(f"[RABBITMQ-MIDDLEWARE] [NACK] Sending NACK with requeue=True for queue '{self.queue_name}'")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            logger.info(f"[RABBITMQ-MIDDLEWARE] [ACK-PREPARE] Callback completed successfully for queue '{self.queue_name}'. About to send ACK.")
            logger.info(f"[RABBITMQ-MIDDLEWARE] [ACK] Sending ACK for queue '{self.queue_name}', delivery_tag={method.delivery_tag}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"[RABBITMQ-MIDDLEWARE] [ACK-SENT] ACK sent successfully for queue '{self.queue_name}'")

    # ------------------------------------------------------------------
    # MessageMiddleware implementation
    # ------------------------------------------------------------------
    def start_consuming(self, on_message_callback: Callable[[Any], None]) -> None:
        if self.consuming:
            logger.warning("Ya se está consumiendo la cola '%s'", self.queue_name)
            return

        self._stop_event.clear()
        attempt = 0

        while not self._stop_event.is_set():
            channel: Optional[pika.channel.Channel] = None
            connection: Optional[pika.BlockingConnection] = None

            try:
                connection = self._connection_manager.get_connection()
                channel = connection.channel()
                channel.basic_qos(prefetch_count=self.prefetch_count)
                self._declare_queue(channel)
                self._set_active_channel(connection, channel)

                channel.basic_consume(
                    queue=self.queue_name,
                    on_message_callback=lambda ch_, method, properties, body: self._handle_queue_message(
                        on_message_callback,
                        ch_,
                        method,
                        body,
                    ),
                    auto_ack=False,
                )

                self.consuming = True
                logger.info("Iniciando consumo de la cola '%s'", self.queue_name)
                channel.start_consuming()

                if self._stop_event.is_set():
                    break

                # Consumption stopped without being asked to; treat as graceful exit.
                return

            except pika.exceptions.ChannelClosedByBroker as exc:
                logger.warning(
                    "Canal cerrado por el broker para la cola '%s': %s. Reintentando...",
                    self.queue_name,
                    exc,
                )
            except pika.exceptions.AMQPConnectionError as exc:
                logger.warning("Conexión perdida con RabbitMQ: %s. Reintentando...", exc)
            except OSError as exc:
                if exc.errno == 9:  # Bad file descriptor
                    logger.info("Conexión cerrada durante shutdown, terminando consumo")
                    break
                else:
                    logger.exception("Error de sistema en '%s': %s", self.queue_name, exc)
                    raise MessageMiddlewareMessageError(f"Error de sistema: {exc}") from exc
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error interno iniciando consumo en '%s': %s", self.queue_name, exc)
                raise MessageMiddlewareMessageError(f"Error interno iniciando consumo: {exc}") from exc
            finally:
                self.consuming = False
                self._clear_active_channel()
                if channel and channel.is_open:
                    with suppress(Exception):
                        channel.close()

            if self._stop_event.is_set():
                break

            attempt += 1
            if attempt > self._max_reconnect_attempts:
                raise MessageMiddlewareDisconnectedError(
                    f"Pérdida de conexión con RabbitMQ después de {attempt} intentos"
                )

            self._wait_before_retry(attempt - 1)

    def send(self, message: Any, routing_key: str = "", exchange: str = "") -> None:
        """
        Send a message using a persistent channel for ordering guarantees.
        The channel is reused across all sends to maintain message order.
        """
        payload = serialize_message(message).encode("utf-8")
        attempt = 0

        while True:
            try:
                # Get or create persistent send channel (thread-safe)
                channel = self._get_or_create_send_channel()
                
                # Ensure queue is declared (idempotent operation)
                self._declare_queue(channel)

                # Publish message using persistent channel
                channel.basic_publish(
                    exchange=exchange if exchange else "",
                    routing_key=self.queue_name if not routing_key else routing_key,
                    body=payload,
                )
                return

            except pika.exceptions.UnroutableError as exc:
                logger.warning("Mensaje no enrutable en '%s': %s", self.queue_name, exc)
                return
            except pika.exceptions.NackError as exc:
                raise MessageMiddlewareMessageError(f"Mensaje rechazado por RabbitMQ: {exc}") from exc
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelWrongStateError) as exc:
                logger.warning("Conexión/canal perdido enviando a '%s': %s, recreando canal...", self.queue_name, exc)
                # Close invalid channel and retry
                self._close_send_channel()
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error enviando mensaje a '%s': %s", self.queue_name, exc)
                # Close channel on unexpected errors and retry
                self._close_send_channel()

            attempt += 1
            if attempt > self._publish_retry_attempts:
                raise MessageMiddlewareDisconnectedError(
                    f"Pérdida de conexión enviando mensaje a '{self.queue_name}'"
                )

            self._wait_before_retry(attempt - 1)

    def delete(self) -> None:
        channel: Optional[pika.channel.Channel] = None
        try:
            connection = self._connection_manager.get_connection()
            channel = connection.channel()
            channel.queue_delete(queue=self.queue_name)
        except Exception as exc:  # noqa: BLE001
            raise MessageMiddlewareDeleteError(f"Error eliminando cola: {exc}") from exc
        finally:
            if channel and channel.is_open:
                with suppress(Exception):
                    channel.close()


class RabbitMQMiddlewareExchange(_BaseRabbitMQMiddleware):
    """Reliable exchange middleware supporting shared or exclusive queues."""

    def __init__(
        self,
        host: str,
        exchange_name: str,
        route_keys: Optional[List[str]],
        *,
        exchange_type: str = "direct",
        port: int = 5672,
        queue_name: Optional[str] = None,
        prefetch_count: int = 10,
    ):
        super().__init__(host, port, prefetch_count=prefetch_count)
        self.exchange_name = exchange_name
        self.route_keys = route_keys or []
        self.exchange_type = exchange_type
        self._queue_override = queue_name

        logger.info(
            "Inicializando RabbitMQ Exchange Middleware: %s:%s/%s",
            host,
            port,
            exchange_name,
        )

    # ------------------------------------------------------------------
    # Exchange helpers
    # ------------------------------------------------------------------
    def _declare_bindings(self, channel: pika.channel.Channel) -> Tuple[str, bool]:
        channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=self.exchange_type,
            durable=False,
        )

        shared_queue = bool(self._queue_override)
        if shared_queue:
            target_queue = self._queue_override or ""
            channel.queue_declare(queue=target_queue, durable=False)
        else:
            result = channel.queue_declare(queue="", exclusive=True)
            target_queue = result.method.queue

        if self.exchange_type == "fanout":
            channel.queue_bind(exchange=self.exchange_name, queue=target_queue)
        else:
            route_keys = self.route_keys or [self.exchange_name]
            for route_key in route_keys:
                channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=target_queue,
                    routing_key=route_key,
                )

        if shared_queue:
            channel.basic_qos(prefetch_count=self.prefetch_count)

        return target_queue, shared_queue

    def _handle_exchange_message(
        self,
        user_callback: Callable[[Any], None],
        shared_queue: bool,
        ch: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        body: bytes,
    ) -> None:
        if self._stop_event.is_set():
            if shared_queue:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        try:
            message = deserialize_message(body.decode("utf-8"))
        except MessageMiddlewareMessageError as exc:
            logger.error("Descartando mensaje inválido del exchange '%s': %s", self.exchange_name, exc)
            if shared_queue:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error deserializando mensaje desde '%s': %s", self.exchange_name, exc)
            if shared_queue:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        try:
            user_callback(message)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error procesando mensaje desde '%s': %s", self.exchange_name, exc)
            if shared_queue:
                logger.info(f"[RABBITMQ-MIDDLEWARE] [NACK] Sending NACK with requeue=True for exchange '{self.exchange_name}'")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            if shared_queue:
                logger.info(f"[RABBITMQ-MIDDLEWARE] [ACK-PREPARE] Callback completed successfully for exchange '{self.exchange_name}'. About to send ACK.")
                logger.info(f"[RABBITMQ-MIDDLEWARE] [ACK] Sending ACK for exchange '{self.exchange_name}', delivery_tag={method.delivery_tag}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info(f"[RABBITMQ-MIDDLEWARE] [ACK-SENT] ACK sent successfully for exchange '{self.exchange_name}'")

    # ------------------------------------------------------------------
    # MessageMiddleware implementation
    # ------------------------------------------------------------------
    def start_consuming(self, on_message_callback: Callable[[Any], None]) -> None:
        if self.consuming:
            logger.warning("Ya se está consumiendo el exchange '%s'", self.exchange_name)
            return

        self._stop_event.clear()
        attempt = 0

        while not self._stop_event.is_set():
            channel: Optional[pika.channel.Channel] = None
            connection: Optional[pika.BlockingConnection] = None

            try:
                connection = self._connection_manager.get_connection()
                channel = connection.channel()
                target_queue, shared_queue = self._declare_bindings(channel)
                self._set_active_channel(connection, channel)

                channel.basic_consume(
                    queue=target_queue,
                    on_message_callback=lambda ch_, method, properties, body: self._handle_exchange_message(
                        on_message_callback,
                        shared_queue,
                        ch_,
                        method,
                        body,
                    ),
                    auto_ack=not shared_queue,
                )

                self.consuming = True
                logger.info("Iniciando consumo del exchange '%s'", self.exchange_name)
                channel.start_consuming()

                if self._stop_event.is_set():
                    break

                return

            except pika.exceptions.ChannelClosedByBroker as exc:
                logger.warning(
                    "Canal cerrado por el broker para exchange '%s': %s. Reintentando...",
                    self.exchange_name,
                    exc,
                )
            except pika.exceptions.AMQPConnectionError as exc:
                logger.warning("Conexión perdida con RabbitMQ: %s. Reintentando...", exc)
            except OSError as exc:
                if exc.errno == 9:  # Bad file descriptor
                    logger.info("Conexión cerrada durante shutdown, terminando consumo")
                    break
                else:
                    logger.exception("Error de sistema en '%s': %s", self.exchange_name, exc)
                    raise MessageMiddlewareMessageError(f"Error de sistema: {exc}") from exc
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error interno iniciando consumo en '%s': %s", self.exchange_name, exc)
                raise MessageMiddlewareMessageError(f"Error interno iniciando consumo: {exc}") from exc
            finally:
                self.consuming = False
                self._clear_active_channel()
                if channel and channel.is_open:
                    with suppress(Exception):
                        channel.close()

            if self._stop_event.is_set():
                break

            attempt += 1
            if attempt > self._max_reconnect_attempts:
                raise MessageMiddlewareDisconnectedError(
                    f"Pérdida de conexión con RabbitMQ después de {attempt} intentos"
                )

            self._wait_before_retry(attempt - 1)

    def send(self, message: Any, routing_key: str = "", exchange: str = "") -> None:
        """
        Send a message using a persistent channel for ordering guarantees.
        The channel is reused across all sends to maintain message order.
        """
        payload = serialize_message(message).encode("utf-8")
        attempt = 0

        while True:
            try:
                # Get or create persistent send channel (thread-safe)
                channel = self._get_or_create_send_channel()
                
                # Ensure exchange is declared (idempotent operation)
                channel.exchange_declare(
                    exchange=self.exchange_name,
                    exchange_type=self.exchange_type,
                    durable=False,
                )

                # Determine routing key
                effective_routing_key = routing_key
                if not effective_routing_key:
                    if self.exchange_type == "fanout":
                        effective_routing_key = ""
                    elif self.route_keys:
                        effective_routing_key = self.route_keys[0]

                # Publish message using persistent channel
                channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=effective_routing_key,
                    body=payload,
                )
                return

            except pika.exceptions.UnroutableError as exc:
                logger.warning("Mensaje no enrutable en exchange '%s': %s", self.exchange_name, exc)
                return
            except pika.exceptions.NackError as exc:
                raise MessageMiddlewareMessageError(f"Mensaje rechazado por RabbitMQ: {exc}") from exc
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelWrongStateError) as exc:
                logger.warning("Conexión/canal perdido enviando a '%s': %s, recreando canal...", self.exchange_name, exc)
                # Close invalid channel and retry
                self._close_send_channel()
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error enviando mensaje a '%s': %s", self.exchange_name, exc)
                # Close channel on unexpected errors and retry
                self._close_send_channel()

            attempt += 1
            if attempt > self._max_reconnect_attempts:
                raise MessageMiddlewareDisconnectedError(
                    f"Pérdida de conexión enviando mensaje a '{self.exchange_name}'"
                )

            self._wait_before_retry(attempt - 1)

    def delete(self) -> None:
        channel: Optional[pika.channel.Channel] = None
        try:
            connection = self._connection_manager.get_connection()
            channel = connection.channel()
            channel.exchange_delete(exchange=self.exchange_name)
        except Exception as exc:  # noqa: BLE001
            raise MessageMiddlewareDeleteError(f"Error eliminando exchange: {exc}") from exc
        finally:
            if channel and channel.is_open:
                with suppress(Exception):
                    channel.close()
