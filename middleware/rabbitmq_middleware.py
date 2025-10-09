#!/usr/bin/env python3

import pika  # type: ignore
import pika.exceptions  # type: ignore
import json
import logging
import time
from typing import Callable, List, Optional, Any
from .middleware_interface import (
    MessageMiddleware, 
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError
)

logger = logging.getLogger(__name__)

def serialize_message(message):
    """Serializa un mensaje a string JSON."""
    try:
        return json.dumps(message, ensure_ascii=False)
    except Exception as e:
        raise

def deserialize_message(serialized_message):
    """Deserializa un mensaje desde string JSON."""
    try:
        return json.loads(serialized_message)
    except Exception as e:
        raise

class RabbitMQMiddlewareQueue(MessageMiddleware):
    """Simple, reliable RabbitMQ middleware without connection pooling."""
    
    def __init__(self, host: str, queue_name: str, port: int = 5672, prefetch_count: int = 10):
        self.host = host
        self.port = port
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        self.connection: Optional[Any] = None
        self.channel: Optional[Any] = None
        self.consuming = False
        
        logger.info(f"Inicializando RabbitMQ Queue Middleware (simple): {host}:{port}/{queue_name}")
    
    def _ensure_connection(self):
        """Asegura que la conexión esté establecida."""
        if self.connection is None or self.connection.is_closed:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host, 
                        port=self.port,
                        heartbeat=600,
                        socket_timeout=10.0
                    )
                )
                self.channel = self.connection.channel()
                logger.info(f"Conectado a RabbitMQ: {self.host}:{self.port}")
            except Exception as e:
                raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ: {e}")
    
    def start_consuming(self, on_message_callback: Callable):
        """Inicia el consumo de mensajes."""
        try:
            self._ensure_connection()
            
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No se pudo establecer el canal")
            
            self.channel.queue_declare(queue=self.queue_name, durable=False)
            
            def callback(ch, method, properties, body):
                try:
                    serialized_message = body.decode('utf-8')
                    message = deserialize_message(serialized_message)
                    on_message_callback(message)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    raise MessageMiddlewareMessageError(f"Error procesando mensaje: {e}")
            
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False
            )
            
            self.consuming = True
            logger.info(f"Iniciando consumo de la cola '{self.queue_name}'")
            self.channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error interno iniciando consumo: {e}")
    
    def send(self, message):
        """Envía un mensaje."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                self._ensure_connection()
                
                if not self.channel:
                    raise MessageMiddlewareDisconnectedError("No se pudo establecer el canal")
                
                self.channel.queue_declare(queue=self.queue_name, durable=False)
                message_body = serialize_message(message)
                
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.queue_name,
                    body=message_body
                )
                
                logger.debug(f"Mensaje enviado a la cola '{self.queue_name}'")
                return
                
            except (pika.exceptions.AMQPConnectionError, ConnectionError) as e:
                logger.warning(f"Error de conexión, intento {attempt + 1}: {e}")
                # Reset connection
                self.connection = None
                self.channel = None
                
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (2 ** attempt))
                else:
                    raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión después de {max_retries} intentos: {e}")
                    
            except Exception as e:
                if "No se pudo conectar" in str(e):
                    raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
                else:
                    raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")
    
    def stop_consuming(self):
        """Detiene el consumo de mensajes."""
        if self.consuming and self.channel:
            try:
                self.channel.stop_consuming()
                self.consuming = False
            except Exception as e:
                logger.warning(f"Error deteniendo consumo: {e}")
    
    def close(self):
        """Cierra la conexión con RabbitMQ."""
        try:
            if self.consuming:
                self.stop_consuming()
            
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                
            logger.info(f"Desconectado de RabbitMQ: {self.host}:{self.port}")
        except Exception as e:
            logger.warning(f"Error cerrando conexión: {e}")
    
    def delete(self):
        """Elimina la cola."""
        try:
            self._ensure_connection()
            if not self.channel:
                raise MessageMiddlewareDeleteError("No se pudo establecer el canal")
            self.channel.queue_delete(queue=self.queue_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error eliminando cola: {e}")


class RabbitMQMiddlewareExchange(MessageMiddleware):
    """Exchange middleware - kept simple."""

    def __init__(
        self,
        host: str,
        exchange_name: str,
        route_keys: List[str],
        exchange_type: str = 'direct',
        port: int = 5672,
        queue_name: Optional[str] = None,
        prefetch_count: int = 10,
    ):
        self.host = host
        self.port = port
        self.exchange_name = exchange_name
        self.route_keys = route_keys
        self.exchange_type = exchange_type
        self.connection: Optional[Any] = None
        self.channel: Optional[Any] = None
        self.consuming = False
        self.queue_name: Optional[str] = None
        self._queue_override = queue_name
        self._prefetch_count = prefetch_count
        
        logger.info(f"Inicializando RabbitMQ Exchange Middleware: {host}:{port}/{exchange_name}")
    
    def _ensure_connection(self):
        """Asegura que la conexión esté establecida."""
        if self.connection is None or self.connection.is_closed:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, port=self.port)
                )
                self.channel = self.connection.channel()
            except Exception as e:
                raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ: {e}")
    
    def start_consuming(self, on_message_callback: Callable):
        """Comienza a escuchar el exchange."""
        try:
            self._ensure_connection()
            
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No se pudo establecer el canal")
            
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=self.exchange_type,
                durable=False
            )
            
            shared_queue = bool(self._queue_override)
            if shared_queue:
                target_queue = self._queue_override or ''
                self.channel.queue_declare(queue=target_queue, durable=False)
            else:
                result = self.channel.queue_declare(queue='', exclusive=True)
                target_queue = result.method.queue

            self.queue_name = target_queue
            
            if self.exchange_type == 'fanout':
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=target_queue
                )
            else:
                for route_key in self.route_keys:
                    self.channel.queue_bind(
                        exchange=self.exchange_name,
                        queue=target_queue,
                        routing_key=route_key
                    )
            
            def callback(ch, method, properties, body):
                try:
                    serialized_message = body.decode('utf-8')
                    message = deserialize_message(serialized_message)
                    on_message_callback(message)

                    if shared_queue:
                        ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    if shared_queue:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    raise MessageMiddlewareMessageError(f"Error procesando mensaje: {e}")

            if shared_queue:
                self.channel.basic_qos(prefetch_count=self._prefetch_count)

            self.channel.basic_consume(
                queue=target_queue,
                on_message_callback=callback,
                auto_ack=not shared_queue,
            )
            
            self.consuming = True
            self.channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error interno iniciando consumo: {e}")
    
    def send(self, message, routing_key: str = ''):
        """Envía un mensaje al exchange."""
        try:
            self._ensure_connection()
            
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No se pudo establecer el canal")
            
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=self.exchange_type,
                durable=False
            )
            
            message_body = serialize_message(message)
            
            if self.exchange_type == 'fanout':
                routing_key = ''
            elif routing_key == '':
                routing_key = self.route_keys[0] if self.route_keys else ''
            
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message_body
            )
            
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            if "No se pudo conectar" in str(e):
                raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
            else:
                raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")
    
    def stop_consuming(self):
        """Detiene el consumo de mensajes."""
        if self.consuming and self.channel:
            try:
                self.channel.stop_consuming()
                self.consuming = False
            except Exception as e:
                pass
    
    def close(self):
        """Cierra la conexión con RabbitMQ."""
        try:
            if self.consuming:
                self.stop_consuming()
            
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                
            logger.info(f"Desconectado de RabbitMQ: {self.host}:{self.port}")
        except Exception as e:
            logger.warning(f"Error cerrando conexión: {e}")
    
    def delete(self):
        """Elimina el exchange."""
        try:
            self._ensure_connection()
            if not self.channel:
                raise MessageMiddlewareDeleteError("No se pudo establecer el canal")
            self.channel.exchange_delete(exchange=self.exchange_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error eliminando exchange: {e}")