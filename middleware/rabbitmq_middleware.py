#!/usr/bin/env python3

import pika  # type: ignore
import pika.exceptions  # type: ignore
import json
import logging
from typing import Callable, List, Optional, Any
from .middleware_interface import (
    MessageMiddleware, 
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError
)

logger = logging.getLogger(__name__)
# Configurar logging
logging.basicConfig(level=logging.INFO)

def serialize_message(message):
    """
    Serializa un mensaje a string JSON.
    
    Args:
        message: Objeto a serializar (dict, list, etc.)
        
    Returns:
        str: Mensaje serializado como JSON
    """
    try:
        return json.dumps(message, ensure_ascii=False)
    except Exception as e:
        #logger.error(f"Error serializando mensaje: {e}")
        raise

def deserialize_message(serialized_message):
    """
    Deserializa un mensaje desde string JSON.
    
    Args:
        serialized_message: String JSON a deserializar
        
    Returns:
        object: Mensaje deserializado
    """
    try:
        return json.loads(serialized_message)
    except Exception as e:
        #logger.error(f"Error deserializando mensaje: {e}")
        raise

class RabbitMQMiddlewareQueue(MessageMiddleware):
    """
    Implementación de MessageMiddleware para comunicación por colas (Working Queue).
    Soporta comunicación 1 a 1 y 1 a N (competing consumers pattern).
    Mejorado con Publisher Confirms y Consumer Acknowledgements según documentación RabbitMQ.
    """
    
    def __init__(self, host: str, queue_name: str, port: int = 5672, prefetch_count: int = 10):
        """
        Inicializa el middleware para comunicación por cola.
        
        Args:
            host: Host de RabbitMQ
            queue_name: Nombre de la cola
            port: Puerto de RabbitMQ (por defecto 5672)
            prefetch_count: Número de mensajes a prefetch (por defecto 10)
        """
        self.host = host
        self.port = port
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        self.connection: Optional[Any] = None
        self.channel: Optional[Any] = None
        self.consuming = False
        
        #logger.info(f"Inicializando RabbitMQ Queue Middleware: {host}:{port}/{queue_name}")
    
    def _ensure_connection(self):
        """Asegura que la conexión esté establecida."""
        if self.connection is None or self.connection.is_closed:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, port=self.port)
                )
                self.channel = self.connection.channel()
                
                # Habilitar confirmaciones de publicación según documentación RabbitMQ
                if self.channel:
                    self.channel.confirm_delivery()
                
                #logger.info(f"Conectado a RabbitMQ con confirmaciones: {self.host}:{self.port}")
            except Exception as e:
                #logger.error(f"Error conectando a RabbitMQ: {e}")
                raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ: {e}")
    
    def start_consuming(self, on_message_callback: Callable):
        """
        Comienza a escuchar la cola e invoca on_message_callback para cada mensaje.
        Implementa Consumer Acknowledgements según documentación RabbitMQ.
        
        Args:
            on_message_callback: Función que se ejecutará para cada mensaje recibido
        """
        try:
            self._ensure_connection()
            
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No se pudo establecer el canal")
            
            # Declarar la cola (es idempotente)
            self.channel.queue_declare(queue=self.queue_name, durable=False)
            
            # Configurar el consumidor con ACK optimizado para chunks
            def callback(ch, method, properties, body):
                try:
                    # Decodificar el mensaje
                    serialized_message = body.decode('utf-8')
                    message = deserialize_message(serialized_message)
                    
                    # Ejecutar callback del usuario
                    on_message_callback(message)
                    
                    # ACK manual optimizado - confirma procesamiento exitoso
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except ValueError as e:
                    # NACK sin reenvío para errores de serialización
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    raise MessageMiddlewareMessageError(f"Error deserializando mensaje: {e}")
                except Exception as e:
                    # NACK con reenvío para errores de procesamiento
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    raise MessageMiddlewareMessageError(f"Error procesando mensaje: {e}")
            
            # Configurar QoS para control de flujo optimizado para múltiples workers
            # Prefetch configurable para mejor load balancing entre workers
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            
            # Configurar el consumidor con ACK manual
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False  # Manual ACK para garantizar procesamiento
            )
            
            self.consuming = True
            #logger.info(f"Iniciando consumo de la cola '{self.queue_name}' con ACK manual")
            
            # Comenzar a consumir mensajes
            self.channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            #logger.error(f"Error de conexión AMQP: {e}")
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            #logger.error(f"Error iniciando consumo: {e}")
            raise MessageMiddlewareMessageError(f"Error interno iniciando consumo: {e}")
    
    def send(self, message):
        """
        Envía un mensaje con Publisher Confirms según documentación RabbitMQ.
        
        Args:
            message: Mensaje a enviar (será serializado manualmente)
        """
        try:
            self._ensure_connection()
            
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No se pudo establecer el canal")
            
            # Declarar la cola (es idempotente)
            self.channel.queue_declare(queue=self.queue_name, durable=False)
            
            # Serializar el mensaje manualmente
            message_body = serialize_message(message)
            
            # Enviar el mensaje con confirmación
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Hacer el mensaje persistente
                ),
                mandatory=True  # Garantiza que el mensaje llegue a una cola
            )
            
            #logger.info(f"Mensaje enviado a la cola '{self.queue_name}': {message}")
            
        except pika.exceptions.UnroutableError:
            #logger.error(f"Mensaje no pudo ser enrutado a la cola '{self.queue_name}'")
            raise MessageMiddlewareMessageError(f"Mensaje no pudo ser enrutado a la cola")
        except pika.exceptions.AMQPConnectionError as e:
            #logger.error(f"Error de conexión AMQP: {e}")
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            #logger.error(f"Error enviando mensaje: {e}")
            # Si es un error de conexión, lanzar MessageMiddlewareDisconnectedError
            if "No se pudo conectar" in str(e) or "Temporary failure in name resolution" in str(e):
                raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
            else:
                raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")
    
    def stop_consuming(self):
        """Detiene el consumo de mensajes."""
        if self.consuming and self.channel:
            try:
                self.channel.stop_consuming()
                self.consuming = False
                #logger.info(f"Detenido consumo de la cola '{self.queue_name}'")
            except Exception as e:
                logger.warning(f"Error deteniendo consumo (normal al cerrar): {e}")
                # No lanzar excepción en el cierre, es normal que falle
    
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
            pass
    
    def delete(self):
        """Elimina la cola."""
        try:
            self._ensure_connection()
            if not self.channel:
                raise MessageMiddlewareDeleteError("No se pudo establecer el canal")
            self.channel.queue_delete(queue=self.queue_name)
            #logger.info(f"Cola '{self.queue_name}' eliminada")
        except Exception as e:
            #logger.error(f"Error eliminando cola: {e}")
            raise MessageMiddlewareDeleteError(f"Error interno eliminando cola: {e}")


class RabbitMQMiddlewareExchange(MessageMiddleware):
    """Exchange middleware supporting shared queues for competing consumers."""

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
        """
        Inicializa el middleware para comunicación por exchange.
        
        Args:
            host: Host de RabbitMQ
            exchange_name: Nombre del exchange
            route_keys: Lista de routing keys para escuchar
            exchange_type: Tipo de exchange ('direct', 'fanout', 'topic')
            port: Puerto de RabbitMQ (por defecto 5672)
        """
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
        
        #logger.info(f"Inicializando RabbitMQ Exchange Middleware: {host}:{port}/{exchange_name} ({exchange_type})")
    
    def _ensure_connection(self):
        """Asegura que la conexión esté establecida."""
        if self.connection is None or self.connection.is_closed:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, port=self.port)
                )
                self.channel = self.connection.channel()
                
                # Habilitar confirmaciones de publicación según documentación RabbitMQ
                if self.channel:
                    self.channel.confirm_delivery()
                
                #logger.info(f"Conectado a RabbitMQ con confirmaciones: {self.host}:{self.port}")
            except Exception as e:
                #logger.error(f"Error conectando a RabbitMQ: {e}")
                raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ: {e}")
    
    def start_consuming(self, on_message_callback: Callable):
        """
        Comienza a escuchar el exchange e invoca on_message_callback para cada mensaje.
        Implementa Consumer Acknowledgements según documentación RabbitMQ.
        
        Args:
            on_message_callback: Función que se ejecutará para cada mensaje recibido
        """
        try:
            self._ensure_connection()
            
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No se pudo establecer el canal")
            
            # Declarar el exchange
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=self.exchange_type,
                durable=False
            )
            
            # Configurar la cola destino. Si se recibe un nombre explícito,
            # reutilizamos esa cola para permitir competing consumers entre
            # réplicas; en caso contrario creamos una cola temporal exclusiva.
            shared_queue = bool(self._queue_override)
            if shared_queue:
                target_queue = self._queue_override or ''
                self.channel.queue_declare(queue=target_queue, durable=False)
            else:
                result = self.channel.queue_declare(queue='', exclusive=True)
                target_queue = result.method.queue

            self.queue_name = target_queue
            
            # Bindear la cola al exchange
            if self.exchange_type == 'fanout':
                # Para fanout, no se necesitan routing keys
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=target_queue
                )
                #logger.info(f"Cola '{self.queue_name}' bindeada al exchange fanout '{self.exchange_name}'")
            else:
                # Para otros tipos de exchange, usar routing keys
                for route_key in self.route_keys:
                    self.channel.queue_bind(
                        exchange=self.exchange_name,
                        queue=target_queue,
                        routing_key=route_key
                    )
                    #logger.info(f"Cola '{self.queue_name}' bindeada a '{route_key}'")
            
            # Configurar el consumidor (simplificado para Exchanges)
            def callback(ch, method, properties, body):
                try:
                    # Decodificar el mensaje
                    serialized_message = body.decode('utf-8')
                    message = deserialize_message(serialized_message)
                    routing_key = method.routing_key
                    #logger.info(f"Mensaje recibido en exchange '{self.exchange_name}' con routing key '{routing_key}': {message}")
                    
                    # Ejecutar callback del usuario
                    on_message_callback(message)

                    if shared_queue:
                        ch.basic_ack(delivery_tag=method.delivery_tag)

                except ValueError as e:
                    if shared_queue:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    raise MessageMiddlewareMessageError(f"Error deserializando mensaje: {e}")
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
            #logger.info(f"Iniciando consumo del exchange '{self.exchange_name}' con routing keys: {self.route_keys} y ACK automático")
            
            
            # Comenzar a consumir mensajes
            self.channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            #logger.error(f"Error de conexión AMQP: {e}")
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            #logger.error(f"Error iniciando consumo: {e}")
            raise MessageMiddlewareMessageError(f"Error interno iniciando consumo: {e}")
    
    def send(self, message, routing_key: str = ''):
        """
        Envía un mensaje al exchange con Publisher Confirms según documentación RabbitMQ.
        
        Args:
            message: Mensaje a enviar
            routing_key: Routing key para el mensaje (ignorado en fanout exchanges)
        """
        try:
            self._ensure_connection()
            
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No se pudo establecer el canal")
            
            # Declarar el exchange
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=self.exchange_type,
                durable=False
            )
            
            # Serializar el mensaje manualmente
            message_body = serialize_message(message)
            
            # Para fanout exchanges, routing_key se ignora
            if self.exchange_type == 'fanout':
                routing_key = ''
            elif routing_key == '':
                # Usar la primera routing key si no se especifica una y no es fanout
                routing_key = self.route_keys[0] if self.route_keys else ''
            
            # Enviar el mensaje con confirmación
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Hacer el mensaje persistente
                ),
                mandatory=True  # Garantiza que el mensaje llegue a una cola
            )
            
            #logger.info(f"Mensaje enviado al exchange '{self.exchange_name}' con routing key '{routing_key}': {message}")
            
        except pika.exceptions.UnroutableError:
            #logger.warning(f"Mensaje no pudo ser enrutado al exchange '{self.exchange_name}' con routing key '{routing_key}' - esto es normal si no hay consumers")
            # No lanzar excepción para mensajes no enrutables, es comportamiento normal
            pass
        except pika.exceptions.AMQPConnectionError as e:
            #logger.error(f"Error de conexión AMQP: {e}")
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            #logger.error(f"Error enviando mensaje: {e}")
            # Si es un error de conexión, lanzar MessageMiddlewareDisconnectedError
            if "No se pudo conectar" in str(e) or "Temporary failure in name resolution" in str(e):
                raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
            else:
                raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")
    
    def stop_consuming(self):
        """Detiene el consumo de mensajes."""
        if self.consuming and self.channel:
            try:
                self.channel.stop_consuming()
                self.consuming = False
                #logger.info(f"Detenido consumo del exchange '{self.exchange_name}'")
            except Exception as e:
                #logger.warning(f"Error deteniendo consumo (normal al cerrar): {e}")
                # No lanzar excepción en el cierre, es normal que falle
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
            pass
    
    def delete(self):
        """Elimina el exchange."""
        try:
            self._ensure_connection()
            if not self.channel:
                raise MessageMiddlewareDeleteError("No se pudo establecer el canal")
            self.channel.exchange_delete(exchange=self.exchange_name)
            #logger.info(f"Exchange '{self.exchange_name}' eliminado")
        except Exception as e:
            #logger.error(f"Error eliminando exchange: {e}")
            raise MessageMiddlewareDeleteError(f"Error interno eliminando exchange: {e}")
