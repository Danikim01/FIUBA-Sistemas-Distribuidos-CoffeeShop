#!/usr/bin/env python3
"""
Pruebas unitarias para el middleware de RabbitMQ.
Cubre todos los escenarios requeridos por la cátedra.
"""

import pytest
import threading
import os
import warnings
import time
from middleware import (
    RabbitMQMiddlewareQueue, 
    RabbitMQMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError
)

# Suprimir warnings específicos de Pika
warnings.filterwarnings("ignore", category=pytest.PytestUnhandledThreadExceptionWarning)


class TestRabbitMQMiddleware:
    """Clase base para las pruebas del middleware."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Configuración inicial para cada prueba."""
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', '5672'))
        
        # Limpiar colas y exchanges antes de cada prueba
        self._cleanup_queues_and_exchanges()
    
    @pytest.fixture(autouse=True)
    def teardown(self):
        """Limpieza después de cada prueba."""
        yield
        # Limpiar colas y exchanges después de cada prueba
        self._cleanup_queues_and_exchanges()
    
    def _cleanup_queues_and_exchanges(self):
        """Limpia colas y exchanges de pruebas anteriores."""
        try:
            # Limpiar colas de prueba
            queue = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1to1")
            queue.delete()
            queue.close()
        except:
            pass
        
        try:
            queue = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1toN")
            queue.delete()
            queue.close()
        except:
            pass
        
        try:
            # Limpiar exchanges de prueba
            exchange = RabbitMQMiddlewareExchange(self.rabbitmq_host, "test_exchange_1to1", ["route1"])
            exchange.delete()
            exchange.close()
        except:
            pass
        
        try:
            exchange = RabbitMQMiddlewareExchange(self.rabbitmq_host, "test_exchange_1toN", ["route1", "route2"])
            exchange.delete()
            exchange.close()
        except:
            pass


class TestWorkingQueue1To1(TestRabbitMQMiddleware):
    """Pruebas para Working Queue 1 a 1."""
    
    def test_queue_1to1_basic_communication(self):
        """Prueba comunicación básica 1 a 1 por cola."""
        # Crear middleware para producer y consumer
        producer = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1to1", prefetch_count=1)
        consumer = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1to1", prefetch_count=1)
        
        # Lista para almacenar mensajes recibidos
        received_messages = []
        
        def message_callback(message):
            received_messages.append(message)
        
        try:
            # Iniciar consumer en un hilo separado
            consumer_thread = threading.Thread(
                target=lambda: consumer.start_consuming(message_callback)
            )
            consumer_thread.daemon = True
            consumer_thread.start()
            
            # Esperar a que el consumer esté listo
            time.sleep(0.5)
            
            # Enviar mensajes
            test_messages = [
                {"id": 1, "content": "Mensaje 1"},
                {"id": 2, "content": "Mensaje 2"},
                {"id": 3, "content": "Mensaje 3"}
            ]
            
            for msg in test_messages:
                producer.send(msg)
            
            # Esperar un poco más para que se procesen todos los mensajes
            time.sleep(1.0)
        
            # Detener consumer
            consumer.stop_consuming()
            consumer_thread.join(timeout=5)
            
            # Verificar que se recibieron todos los mensajes
            assert len(received_messages) == 3, f"Se esperaban 3 mensajes, se recibieron {len(received_messages)}"
            
            # Verificar que los mensajes son correctos
            for i, msg in enumerate(test_messages):
                assert received_messages[i]["id"] == msg["id"]
                assert received_messages[i]["content"] == msg["content"]
            
        finally:
            producer.close()
            consumer.close()
    
    def test_queue_1to1_message_ordering(self):
        """Prueba que los mensajes se reciben en orden."""
        producer = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1to1", prefetch_count=1)
        consumer = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1to1", prefetch_count=1)
        
        received_messages = []
        
        def message_callback(message):
            received_messages.append(message)
        
        try:
            consumer_thread = threading.Thread(
                target=lambda: consumer.start_consuming(message_callback)
            )
            consumer_thread.daemon = True
            consumer_thread.start()
            
            # Esperar a que el consumer esté listo
            time.sleep(0.5)
            
            # Enviar mensajes numerados
            for i in range(10):
                producer.send({"order": i, "content": f"Mensaje {i}"})
            
            # Esperar a que se procesen todos los mensajes
            time.sleep(1.0)

            consumer.stop_consuming()
            consumer_thread.join(timeout=5)
            
            # Verificar orden
            assert len(received_messages) == 10
            for i, msg in enumerate(received_messages):
                assert msg["order"] == i, f"Mensaje {i} no está en orden correcto"
            
        finally:
            producer.close()
            consumer.close()


class TestWorkingQueue1ToN(TestRabbitMQMiddleware):
    """Pruebas para Working Queue 1 a N (competing consumers)."""
    
    def test_queue_1toN_competing_consumers(self):
        """Prueba que múltiples consumers compiten por mensajes."""
        producer = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1toN")
        consumer1 = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1toN")
        consumer2 = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1toN")
        consumer3 = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1toN")
        
        # Listas para almacenar mensajes recibidos por cada consumer
        consumer1_messages = []
        consumer2_messages = []
        consumer3_messages = []
        
        def consumer1_callback(message):
            consumer1_messages.append(message)
        
        def consumer2_callback(message):
            consumer2_messages.append(message)
        
        def consumer3_callback(message):
            consumer3_messages.append(message)
        
        try:
            # Iniciar consumers en hilos separados
            consumer1_thread = threading.Thread(
                target=lambda: consumer1.start_consuming(consumer1_callback)
            )
            consumer2_thread = threading.Thread(
                target=lambda: consumer2.start_consuming(consumer2_callback)
            )
            consumer3_thread = threading.Thread(
                target=lambda: consumer3.start_consuming(consumer3_callback)
            )
            
            for thread in [consumer1_thread, consumer2_thread, consumer3_thread]:
                thread.daemon = True
                thread.start()

            # Esperar a que los consumers estén listos
            time.sleep(1.0)

            # Enviar mensajes
            total_messages = 15
            for i in range(total_messages):
                producer.send({"id": i, "content": f"Mensaje {i}"})
            
            # Esperar a que se procesen todos los mensajes
            time.sleep(2.0)

            # Detener consumers
            for consumer in [consumer1, consumer2, consumer3]:
                consumer.stop_consuming()
            
            for thread in [consumer1_thread, consumer2_thread, consumer3_thread]:
                thread.join(timeout=5)
            
            # Verificar que todos los mensajes fueron procesados
            total_received = len(consumer1_messages) + len(consumer2_messages) + len(consumer3_messages)
            assert total_received == total_messages, f"Se esperaban {total_messages} mensajes, se procesaron {total_received}"
            
            # Verificar que al menos dos consumers recibieron mensajes (distribución)
            consumers_with_messages = sum([
                len(consumer1_messages) > 0,
                len(consumer2_messages) > 0,
                len(consumer3_messages) > 0
            ])
            assert consumers_with_messages >= 2, "Los mensajes no se distribuyeron entre múltiples consumers"
            
        finally:
            producer.close()
            consumer1.close()
            consumer2.close()
            consumer3.close()


class TestExchange1To1(TestRabbitMQMiddleware):
    """Pruebas para Exchange 1 a 1."""
    
    def test_exchange_1to1_basic_communication(self):
        """Prueba comunicación básica 1 a 1 por exchange."""
        producer = RabbitMQMiddlewareExchange(self.rabbitmq_host, "test_exchange_1to1", ["route1"])
        consumer = RabbitMQMiddlewareExchange(self.rabbitmq_host, "test_exchange_1to1", ["route1"])
        
        received_messages = []
        
        def message_callback(message):
            received_messages.append(message)
        
        try:
            consumer_thread = threading.Thread(
                target=lambda: consumer.start_consuming(message_callback)
            )
            consumer_thread.daemon = True
            consumer_thread.start()
            
            # Esperar a que el consumer se configure (necesario para Exchanges)
            time.sleep(1.0)
            
            # Enviar mensajes
            test_messages = [
                {"id": 1, "content": "Mensaje Exchange 1"},
                {"id": 2, "content": "Mensaje Exchange 2"},
                {"id": 3, "content": "Mensaje Exchange 3"}
            ]
            
            for msg in test_messages:
                producer.send(msg, "route1")
            
            # Esperar a que se procesen los mensajes
            time.sleep(1.0)
            
            consumer.stop_consuming()
            consumer_thread.join(timeout=5)
            
            # Verificar que se recibieron todos los mensajes
            assert len(received_messages) == 3, f"Se esperaban 3 mensajes, se recibieron {len(received_messages)}"
            
            # Verificar que los mensajes son correctos
            for i, msg in enumerate(test_messages):
                assert received_messages[i]["id"] == msg["id"]
                assert received_messages[i]["content"] == msg["content"]
            
        finally:
            producer.close()
            consumer.close()
    
    def test_exchange_1to1_routing_key_filtering(self):
        """Prueba que solo se reciben mensajes con la routing key correcta."""
        producer = RabbitMQMiddlewareExchange(self.rabbitmq_host, "test_exchange_1to1", ["route1", "route2"])
        consumer = RabbitMQMiddlewareExchange(self.rabbitmq_host, "test_exchange_1to1", ["route1"])
        
        received_messages = []
        
        def message_callback(message):
            received_messages.append(message)
        
        try:
            consumer_thread = threading.Thread(
                target=lambda: consumer.start_consuming(message_callback)
            )
            consumer_thread.daemon = True
            consumer_thread.start()
            
            # Esperar a que el consumer se configure (necesario para Exchanges)
            time.sleep(1.0)
            
            # Enviar mensajes a diferentes routing keys
            producer.send({"id": 1, "content": "Para route1"}, "route1")
            producer.send({"id": 2, "content": "Para route2"}, "route2")  # No debería recibirse
            producer.send({"id": 3, "content": "Para route1 otra vez"}, "route1")
            
            # Esperar a que se procesen los mensajes
            time.sleep(1.0)
            
            consumer.stop_consuming()
            consumer_thread.join(timeout=5)
            
            # Verificar que solo se recibieron mensajes de route1
            assert len(received_messages) == 2, f"Se esperaban 2 mensajes, se recibieron {len(received_messages)}"
            assert received_messages[0]["id"] == 1
            assert received_messages[1]["id"] == 3
            
        finally:
            producer.close()
            consumer.close()


class TestExchange1ToN(TestRabbitMQMiddleware):
    """Pruebas para Exchange 1 a N."""
    
    def test_exchange_1toN_multiple_routing_keys(self):
        """Prueba que múltiples consumers reciben mensajes según sus routing keys."""
        producer = RabbitMQMiddlewareExchange(self.rabbitmq_host, "test_exchange_1toN", ["route1", "route2"])
        consumer1 = RabbitMQMiddlewareExchange(self.rabbitmq_host, "test_exchange_1toN", ["route1"])
        consumer2 = RabbitMQMiddlewareExchange(self.rabbitmq_host, "test_exchange_1toN", ["route2"])
        consumer3 = RabbitMQMiddlewareExchange(self.rabbitmq_host, "test_exchange_1toN", ["route1", "route2"])
        
        # Listas para almacenar mensajes recibidos por cada consumer
        consumer1_messages = []
        consumer2_messages = []
        consumer3_messages = []
        
        def consumer1_callback(message):
            consumer1_messages.append(message)
        
        def consumer2_callback(message):
            consumer2_messages.append(message)
        
        def consumer3_callback(message):
            consumer3_messages.append(message)
        
        try:
            # Iniciar consumers en hilos separados
            consumer1_thread = threading.Thread(
                target=lambda: consumer1.start_consuming(consumer1_callback)
            )
            consumer2_thread = threading.Thread(
                target=lambda: consumer2.start_consuming(consumer2_callback)
            )
            consumer3_thread = threading.Thread(
                target=lambda: consumer3.start_consuming(consumer3_callback)
            )
            
            for thread in [consumer1_thread, consumer2_thread, consumer3_thread]:
                thread.daemon = True
                thread.start()
            
            # Esperar a que los consumers se configuren (necesario para Exchanges)
            time.sleep(1.5)
            
            # Enviar mensajes a diferentes routing keys
            messages_route1 = [
                {"id": i, "content": f"Mensaje Route1 {i}"}
                for i in range(1, 4)
            ]
            
            messages_route2 = [
                {"id": i, "content": f"Mensaje Route2 {i}"}
                for i in range(1, 4)
            ]
            
            # Enviar mensajes a route1
            for msg in messages_route1:
                producer.send(msg, "route1")
            
            # Enviar mensajes a route2
            for msg in messages_route2:
                producer.send(msg, "route2")
            
            # Esperar a que se procesen todos los mensajes
            time.sleep(2.0)
                    
            # Detener consumers
            for consumer in [consumer1, consumer2, consumer3]:
                consumer.stop_consuming()
            
            for thread in [consumer1_thread, consumer2_thread, consumer3_thread]:
                thread.join(timeout=5)
            
            # Verificar distribución de mensajes
            # Consumer1 solo debería recibir mensajes de route1
            assert len(consumer1_messages) == 3, f"Consumer1 debería recibir 3 mensajes, recibió {len(consumer1_messages)}"
            
            # Consumer2 solo debería recibir mensajes de route2
            assert len(consumer2_messages) == 3, f"Consumer2 debería recibir 3 mensajes, recibió {len(consumer2_messages)}"
            
            # Consumer3 debería recibir mensajes de ambas rutas
            assert len(consumer3_messages) == 6, f"Consumer3 debería recibir 6 mensajes, recibió {len(consumer3_messages)}"
            
            # Verificar que los mensajes son correctos
            for msg in consumer1_messages:
                assert "Route1" in msg["content"]
            
            for msg in consumer2_messages:
                assert "Route2" in msg["content"]
            
        finally:
            producer.close()
            consumer1.close()
            consumer2.close()
            consumer3.close()


class TestErrorHandling(TestRabbitMQMiddleware):
    """Pruebas de manejo de errores."""
    
    def test_connection_error(self):
        """Prueba manejo de errores de conexión."""
        # Intentar conectar a un host inexistente
        with pytest.raises(MessageMiddlewareDisconnectedError):
            producer = RabbitMQMiddlewareQueue("host_inexistente", "test_queue")
            producer.send({"test": "message"})
    
    def test_serialization_error(self):
        """Prueba manejo de errores de serialización."""
        producer = RabbitMQMiddlewareQueue(self.rabbitmq_host, "test_queue_1to1")
        
        # Intentar enviar un objeto no serializable
        with pytest.raises(MessageMiddlewareMessageError):
            producer.send(lambda x: x)  # Función no serializable
        
        producer.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
