import socket
import logging
import threading
import os
import json
from typing import Any
from protocol import (
    MessageType, DataType, send_response, receive_message, 
    parse_batch_message, parse_eof_message
)
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CoffeeShopGateway:
    def __init__(self, port=12345):
        self.port = port
        self.socket = None
        self.running = False
        self.data_storage = {
            DataType.USERS: [],
            DataType.TRANSACTIONS: [],
            DataType.TRANSACTION_ITEMS: [],
            DataType.MENU_ITEMS: [],
        }
        
        # Configurar RabbitMQ para enviar transacciones a workers
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        
        # Cola de salida configurable para enviar transacciones
        self.transactions_queue_name = os.getenv('OUTPUT_QUEUE', 'transactions_raw')
        
        # Cola para enviar stores al enrichment worker
        self.stores_queue_name = os.getenv('STORES_QUEUE', 'stores_raw')

        # Cola para enviar transaction items a su pipeline
        self.transaction_items_queue_name = os.getenv('TRANSACTION_ITEMS_QUEUE', 'transaction_items_raw')

        # Cola para enviar menu items al pipeline de agregación
        self.menu_items_queue_name = os.getenv('MENU_ITEMS_QUEUE', 'menu_items_raw')

        # Cola para recibir resultados procesados desde ResultsWorker
        self.results_queue_name = os.getenv('RESULTS_QUEUE', 'gateway_results')

        # Chunking configuration
        self.chunk_size = int(os.getenv('CHUNK_SIZE', 100))

        # Middleware para enviar transacciones a la cola de procesamiento
        self.transactions_queue = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.transactions_queue_name,
            port=self.rabbitmq_port
        )
        
        # Middleware para enviar stores al enrichment worker
        self.stores_queue = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.stores_queue_name,
            port=self.rabbitmq_port
        )

        # Middleware para enviar transaction items a su pipeline
        self.transaction_items_queue = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.transaction_items_queue_name,
            port=self.rabbitmq_port
        )

        # Middleware para enviar menu items al pipeline de agregación
        self.menu_items_queue = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.menu_items_queue_name,
            port=self.rabbitmq_port
        )

        logger.info(f"Gateway configurado con RabbitMQ: {self.rabbitmq_host}:{self.rabbitmq_port}")
        logger.info(f"Cola de transacciones: {self.transactions_queue_name}")
        logger.info(f"Cola de stores: {self.stores_queue_name}")
        logger.info(f"Cola de transaction items: {self.transaction_items_queue_name}")
        logger.info(f"Cola de menu items: {self.menu_items_queue_name}")
        logger.info(f"Cola de resultados: {self.results_queue_name}")
        logger.info(f"Chunking configurado: {self.chunk_size} transacciones por chunk")
    
    def create_chunks(self, transactions):
        """Divide las transacciones en chunks para procesamiento optimizado"""
        chunks = []
        for i in range(0, len(transactions), self.chunk_size):
            chunk = transactions[i:i + self.chunk_size]
            chunks.append(chunk)
        return chunks
    
    def start_server(self):
        """Start the gateway server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(('0.0.0.0', self.port))  # Listen on all interfaces for Docker
            self.socket.listen(5)
            self.running = True
            
            #logger.info(f"Gateway server started on port {self.port}")
            
            while self.running:
                try:
                    client_socket, address = self.socket.accept()
                    # logger.info(f"New client connected from {address}")
                    
                    # Handle client in a separate thread
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                except Exception as e:
                    if self.running:
                        logger.error(f"Error accepting connection: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to start server: {e}")
        finally:
            self.stop_server()
    
    def stop_server(self):
        """Stop the gateway server"""
        self.running = False
        if self.socket:
            self.socket.close()
            logger.info("Gateway server stopped")
    
    def handle_client(self, client_socket: socket.socket, address):
        """Handle a client connection with parallel result processing"""
        eof_received = {
            DataType.USERS: False,
            DataType.TRANSACTIONS: False,
            DataType.TRANSACTION_ITEMS: False,
            DataType.STORES: False,
            DataType.MENU_ITEMS: False
        }
        
        # Flag para controlar el inicio del consumo de resultados
        results_consuming_started = False

        try:
            while self.running:
                try:
                    # Receive message
                    message_type, message_data = receive_message(client_socket)
                    # logger.debug(f"Received message type {message_type}, data size {len(message_data)}")
                    
                    if message_type == MessageType.BATCH:
                        self.handle_batch_message(client_socket, message_data)
                        
                    elif message_type == MessageType.EOF:
                        data_type, newly_marked = self.handle_eof_message(
                            client_socket,
                            message_data,
                            eof_received
                        )

                        if newly_marked and data_type == DataType.TRANSACTIONS:
                            self.propagate_transactions_eof()
                        elif newly_marked and data_type == DataType.STORES:
                            self.propagate_stores_eof()
                        elif newly_marked and data_type == DataType.TRANSACTION_ITEMS:
                            self.propagate_transaction_items_eof()
                        elif newly_marked and data_type == DataType.MENU_ITEMS:
                            self.propagate_menu_items_eof()

                        if all(eof_received.values()):
                            self.forward_results_to_client(client_socket)
                            break

                    else:
                        logger.warning(f"Unknown message type: {message_type}")
                        send_response(client_socket, False)
                        
                except ConnectionError:
                    logger.info(f"Client {address} disconnected")
                    break
                except Exception as e:
                    logger.error(f"Error handling message from {address}: {e}")
                    try:
                        send_response(client_socket, False)
                    except:
                        pass
                    break
                    
        except Exception as e:
            logger.error(f"Error in client handler for {address}: {e}")
        finally:
            client_socket.close()
            logger.info(f"Connection with {address} closed")

    def handle_batch_message(self, client_socket: socket.socket, message_data: bytes):
        """Handle a batch of data rows"""
        try:
            data_type, rows = parse_batch_message(message_data)
            
            # logger.info(f"Received batch: type={data_type.name}, size={len(rows)}")
            
            # Si son transacciones, enviarlas a la cola de procesamiento
            if data_type == DataType.TRANSACTIONS:
                logger.info(f"Procesando {len(rows)} transacciones con chunking (chunk_size={self.chunk_size})")
                try:
                    # Crear chunks de transacciones para procesamiento optimizado
                    chunks = self.create_chunks(rows)
                    logger.info(f"Creando {len(chunks)} chunks de transacciones")
                    
                    # Enviar cada chunk completo a la cola
                    for i, chunk in enumerate(chunks):
                        self.transactions_queue.send(chunk)
                        logger.info(f"Enviado chunk {i+1}/{len(chunks)} con {len(chunk)} transacciones")
                    
                    logger.info(f"Enviados {len(chunks)} chunks con {len(rows)} transacciones totales")
                except Exception as e:
                    logger.error(f"Error enviando chunks a RabbitMQ: {e}")
            elif data_type == DataType.STORES:
                logger.info(f"Procesando {len(rows)} stores para enriquecimiento")
                try:
                    self.stores_queue.send(rows)
                    logger.info(f"Enviados {len(rows)} stores al enrichment worker")
                except Exception as e:
                    logger.error(f"Error enviando stores a RabbitMQ: {e}")
            elif data_type == DataType.TRANSACTION_ITEMS:
                logger.info(
                    "Procesando %s transaction items con chunking (chunk_size=%s)",
                    len(rows),
                    self.chunk_size,
                )
                try:
                    chunks = self.create_chunks(rows)
                    logger.info("Creando %s chunks de transaction items", len(chunks))

                    for i, chunk in enumerate(chunks):
                        self.transaction_items_queue.send(chunk)
                        logger.info(
                            "Enviado chunk %s/%s con %s transaction items",
                            i + 1,
                            len(chunks),
                            len(chunk),
                        )
                except Exception as e:
                    logger.error(f"Error enviando transaction items a RabbitMQ: {e}")
            elif data_type == DataType.MENU_ITEMS:
                logger.info(f"Procesando {len(rows)} menu items para agregación")
                try:
                    self.menu_items_queue.send(rows)
                    logger.info("Menu items enviados a la cola %s", self.menu_items_queue_name)
                except Exception as e:
                    logger.error(f"Error enviando menu items a RabbitMQ: {e}")
            
            # Send success response
            send_response(client_socket, True)
            
        except Exception as e:
            logger.error(f"Failed to process batch message: {e}")
            send_response(client_socket, False)
    
    def handle_eof_message(
        self,
        client_socket: socket.socket,
        message_data: bytes,
        eof_state: dict[DataType, bool]
    ) -> tuple[DataType | None, bool]:
        """Handle EOF message for a data type.

        Returns a tuple (data_type, is_new) where is_new indicates whether this is
        the first EOF received for the given type.
        """
        try:
            data_type = parse_eof_message(message_data)

            is_new = not eof_state[data_type]
            if is_new:
                eof_state[data_type] = True
                logger.info(f"Received EOF for {data_type.name}.")
            else:
                logger.warning(f"Duplicate EOF received for {data_type.name}")

            send_response(client_socket, True)
            return data_type, is_new

        except Exception as e:
            logger.error(f"Failed to process EOF message: {e}")
            send_response(client_socket, False)
            return None, False

    @staticmethod
    def _is_eof_message(message: Any) -> bool:
        return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'

    def propagate_transactions_eof(self):
        """Propaga un mensaje EOF a la cadena de procesamiento de transacciones."""
        try:
            self.transactions_queue.send({'type': 'EOF'})
            logger.info("Propagated EOF to transactions pipeline")
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to transactions queue: {exc}")

    def propagate_stores_eof(self):
        """Propaga un mensaje EOF a la cola de stores."""
        try:
            self.stores_queue.send({'type': 'EOF'})
            logger.info("Propagated EOF to stores queue")
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to stores queue: {exc}")

    def propagate_transaction_items_eof(self):
        """Propaga EOF a la cola de transaction items."""
        try:
            self.transaction_items_queue.send({'type': 'EOF'})
            logger.info("Propagated EOF to transaction items queue")
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to transaction items queue: {exc}")

    def propagate_menu_items_eof(self):
        """Propaga EOF a la cola de menu items."""
        try:
            self.menu_items_queue.send({'type': 'EOF'})
            logger.info("Propagated EOF to menu items queue")
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to menu items queue: {exc}")

    def _send_json_line(self, client_socket: socket.socket, payload: Any) -> None:
        message = json.dumps(payload, ensure_ascii=False) + '\n'
        client_socket.sendall(message.encode('utf-8'))

    def forward_results_to_client(self, client_socket: socket.socket) -> None:
        """Consume resultados desde RabbitMQ y enviarlos al cliente por TCP."""
        logger.info("Forwarding results to connected client")

        results_queue = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.results_queue_name,
            port=self.rabbitmq_port
        )

        eof_sent = False

        def handle_payload(payload: Any) -> None:
            nonlocal eof_sent
            if isinstance(payload, list):
                for item in payload:
                    handle_payload(item)
                return

            if self._is_eof_message(payload):
                logger.info("Received EOF from results queue; notifying client")
                try:
                    self._send_json_line(client_socket, {'type': 'EOF'})
                    eof_sent = True
                except Exception as exc:
                    logger.error(f"Failed to send EOF to client: {exc}")
                finally:
                    results_queue.stop_consuming()
                return

            try:
                logger.info(f"Gateway enviando resultado al cliente: {payload}")
                self._send_json_line(client_socket, payload)
            except Exception as exc:
                logger.error(f"Failed to forward result to client: {exc}")
                results_queue.stop_consuming()

        def on_message(message: Any) -> None:
            try:
                logger.debug(f"Gateway recibió mensaje de results queue: {type(message)} - {message}")
                handle_payload(message)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Unexpected error forwarding results: {exc}")
                results_queue.stop_consuming()

        try:
            results_queue.start_consuming(on_message)
        except KeyboardInterrupt:
            logger.info("Results forwarding interrupted")
        except Exception as exc:
            logger.error(f"Error while consuming results queue: {exc}")
        finally:
            results_queue.close()
            if not eof_sent:
                try:
                    self._send_json_line(client_socket, {'type': 'EOF'})
                except Exception:
                    pass
    
def main():
    """Entry point"""
    gateway = CoffeeShopGateway()
    
    try:
        gateway.start_server()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        gateway.stop_server()

if __name__ == "__main__":
    main()
