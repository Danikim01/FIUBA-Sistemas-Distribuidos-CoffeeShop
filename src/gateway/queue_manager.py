"""Queue manager module for handling RabbitMQ operations."""

import logging
import queue
import threading
from typing import List, Any
from contextlib import suppress
from middleware.rabbitmq_middleware import RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue
from middleware.thread_aware_publishers import ThreadAwareExchangePublisher, ThreadAwareQueuePublisher
from config import GatewayConfig

logger = logging.getLogger(__name__)

class _ResultsRouter:
    """Single-consumer router that delivers results to the correct client."""

    def __init__(self, queue_factory):
        self._queue_factory = queue_factory
        self._buffers: dict[str, queue.Queue] = {}
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._consumer: RabbitMQMiddlewareQueue | None = None
        self._shutdown = threading.Event()

    def close(self) -> None:
        """Stop the consumer thread and close the underlying middleware."""
        self._shutdown.set()
        with self._lock:
            consumer = self._consumer
        if consumer is not None:
            with suppress(Exception):
                consumer.stop_consuming()
            with suppress(Exception):
                consumer.close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        with self._lock:
            self._buffers.clear()
            self._thread = None
            self._consumer = None

    def iter_client(self, client_id: str):
        """Yield messages for the requested client, blocking until EOF arrives."""
        buffer = self._get_buffer(client_id)
        self._ensure_thread_started()
        while not self._shutdown.is_set():
            try:
                message = buffer.get(timeout=0.5)
            except queue.Empty:
                continue
            yield message
            if isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF':
                break
        self._cleanup_buffer(client_id)

    def _get_buffer(self, client_id: str) -> queue.Queue:
        with self._lock:
            buffer = self._buffers.get(client_id)
            if buffer is None:
                buffer = queue.Queue()
                self._buffers[client_id] = buffer
            return buffer

    def _cleanup_buffer(self, client_id: str) -> None:
        with self._lock:
            self._buffers.pop(client_id, None)

    def _ensure_thread_started(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            if self._shutdown.is_set():
                return
            consumer = self._queue_factory()
            self._consumer = consumer
            thread = threading.Thread(target=self._consume_loop, args=(consumer,), daemon=True)
            self._thread = thread
            thread.start()

    def _consume_loop(self, consumer: RabbitMQMiddlewareQueue) -> None:
        def on_message(message):
            client_id = message.get('client_id') if isinstance(message, dict) else None
            if not client_id:
                logger.warning('Discarding results payload without client metadata: %s', message)
                return
            buffer = self._get_buffer(client_id)
            buffer.put(message)

        try:
            consumer.start_consuming(on_message)
        except Exception as exc:  # noqa: BLE001
            if not self._shutdown.is_set():
                logger.error('Results router stopped unexpectedly: %s', exc)




class QueueManager:
    """Manages all RabbitMQ queues for the gateway."""
    
    def __init__(self, config: GatewayConfig):
        self.config = config
        
        # Initialize all queue connections
        self._init_queues()
        
        logger.info(f"Queue manager configured with RabbitMQ: {config.rabbitmq_host}:{config.rabbitmq_port}")
        logger.info(f"Transactions queue: {config.transactions_queue_name}")
        logger.info("Stores queues: %s", config.stores_exchange_name)
        logger.info(f"Users queue: {config.users_queue_name}")
        logger.info(f"Transaction items queue: {config.transaction_items_queue_name}")
        logger.info(f"Menu items queue: {config.menu_items_queue_name}")
        logger.info(f"Results queue: {config.results_queue_name}")
    
    def _init_queues(self):
        """Initialize all RabbitMQ queue connections."""
        connection_params = self.config.get_rabbitmq_connection_params()
        
        # Transactions queue publisher per thread
        self.transactions_queue = ThreadAwareQueuePublisher(
            lambda: RabbitMQMiddlewareQueue(
                queue_name=self.config.transactions_queue_name,
                **connection_params,
            )
        )
        
        # Stores exchange publisher per thread
        self.stores_exchange = ThreadAwareExchangePublisher(
            lambda: RabbitMQMiddlewareExchange(
                exchange_name=self.config.stores_exchange_name,
                exchange_type='direct',
                route_keys=[self.config.stores_exchange_name],
                **connection_params,
            )
        )
        
        # Users queue publisher per thread
        self.users_queue = ThreadAwareQueuePublisher(
            lambda: RabbitMQMiddlewareQueue(
                queue_name=self.config.users_queue_name,
                **connection_params,
            )
        )
        
        # Transaction items queue publisher per thread
        self.transaction_items_queue = ThreadAwareQueuePublisher(
            lambda: RabbitMQMiddlewareQueue(
                queue_name=self.config.transaction_items_queue_name,
                **connection_params,
            )
        )
        
        # Menu items queue publisher per thread
        self.menu_items_queue = ThreadAwareQueuePublisher(
            lambda: RabbitMQMiddlewareQueue(
                queue_name=self.config.menu_items_queue_name,
                **connection_params,
            )
        )

        self._results_router = _ResultsRouter(
            lambda: RabbitMQMiddlewareQueue(
                queue_name=self.config.results_queue_name,
                **connection_params,
            )
        )
    
    def send_transactions_chunks(self, transactions: List[Any], client_id: str) -> None:
        """Send transactions in chunks to the processing queue with client metadata."""
        chunks = self._create_chunks(transactions, self.config.chunk_size)
        logger.debug(f"Creating {len(chunks)} chunks of transactions for client {client_id}")
        
        try:
            for i, chunk in enumerate(chunks):
                message_with_metadata = {
                    'client_id': client_id,
                    'data': chunk,
                    'chunk_index': i,
                    'total_chunks': len(chunks)
                }
                self.transactions_queue.send(message_with_metadata)
                logger.debug(f"Sent chunk {i+1}/{len(chunks)} with {len(chunk)} transactions for client {client_id}")
            
            logger.info(f"Sent {len(chunks)} chunks with {len(transactions)} total transactions for client {client_id}")
        except Exception as e:
            logger.error(f"Error sending transaction chunks to RabbitMQ for client {client_id}: {e}")
            raise
    
    def send_stores(self, stores: List[Any], client_id: str) -> None:
        """Send stores to all configured stores queues with client metadata."""
        logger.info("Processing %s stores for pipelines for client %s", len(stores), client_id)
        
        try:
            message_with_metadata = {
                'client_id': client_id,
                'data': stores
            }
            self.stores_exchange.send(message_with_metadata)
            logger.info("Sent %s stores to exchange %s for client %s", len(stores), self.config.stores_exchange_name, client_id)
        except Exception as e:
            logger.error("Error sending stores to RabbitMQ (%s) for client %s: %s", self.config.stores_exchange_name, client_id, e)
            raise
    
    def send_users(self, users: List[Any], client_id: str) -> None:
        """Send users to the users processing queue with client metadata."""
        logger.info("Processing %s users for client pipeline for client %s", len(users), client_id)
        
        try:
            message_with_metadata = {
                'client_id': client_id,
                'data': users
            }
            self.users_queue.send(message_with_metadata)
            logger.info("Sent %s users to queue %s for client %s", len(users), self.config.users_queue_name, client_id)
        except Exception as e:
            logger.error(f"Error sending users to RabbitMQ for client {client_id}: {e}")
            raise
    
    def send_transaction_items_chunks(self, transaction_items: List[Any], client_id: str) -> None:
        """Send transaction items in chunks to the processing queue with client metadata."""
        logger.info(
            "Processing %s transaction items with chunking (chunk_size=%s) for client %s",
            len(transaction_items),
            self.config.chunk_size,
            client_id
        )
        
        try:
            chunks = self._create_chunks(transaction_items, self.config.chunk_size)
            logger.debug("Creating %s chunks of transaction items for client %s", len(chunks), client_id)

            for i, chunk in enumerate(chunks):
                message_with_metadata = {
                    'client_id': client_id,
                    'data': chunk,
                    'chunk_index': i,
                    'total_chunks': len(chunks)
                }
                self.transaction_items_queue.send(message_with_metadata)
                logger.debug(
                    "Sent chunk %s/%s with %s transaction items for client %s",
                    i + 1,
                    len(chunks),
                    len(chunk),
                    client_id
                )
        except Exception as e:
            logger.error(f"Error sending transaction items to RabbitMQ for client {client_id}: {e}")
            raise
    
    def send_menu_items(self, menu_items: List[Any], client_id: str) -> None:
        """Send menu items to the aggregation queue with client metadata."""
        logger.info(f"Processing {len(menu_items)} menu items for aggregation for client {client_id}")
        
        try:
            message_with_metadata = {
                'client_id': client_id,
                'data': menu_items
            }
            self.menu_items_queue.send(message_with_metadata)
            logger.info("Menu items sent to queue %s for client %s", self.config.menu_items_queue_name, client_id)
        except Exception as e:
            logger.error(f"Error sending menu items to RabbitMQ for client {client_id}: {e}")
            raise
    
    def propagate_transactions_eof(self, client_id: str) -> None:
        """Propagate EOF message to transactions processing pipeline with client metadata."""
        try:
            eof_message = {
                'client_id': client_id,
                'type': 'EOF',
                'data_type': 'TRANSACTIONS'
            }
            self.transactions_queue.send(eof_message)
            logger.info("Propagated EOF to transactions pipeline for client %s", client_id)
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to transactions queue for client {client_id}: {exc}")
            raise
    
    def propagate_users_eof(self, client_id: str) -> None:
        """Propagate EOF message to users queue with client metadata."""
        try:
            eof_message = {
                'client_id': client_id,
                'type': 'EOF',
                'data_type': 'USERS'
            }
            self.users_queue.send(eof_message)
            logger.info("Propagated EOF to users queue for client %s", client_id)
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to users queue for client {client_id}: {exc}")
            raise
    
    def propagate_stores_eof(self, client_id: str) -> None:
        """Propagate EOF message to all stores queues with client metadata."""
        try:
            eof_message = {
                'client_id': client_id,
                'type': 'EOF',
                'data_type': 'STORES'
            }
            self.stores_exchange.send(eof_message)
            logger.info("Propagated EOF to stores exchange %s for client %s", self.config.stores_exchange_name, client_id)
        except Exception as exc:
            logger.error("Failed to propagate EOF to stores exchange %s for client %s: %s", self.config.stores_exchange_name, client_id, exc)
    
    def propagate_transaction_items_eof(self, client_id: str) -> None:
        """Propagate EOF message to transaction items queue with client metadata."""
        try:
            eof_message = {
                'client_id': client_id,
                'type': 'EOF',
                'data_type': 'TRANSACTION_ITEMS'
            }
            self.transaction_items_queue.send(eof_message)
            logger.info("Propagated EOF to transaction items queue for client %s", client_id)
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to transaction items queue for client {client_id}: {exc}")
            raise
    
    def iter_client_results(self, client_id: str):
        """Iterate over result payloads destined to the specified client."""
        return self._results_router.iter_client(client_id)

    def propagate_menu_items_eof(self, client_id: str) -> None:
        """Propagate EOF message to menu items queue with client metadata."""
        try:
            eof_message = {
                'client_id': client_id,
                'type': 'EOF',
                'data_type': 'MENU_ITEMS'
            }
            self.menu_items_queue.send(eof_message)
            logger.info("Propagated EOF to menu items queue for client %s", client_id)
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to menu items queue for client {client_id}: {exc}")
            raise
    
    def close(self) -> None:
        """Close all RabbitMQ publishers created by the manager."""
        publishers = [
            self.transactions_queue,
            self.users_queue,
            self.transaction_items_queue,
            self.menu_items_queue,
            self.stores_exchange,
        ]
        for publisher in publishers:
            try:
                publisher.close_all()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Error closing publisher: %s", exc)
        try:
            self._results_router.close()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Error closing results router: %s", exc)

    def create_results_consumer(self) -> RabbitMQMiddlewareQueue:
        """Create a new results queue consumer."""
        connection_params = self.config.get_rabbitmq_connection_params()
        return RabbitMQMiddlewareQueue(
            queue_name=self.config.results_queue_name,
            **connection_params
        )
    
    @staticmethod
    def _create_chunks(items: List[Any], chunk_size: int) -> List[List[Any]]:
        """Divide items into chunks for optimized processing."""
        chunks = []
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]
            chunks.append(chunk)
        return chunks