"""Queue manager module for handling RabbitMQ operations."""

import logging
from typing import List, Any
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue
from config import GatewayConfig

logger = logging.getLogger(__name__)


class QueueManager:
    """Manages all RabbitMQ queues for the gateway."""
    
    def __init__(self, config: GatewayConfig):
        self.config = config
        
        # Initialize all queue connections
        self._init_queues()
        
        logger.info(f"Queue manager configured with RabbitMQ: {config.rabbitmq_host}:{config.rabbitmq_port}")
        logger.info(f"Transactions queue: {config.transactions_queue_name}")
        logger.info("Stores queues: %s", config.stores_queue_names)
        logger.info(f"Users queue: {config.users_queue_name}")
        logger.info(f"Transaction items queue: {config.transaction_items_queue_name}")
        logger.info(f"Menu items queue: {config.menu_items_queue_name}")
        logger.info(f"Results queue: {config.results_queue_name}")
    
    def _init_queues(self):
        """Initialize all RabbitMQ queue connections."""
        connection_params = self.config.get_rabbitmq_connection_params()
        
        # Transactions queue
        self.transactions_queue = RabbitMQMiddlewareQueue(
            queue_name=self.config.transactions_queue_name,
            **connection_params
        )
        
        # Stores queues (multiple)
        self.stores_queues = [
            RabbitMQMiddlewareQueue(
                queue_name=queue_name,
                **connection_params
            )
            for queue_name in self.config.stores_queue_names
        ]
        
        # Users queue
        self.users_queue = RabbitMQMiddlewareQueue(
            queue_name=self.config.users_queue_name,
            **connection_params
        )
        
        # Transaction items queue
        self.transaction_items_queue = RabbitMQMiddlewareQueue(
            queue_name=self.config.transaction_items_queue_name,
            **connection_params
        )
        
        # Menu items queue
        self.menu_items_queue = RabbitMQMiddlewareQueue(
            queue_name=self.config.menu_items_queue_name,
            **connection_params
        )
        
        # Results queue
        self.results_queue = RabbitMQMiddlewareQueue(
            queue_name=self.config.results_queue_name,
            **connection_params
        )
    
    def send_transactions_chunks(self, transactions: List[Any], client_id: str) -> None:
        """Send transactions in chunks to the processing queue with client metadata."""
        chunks = self._create_chunks(transactions, self.config.chunk_size)
        logger.info(f"Creating {len(chunks)} chunks of transactions for client {client_id}")
        
        try:
            for i, chunk in enumerate(chunks):
                message_with_metadata = {
                    'client_id': client_id,
                    'data': chunk,
                    'chunk_index': i,
                    'total_chunks': len(chunks)
                }
                self.transactions_queue.send(message_with_metadata)
                logger.info(f"Sent chunk {i+1}/{len(chunks)} with {len(chunk)} transactions for client {client_id}")
            
            logger.info(f"Sent {len(chunks)} chunks with {len(transactions)} total transactions for client {client_id}")
        except Exception as e:
            logger.error(f"Error sending transaction chunks to RabbitMQ for client {client_id}: {e}")
            raise
    
    def send_stores(self, stores: List[Any], client_id: str) -> None:
        """Send stores to all configured stores queues with client metadata."""
        logger.info("Processing %s stores for pipelines for client %s", len(stores), client_id)
        
        for queue_name, queue in zip(self.config.stores_queue_names, self.stores_queues):
            try:
                message_with_metadata = {
                    'client_id': client_id,
                    'data': stores
                }
                queue.send(message_with_metadata)
                logger.info("Sent %s stores to queue %s for client %s", len(stores), queue_name, client_id)
            except Exception as e:
                logger.error("Error sending stores to RabbitMQ (%s) for client %s: %s", queue_name, client_id, e)
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
            logger.info("Creating %s chunks of transaction items for client %s", len(chunks), client_id)

            for i, chunk in enumerate(chunks):
                message_with_metadata = {
                    'client_id': client_id,
                    'data': chunk,
                    'chunk_index': i,
                    'total_chunks': len(chunks)
                }
                self.transaction_items_queue.send(message_with_metadata)
                logger.info(
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
        for queue_name, queue in zip(self.config.stores_queue_names, self.stores_queues):
            try:
                eof_message = {
                    'client_id': client_id,
                    'type': 'EOF',
                    'data_type': 'STORES'
                }
                queue.send(eof_message)
                logger.info("Propagated EOF to stores queue %s for client %s", queue_name, client_id)
            except Exception as exc:
                logger.error("Failed to propagate EOF to stores queue %s for client %s: %s", queue_name, client_id, exc)
                # Don't raise here to allow other queues to receive EOF
    
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