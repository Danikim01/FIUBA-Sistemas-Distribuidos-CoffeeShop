"""Queue manager module for handling RabbitMQ operations."""

import logging
import queue
import threading
from typing import List, Any, Union
from contextlib import suppress
import uuid
from middleware.rabbitmq_middleware import RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue
from middleware.thread_aware_publishers import ThreadAwareExchangePublisher, ThreadAwareQueuePublisher
from config import GatewayConfig
from workers.utils.sharding_utils import (
    get_routing_key_by_store_id,
    get_routing_key_by_item_id,
    extract_store_id_from_payload,
    extract_item_id_from_payload
)
from workers.utils.message_utils import create_message_with_metadata
from common.models import (
    ChunkedDataMessage,
    DataMessage,
    EOFMessage,
    MenuItem,
    Store,
    Transaction,
    TransactionItem,
    User,
)

logger = logging.getLogger(__name__)
CONTROL_CLIENT_ID = "gateway-control"

class _ResultsRouter:
    """Single-consumer router that delivers results to the correct client."""

    def __init__(self, queue_factory, expected_eofs: int):
        self._queue_factory = queue_factory
        self._expected_eofs = max(1, int(expected_eofs))
        self._buffers: dict[str, queue.Queue] = {}
        self._eof_counts: dict[str, int] = {}
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
            self._eof_counts.clear()
            self._thread = None
            self._consumer = None

    def iter_client(self, client_id: str):
        """Yield messages for the requested client, blocking until all EOF markers arrive."""
        buffer = self._get_buffer(client_id)
        self._ensure_thread_started()
        while not self._shutdown.is_set():
            try:
                message = buffer.get(timeout=0.5)
            except queue.Empty:
                continue
            yield message
            if isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF':
                count = self._eof_counts.get(client_id, 0) + 1
                self._eof_counts[client_id] = count
                if count >= self._expected_eofs:
                    break
        self._cleanup_buffer(client_id)
        self._eof_counts.pop(client_id, None)

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
        
        # Batch counters for sequence ID generation per client
        self._batch_counters: dict[str, int] = {}
        self._batch_counters_lock = threading.Lock()
        
        # Initialize all queue connections
        self._init_queues()
        
        logger.info(f"Queue manager configured with RabbitMQ: {config.rabbitmq_host}:{config.rabbitmq_port}")
        logger.info(f"Transactions exchange: {config.transactions_exchange_name} (sharded by store_id)")
        logger.info("Stores exchange: %s", config.stores_exchange_name)
        logger.info(f"Users queue: {config.users_queue_name}")
        logger.info(f"Transaction items exchange: {config.transaction_items_exchange_name} (sharded by item_id)")
        logger.info(f"Menu items queue: {config.menu_items_queue_name}")
        logger.info(f"Results queue: {config.results_queue_name}")
        logger.info(f"Filter replica count: {config.filter_replica_count}")
    
    def _init_queues(self):
        """Initialize all RabbitMQ queue connections."""
        connection_params = self.config.get_rabbitmq_connection_params()
        
        # Transactions exchange publisher per thread (with sharding support)
        replica_count = self.config.filter_replica_count
        transaction_route_keys = [f"shard_{i}" for i in range(replica_count)]
        self._transaction_route_keys = transaction_route_keys.copy()
        self.transactions_exchange = ThreadAwareExchangePublisher(
            lambda: RabbitMQMiddlewareExchange(
                exchange_name=self.config.transactions_exchange_name,
                exchange_type='direct',
                route_keys=transaction_route_keys,
                **connection_params,
            )
        )
        
        # Stores exchange publisher per thread
        # Use fanout exchange so all aggregators receive all messages
        self.stores_exchange = ThreadAwareExchangePublisher(
            lambda: RabbitMQMiddlewareExchange(
                exchange_name=self.config.stores_exchange_name,
                exchange_type='fanout',
                route_keys=[],  # Fanout ignores routing keys
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
        
        # Transaction items exchange publisher per thread (with sharding support)
        transaction_items_route_keys = [f"shard_{i}" for i in range(replica_count)]
        self._transaction_items_route_keys = transaction_items_route_keys.copy()
        self.transaction_items_exchange = ThreadAwareExchangePublisher(
            lambda: RabbitMQMiddlewareExchange(
                exchange_name=self.config.transaction_items_exchange_name,
                exchange_type='direct',
                route_keys=transaction_items_route_keys,
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
            ),
            expected_eofs=self.config.results_expected,
        )
    
    def send_transactions_chunks(self, transactions: List[Transaction], client_id: str) -> None:
        """Send transactions in chunks to the sharded exchange with client metadata.
        
        Each transaction is sharded by store_id to ensure consistent routing.
        """
        chunks = self._create_chunks(transactions, self.config.chunk_size)
        logger.debug(f"Creating {len(chunks)} chunks of transactions for client {client_id}")
        
        try:
            replica_count = self.config.filter_replica_count
            
            for i, chunk in enumerate(chunks):
                # Get batch number and generate sequence_id for this batch
                batch_num = self._get_next_batch_num(client_id)
                sequence_id = self._generate_sequence_id(client_id, batch_num)
                
                # Shard transactions within chunk by store_id
                sharded_chunks = {}
                for transaction in chunk:
                    if isinstance(transaction, dict):
                        store_id = extract_store_id_from_payload(transaction)
                        if store_id is not None:
                            routing_key = get_routing_key_by_store_id(store_id, replica_count)
                            if routing_key not in sharded_chunks:
                                sharded_chunks[routing_key] = []
                            sharded_chunks[routing_key].append(transaction)
                        else:
                            # Fallback: distribute to shard_0 if no store_id
                            logger.warning(f"Transaction without store_id, routing to shard_0")
                            if 'shard_0' not in sharded_chunks:
                                sharded_chunks['shard_0'] = []
                            sharded_chunks['shard_0'].append(transaction)
                    else:
                        # Fallback for non-dict transactions
                        if 'shard_0' not in sharded_chunks:
                            sharded_chunks['shard_0'] = []
                        sharded_chunks['shard_0'].append(transaction)
                
                # Send each sharded chunk to its routing key
                for routing_key, sharded_chunk in sharded_chunks.items():
                    message_with_metadata = create_message_with_metadata(
                        client_id=client_id,
                        data=sharded_chunk,
                        chunk_index=i,
                        total_chunks=len(chunks),
                        sequence_id=sequence_id
                    )
                    self.transactions_exchange.send(message_with_metadata, routing_key=routing_key)
                    logger.debug(
                        f"Sent chunk {i+1}/{len(chunks)} with {len(sharded_chunk)} transactions "
                        f"to {routing_key} for client {client_id}, sequence_id: {sequence_id}"
                    )
            
            logger.debug(f"Sent {len(chunks)} chunks with {len(transactions)} total transactions for client {client_id}")
        except Exception as e:
            logger.error(f"Error sending transaction chunks to RabbitMQ for client {client_id}: {e}")
            raise
    
    def send_stores(self, stores: List[Store], client_id: str) -> None:
        """Send stores to all configured stores queues with client metadata."""
        logger.info("Processing %s stores for pipelines for client %s", len(stores), client_id)
        
        try:
            # Get batch number and generate sequence_id
            batch_num = self._get_next_batch_num(client_id)
            sequence_id = self._generate_sequence_id(client_id, batch_num)
            
            message_with_metadata = create_message_with_metadata(
                client_id=client_id,
                data=stores,
                sequence_id=sequence_id
            )
            self.stores_exchange.send(message_with_metadata)
            logger.info("Sent %s stores to exchange %s for client %s, sequence_id: %s", len(stores), self.config.stores_exchange_name, client_id, sequence_id)
        except Exception as e:
            logger.error("Error sending stores to RabbitMQ (%s) for client %s: %s", self.config.stores_exchange_name, client_id, e)
            raise
    
    def send_users(self, users: List[User], client_id: str) -> None:
        """Send users to the users processing queue with client metadata."""
        logger.debug("Processing %s users for client pipeline for client %s", len(users), client_id)
        
        try:
            # Get batch number and generate sequence_id
            batch_num = self._get_next_batch_num(client_id)
            sequence_id = self._generate_sequence_id(client_id, batch_num)
            
            message_with_metadata = create_message_with_metadata(
                client_id=client_id,
                data=users,
                sequence_id=sequence_id
            )
            self.users_queue.send(message_with_metadata)
            logger.debug("Sent %s users to queue %s for client %s, sequence_id: %s", len(users), self.config.users_queue_name, client_id, sequence_id)
        except Exception as e:
            logger.error(f"Error sending users to RabbitMQ for client {client_id}: {e}")
            raise
    
    def send_transaction_items_chunks(self, transaction_items: List[TransactionItem], client_id: str) -> None:
        """Send transaction items in chunks to the sharded exchange with client metadata.
        
        Each transaction item is sharded by item_id to ensure consistent routing.
        """
        logger.debug(
            "Processing %s transaction items with chunking (chunk_size=%s) for client %s",
            len(transaction_items),
            self.config.chunk_size,
            client_id
        )
        
        try:
            chunks = self._create_chunks(transaction_items, self.config.chunk_size)
            logger.debug("Creating %s chunks of transaction items for client %s", len(chunks), client_id)
            replica_count = self.config.filter_replica_count

            for i, chunk in enumerate(chunks):
                # Get batch number and generate sequence_id for this batch
                batch_num = self._get_next_batch_num(client_id)
                sequence_id = self._generate_sequence_id(client_id, batch_num)
                
                # Shard transaction items within chunk by item_id
                sharded_chunks = {}
                for item in chunk:
                    if isinstance(item, dict):
                        item_id = extract_item_id_from_payload(item)
                        if item_id is not None:
                            routing_key = get_routing_key_by_item_id(item_id, replica_count)
                            if routing_key not in sharded_chunks:
                                sharded_chunks[routing_key] = []
                            sharded_chunks[routing_key].append(item)
                        else:
                            # Fallback: distribute to shard_0 if no item_id
                            logger.warning(f"Transaction item without item_id, routing to shard_0")
                            if 'shard_0' not in sharded_chunks:
                                sharded_chunks['shard_0'] = []
                            sharded_chunks['shard_0'].append(item)
                    else:
                        # Fallback for non-dict items
                        if 'shard_0' not in sharded_chunks:
                            sharded_chunks['shard_0'] = []
                        sharded_chunks['shard_0'].append(item)
                
                # Send each sharded chunk to its routing key
                for routing_key, sharded_chunk in sharded_chunks.items():
                    message_with_metadata = create_message_with_metadata(
                        client_id=client_id,
                        data=sharded_chunk,
                        chunk_index=i,
                        total_chunks=len(chunks),
                        sequence_id=sequence_id
                    )
                    self.transaction_items_exchange.send(message_with_metadata, routing_key=routing_key)
                    logger.debug(
                        "Sent chunk %s/%s with %s transaction items to %s for client %s, sequence_id: %s",
                        i + 1,
                        len(chunks),
                        len(sharded_chunk),
                        routing_key,
                        client_id,
                        sequence_id
                    )
        except Exception as e:
            logger.error(f"Error sending transaction items to RabbitMQ for client {client_id}: {e}")
            raise
    
    def send_menu_items(self, menu_items: List[MenuItem], client_id: str) -> None:
        """Send menu items to the aggregation queue with client metadata."""
        logger.info(f"Processing {len(menu_items)} menu items for aggregation for client {client_id}")
        
        try:
            # Get batch number and generate sequence_id
            batch_num = self._get_next_batch_num(client_id)
            sequence_id = self._generate_sequence_id(client_id, batch_num)
            
            message_with_metadata = create_message_with_metadata(
                client_id=client_id,
                data=menu_items,
                sequence_id=sequence_id
            )
            self.menu_items_queue.send(message_with_metadata)
            logger.info("Menu items sent to queue %s for client %s, sequence_id: %s", self.config.menu_items_queue_name, client_id, sequence_id)
        except Exception as e:
            logger.error(f"Error sending menu items to RabbitMQ for client {client_id}: {e}")
            raise
    
    def propagate_transactions_eof(self, client_id: str) -> None:
        """Propagate EOF message to transactions processing pipeline with client metadata.
        
        Sends EOF to each routing key (shard_0, shard_1, shard_2) so each filter worker receives one.
        CRITICAL: Generate sequence_id ONCE before the loop to ensure all Filter Workers receive the same sequence_id.
        Each EOF includes routing_key in metadata to allow filter workers to identify their replica.
        """
        try:
            replica_count = self.config.filter_replica_count
            
            # Generate sequence_id ONCE before the loop (CRITICAL for consistency)
            last_batch_num = self._get_last_batch_num(client_id)
            eof_batch_num = last_batch_num + 1
            sequence_id = self._generate_sequence_id(client_id, eof_batch_num)
            
            # Send EOF to each routing key with routing_key in metadata
            for i in range(replica_count):
                routing_key = f"shard_{i}"
                # Create EOF message with routing_key in metadata for each replica
                message_uuid = str(uuid.uuid4())
                eof_message = create_message_with_metadata(
                    client_id=client_id,
                    data={},
                    message_type='EOF',
                    data_type='TRANSACTIONS',
                    sequence_id=sequence_id,
                    message_uuid=message_uuid
                )
                logger.info(f"==================PROPAGATING EOF MESSAGE FOR TRANSACTIONS==================")
                logger.info(f"\033[33m[QUEUE-MANAGER] EOF MESSAGE: {eof_message}\033[0m")
                self.transactions_exchange.send(eof_message, routing_key=routing_key)
            logger.info(
                "Propagated %d EOFs to transactions exchange for client %s (one per routing key), sequence_id: %s",
                replica_count, client_id, sequence_id
            )
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to transactions exchange for client {client_id}: {exc}")
            raise
    
    def propagate_users_eof(self, client_id: str) -> None:
        """Propagate EOF message to users queue with client metadata."""
        try:
            # Generate sequence_id for EOF
            last_batch_num = self._get_last_batch_num(client_id)
            eof_batch_num = last_batch_num + 1
            sequence_id = self._generate_sequence_id(client_id, eof_batch_num)
            
            eof_message = create_message_with_metadata(
                client_id=client_id,
                data={},
                message_type='EOF',
                data_type='USERS',
                sequence_id=sequence_id
            )
            self.users_queue.send(eof_message)
            logger.info("Propagated EOF to users queue for client %s, sequence_id: %s", client_id, sequence_id)
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to users queue for client {client_id}: {exc}")
            raise
    
    def propagate_stores_eof(self, client_id: str) -> None:
        """Propagate EOF message to all stores queues with client metadata."""
        try:
            # Generate sequence_id for EOF
            last_batch_num = self._get_last_batch_num(client_id)
            eof_batch_num = last_batch_num + 1
            sequence_id = self._generate_sequence_id(client_id, eof_batch_num)
            
            eof_message = create_message_with_metadata(
                client_id=client_id,
                data={},
                message_type='EOF',
                data_type='STORES',
                sequence_id=sequence_id
            )
            self.stores_exchange.send(eof_message)
            logger.info("Propagated EOF to stores exchange %s for client %s, sequence_id: %s", self.config.stores_exchange_name, client_id, sequence_id)
        except Exception as exc:
            logger.error("Failed to propagate EOF to stores exchange %s for client %s: %s", self.config.stores_exchange_name, client_id, exc)
    
    def propagate_transaction_items_eof(self, client_id: str) -> None:
        """Propagate EOF message to transaction items exchange with client metadata.
        
        Sends EOF to each routing key (shard_0, shard_1, shard_2) so each filter worker receives one.
        CRITICAL: Generate sequence_id ONCE before the loop to ensure all Filter Workers receive the same sequence_id.
        Each EOF includes routing_key in metadata to allow filter workers to identify their replica.
        """
        try:
            replica_count = self.config.filter_replica_count
            
            # Generate sequence_id ONCE before the loop (CRITICAL for consistency)
            last_batch_num = self._get_last_batch_num(client_id)
            eof_batch_num = last_batch_num + 1
            sequence_id = self._generate_sequence_id(client_id, eof_batch_num)
            
            # Send EOF to each routing key with routing_key in metadata
            for i in range(replica_count):
                routing_key = f"shard_{i}"
                # Create EOF message with routing_key in metadata for each replica
                message_uuid = str(uuid.uuid4())
                eof_message = create_message_with_metadata(
                    client_id=client_id,
                    data={},
                    message_type='EOF',
                    data_type='TRANSACTION_ITEMS',
                    sequence_id=sequence_id,
                    message_uuid=message_uuid
                )
                logger.info(f"==================PROPAGATING EOF MESSAGE FOR TRANSACTION ITEMS==================")
                logger.info(f"\033[33m[QUEUE-MANAGER] EOF MESSAGE: {eof_message}\033[0m")
                self.transaction_items_exchange.send(eof_message, routing_key=routing_key)
            logger.info(
                "Propagated %d EOFs to transaction items exchange for client %s (one per routing key), sequence_id: %s",
                replica_count, client_id, sequence_id
            )
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to transaction items exchange for client {client_id}: {exc}")
            raise
    
    def iter_client_results(self, client_id: str):
        """Iterate over result payloads destined to the specified client."""
        return self._results_router.iter_client(client_id)

    def propagate_menu_items_eof(self, client_id: str) -> None:
        """Propagate EOF message to menu items queue with client metadata."""
        try:
            # Generate sequence_id for EOF
            last_batch_num = self._get_last_batch_num(client_id)
            eof_batch_num = last_batch_num + 1
            sequence_id = self._generate_sequence_id(client_id, eof_batch_num)
            
            eof_message = create_message_with_metadata(
                client_id=client_id,
                data={},
                message_type='EOF',
                data_type='MENU_ITEMS',
                sequence_id=sequence_id
            )
            self.menu_items_queue.send(eof_message)
            logger.info("Propagated EOF to menu items queue for client %s, sequence_id: %s", client_id, sequence_id)
        except Exception as exc:
            logger.error(f"Failed to propagate EOF to menu items queue for client {client_id}: {exc}")
            raise

    def propagate_client_reset(self, client_id: str) -> None:
        """Broadcast a control message instructing workers to drop state for a client."""
        control_message = create_message_with_metadata(
            client_id=client_id,
            data=None,
            message_type="CLIENT_RESET",
            reason="gateway_client_disconnect",
        )
        logger.info("Broadcasting client reset control message for %s", client_id)
        self._broadcast_control_message(control_message)

    def propagate_global_reset(self) -> None:
        """Broadcast a control message instructing workers to drop all state."""
        control_message = create_message_with_metadata(
            client_id=CONTROL_CLIENT_ID,
            data=None,
            message_type="RESET_ALL",
            reason="gateway_startup",
        )
        logger.info("Broadcasting global reset control message")
        self._broadcast_control_message(control_message)

    def _broadcast_control_message(self, message: dict[str, Any]) -> None:
        """Send a control message to every pipeline (transactions, items, users, menu, stores)."""
        send_errors: list[str] = []

        def _safe_send(publisher, **kwargs):
            try:
                publisher.send(message, **kwargs)
            except Exception as exc:
                send_errors.append(f"{type(publisher).__name__}: {exc}")

        for routing_key in self._transaction_route_keys:
            _safe_send(self.transactions_exchange, routing_key=routing_key)

        for routing_key in self._transaction_items_route_keys:
            _safe_send(self.transaction_items_exchange, routing_key=routing_key)

        _safe_send(self.stores_exchange)
        _safe_send(self.users_queue)
        _safe_send(self.menu_items_queue)

        if send_errors:
            logger.warning("Some control broadcasts failed: %s", "; ".join(send_errors))
    
    def close(self) -> None:
        """Close all RabbitMQ publishers created by the manager."""
        publishers = [
            self.transactions_exchange,
            self.users_queue,
            self.transaction_items_exchange,
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
    
    def _get_next_batch_num(self, client_id: str) -> int:
        """Get and increment the next batch number for a client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Next batch number (1-indexed)
        """
        with self._batch_counters_lock:
            if client_id not in self._batch_counters:
                self._batch_counters[client_id] = 0
            self._batch_counters[client_id] += 1
            return self._batch_counters[client_id]
    
    def _get_last_batch_num(self, client_id: str) -> int:
        """Get the last assigned batch number for a client without incrementing.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Last batch number (0 if no batches sent yet)
        """
        with self._batch_counters_lock:
            return self._batch_counters.get(client_id, 0)
    
    def _generate_sequence_id(self, client_id: str, batch_num: int) -> str:
        """Generate sequence ID in format {client_id_sin_guiones}-{batch_num}.
        
        Args:
            client_id: Client identifier (may contain hyphens)
            batch_num: Batch number
            
        Returns:
            Sequence ID string
        """
        client_id_sin_guiones = client_id.replace('-', '')
        return f"{client_id_sin_guiones}-{batch_num}"
    
    def _clear_client_batch_counter(self, client_id: str) -> None:
        """Clear batch counter for a client.
        
        Args:
            client_id: Client identifier
        """
        with self._batch_counters_lock:
            self._batch_counters.pop(client_id, None)
    
    @staticmethod
    def _create_chunks(items: List[Union[Transaction, TransactionItem, User, Store, MenuItem]], chunk_size: int) -> List[List[Union[Transaction, TransactionItem, User, Store, MenuItem]]]:
        """Divide items into chunks for optimized processing."""
        chunks = []
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]
            chunks.append(chunk)
        return chunks
