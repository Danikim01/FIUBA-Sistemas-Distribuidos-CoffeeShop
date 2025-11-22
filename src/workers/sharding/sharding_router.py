#!/usr/bin/env python3

"""Sharding router that distributes transactions to workers based on store_id."""

import logging
import os
import threading
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional

from workers.utils.worker_utils import run_main
from workers.utils.sharding_utils import get_routing_key, extract_store_id_from_payload
from workers.utils.message_utils import ClientId
from workers.utils.processed_message_store import ProcessedMessageStore
from workers.base_worker import BaseWorker
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ShardingRouter(BaseWorker):
    """
    Router that receives transactions and distributes them to sharded workers
    based on store_id using routing keys.
    
    Batches are sent immediately when they reach batch_size, or when EOF arrives.
    No timer-based flushing - simpler and eliminates race conditions.
    """
    
    def __init__(self):
        super().__init__()
        self.num_shards = int(os.getenv('NUM_SHARDS', '2'))
        
        # Batch configuration - maximum batch size
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        
        # Batch storage: {client_id: {routing_key: [messages]}}
        self.batches: Dict[ClientId, Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))
        self.batch_lock = threading.RLock()
        
        # Track statistics for debugging: {client_id: {routing_key: {'count': int, 'batches_sent': int}}}
        self.stats: Dict[ClientId, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'batches_sent': 0}))
        
        # Add deduplication support
        worker_label = f"{self.__class__.__name__}-{os.getenv('WORKER_ID', '0')}"
        self._processed_store = ProcessedMessageStore(worker_label)
        
        logger.info(f"ShardingRouter initialized with {self.num_shards} shards, batch_size={self.batch_size}")
        logger.info(
            f"\033[33m[SHARDING-ROUTER] Robust deduplication enabled with two-phase commit persistence "
            f"to handle batch retries\033[0m"
        )

    def _get_current_message_uuid(self) -> str | None:
        """Get the message UUID from the current message metadata."""
        metadata = self._get_current_message_metadata()
        if not metadata:
            logger.debug(f"[SHARDING-ROUTER] No metadata available for message UUID extraction")
            return None
        message_uuid = metadata.get("message_uuid")
        if not message_uuid:
            logger.debug(f"[SHARDING-ROUTER] No message_uuid found in metadata: {metadata.keys()}")
            return None
        return str(message_uuid)
    
    def _mark_processed(self, client_id: str, message_uuid: str | None) -> None:
        """Mark a message as processed."""
        if message_uuid:
            self._processed_store.mark_processed(client_id, message_uuid)
    
    def process_message(self, message: Any, client_id: ClientId) -> None:
        """
        Process incoming transaction and add it to appropriate shard batch.
        
        Args:
            message: Transaction data
            client_id: Client identifier
        """            
        # Extract store_id for sharding
        store_id = extract_store_id_from_payload(message)
        if store_id is None:
            logger.warning(f"Transaction without store_id, skipping. Message keys: {list(message.keys()) if isinstance(message, dict) else 'Not a dict'}")
            return
            
        # Calculate routing key for this store_id
        routing_key = get_routing_key(store_id, self.num_shards)
        
        # Add message to batch and flush if full
        self._add_to_batch(client_id, routing_key, message)
    
    def process_batch(self, batch: list, client_id: ClientId) -> None:
        """
        Process a batch of messages and add them to appropriate shard batches.
        
        Each incoming batch has a unique message_uuid (from filter_eof_barrier or previous workers).
        We use this UUID to detect duplicates when batches are re-sent after worker crashes.
        
        CRITICAL: Mark as processed AFTER successfully processing to ensure we don't lose data
        if the worker crashes. If the worker crashes after processing but before marking,
        the batch will be re-sent and we'll process it again (acceptable - better than losing data).
        The duplicate detection prevents processing the same batch twice if it's already persisted.
        
        Args:
            batch: List of messages to process
            client_id: Client identifier
        """
        # Get message_uuid from current message metadata
        message_uuid = self._get_current_message_uuid()
        batch_size = len(batch) if isinstance(batch, list) else 1
        
        # Log batch reception for debugging
        logger.debug(
            f"[SHARDING-ROUTER] Received batch for client {client_id}, "
            f"size: {batch_size}, message_uuid: {message_uuid}"
        )
        
        # Check for duplicates using message_uuid
        if message_uuid and self._processed_store.has_processed(client_id, message_uuid):
            logger.info(
                f"\033[33m[SHARDING-ROUTER] Duplicate batch {message_uuid} for client {client_id} "
                f"detected; skipping {batch_size} messages\033[0m"
            )
            return
        
        try:
            # Process batch: distribute messages to appropriate shard batches
            for message in batch:
                self.process_message(message, client_id)
            
            # Mark as processed AFTER successfully processing
            # This ensures that if the worker crashes, the batch can be re-sent
            # and will be detected as duplicate if it was already persisted
            if message_uuid:
                # logger.debug(
                #     f"[SHARDING-ROUTER] Marking batch {message_uuid} as processed for client {client_id}"
                # )
                self._mark_processed(client_id, message_uuid)
        except Exception as e:
            # If processing fails, don't mark as processed so it can be retried
            logger.error(
                f"\033[31m[SHARDING-ROUTER] Failed to process batch for client {client_id}, "
                f"message_uuid: {message_uuid}: {e}\033[0m"
            )
            raise

    def _add_to_batch(self, client_id: ClientId, routing_key: str, message: Any) -> None:
        """
        Add a message to the appropriate shard batch and flush if batch is full.
        
        Args:
            client_id: Client identifier
            routing_key: Routing key for the shard
            message: Message to add to batch
        """
        with self.batch_lock:
            # Add message to batch
            self.batches[client_id][routing_key].append(message)
            
            # Update statistics
            self.stats[client_id][routing_key]['count'] += 1
            
            # If batch is full, flush it immediately
            if len(self.batches[client_id][routing_key]) >= self.batch_size:
                self._flush_batch(client_id, routing_key)
    
    def _flush_batch(self, client_id: ClientId, routing_key: str) -> None:
        """
        Flush a specific batch for a client and routing key.
        
        Args:
            client_id: Client identifier
            routing_key: Routing key for the shard
        """
        with self.batch_lock:
            if routing_key not in self.batches[client_id] or not self.batches[client_id][routing_key]:
                return
                
            batch = self.batches[client_id][routing_key]
            if not batch:
                return
            
            # Send batch
            # batch_size = len(batch)
            #logger.info(f"Flushing batch for client {client_id}, shard {routing_key}, size: {batch_size}")
            self.send_message(
                client_id,
                batch,
                routing_key=routing_key,
                message_uuid=str(uuid.uuid4()),
            )
            
            # Update statistics
            self.stats[client_id][routing_key]['batches_sent'] += 1
            
            # Clear batch
            self.batches[client_id][routing_key] = []
    
    def handle_eof(self, message: Dict[str, Any], client_id: ClientId, message_uuid: Optional[str] = None) -> None:
        """
        Handle EOF by flushing all remaining batches for the client, then sending EOF.
        
        This ensures all batches are sent BEFORE the EOF for each shard.
        
        Args:
            message: EOF message
            client_id: Client identifier
        """
        logger.info(
            f"\033[36m[SHARDING-ROUTER] Received EOF for client {client_id}, "
            f"flushing all remaining batches per shard\033[0m"
        )
        
        with self._pause_message_processing():
            # Log statistics before flushing
            with self.batch_lock:
                if client_id in self.stats:
                    logger.info(f"\033[35m[SHARDING-ROUTER] Statistics for client {client_id}:\033[0m")
                    for routing_key in sorted(self.stats[client_id].keys()):
                        stats = self.stats[client_id][routing_key]
                        current_batch_size = len(self.batches[client_id].get(routing_key, []))
                        logger.info(
                            f"  {routing_key}: {stats['count']} messages processed, "
                            f"{stats['batches_sent']} batches sent, "
                            f"{current_batch_size} messages in current batch"
                        )
            
            # Collect all remaining batches to flush, grouped by shard
            with self.batch_lock:
                batches_by_shard = {}
                if client_id in self.batches:
                    for routing_key in list(self.batches[client_id].keys()):
                        batch = self.batches[client_id][routing_key]
                        if batch:
                            batches_by_shard[routing_key] = batch.copy()  # Make a copy to avoid issues
                            logger.info(f"Found final batch for client {client_id}, shard {routing_key}, size: {len(batch)}")
                    # Log if any shards have no batches
                    all_shards = [f"shard_{i}" for i in range(self.num_shards)]
                    shards_with_batches = set(batches_by_shard.keys())
                    shards_without_batches = set(all_shards) - shards_with_batches
                    if shards_without_batches:
                        logger.info(
                            f"Shards with no final batches for client {client_id}: {sorted(shards_without_batches)} "
                            f"(likely already flushed when batch_size was reached)"
                        )
            
            # Send all batches first (synchronously), grouped by shard
            # We do this outside the lock to avoid holding it during I/O
            batches_items = batches_by_shard.items()
            if len(batches_items) > 0:
                for routing_key, batch in batches_items:
                    logger.info(f"Flushing final batch for client {client_id}, shard {routing_key}, size: {len(batch)}")
                    self.send_message(
                        client_id,
                        batch,
                        routing_key=routing_key,
                        message_uuid=str(uuid.uuid4()),
                    )
                    # Update statistics
                    with self.batch_lock:
                        self.stats[client_id][routing_key]['batches_sent'] += 1
            else:
                logger.info(f"No final batches to flush for client {client_id} (all batches already sent when batch_size was reached)")
            
            # Clean up client data after all batches are sent
            with self.batch_lock:
                if client_id in self.batches:
                    del self.batches[client_id]
                # Keep stats for debugging but could clear them here if needed
                if client_id in self.stats:
                    del self.stats[client_id]

            logger.info(
                f"\033[36m[SHARDING-ROUTER] All batches flushed for client {client_id}. "
                f"Now propagating EOF to all {self.num_shards} shards\033[0m"
            )
            
                        
            logger.info(f"\033[32m[SHARDING-ROUTER] Clearing processed state for client {client_id} after EOF propagation\033[0m")
            self._processed_store.clear_client(client_id)

            for shard_id in range(self.num_shards):
                routing_key = f"shard_{shard_id}"
                logger.info(
                    f"\033[36m[SHARDING-ROUTER] Sending EOF to shard {routing_key} for client {client_id}\033[0m"
                )
                try:
                    # Send EOF directly to each routing key (no coordination needed for sharding router)
                    new_message_uuid_from_sharding_router = str(uuid.uuid4())
                    self.eof_handler.handle_eof_with_routing_key(client_id=client_id, routing_key=routing_key, message=message,exchange=self.middleware_config.output_exchange, message_uuid=new_message_uuid_from_sharding_router)
                    logger.debug(
                        f"[SHARDING-ROUTER] EOF sent successfully to {routing_key} for client {client_id}"
                    )
                except Exception as e:
                    logger.error(
                        f"\033[31m[SHARDING-ROUTER] Failed to send EOF to {routing_key} for client {client_id}: {e}\033[0m",
                        exc_info=True
                    )
                    raise
            
            logger.info(
                f"\033[32m[SHARDING-ROUTER] EOF propagation completed for client {client_id} "
                f"to all {self.num_shards} shards\033[0m"
            )

    
    def cleanup(self) -> None:
        """
        Clean up resources and flush any remaining batches.
        """
        logger.info("Cleaning up ShardingRouter, flushing all remaining batches")
        
        # Flush all remaining batches
        with self.batch_lock:
            for client_id in list(self.batches.keys()):
                for routing_key in list(self.batches[client_id].keys()):
                    self._flush_batch(client_id, routing_key)
        
        # Call parent cleanup
        super().cleanup()

if __name__ == '__main__':
    run_main(ShardingRouter)
