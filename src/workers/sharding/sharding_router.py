#!/usr/bin/env python3

"""Sharding router that distributes transactions to workers based on store_id."""

import logging
import os
import threading
import uuid
from collections import defaultdict
from typing import Any, Dict, List

from workers.utils.worker_utils import run_main
from workers.utils.sharding_utils import get_routing_key, extract_store_id_from_payload
from workers.utils.message_utils import ClientId
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
        
        logger.info(f"ShardingRouter initialized with {self.num_shards} shards, batch_size={self.batch_size}")

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
        
        Messages are distributed by shard and sent immediately when batch_size is reached.
        
        Args:
            batch: List of messages to process
            client_id: Client identifier
        """
        for message in batch:
            self.process_message(message, client_id)

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
            logger.info(f"Flushing batch for client {client_id}, shard {routing_key}, size: {len(batch)}")
            self.send_message(
                client_id,
                batch,
                routing_key=routing_key,
                message_uuid=str(uuid.uuid4()),
            )
            
            # Clear batch
            self.batches[client_id][routing_key] = []
    
    def handle_eof(self, message: Dict[str, Any], client_id: ClientId) -> None:
        """
        Handle EOF by flushing all remaining batches for the client, then sending EOF.
        
        This ensures all batches are sent BEFORE the EOF for each shard.
        
        Args:
            message: EOF message
            client_id: Client identifier
        """
        logger.info(f"Received EOF for client {client_id}, flushing all remaining batches per shard")
        
        with self._pause_message_processing():
            # Collect all remaining batches to flush, grouped by shard
            with self.batch_lock:
                batches_by_shard = {}
                if client_id in self.batches:
                    for routing_key in list(self.batches[client_id].keys()):
                        batch = self.batches[client_id][routing_key]
                        if batch:
                            batches_by_shard[routing_key] = batch.copy()  # Make a copy to avoid issues
                            logger.info(f"Found final batch for client {client_id}, shard {routing_key}, size: {len(batch)}")
            
            # Send all batches first (synchronously), grouped by shard
            # We do this outside the lock to avoid holding it during I/O
            for routing_key, batch in batches_by_shard.items():
                logger.info(f"Flushing final batch for client {client_id}, shard {routing_key}, size: {len(batch)}")
                self.send_message(
                    client_id,
                    batch,
                    routing_key=routing_key,
                    message_uuid=str(uuid.uuid4()),
                )
            
            # Clean up client data after all batches are sent
            with self.batch_lock:
                if client_id in self.batches:
                    del self.batches[client_id]
            
            # Only after all batches are sent, send EOF to each shard
            # Send EOF to all shards (even if they didn't receive batches) to ensure
            # all sharded workers know the stream has ended
            logger.info(f"All batches flushed for client {client_id}. Now propagating EOF to all {self.num_shards} shards")
            
            for shard_id in range(self.num_shards):
                routing_key = f"shard_{shard_id}"
                logger.info(f"Sending EOF to shard {routing_key} for client {client_id}")
                self.eof_handler.handle_eof_with_routing_key(
                    client_id=client_id,
                    message=message,
                    routing_key=routing_key,
                    exchange=self.middleware_config.output_exchange,
                )
            
            logger.info(f"EOF propagation completed for client {client_id} to all {self.num_shards} shards")
    
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

    def handle_client_reset(self, client_id: ClientId) -> None:
        """Drop any pending batches for a disconnected client."""
        with self.batch_lock:
            if client_id in self.batches:
                del self.batches[client_id]
        logger.info("[CONTROL] ShardingRouter dropped pending batches for client %s", client_id)

    def handle_reset_all_clients(self) -> None:
        """Drop all pending batches."""
        with self.batch_lock:
            self.batches.clear()
        logger.info("[CONTROL] ShardingRouter dropped all pending batches")

if __name__ == '__main__':
    run_main(ShardingRouter)
