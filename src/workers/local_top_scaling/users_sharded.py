#!/usr/bin/env python3

"""Sharded top clients worker that aggregates purchase quantities per branch."""

import logging
import os
from collections import defaultdict
from typing import Any, DefaultDict, Dict

from message_utils import ClientId
from worker_utils import run_main, safe_int_conversion
from workers.local_top_scaling.aggregator_worker import AggregatorWorker
from workers.utils.sharding_utils import get_routing_key, extract_store_id_from_payload

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ShardedClientsWorker(AggregatorWorker):
    """
    Sharded version of ClientsWorker that processes transactions based on store_id sharding.
    Each worker processes a specific shard of stores.
    """
    
    def __init__(self) -> None:
        super().__init__()
        
        # Get sharding configuration from environment
        self.num_shards = int(os.getenv('NUM_SHARDS', '2'))
        self.worker_id = int(os.getenv('WORKER_ID', '0'))
        
        # Validate worker_id is within shard range
        if self.worker_id >= self.num_shards:
            raise ValueError(f"WORKER_ID {self.worker_id} must be less than NUM_SHARDS {self.num_shards}")
        
        # Configure routing key for this worker's shard
        self.expected_routing_key = f"shard_{self.worker_id}"
        
        logger.info(f"ShardedClientsWorker initialized: worker_id={self.worker_id}, num_shards={self.num_shards}, routing_key={self.expected_routing_key}")

        self.clients_data: DefaultDict[
            ClientId, DefaultDict[int, DefaultDict[int, int]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    def reset_state(self, client_id: ClientId) -> None:
        self.clients_data[client_id] = defaultdict(lambda: defaultdict(int))

    def should_process_transaction(self, payload: Dict[str, Any]) -> bool:
        """
        Determine if this worker should process the transaction based on store_id sharding.
        Also handles coordinated EOF messages that don't have store_id.
        
        Args:
            payload: Transaction data
            
        Returns:
            True if this worker should process the transaction
        """
        store_id = extract_store_id_from_payload(payload)
        if store_id is None:
            logger.warning(f"Transaction without store_id, skipping. Payload keys: {list(payload.keys()) if isinstance(payload, dict) else 'Not a dict'}")
            return False
            
        # For regular transactions, verify they belong to our shard
        expected_routing_key = get_routing_key(store_id, self.num_shards)
        if expected_routing_key != self.expected_routing_key:
            logger.warning(f"Received transaction for wrong shard: store_id={store_id}, expected={self.expected_routing_key}, got={expected_routing_key}")
            return False
            
        return True

    def accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        # Only process transactions that belong to this worker's shard
        if not self.should_process_transaction(payload):
            return
            
        store_id = safe_int_conversion(payload.get('store_id'), minimum=0)
        user_id = safe_int_conversion(payload.get('user_id'), minimum=0)
        
        logger.info(f"Processing transaction for store_id={store_id}")
        self.clients_data[client_id][store_id][user_id] += 1

    def create_payload(self, client_id: str) -> list[Dict[str, Any]]:
        counts_for_client = self.clients_data.pop(client_id, {})
        results: list[Dict[str, Any]] = []

        # For each store, get the top 3 users by purchase quantity
        for store_id, user_counts in counts_for_client.items():
            # Sort users by purchase quantity (descending) and take top 3
            sorted_users = sorted(
                user_counts.items(), 
                key=lambda x: x[1],  # Sort by purchase quantity
                reverse=True
            )[:3]  # Take only top 3
            
            for user_id, purchases_qty in sorted_users:
                results.append(
                    {
                        'store_id': store_id,
                        'user_id': user_id,
                        'purchases_qty': purchases_qty,
                    }
                )

        logger.info(f"ShardedClientsWorker {self.worker_id}: Sending {len(results)} top 3 local results for client {client_id}")
        return results


if __name__ == '__main__':
    run_main(ShardedClientsWorker)
