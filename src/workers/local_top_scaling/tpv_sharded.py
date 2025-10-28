#!/usr/bin/env python3

"""Sharded TPV worker that aggregates semester totals per store based on store_id sharding."""

import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict

from message_utils import ClientId
from worker_utils import run_main, safe_float_conversion, safe_int_conversion, extract_year_half
from workers.local_top_scaling.aggregator_worker import AggregatorWorker
from workers.utils.sharding_utils import get_routing_key, extract_store_id_from_payload
from workers.utils.state_manager import TPVStateManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

YearHalf = str
StoreId = int

class ShardedTPVWorker(AggregatorWorker):
    """
    Sharded version of TPVWorker that processes transactions based on store_id sharding.
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
        
        logger.info(f"ShardedTPVWorker initialized: worker_id={self.worker_id}, num_shards={self.num_shards}, routing_key={self.expected_routing_key}")

        # Configure state manager
        state_path_env = os.getenv('STATE_FILE')
        state_dir_env = os.getenv('STATE_DIR')
        
        state_path = None
        state_dir = None
        
        if state_path_env:
            state_path = Path(state_path_env)
        elif state_dir_env:
            state_dir = Path(state_dir_env)

        # Initialize state manager (it will create its own state)
        self.state_manager = TPVStateManager(
            state_data=None,  # Let StateManager create its own state
            state_path=state_path,
            state_dir=state_dir,
            worker_id=str(self.worker_id)
        )
        
        # Get reference to the state data managed by StateManager
        self.partial_tpv = self.state_manager.get_state_data()
        
        # Sync state after potential load from disk
        self.state_manager.sync_state_after_load()

    def reset_state(self, client_id: ClientId) -> None:
        self.partial_tpv[client_id] = defaultdict(lambda: defaultdict(float))

    def should_process_transaction(self, payload: Dict[str, Any]) -> bool:
        """
        Determine if this worker should process the transaction based on store_id sharding.
        Also handles coordinated EOF messages that don't have store_id.
        
        Args:
            payload: Transaction data
            
        Returns:
            True if this worker should process the transaction
        """
        # Check if this is a control message (EOF, heartbeat, etc.)
        # Control messages like EOF don't have store_id and should be processed by all workers
        store_id = extract_store_id_from_payload(payload)
        if store_id is None:
            # This is likely a control message (EOF, etc.) - process it
            logger.debug(f"Received control message (no store_id): {payload}")
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
            
        # Skip control messages (EOF, etc.) - they don't have store_id
        store_id = extract_store_id_from_payload(payload)
        if store_id is None:
            logger.debug(f"Skipping control message in accumulate_transaction: {payload}")
            return
            
        year_half: YearHalf | None = extract_year_half(payload.get('created_at'))
        if not year_half:
            return
        
        store_id = safe_int_conversion(payload.get('store_id'), minimum=0)
        amount: float = safe_float_conversion(payload.get('final_amount'), 0.0)

        logger.info(f"Processing TPV transaction for store_id={store_id}, year_half={year_half}, amount={amount}")
        self.partial_tpv[client_id][year_half][store_id] += amount

    def process_batch(self, batch: list[Dict[str, Any]], client_id: ClientId):
        message_uuid = self._get_current_message_uuid()
        if message_uuid and self.state_manager.get_last_processed_message(client_id) == message_uuid:
            logger.info(
                "Skipping duplicate batch %s for client %s",
                message_uuid,
                client_id,
            )
            return

        with self._state_lock:
            previous_state = self.state_manager.clone_client_state(client_id)
            previous_uuid = self.state_manager.get_last_processed_message(client_id)

            try:
                for entry in batch:
                    self.accumulate_transaction(client_id, entry)

                if message_uuid:
                    self.state_manager.set_last_processed_message(client_id, message_uuid)

                self.state_manager.persist_state()
            except Exception:
                self.state_manager.restore_client_state(client_id, previous_state)
                if message_uuid:
                    if previous_uuid is None:
                        self.state_manager.clear_last_processed_message(client_id)
                    else:
                        self.state_manager.set_last_processed_message(client_id, previous_uuid)
                raise

    def create_payload(self, client_id: str) -> list[Dict[str, Any]]:
        totals = self.partial_tpv.get(client_id, {})
        results: list[Dict[str, Any]] = []

        for year_half, stores in totals.items():
            for store_id, tpv_value in stores.items():
                results.append(
                    {
                        'year_half_created_at': year_half,
                        'store_id': store_id,
                        'tpv': tpv_value,
                    }
                )

        return results

    def handle_eof(self, message: Dict[str, Any], client_id: ClientId):
        message_uuid = self._get_current_message_uuid()
        with self._state_lock:
            previous_state = self.state_manager.clone_client_state(client_id)
            previous_uuid = self.state_manager.get_last_processed_message(client_id)

        try:
            super().handle_eof(message, client_id)
        except Exception:
            raise

        with self._state_lock:
            try:
                if message_uuid:
                    self.state_manager.set_last_processed_message(client_id, message_uuid)
                else:
                    logger.warning(f"EOF message for client {client_id} has no UUID; clearing last processed message")
                    self.state_manager.clear_last_processed_message(client_id)

                self.state_manager.drop_empty_client_state(client_id)
                self.state_manager.persist_state()
            except Exception:
                self.state_manager.restore_client_state(client_id, previous_state)
                if message_uuid:
                    if previous_uuid is None:
                        self.state_manager.clear_last_processed_message(client_id)
                    else:
                        self.state_manager.set_last_processed_message(client_id, previous_uuid)
                elif previous_uuid is not None:
                    self.state_manager.set_last_processed_message(client_id, previous_uuid)
                raise

    def _get_current_message_uuid(self) -> str | None:
        metadata = self._get_current_message_metadata()
        if not metadata:
            return None
        message_uuid = metadata.get('message_uuid')
        if not message_uuid:
            logger.warning("Missing message_uuid in metadata: %s", metadata.keys())
            return None
        return str(message_uuid)


if __name__ == '__main__':
    run_main(ShardedTPVWorker)
