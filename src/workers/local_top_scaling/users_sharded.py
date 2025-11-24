#!/usr/bin/env python3

"""Sharded top clients worker that aggregates purchase quantities per branch."""

import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict

from message_utils import ClientId # pyright: ignore[reportMissingImports]
from worker_utils import run_main, safe_int_conversion # pyright: ignore[reportMissingImports]
from workers.local_top_scaling.aggregator_worker import AggregatorWorker
from workers.utils.sharding_utils import get_routing_key, extract_store_id_from_payload
from workers.utils.state_manager import UsersStateManager

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

        self.top_n = safe_int_conversion(os.getenv('TOP_USERS_COUNT'), minimum=1, default=3)

        # Configure state manager
        state_path_env = os.getenv('STATE_FILE')
        state_dir_env = os.getenv('STATE_DIR')
        
        state_path = None
        state_dir = None
        
        if state_path_env:
            state_path = Path(state_path_env)
        elif state_dir_env:
            state_dir = Path(state_dir_env)

        self.state_manager = UsersStateManager(
            state_data=None,
            state_path=state_path,
            state_dir=state_dir,
            worker_id=str(self.worker_id)
        )
        
        from collections import defaultdict
        state_data = self.state_manager.state_data
        if state_data is None or not isinstance(state_data, defaultdict):
            state_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
            self.state_manager.state_data = state_data
        
        self.clients_data = self.state_manager.state_data
        
        # Track clients that have already received EOF to reject batches that arrive after EOF
        self._clients_with_eof: set[ClientId] = set()

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
        
        #logger.info(f"Processing transaction for store_id={store_id}")
        self.clients_data[client_id][store_id][user_id] += 1

    def create_payload(self, client_id: str) -> list[Dict[str, Any]]:
        counts_for_client = self.clients_data.pop(client_id, {})
        results: list[Dict[str, Any]] = []

        # For each store, get the top N users by purchase quantity
        for store_id, user_counts in counts_for_client.items():
            # Sort users by purchase quantity (descending) and take top N
            sorted_users = sorted(
                user_counts.items(), 
                key=lambda x: x[1],  # Sort by purchase quantity
                reverse=True
            )[: self.top_n]  # Take only top-N
            
            for user_id, purchases_qty in sorted_users:
                results.append(
                    {
                        'store_id': store_id,
                        'user_id': user_id,
                        'purchases_qty': purchases_qty,
                    }
                )

        logger.info(
            "ShardedClientsWorker %s: Sending %s top %s local results for client %s",
            self.worker_id,
            len(results),
            self.top_n,
            client_id,
        )
        return results

    def process_batch(self, batch: list[Dict[str, Any]], client_id: ClientId):
        if self.shutdown_requested:
            logger.info("[BATCH-FLOW] Shutdown requested, rejecting batch to requeue")
            raise InterruptedError("Shutdown requested before batch processing")
        
        # Reject batches that arrive after EOF for this client
        if client_id in self._clients_with_eof:
            message_uuid = self._get_current_message_uuid()
            logger.info(
                "[PROCESSING - BATCH] [PROTOCOL-ERROR] Batch %s for client %s arrived AFTER EOF (protocol violation). "
                "Discarding batch to prevent incorrect processing.",
                message_uuid,
                client_id,
            )
            raise Exception(f"Batch arrived after EOF for client {client_id} (protocol error), discarding")
            
        message_uuid = self._get_current_message_uuid()

        if message_uuid and self.state_manager.get_last_processed_message(client_id) == message_uuid:
            logger.info(
                "[PROCESSING - BATCH] [DUPLICATE] Skipping duplicate batch %s for client %s (already processed)",
                message_uuid,
                client_id,
            )
            return

        with self._state_lock:
            previous_state = self.state_manager.clone_client_state(client_id)
            previous_uuid = self.state_manager.get_last_processed_message(client_id)

            try:
                for idx, entry in enumerate(batch):
                    if self.shutdown_requested:
                        logger.info("[PROCESSING - BATCH] [INTERRUPT] Shutdown requested during batch processing, rolling back")
                        raise InterruptedError("Shutdown requested during batch processing")

                    self.accumulate_transaction(client_id, entry)

                logger.info(f"All {len(batch)} transactions accumulated successfully")

                if message_uuid:
                    self.state_manager.set_last_processed_message(client_id, message_uuid)

                if not self.shutdown_requested:
                    logger.info(f"[BATCH-FLOW] [PERSIST] About to persist state for client {client_id}, message_uuid={message_uuid}")
                    self.state_manager.persist_state(client_id)  # Persist only this client
                    logger.info(f"[BATCH-FLOW] [PERSIST] State persisted successfully for client {client_id}, message_uuid={message_uuid}")
                else:
                    logger.info("[PROCESSING - BATCH] [INTERRUPT] Shutdown requested after batch processing, rolling back state")
                    raise InterruptedError("Shutdown requested, preventing state persistence")
                
                logger.info(f"[BATCH-FLOW] [SUCCESS] Batch processing completed successfully for client {client_id}, message_uuid={message_uuid}. Returning from process_batch.")
            except Exception as e:
                logger.error(f"[BATCH-FLOW] [ERROR] Exception during batch processing: {type(e).__name__}: {e}")
                logger.info("[BATCH-FLOW] [ROLLBACK] Restoring previous state and UUID")
                self.state_manager.restore_client_state(client_id, previous_state)
                if message_uuid:
                    if previous_uuid is None:
                        logger.info("[BATCH-FLOW] [ROLLBACK] Clearing last_processed_message (was None)")
                        self.state_manager.clear_last_processed_message(client_id)
                    else:
                        logger.info(f"[BATCH-FLOW] [ROLLBACK] Restoring previous UUID: {previous_uuid}")
                        self.state_manager.set_last_processed_message(client_id, previous_uuid)
                raise

    def handle_eof(self, message: Dict[str, Any], client_id: ClientId):
        """
        Handle EOF by sending data to aggregator and then sending EOF directly to output.
        
        Sharded workers do NOT use the consensus mechanism (requeue) between them.
        Each sharded worker sends EOF directly to the Top Clients aggregator after sending its data.
        The aggregator will wait for EOFs from all sharded workers (using REPLICA_COUNT).
        """
        if self.shutdown_requested:
            logger.info("Shutdown requested, rejecting EOF to requeue")
            raise InterruptedError("Shutdown requested before EOF handling")
        
        # Mark this client as having received EOF
        with self._state_lock:
            self._clients_with_eof.add(client_id)
            logger.info(f"[EOF] Marking client {client_id} as having received EOF. Future batches for this client will be rejected.")
        
        logger.info(f"[EOF] [SHARDED-CLIENTS-WORKER] Received EOF for client {client_id}")
        
        with self._pause_message_processing():
            try:
                # Send data to aggregator
                payload_batches: list[list[Dict[str, Any]]] = []
                with self._state_lock:
                    payload = self.create_payload(client_id)
                    if payload:
                        self.reset_state(client_id)
                        if self.chunk_payload:
                            payload_batches = [payload]
                        else:
                            payload_batches = self._chunk_payload(payload, self.chunk_size)

                for chunk in payload_batches:
                    self.send_payload(chunk, client_id)
                
                # Send EOF directly to output WITHOUT consensus mechanism
                logger.info(f"[EOF] [SHARDED-CLIENTS-WORKER] Sending EOF directly to Top Clients aggregator for client {client_id} (no consensus)")
                self.eof_handler.output_eof(client_id=client_id)
                
            except Exception:
                raise

            with self._state_lock:
                previous_state = self.state_manager.clone_client_state(client_id)
                previous_uuid = self.state_manager.get_last_processed_message(client_id)
                message_uuid = self._get_current_message_uuid()
                try:
                    if message_uuid:
                        self.state_manager.set_last_processed_message(client_id, message_uuid)
                    else:
                        logger.warning(f"EOF message for client {client_id} has no UUID; clearing last processed message")
                        self.state_manager.clear_last_processed_message(client_id)

                    self.state_manager.drop_empty_client_state(client_id)
                    
                    if not self.shutdown_requested:
                        self.state_manager.persist_state(client_id)  # Persist only this client
                    else:
                        logger.info("Shutdown requested during EOF handling, skipping state persistence")
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

    def handle_client_reset(self, client_id: ClientId) -> None:
        """Drop any partial users state for a disconnected client."""
        with self._state_lock:
            self.clients_data.pop(client_id, None)
            self._clients_with_eof.discard(client_id)
            self.state_manager.clear_last_processed_message(client_id)
            self.state_manager.drop_empty_client_state(client_id)
            self.state_manager.remove_client_files(client_id)
            self.state_manager.persist_state(client_id)
        logger.info("[CONTROL] Cleared Users state for client %s", client_id)

    def handle_reset_all_clients(self) -> None:
        """Drop all users state across clients."""
        with self._state_lock:
            self.clients_data.clear()
            self._clients_with_eof.clear()
            self.state_manager._last_processed_message.clear()
            self.state_manager.remove_all_client_files()
            self.state_manager.persist_state()
        logger.info("[CONTROL] Cleared Users state for all clients")


if __name__ == '__main__':
    run_main(ShardedClientsWorker)
