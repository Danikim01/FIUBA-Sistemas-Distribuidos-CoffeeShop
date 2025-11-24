#!/usr/bin/env python3

"""Aggregator that enriches top clients with birthdays and re-ranks globally."""

import logging
import os
import threading
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional
from message_utils import ClientId # pyright: ignore[reportMissingImports]
from worker_utils import run_main, safe_int_conversion # pyright: ignore[reportMissingImports]
from workers.sharded_process.process_worker import ProcessWorker
from workers.metadata_store.users import UsersMetadataStore
from workers.metadata_store.stores import StoresMetadataStore
from workers.utils.processed_message_store import ProcessedMessageStore
from workers.utils.eof_counter_store import EOFCounterStore
from workers.utils.aggregator_state_store import AggregatorStateStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Birthday date of the 3 customers who have made the most purchases for each branch

class TopClientsAggregator(ProcessWorker):
    """Aggregates top-client partials and injects client birthdays."""

    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload = False
        
        self.top_n = safe_int_conversion(os.getenv('TOP_USERS_COUNT'), default=3)

        self.stores_source = StoresMetadataStore(self.middleware_config)
        self.stores_source.start_consuming()
        self.birthdays_source = UsersMetadataStore(self.middleware_config)
        self.birthdays_source.start_consuming()
        
        # Number of sharded workers we expect EOFs from (must be set before loading state)
        self.expected_eof_count = int(os.getenv('REPLICA_COUNT', '2'))
        
        # Fault tolerance: Track processed message UUIDs
        self.processed_messages = ProcessedMessageStore(worker_label="top_clients_aggregator")
        
        # Fault tolerance: Track EOF counters with deduplication
        self.eof_counter_store = EOFCounterStore(worker_label="top_clients_aggregator")
        
        # Fault tolerance: Persist intermediate aggregation state
        self.state_store = AggregatorStateStore(worker_label="top_clients_aggregator")
        
        # Load persisted state on startup
        self.recieved_payloads: Dict[ClientId, list[dict[str, Any]]] = {}
        self._load_persisted_state()
        
        # Track how many EOFs we've received from sharded workers per client
        # Load persisted EOF counters on startup
        self.eof_count_per_client: Dict[ClientId, int] = {}
        self._load_persisted_eof_counters()
        
        self.eof_count_lock = threading.Lock()
    
    def _load_persisted_state(self) -> None:
        """Load persisted aggregation state for all clients on startup."""
        # AggregatorStateStore already loads all clients on init automatically
        # We need to restore recieved_payloads from persisted state
        # BUT: Only restore if the client hasn't completed yet (EOF count < expected)
        logger.info("[TOP-CLIENTS-AGGREGATOR] Loading persisted aggregation state...")
        
        # Get all clients that have persisted state
        # The state_store loads all clients automatically, but we need to access them
        # to restore recieved_payloads
        for client_id in list(self.state_store._cache.keys()):
            # Check if client already completed (should not restore state if completed)
            eof_count = self.eof_counter_store.get_counter(client_id)
            if eof_count >= self.expected_eof_count:
                # Client already completed, don't restore state
                # BUT: Don't clean up yet - we need to keep EOF UUIDs for deduplication
                # The state will be cleaned up when we actually process and send results
                logger.info(
                    f"[TOP-CLIENTS-AGGREGATOR] Client {client_id} already completed "
                    f"(EOF count: {eof_count}), skipping state restoration "
                    f"(will clean up after sending results)"
                )
                continue
            
            # Client hasn't completed, restore state
            state = self.state_store.get_state(client_id)
            if 'recieved_payloads' in state:
                self.recieved_payloads[client_id] = state['recieved_payloads']
                logger.info(
                    f"[TOP-CLIENTS-AGGREGATOR] Restored {len(state['recieved_payloads'])} payloads "
                    f"for client {client_id} (EOF count: {eof_count}/{self.expected_eof_count})"
                )
    
    def _load_persisted_eof_counters(self) -> None:
        """Load persisted EOF counters for all clients on startup."""
        # EOFCounterStore already loads all clients on init automatically
        # We sync our in-memory cache with persisted counters
        # Counters are loaded lazily when accessed via get_counter(), but we can
        # pre-load them if needed. For now, they'll be loaded on first access.
        logger.info("[TOP-CLIENTS-AGGREGATOR] EOF counters will be loaded from persistence on first access")

    def reset_state(self, client_id: ClientId) -> None:
        """Reset state for a client. 
        
        NOTE: This is called after sending final results. We keep EOF counter
        and processed UUIDs to handle duplicate EOFs that may arrive later.
        """
        try:
            del self.recieved_payloads[client_id]
        except KeyError:
            pass
        # NOTE: We NEVER reset stores_source - stores metadata must persist permanently
        # self.stores_source.reset_state(client_id)  # NEVER CALL THIS
        #self.birthdays_source.reset_state(client_id)
        self.processed_messages.clear_client(client_id)
        self.state_store.clear_client(client_id)

    
    def process_transaction(self, client_id: str, payload: dict[str, Any]) -> None:
        self.recieved_payloads.setdefault(client_id, []).append(payload)
    
    def _persist_state(self, client_id: ClientId) -> None:
        """Persist the current aggregation state for a client."""
        state_data = {
            'recieved_payloads': self.recieved_payloads.get(client_id, [])
        }
        self.state_store.save_state(client_id, state_data)
    
    def process_batch(self, batch: list[dict[str, Any]], client_id: ClientId):
        """Process a batch with deduplication."""
        message_uuid = self._get_current_message_uuid()
        
        # Check for duplicate message
        if message_uuid and self.processed_messages.has_processed(client_id, message_uuid):
            logger.info(
                f"[TOP-CLIENTS-AGGREGATOR] [DUPLICATE] Skipping duplicate batch {message_uuid} "
                f"for client {client_id} (already processed)"
            )
            return
        
        # Process the batch
        with self._state_lock:
            for entry in batch:
                self.process_transaction(client_id, entry)
        
        # Persist state after processing batch (for fault tolerance)
        self._persist_state(client_id)
        
        # Mark message as processed after successful processing
        if message_uuid:
            self.processed_messages.mark_processed(client_id, message_uuid)

    def create_payload(self, client_id: ClientId) -> list[Dict[str, Any]]:
        client_payloads = self.recieved_payloads.pop(client_id, [])
        aggregated: Dict[tuple[str, int], Dict[str, Any]] = {}

        for payload in client_payloads:
            store_id = str(payload.get("store_id", "")).strip()
            try:
                user_id = int(payload.get("user_id", 0))
            except (TypeError, ValueError):
                user_id = 0

            raw_qty = payload.get("purchases_qty") or payload.get("purchase_qty") or 0
            try:
                purchase_qty = int(raw_qty)
            except (TypeError, ValueError):
                purchase_qty = 0

            if not store_id or user_id <= 0 or purchase_qty <= 0:
                continue

            key = (store_id, user_id)

            if key not in aggregated:
                aggregated[key] = {
                    "store_id": store_id,
                    "user_id": user_id,
                    "purchases_qty": 0,
                }

            aggregated[key]["purchases_qty"] += purchase_qty

        # Enrich with birthdays and store names
        grouped_results: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        for entry in aggregated.values():
            user_id = entry["user_id"]
            store_id = entry["store_id"]

            birthdate = self.birthdays_source.get_item_when_done(
                client_id,
                str(user_id),
            )
            store_name = self.stores_source.get_item_when_done(client_id, store_id)

            grouped_results[store_id].append(
                {
                    "user_id": user_id,
                    "store_id": store_id,
                    "store_name": store_name,
                    "birthdate": birthdate,
                    "purchases_qty": entry["purchases_qty"],
                }
            )

        limited_results: List[Dict[str, Any]] = []
        for store_id, entries in grouped_results.items():
            entries.sort(
                key=lambda row: -safe_int_conversion(row.get("purchases_qty"), default=0)
            )
            limited_results.extend(entries[: self.top_n])

        limited_results.sort(
            key=lambda row: (
                row.get("store_name", ""),
                row.get("birthdate", ""),
            )
        )

        return limited_results

    def gateway_type_metadata(self) -> dict:
        return {
            "list_type": "TOP_CLIENTS_BIRTHDAYS",
        }

    def handle_eof(self, message: Dict[str, Any], client_id: ClientId, message_uuid: Optional[str] = None):
        """
        Handle EOF from sharded workers with deduplication.
        
        This aggregator waits for EOFs from ALL sharded workers before sending
        the final results. The mechanism works as follows:
        1. Each sharded worker sends EOF directly to this aggregator
        2. This aggregator accumulates data from all sharded workers
        3. We track how many EOFs we've received per client (with deduplication)
        4. Only when ALL EOFs are received (expected_eof_count), we send the
           final aggregated results
        """
        # Use message_uuid from parameter or extract from message
        if not message_uuid:
            message_uuid = message.get('message_uuid')
        
        # Check for duplicate EOF first
        if message_uuid and self.eof_counter_store.has_processed(client_id, message_uuid):
            logger.info(
                f"[TOP-CLIENTS-AGGREGATOR] [DUPLICATE-EOF] Skipping duplicate EOF {message_uuid} "
                f"for client {client_id} (already processed)"
            )
            return
        
        # Check if client already completed (before processing this EOF)
        current_eof_count = self.eof_counter_store.get_counter(client_id)
        if current_eof_count >= self.expected_eof_count:
            # Client already completed, ignore this EOF (may be a duplicate or retry)
            logger.info(
                f"[TOP-CLIENTS-AGGREGATOR] [ALREADY-COMPLETED] Ignoring EOF for client {client_id} "
                f"(already completed with {current_eof_count} EOFs). "
                f"UUID: {message_uuid}"
            )
            # Mark as processed to prevent future duplicates
            if message_uuid:
                self.eof_counter_store.mark_processed(client_id, message_uuid)
            return
        
        logger.info(f"[TOP-CLIENTS-AGGREGATOR] Received EOF for client {client_id} (UUID: {message_uuid})")
        
        # Mark EOF as processed and increment counter (persisted atomically)
        if message_uuid:
            self.eof_counter_store.mark_processed(client_id, message_uuid)
        
        # Increment EOF counter for this client (persisted)
        with self.eof_count_lock:
            eof_count = self.eof_counter_store.increment_counter(client_id)
            self.eof_count_per_client[client_id] = eof_count
        
        logger.info(
            f"[TOP-CLIENTS-AGGREGATOR] EOF count for client {client_id}: {eof_count}/{self.expected_eof_count}"
        )
        
        with self._pause_message_processing():
            if eof_count >= self.expected_eof_count:
                # We've received EOFs from all sharded workers
                # Now send the final aggregated results
                logger.info(
                    f"[TOP-CLIENTS-AGGREGATOR] All EOFs received for client {client_id} "
                    f"({eof_count}/{self.expected_eof_count}). "
                    f"Sending final aggregated results."
                )
                
                # Send final aggregated results
                payload_batches: list[list[Dict[str, Any]]] = []
                with self._state_lock:
                    payload = self.create_payload(client_id)
                    if payload:
                        if self.chunk_payload:
                            payload_batches = [payload]
                        else:
                            payload_batches = self._chunk_payload(payload, self.chunk_size)

                for chunk in payload_batches:
                    self.send_payload(chunk, client_id)
                
                
                # Clean up EOF counter for this client (but keep processed UUIDs in memory for a bit)
                with self.eof_count_lock:
                    if client_id in self.eof_count_per_client:
                        del self.eof_count_per_client[client_id]
            else:
                # Not all EOFs received yet, just discard this EOF
                # (we don't need requeue since each sharded worker sends its own EOF)
                logger.info(
                    f"[TOP-CLIENTS-AGGREGATOR] Not all EOFs received yet for client {client_id} "
                    f"({eof_count}/{self.expected_eof_count}). "
                    f"Waiting for more EOFs..."
                )

    def _get_current_message_uuid(self) -> Optional[str]:
        """Get the UUID of the message currently being processed."""
        metadata = self._get_current_message_metadata()
        if metadata:
            return metadata.get('message_uuid')
        return None
    
    def cleanup(self):
        super().cleanup()
        try:
            self.stores_source.close()
            self.birthdays_source.close()
        except Exception:  # noqa: BLE001
            logger.warning("Failed to close extra sources", exc_info=True)


if __name__ == '__main__':
    run_main(TopClientsAggregator)
