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
from workers.utils.metadata_eof_state_store import MetadataEOFStateStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Birthday date of the 3 customers who have made the most purchases for each branch

class TopClientsAggregator(ProcessWorker):
    """Aggregates top-client partials and injects client birthdays."""

    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload = False
        
        self.top_n = safe_int_conversion(os.getenv('TOP_USERS_COUNT'), default=3)

        # Metadata EOF tracking: Track EOFs for users and stores
        self.metadata_eof_state = MetadataEOFStateStore(required_metadata_types={'users', 'stores'})
        
        # Create metadata stores with EOF state tracking
        self.stores_source = StoresMetadataStore(self.middleware_config, eof_state_store=self.metadata_eof_state)
        self.stores_source.start_consuming()
        self.birthdays_source = UsersMetadataStore(self.middleware_config, eof_state_store=self.metadata_eof_state)
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
        
    def _load_persisted_state(self) -> None:
        """Load persisted aggregation state for all clients on startup."""
        
        for client_id in list(self.state_store._cache.keys()):
            eof_count = self.eof_counter_store.get_counter(client_id)
            if eof_count >= self.expected_eof_count:
                # Client already completed, don't restore state
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
    
    def reset_state(self, client_id: ClientId) -> None:
        """Reset state for a client. 
        """
        self._clear_client_state(client_id)

    def _clear_client_state(self, client_id: ClientId) -> None:
        with self._state_lock:
            self.recieved_payloads.pop(client_id, None)

        self.stores_source.reset_state(client_id)
        self.birthdays_source.reset_state(client_id)
        self.metadata_eof_state.clear_client(client_id)
        self.state_store.clear_client(client_id)
        self.processed_messages.clear_client(client_id)
        self.eof_counter_store.clear_client(client_id)

    def _clear_all_state(self) -> None:
        with self._state_lock:
            self.recieved_payloads.clear()

        self.stores_source.reset_all()
        self.birthdays_source.reset_all()
        self.metadata_eof_state.clear_all()
        self.state_store.clear_all()
        self.processed_messages.clear_all()
        self.eof_counter_store.clear_all()

    
    def process_transaction(self, client_id: str, payload: dict[str, Any]) -> None:
        self.recieved_payloads.setdefault(client_id, []).append(payload)
    
    def _persist_state(self, client_id: ClientId) -> None:
        """Persist the current aggregation state for a client."""
        state_data = {
            'recieved_payloads': self.recieved_payloads.get(client_id, [])
        }
        self.state_store.save_state(client_id, state_data)
    
    def process_batch(self, batch: list[dict[str, Any]], client_id: ClientId):
        """Process a batch with deduplication and metadata readiness check."""
        # Check if all metadata EOFs have been received for this client
        if not self.metadata_eof_state.are_all_metadata_done(client_id):
            # Not all metadata EOFs received yet, reject and requeue the message
            logger.info(
                f"\033[91m[TOP-CLIENTS-AGGREGATOR] Not all metadata EOFs received for client {client_id}, "
                f"requeuing transaction batch\033[0m"
            )
            raise InterruptedError(
                f"\033[91m[TOP-CLIENTS-AGGREGATOR] Metadata not ready for client {client_id}, message will be requeued\033[0m"
            )
        
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
                f"\033[92m[TOP-CLIENTS-AGGREGATOR] [DUPLICATE-EOF] Skipping duplicate EOF {message_uuid} "
                f"for client {client_id} (already processed)\033[0m"
            )
            return
                
        logger.info(f"\033[92m[TOP-CLIENTS-AGGREGATOR] Received EOF for client {client_id} (UUID: {message_uuid})\033[0m")
        
        # Mark EOF as processed and increment counter (persisted atomically)
        if message_uuid:
            self.eof_counter_store.mark_processed(client_id, message_uuid)
        
        eof_count = self.eof_counter_store.increment_counter(client_id)
        
        logger.info(
            f"[TOP-CLIENTS-AGGREGATOR] EOF count for client {client_id}: {eof_count}/{self.expected_eof_count}"
        )
        
        with self._pause_message_processing():
            if eof_count >= self.expected_eof_count:
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

                self._clear_client_state(client_id)
                
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

    def handle_client_reset(self, client_id: ClientId) -> None:
        """Handle reset instructions for a specific client."""
        logger.info("[CONTROL] TopClientsAggregator received client reset for %s", client_id)
        self._clear_client_state(client_id)

    def handle_reset_all_clients(self) -> None:
        """Handle reset instructions for all clients."""
        logger.info("[CONTROL] TopClientsAggregator received global reset request")
        self._clear_all_state()


if __name__ == '__main__':
    run_main(TopClientsAggregator)
