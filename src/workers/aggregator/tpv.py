from collections import defaultdict
import logging
import os
import threading
from typing import Any, DefaultDict, Dict, List, Optional
from message_utils import ClientId # pyright: ignore[reportMissingImports]
from workers.sharded_process.process_worker import ProcessWorker
from workers.sharded_process.tpv import StoreId, YearHalf
from workers.metadata_store.stores import StoresMetadataStore
from workers.utils.processed_message_store import ProcessedMessageStore
from workers.utils.eof_counter_store import EOFCounterStore
from workers.utils.aggregator_state_store import AggregatorStateStore
from workers.utils.metadata_eof_state_store import MetadataEOFStateStore
from worker_utils import normalize_tpv_entry, safe_int_conversion, tpv_sort_key, run_main # pyright: ignore[reportMissingImports]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TPV (Total Payment Value) per each semester during 2024 and 2025, per branch, created between 06:00 AM and 11:00 PM.

class TPVAggregator(ProcessWorker): 
    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload = False
        
        # Metadata EOF tracking: Track EOFs for stores
        self.metadata_eof_state = MetadataEOFStateStore(required_metadata_types={'stores'})
        
        # Create metadata store with EOF state tracking
        self.stores_source = StoresMetadataStore(self.middleware_config, eof_state_store=self.metadata_eof_state)
        self.stores_source.start_consuming()
        
        # Number of sharded workers we expect EOFs from (must be set before loading state)
        self.expected_eof_count = int(os.getenv('REPLICA_COUNT', '2'))
        
        # Fault tolerance: Track processed message UUIDs
        self.processed_messages = ProcessedMessageStore(worker_label="tpv_aggregator")
        
        # Fault tolerance: Track EOF counters with deduplication
        self.eof_counter_store = EOFCounterStore(worker_label="tpv_aggregator")
        
        # Fault tolerance: Persist intermediate aggregation state
        self.state_store = AggregatorStateStore(worker_label="tpv_aggregator")
        
        # Load persisted state on startup
        self.recieved_payloads: DefaultDict[
            ClientId, DefaultDict[YearHalf, DefaultDict[StoreId, float]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        self._load_persisted_state()
        
    
    def _load_persisted_state(self) -> None:
        """Load persisted aggregation state for all clients on startup."""
        logger.info("[TPV-AGGREGATOR] Loading persisted aggregation state...")
        
        for client_id in list(self.state_store._cache.keys()):
            eof_count = self.eof_counter_store.get_counter(client_id)
            if eof_count >= self.expected_eof_count:
                logger.info(
                    f"[TPV-AGGREGATOR] Client {client_id} already completed "
                    f"(EOF count: {eof_count}), skipping state restoration "
                    f"(will clean up after sending results)"
                )
                continue
            
            # Client hasn't completed, restore state
            state = self.state_store.get_state(client_id)
            if 'recieved_payloads' in state:
                # Convert back from dict to nested defaultdict structure
                for year_half, stores in state['recieved_payloads'].items():
                    for store_id, tpv_value in stores.items():
                        self.recieved_payloads[client_id][year_half][int(store_id)] = float(tpv_value)
                
                logger.info(
                    f"[TPV-AGGREGATOR] Restored aggregation state for client {client_id} "
                    f"(EOF count: {eof_count}/{self.expected_eof_count})"
                )
    
    def _load_persisted_eof_counters(self) -> None:
        """Load persisted EOF counters for all clients on startup."""
        logger.info("[TPV-AGGREGATOR] EOF counters will be loaded from persistence on first access")

    def _clear_client_state(self, client_id: ClientId) -> None:
        """Clear all state and persistence for a client."""
        with self._state_lock:
            self.recieved_payloads.pop(client_id, None)

        self.stores_source.reset_state(client_id)
        self.state_store.clear_client(client_id)
        self.metadata_eof_state.clear_client(client_id)
        self.processed_messages.clear_client(client_id)
        self.eof_counter_store.clear_client(client_id)

    def _clear_all_state(self) -> None:
        """Clear all state and persistence for every client."""
        with self._state_lock:
            self.recieved_payloads.clear()

        self.stores_source.reset_all()
        self.state_store.clear_all()
        self.metadata_eof_state.clear_all()
        self.processed_messages.clear_all()
        self.eof_counter_store.clear_all()

    def reset_state(self, client_id: ClientId) -> None:
        self._clear_client_state(client_id)

    def process_transaction(self, client_id: ClientId, payload: Dict[str, Any]) -> None:
        """Accumulate data from a single transaction payload."""
        store_id: StoreId = safe_int_conversion(payload.get("store_id"), minimum=0)
        year_half: YearHalf = payload.get("year_half_created_at", "Unknown")
        tpv: float = float(payload.get("tpv", 0.0))

        client_payloads = self.recieved_payloads.setdefault(client_id, defaultdict(lambda: defaultdict(float)))
        client_payloads[year_half][store_id] += tpv
    
    def process_batch(self, batch: list[Dict[str, Any]], client_id: ClientId):
        """Process a batch with deduplication and metadata readiness check."""
        # Check if all metadata EOFs have been received for this client
        metadata_state = self.metadata_eof_state.get_metadata_state(client_id)
        if not self.metadata_eof_state.are_all_metadata_done(client_id):
            # Not all metadata EOFs received yet, reject and requeue the message
            logger.info(
                    f"\033[91m[TPV-AGGREGATOR] Not all metadata EOFs received for client {client_id}, "
                    f"requeuing transaction batch. Required: {self.metadata_eof_state.required_metadata_types}, "
                    f"Current state: {metadata_state}\033[0m"
            )
            raise InterruptedError(
                f"\033[91mMetadata not ready for client {client_id}, message will be requeued\033[0m"
            )
        
        message_uuid = self._get_current_message_uuid()
        
        # Check for duplicate message
        if message_uuid and self.processed_messages.has_processed(client_id, message_uuid):
            logger.info(
                f"[TPV-AGGREGATOR] [DUPLICATE] Skipping duplicate batch {message_uuid} "
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
    
    def _persist_state(self, client_id: ClientId) -> None:
        """Persist the current aggregation state for a client."""
        payloads_data = {}
        for year_half, stores in self.recieved_payloads.get(client_id, {}).items():
            payloads_data[year_half] = {str(k): v for k, v in stores.items()}
        
        state_data = {
            'recieved_payloads': payloads_data
        }
        self.state_store.save_state(client_id, state_data)

    def get_store_name(self, client_id: ClientId, store_id: StoreId) -> str:
        return self.stores_source.get_item_when_done(client_id, str(store_id))

    def create_payload(self, client_id: ClientId) -> List[Dict[str, Any]]:
        client_payloads = self.recieved_payloads.pop(client_id, {})
        results: List[Dict[str, Any]] = []

        for year_half, stores in client_payloads.items():
            for store_id, tpv_value in stores.items():
                store_name = self.get_store_name(client_id, store_id)
                entry = normalize_tpv_entry(
                    {
                        "year_half_created_at": year_half,
                        "store_id": store_id,
                        "tpv": tpv_value,
                        "store_name": store_name,
                    }
                )
                results.append(entry)

        results.sort(key=tpv_sort_key)
        return results

    def gateway_type_metadata(self) -> dict:
        return {
            "list_type": "TPV_SUMMARY",
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
                f"\033[91m[TPV-AGGREGATOR] [DUPLICATE-EOF] Skipping duplicate EOF {message_uuid} "
                f"for client {client_id} (already processed)\033[0m"
                f"for client {client_id} (already processed)"
            )
            return
                
        logger.info(f"[TPV-AGGREGATOR] Received EOF for client {client_id} (UUID: {message_uuid})")
        
        # Mark EOF as processed and increment counter (persisted atomically)
        if message_uuid:
            self.eof_counter_store.mark_processed(client_id, message_uuid)
        
        # Increment EOF counter for this client (persisted)
        eof_count = self.eof_counter_store.increment_counter(client_id)
        
        logger.info(
            f"[TPV-AGGREGATOR] EOF count for client {client_id}: {eof_count}/{self.expected_eof_count}"
        )
        
        with self._pause_message_processing():
            if eof_count >= self.expected_eof_count:
                logger.info(
                    f"[TPV-AGGREGATOR] All EOFs received for client {client_id} "
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
                    f"[TPV-AGGREGATOR] Not all EOFs received yet for client {client_id} "
                    f"({eof_count}/{self.expected_eof_count}). "
                    f"Waiting for more EOFs..."
                )

    def _get_current_message_uuid(self) -> Optional[str]:
        """Get the UUID of the message currently being processed."""
        metadata = self._get_current_message_metadata()
        if metadata:
            return metadata.get('message_uuid')
        return None
    
    def cleanup(self) -> None:
        try:
            self.stores_source.close()
        finally:
            super().cleanup()

    def handle_client_reset(self, client_id: ClientId) -> None:
        """Delete the client's state when a reset control message arrives."""
        self._clear_client_state(client_id)

    def handle_reset_all_clients(self) -> None:
        """Delete all clients' state when a global reset control message arrives."""
        self._clear_all_state()


if __name__ == "__main__":
    run_main(TPVAggregator)
