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
from worker_utils import normalize_tpv_entry, safe_int_conversion, tpv_sort_key, run_main # pyright: ignore[reportMissingImports]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TPV (Total Payment Value) per each semester during 2024 and 2025, per branch, created between 06:00 AM and 11:00 PM.

class TPVAggregator(ProcessWorker): 
    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload = False
        
        self.stores_source = StoresMetadataStore(self.middleware_config)
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
        
        # Track how many EOFs we've received from sharded workers per client
        # Load persisted EOF counters on startup
        self.eof_count_per_client: Dict[ClientId, int] = {}
        self._load_persisted_eof_counters()
        
        self.eof_count_lock = threading.Lock()
    
    def _load_persisted_state(self) -> None:
        """Load persisted aggregation state for all clients on startup."""
        logger.info("[TPV-AGGREGATOR] Loading persisted aggregation state...")
        
        # Restore recieved_payloads from persisted state
        # BUT: Only restore if the client hasn't completed yet (EOF count < expected)
        for client_id in list(self.state_store._cache.keys()):
            # Check if client already completed (should not restore state if completed)
            eof_count = self.eof_counter_store.get_counter(client_id)
            if eof_count >= self.expected_eof_count:
                # Client already completed, don't restore state
                # BUT: Don't clean up yet - we need to keep EOF UUIDs for deduplication
                # The state will be cleaned up when we actually process and send results
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

    def reset_state(self, client_id: ClientId) -> None:
        """Reset state for a client. 
        
        NOTE: This is called after sending final results. We keep EOF counter
        and processed UUIDs to handle duplicate EOFs that may arrive later.
        """
        try:
            del self.recieved_payloads[client_id]
        except KeyError:
            pass
        #self.stores_source.reset_state(client_id)
        # Clear processed messages and aggregation state
        self.processed_messages.clear_client(client_id)
        self.state_store.clear_client(client_id)
        # NOTE: We DON'T clear eof_counter_store here to keep UUIDs for deduplication
        # The EOF counter will be cleared when the client disconnects or after a timeout

    def process_transaction(self, client_id: ClientId, payload: Dict[str, Any]) -> None:
        """Accumulate data from a single transaction payload."""
        store_id: StoreId = safe_int_conversion(payload.get("store_id"), minimum=0)
        year_half: YearHalf = payload.get("year_half_created_at", "Unknown")
        tpv: float = float(payload.get("tpv", 0.0))

        client_payloads = self.recieved_payloads.setdefault(client_id, defaultdict(lambda: defaultdict(float)))
        client_payloads[year_half][store_id] += tpv
    
    def process_batch(self, batch: list[Dict[str, Any]], client_id: ClientId):
        """Process a batch with deduplication."""
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
        # Convert nested defaultdicts to regular dicts for JSON serialization
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
        # Inject store names into the payload
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
                f"[TPV-AGGREGATOR] [DUPLICATE-EOF] Skipping duplicate EOF {message_uuid} "
                f"for client {client_id} (already processed)"
            )
            return
        
        # Check if client already completed (before processing this EOF)
        current_eof_count = self.eof_counter_store.get_counter(client_id)
        if current_eof_count >= self.expected_eof_count:
            # Client already completed, ignore this EOF (may be a duplicate or retry)
            logger.info(
                f"[TPV-AGGREGATOR] [ALREADY-COMPLETED] Ignoring EOF for client {client_id} "
                f"(already completed with {current_eof_count} EOFs). "
                f"UUID: {message_uuid}"
            )
            # Mark as processed to prevent future duplicates
            if message_uuid:
                self.eof_counter_store.mark_processed(client_id, message_uuid)
            return
        
        logger.info(f"[TPV-AGGREGATOR] Received EOF for client {client_id} (UUID: {message_uuid})")
        
        # Mark EOF as processed and increment counter (persisted atomically)
        if message_uuid:
            self.eof_counter_store.mark_processed(client_id, message_uuid)
        
        # Increment EOF counter for this client (persisted)
        with self.eof_count_lock:
            eof_count = self.eof_counter_store.increment_counter(client_id)
            self.eof_count_per_client[client_id] = eof_count
        
        logger.info(
            f"[TPV-AGGREGATOR] EOF count for client {client_id}: {eof_count}/{self.expected_eof_count}"
        )
        
        with self._pause_message_processing():
            if eof_count >= self.expected_eof_count:
                # We've received EOFs from all sharded workers
                # Now send the final aggregated results
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
                
                # Clean up EOF counter for this client (but keep processed UUIDs in memory for a bit)
                with self.eof_count_lock:
                    if client_id in self.eof_count_per_client:
                        del self.eof_count_per_client[client_id]
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


if __name__ == "__main__":
    run_main(TPVAggregator)
