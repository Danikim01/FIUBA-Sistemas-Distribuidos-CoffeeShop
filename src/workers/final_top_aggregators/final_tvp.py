from collections import defaultdict
import logging
import os
import threading
from typing import Any, DefaultDict, Dict, List
from pathlib import Path
from message_utils import ClientId # pyright: ignore[reportMissingImports]
from workers.local_top_scaling.aggregator_worker import AggregatorWorker
from workers.local_top_scaling.tpv_sharded import StoreId, YearHalf
from workers.extra_source.stores import StoresExtraSource
from worker_utils import normalize_tpv_entry, safe_int_conversion, tpv_sort_key, run_main # pyright: ignore[reportMissingImports]
from workers.state_manager.tpv_state_manager import TPVStateManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TPV (Total Payment Value) per each semester during 2024 and 2025, per branch, created between 06:00 AM and 11:00 PM.

class TPVAggregator(AggregatorWorker): 
    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload = False
        
        self.stores_source = StoresExtraSource(self.middleware_config)
        self.stores_source.start_consuming()

        state_path_env = os.getenv("STATE_FILE")
        state_dir_env = os.getenv("STATE_DIR")

        state_path = Path(state_path_env) if state_path_env else None
        state_dir = Path(state_dir_env) if state_dir_env else None

        self.state_manager = TPVStateManager(
            state_data=None,
            state_path=state_path,
            state_dir=state_dir,
            worker_id=os.getenv("WORKER_ID", "0"),
        )
        if self.state_manager.state_data is None:
            self.state_manager.update_state_data(defaultdict(lambda: defaultdict(lambda: defaultdict(float))))
        
        self.recieved_payloads = self.state_manager.state_data
        
        # Track how many EOFs we've received from sharded workers per client
        # This is simpler than using the consensus mechanism since each sharded worker
        # sends its own EOF directly (not the same EOF passing through all workers)
        self.eof_count_per_client: Dict[ClientId, int] = {}
        self.eof_count_lock = threading.Lock()
        
        # Number of sharded workers we expect EOFs from
        self.expected_eof_count = int(os.getenv('REPLICA_COUNT', '2'))

    def reset_state(self, client_id: ClientId) -> None:
        try:
            del self.recieved_payloads[client_id]
        except KeyError:
            pass
        self.stores_source.reset_state(client_id)

    def _process_entries(self, entries: list[Dict[str, Any]], client_id: ClientId) -> None:
        if self.shutdown_requested:
            logger.info("[TPV-AGGREGATOR] Shutdown requested, rejecting batch to requeue")
            raise InterruptedError("Shutdown requested before batch processing")

        message_uuid = self._get_current_message_uuid()
        if message_uuid and self.state_manager.get_last_processed_message(client_id) == message_uuid:
            logger.info(
                "[TPV-AGGREGATOR] [DUPLICATE] Skipping duplicate batch %s for client %s (already processed)",
                message_uuid,
                client_id,
            )
            return

        with self._state_lock:
            previous_state = self.state_manager.clone_client_state(client_id)
            previous_uuid = self.state_manager.get_last_processed_message(client_id)

            try:
                for entry in entries:
                    if self.shutdown_requested:
                        logger.info("[TPV-AGGREGATOR] [INTERRUPT] Shutdown requested during batch processing")
                        raise InterruptedError("Shutdown requested during batch processing")
                    self.accumulate_transaction(client_id, entry)

                if message_uuid:
                    self.state_manager.set_last_processed_message(client_id, message_uuid)

                if not self.shutdown_requested:
                    self.state_manager.persist_state(client_id)
                else:
                    raise InterruptedError("Shutdown requested, preventing state persistence")
            except Exception:
                self.state_manager.restore_client_state(client_id, previous_state)
                if message_uuid:
                    if previous_uuid is None:
                        self.state_manager.clear_last_processed_message(client_id)
                    else:
                        self.state_manager.set_last_processed_message(client_id, previous_uuid)
                raise

    def process_batch(self, batch: list[Dict[str, Any]], client_id: ClientId):
        self._process_entries(batch, client_id)

    def process_message(self, message: dict, client_id: ClientId):
        self._process_entries([message], client_id)

    def accumulate_transaction(self, client_id: ClientId, payload: Dict[str, Any]) -> None:
        """Accumulate data from a single transaction payload."""
        store_id: StoreId = safe_int_conversion(payload.get("store_id"), minimum=0)
        year_half: YearHalf = payload.get("year_half_created_at", "Unknown")
        tpv: float = float(payload.get("tpv", 0.0))

        client_payloads = self.recieved_payloads.setdefault(client_id, defaultdict(lambda: defaultdict(float)))
        client_payloads[year_half][store_id] += tpv

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

    def handle_eof(self, message: Dict[str, Any], client_id: ClientId):
        """
        Handle EOF from sharded workers.
        
        This aggregator waits for EOFs from ALL sharded workers before sending
        the final results. The mechanism works as follows:
        1. Each sharded worker sends EOF directly to this aggregator
        2. This aggregator accumulates data from all sharded workers
        3. We track how many EOFs we've received per client
        4. Only when ALL EOFs are received (expected_eof_count), we send the
           final aggregated results
        """
        logger.info(f"[TPV-AGGREGATOR] Received EOF for client {client_id}")

        message_uuid = self._get_current_message_uuid()

        if message_uuid and self.state_manager.get_last_processed_message(client_id) == message_uuid:
            logger.info(
                "[TPV-AGGREGATOR] [DUPLICATE] Skipping duplicate EOF %s for client %s (already processed)",
                message_uuid,
                client_id,
            )
            return
        
        # Increment EOF counter for this client
        with self.eof_count_lock:
            self.eof_count_per_client[client_id] = self.eof_count_per_client.get(client_id, 0) + 1
            eof_count = self.eof_count_per_client[client_id]
        
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

                with self._state_lock:
                    previous_state = self.state_manager.clone_client_state(client_id)
                    previous_uuid = self.state_manager.get_last_processed_message(client_id)
                    try:
                        if message_uuid:
                            self.state_manager.set_last_processed_message(client_id, message_uuid)
                        else:
                            self.state_manager.clear_last_processed_message(client_id)

                        self.reset_state(client_id)
                        self.state_manager.drop_empty_client_state(client_id)
                        self.state_manager.persist_state(client_id)
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
                
                # Clean up EOF counter for this client
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

    def cleanup(self) -> None:
        try:
            self.stores_source.close()
        finally:
            super().cleanup()


if __name__ == "__main__":
    run_main(TPVAggregator)
