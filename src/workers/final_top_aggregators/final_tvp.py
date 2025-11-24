from collections import defaultdict
import logging
import os
import threading
from typing import Any, DefaultDict, Dict, List
from message_utils import ClientId # pyright: ignore[reportMissingImports]
from workers.local_top_scaling.aggregator_worker import AggregatorWorker
from workers.local_top_scaling.tpv_sharded import StoreId, YearHalf
from workers.extra_source.stores import StoresExtraSource
from worker_utils import normalize_tpv_entry, safe_int_conversion, tpv_sort_key, run_main # pyright: ignore[reportMissingImports]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TPV (Total Payment Value) per each semester during 2024 and 2025, per branch, created between 06:00 AM and 11:00 PM.

class TPVAggregator(AggregatorWorker): 
    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload = False
        
        self.stores_source = StoresExtraSource(self.middleware_config)
        self.stores_source.start_consuming()
        
        self.recieved_payloads: DefaultDict[
            ClientId, DefaultDict[YearHalf, DefaultDict[StoreId, float]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        
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
           final aggregated results along with our own EOF
        """
        logger.info(f"[TPV-AGGREGATOR] Received EOF for client {client_id}")
        
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
                # Now send the final aggregated results along with our own EOF
                logger.info(
                    f"[TPV-AGGREGATOR] All EOFs received for client {client_id} "
                    f"({eof_count}/{self.expected_eof_count}). "
                    f"Sending final aggregated results and EOF."
                )
                
                # Send final aggregated results
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
                
                # Send our own EOF to the next worker (gateway)
                logger.info(f"[TPV-AGGREGATOR] Sending EOF to gateway for client {client_id}")
                self.eof_handler.output_eof(client_id=client_id)
                
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

    def handle_client_reset(self, client_id: ClientId) -> None:
        """Drop any partial aggregation for a disconnected client."""
        with self._state_lock:
            self.recieved_payloads.pop(client_id, None)
            with self.eof_count_lock:
                self.eof_count_per_client.pop(client_id, None)
            self.stores_source.reset_state(client_id)
        logger.info("[CONTROL] Cleared TPV aggregator state for client %s", client_id)

    def handle_reset_all_clients(self) -> None:
        """Drop all aggregation state."""
        with self._state_lock:
            self.recieved_payloads.clear()
            with self.eof_count_lock:
                self.eof_count_per_client.clear()
            self.stores_source.reset_all()
        logger.info("[CONTROL] Cleared TPV aggregator state for all clients")


if __name__ == "__main__":
    run_main(TPVAggregator)
