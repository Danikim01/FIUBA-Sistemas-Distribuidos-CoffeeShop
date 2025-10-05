from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Tuple
from message_utils import ClientId
from workers.aggregator.extra_source.stores import StoresExtraSource
from workers.top.top_worker import TopWorker
from worker_utils import normalize_tpv_entry, safe_int_conversion, tpv_sort_key, run_main
from workers.top.tpv import StoreId, YearHalf

# TPV (Total Payment Value) per each semester during 2024 and 2025, per branch, created between 06:00 AM and 11:00 PM.

class TPVAggregator(TopWorker): 
    def __init__(self) -> None:
        super().__init__()
        self.stores_source = StoresExtraSource(self.middleware_config)
        self.stores_source.start_consuming()
        
        self.recieved_payloads: DefaultDict[
            ClientId, DefaultDict[YearHalf, DefaultDict[StoreId, float]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

    def reset_state(self, client_id: ClientId) -> None:
        self.recieved_payloads[client_id] = defaultdict(lambda: defaultdict(float))

    def accumulate_transaction(self, client_id: ClientId, payload: Dict[str, Any]) -> None:
        """Accumulate data from a single transaction payload."""
        store_id: StoreId = safe_int_conversion(payload.get("store_id"), minimum=0)
        year_half: YearHalf = payload.get("year_half_created_at", "Unknown")
        tpv: float = float(payload.get("tpv", 0.0))
        self.recieved_payloads[client_id][year_half][store_id] += tpv

    def get_store_name(self, client_id: ClientId, store_id: StoreId) -> str:
        return self.stores_source.get_item_when_done(client_id, str(store_id))

    def create_payload(self, client_id: ClientId) -> List[Dict[str, Any]]:
        # Inject store names into the payload
        client_payloads = self.recieved_payloads.get(client_id, {})
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

        results.sort(key=tpv_sort_key, reverse=True)
        return results

    def gateway_type_metadata(self) -> dict:
        return {
            "list_type": "TPV_SUMMARY",
        }

    def cleanup(self) -> None:
        try:
            self.stores_source.close()
        finally:
            super().cleanup()


if __name__ == "__main__":
    run_main(TPVAggregator)
