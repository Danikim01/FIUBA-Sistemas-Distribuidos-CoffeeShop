from collections import defaultdict
import threading
from typing import Any, Dict, List, Tuple
from message_utils import ClientId
from workers.aggregator.extra_source.stores import StoresExtraSource
from workers.top.top_worker import TopWorker
from worker_utils import run_main

class StoreData:
    def __init__(self, year_half: str, tpv: float, store_name: str = "Unknown"):
        self.year_half = year_half
        self.tpv = tpv
        self.store_name = store_name

StoreId = str

class TPVAggregator(TopWorker): 
    def __init__(self) -> None:
        super().__init__()
        self.stores_source = StoresExtraSource(self.middleware_config)
        self._stores_thread = threading.Thread(
            target=self.stores_source.start_consuming,
            name="stores-extra-source",
            daemon=True,   
        )
        self._stores_thread.start()
        self.recieved_payloads: Dict[ClientId, Dict[StoreId, List[StoreData]]] = {}

    def reset_state(self, client_id: ClientId) -> None:
        self.recieved_payloads[client_id] = {}

    def _accumulate_transaction(self, client_id: ClientId, payload: Dict[str, Any]) -> None:
        """Accumulate data from a single transaction payload."""
        store_id: StoreId = str(payload.get("store_id", ""))
        entry = StoreData(
            year_half=str(payload.get("year_half_created_at", "")),
            tpv=float(payload.get("tpv", 0.0)),
        )
        self.recieved_payloads\
            .setdefault(client_id, {})\
            .setdefault(store_id, [])\
            .append(entry)

    def _get_store_name(self, client_id: ClientId, store_id: StoreId) -> str:
        """Resolve store name from the extra source; fall back to 'Unknown'."""
        while not self.stores_source.is_done(client_id):
            pass  # Wait until the stores source is done for this client
        return self.stores_source.get_item(client_id, store_id)

    def _aggregate_payloads(
        self,
        client_id: ClientId,
        client_payloads: Dict[StoreId, List[StoreData]],
    ) -> List[Dict[str, Any]]:
        """
        Aggregate per (year_half, store_id) summing tpv, then enrich with store_name.
        Returns a list of rows sorted by (year_half, -tpv).
        """
        totals: Dict[Tuple[str, StoreId], float] = defaultdict(float)

        for store_id, entries in client_payloads.items():
            for e in entries:
                totals[(e.year_half, store_id)] += e.tpv

        # Build final rows with enrichment
        rows: List[Dict[str, Any]] = []
        for (year_half, store_id), tpv_sum in totals.items():
            rows.append({
                "year_half_created_at": year_half,
                "store_id": store_id,
                "tpv": tpv_sum,
                "store_name": self._get_store_name(client_id, store_id),
            })

        def _store_id_sort(value: str) -> tuple[int, str]:
            try:
                return (int(value), value)
            except (TypeError, ValueError):
                return (0, str(value))

        rows.sort(
            key=lambda r: (
                r["year_half_created_at"],
                (r.get("store_name") or ""),
                _store_id_sort(r.get("store_id", "")),
            )
        )
        return rows

    def create_payload(self, client_id: ClientId) -> List[Dict[str, Any]]:
        client_payloads = self.recieved_payloads.get(client_id, {})
        return self._aggregate_payloads(client_id, client_payloads)

    def cleanup(self) -> None:
        try:
            self.stores_source.close()
        finally:
            super().cleanup()


if __name__ == "__main__":
    run_main(TPVAggregator)
