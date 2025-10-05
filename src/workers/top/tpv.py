#!/usr/bin/env python3

"""TPV worker that aggregates semester totals per store."""

import logging
from collections import defaultdict
from typing import Any, DefaultDict, Dict
from message_utils import ClientId
from worker_utils import extract_year_half, run_main, safe_float_conversion, safe_int_conversion
from workers.top.top_worker import TopWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

YearHalf = str
StoreId = int

# TPV (Total Payment Value) per each semester during 2024 and 2025, per branch, created between 06:00 AM and 11:00 PM.

class TPVWorker(TopWorker):
    """Computes Total Payment Value per semester for each store."""

    def __init__(self) -> None:
        super().__init__()
        self.partial_tpv: DefaultDict[
            ClientId, DefaultDict[YearHalf, DefaultDict[StoreId, float]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        logger.info("TPVWorker initialized")

    def reset_state(self, client_id: ClientId) -> None:
        self.partial_tpv[client_id] = defaultdict(lambda: defaultdict(float))

    def accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        year_half: YearHalf | None = extract_year_half(payload.get('created_at'))
        if not year_half:
            return
        
        store_id: StoreId = safe_int_conversion(payload.get('store_id'), minimum=0)
        amount: float = safe_float_conversion(payload.get('final_amount'), 0.0)

        self.partial_tpv[client_id][year_half][store_id] += amount

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

if __name__ == '__main__':
    run_main(TPVWorker)
