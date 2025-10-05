#!/usr/bin/env python3

"""Top items worker that aggregates per-month best sellers and profits."""

import logging
import os
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List
from message_utils import ClientId
from worker_utils import extract_year_month, get_top_number, run_main, safe_float_conversion, safe_int_conversion
from workers.top.top_worker import TopWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

YearMonth = str
ItemId = int

QuantityTotals = DefaultDict[ClientId, DefaultDict[YearMonth, DefaultDict[ItemId, int]]]
ProfitTotals = DefaultDict[ClientId, DefaultDict[YearMonth, DefaultDict[ItemId, float]]]

def _new_quantity_bucket() -> DefaultDict[ItemId, int]:
    return defaultdict(int)
def _new_profit_bucket() -> DefaultDict[ItemId, float]:
    return defaultdict(float)
def _new_monthly_quantity_map() -> DefaultDict[YearMonth, DefaultDict[ItemId, int]]:
    return defaultdict(_new_quantity_bucket)
def _new_monthly_profit_map() -> DefaultDict[YearMonth, DefaultDict[ItemId, float]]:
    return defaultdict(_new_profit_bucket)

class TopItemsWorker(TopWorker):
    """Computes best-selling and most profitable items per month."""

    def __init__(self) -> None:
        super().__init__()

        self.top_per_month = get_top_number('TOP_ITEMS_COUNT', default=1)

        self._quantity_totals: QuantityTotals = defaultdict(_new_monthly_quantity_map)
        self._profit_totals: ProfitTotals = defaultdict(_new_monthly_profit_map)

        logger.info("TopItemsWorker configured with top_per_month=%s", self.top_per_month)

    def reset_state(self, client_id: ClientId) -> None:
        self._quantity_totals[client_id] = _new_monthly_quantity_map()
        self._profit_totals[client_id] = _new_monthly_profit_map()

    def accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        year_month = extract_year_month(payload.get('created_at'))
        if not year_month:
            return

        item_id = safe_int_conversion(payload.get('item_id'))

        quantity = safe_int_conversion(payload.get('quantity'), 0)
        subtotal = safe_float_conversion(payload.get('subtotal'), 0.0)

        self._quantity_totals[client_id][year_month][item_id] += quantity
        self._profit_totals[client_id][year_month][item_id] += subtotal

    def _build_results(
        self,
        totals: Dict[YearMonth, Dict[ItemId, int | float]],
        metric_key: str,
    ) -> list[Dict[str, Any]]:
        results: list[Dict[str, Any]] = []
        for year_month, items_map in totals.items():
            ranked = sorted(
                items_map.items(), 
                key=lambda item: (-item[1], item[0])
            )

            for item_id, value in ranked:
                results.append(
                    {
                        'year_month_created_at': year_month,
                        'item_id': item_id,
                        metric_key: value,
                    }
                )
        return results

    def create_payload(self, client_id: str) -> List[Dict[str, Any]]:
        """Emit per-replica totals so the aggregator can compute exact top-K.

        We send full per-month per-item totals for both quantity and profit,
        allowing the final aggregator to merge across replicas without loss.
        """
        quantity_totals = self._quantity_totals.pop(client_id, {})
        profit_totals = self._profit_totals.pop(client_id, {})

        # Convert defaultdict structures into plain dicts for serialization
        q_out: Dict[str, Dict[int, int]] = {}
        for ym, items_map in quantity_totals.items():
            q_out[str(ym)] = {int(item_id): int(value) for item_id, value in items_map.items()}

        p_out: Dict[str, Dict[int, float]] = {}
        for ym, items_map in profit_totals.items():
            p_out[str(ym)] = {int(item_id): float(value) for item_id, value in items_map.items()}

        payload = {
            'quantity_totals': q_out,
            'profit_totals': p_out,
        }

        return [payload]

if __name__ == '__main__':
    run_main(TopItemsWorker)
