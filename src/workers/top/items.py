#!/usr/bin/env python3

"""Top items worker that aggregates per-month best sellers and profits."""

import logging
import os
from collections import defaultdict
from typing import Any, DefaultDict, Dict
from worker_utils import extract_year_month, run_main, safe_float_conversion, safe_int_conversion
from workers.top.top_worker import TopWorker


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopItemsWorker(TopWorker):
    """Computes best-selling and most profitable items per month."""

    def __init__(self) -> None:
        super().__init__()
        self.top_per_month = safe_int_conversion(os.getenv('TOP_ITEMS_COUNT', '1')) or 1
        self._quantity_totals: DefaultDict[
            str, DefaultDict[str, DefaultDict[int, int]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        self._profit_totals: DefaultDict[
            str, DefaultDict[str, DefaultDict[int, float]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        logger.info("TopItemsWorker configured with top_per_month=%s", self.top_per_month)

    def _accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        year_month = extract_year_month(payload.get('created_at'))
        if not year_month:
            return

        try:
            item_id = safe_int_conversion(payload.get('item_id'))
        except Exception:  # noqa: BLE001
            logger.debug("Transaction item without valid item_id: %s", payload)
            return

        quantity = safe_int_conversion(payload.get('quantity'), 0)
        subtotal = safe_float_conversion(payload.get('subtotal'), 0.0)

        qty_bucket = self._quantity_totals[client_id][year_month]
        qty_bucket[item_id] += quantity

        profit_bucket = self._profit_totals[client_id][year_month]
        profit_bucket[item_id] += subtotal

    def _build_results(self, totals: Dict[str, Dict[int, float]], metric_key: str) -> list[Dict[str, Any]]:
        results: list[Dict[str, Any]] = []
        for year_month, items_map in totals.items():
            ranked = sorted(items_map.items(), key=lambda item: (-item[1], item[0]))
            for item_id, value in ranked[: self.top_per_month]:
                results.append(
                    {
                        'year_month_created_at': year_month,
                        'item_id': item_id,
                        metric_key: value,
                    }
                )

        results.sort(
            key=lambda row: (
                row['year_month_created_at'],
                -row[metric_key],
                row['item_id'],
            )
        )
        return results

    def create_payload(self, client_id: str) -> Dict[str, Any]:
        quantity_totals = self._quantity_totals.pop(client_id, {})
        profit_totals = self._profit_totals.pop(client_id, {})

        quantity_results = self._build_results(quantity_totals, 'sellings_qty')
        profit_results = self._build_results(profit_totals, 'profit_sum')

        payload = {
            'type': 'top_items_partial',
            'quantity': quantity_results,
            'profit': profit_results,
        }

        return payload

if __name__ == '__main__':
    run_main(TopItemsWorker)
