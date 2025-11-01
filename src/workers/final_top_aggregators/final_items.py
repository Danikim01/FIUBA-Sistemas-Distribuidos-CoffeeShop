#!/usr/bin/env python3

"""Aggregate top-item partial results from multiple replicas."""

import logging
import os
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Mapping

from message_utils import ClientId # pyright: ignore[reportMissingImports]
from worker_utils import run_main, safe_int_conversion, top_items_sort_key # pyright: ignore[reportMissingImports]
from workers.extra_source.menu_items import MenuItemsExtraSource
from workers.local_top_scaling.aggregator_worker import AggregatorWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Best-selling products (name and quantity) and products that generated the biggest profits (name and profit), for each month of 2024 and 2025.

YearMonth = str
ItemId = int
ItemMetricValue = int | float

QuantityTotals = DefaultDict[YearMonth, DefaultDict[ItemId, int]]
ProfitTotals = DefaultDict[YearMonth, DefaultDict[ItemId, float]]

def _new_quantity_bucket() -> DefaultDict[ItemId, int]:
    return defaultdict(int)
def _new_profit_bucket() -> DefaultDict[ItemId, float]:
    return defaultdict(float)
def _new_quantity_totals() -> QuantityTotals:
    return defaultdict(_new_quantity_bucket)
def _new_profit_totals() -> ProfitTotals:
    return defaultdict(_new_profit_bucket)

class FinalItemsAggregator(AggregatorWorker):
    """Aggregates per-client quantity and profit rankings across replicas."""

    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload = False

        self.top_per_month = safe_int_conversion(os.getenv("TOP_ITEMS_COUNT"), default=1)

        self._quantity_totals: DefaultDict[ClientId, QuantityTotals]
        self._quantity_totals = defaultdict(_new_quantity_totals)

        self._profit_totals: DefaultDict[ClientId, ProfitTotals]
        self._profit_totals = defaultdict(_new_profit_totals)

        self.menu_items_source = MenuItemsExtraSource(self.middleware_config)
        self.menu_items_source.start_consuming()

        logger.info(
            "%s configured with top_per_month=%s",
            self.__class__.__name__,
            self.top_per_month,
        )

    def reset_state(self, client_id: ClientId) -> None:
        for store in (self._quantity_totals, self._profit_totals):
            try:
                del store[client_id]
            except KeyError:
                continue
        self.menu_items_source.reset_state(client_id)

    def _merge_quantity_totals_map(self, client_id: ClientId, totals: Any) -> None:
        if not isinstance(totals, dict):
            return
        client_totals = self._quantity_totals[client_id]
        for year_month, items_map in totals.items():
            if not isinstance(items_map, dict):
                continue
            ym = str(year_month)
            ym_bucket = client_totals[ym]
            for item_id, value in items_map.items():
                try:
                    iid = int(item_id)
                    qty = int(value)
                except (TypeError, ValueError):
                    continue
                ym_bucket[iid] += qty

    def _merge_profit_totals_map(self, client_id: ClientId, totals: Any) -> None:
        if not isinstance(totals, dict):
            return
        client_totals = self._profit_totals[client_id]
        for year_month, items_map in totals.items():
            if not isinstance(items_map, dict):
                continue
            ym = str(year_month)
            ym_bucket = client_totals[ym]
            for item_id, value in items_map.items():
                try:
                    iid = int(item_id)
                    profit = float(value)
                except (TypeError, ValueError):
                    continue
                ym_bucket[iid] += profit

    def accumulate_transaction(self, client_id: ClientId, payload: Dict[str, Any]) -> None:
        self._merge_quantity_totals_map(client_id, payload.get('quantity'))
        self._merge_profit_totals_map(client_id, payload.get('profit'))

    def get_item_name(self, clientId: ClientId, item_id: ItemId) -> str:
        return self.menu_items_source.get_item_when_done(clientId, str(item_id))

    def _build_results(
        self,
        client_id: ClientId,
        totals: Mapping[YearMonth, Mapping[ItemId, ItemMetricValue]],
        metric_key: str,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        for year_month, items_map in totals.items():
            ranked = sorted(
                items_map.items(),
                key=lambda item: (-item[1], item[0]),
            )

            for item_id, value in ranked[: self.top_per_month]:
                results.append(
                    {
                        "year_month_created_at": year_month,
                        "item_id": item_id,
                        "item_name": self.get_item_name(client_id, item_id),
                        metric_key: value,
                    }
                )

        results.sort(key=lambda row: top_items_sort_key(row, metric_key))
        return results

    def create_payload(self, client_id: ClientId) -> List[Dict[str, Any]]:
        quantity_totals = self._quantity_totals.pop(client_id, _new_quantity_totals())
        profit_totals = self._profit_totals.pop(client_id, _new_profit_totals())

        quantity_results = self._build_results(client_id, quantity_totals, "sellings_qty")
        profit_results = self._build_results(client_id, profit_totals, "profit_sum")

        if not quantity_results and not profit_results:
            logging.info(f"No results to send for client {client_id}")
            return []

        payload = {
            "quantity": quantity_results,
            "profit": profit_results,
        }

        return [payload]

    def gateway_type_metadata(self) -> dict:
        return {
            "bundle_types": {
                "quantity": "TOP_ITEMS_BY_QUANTITY",
                "profit": "TOP_ITEMS_BY_PROFIT",
            }
        }
    
    def cleanup(self) -> None:
        try:
            self.menu_items_source.close()
        finally:
            super().cleanup()


if __name__ == "__main__":
    run_main(FinalItemsAggregator)
