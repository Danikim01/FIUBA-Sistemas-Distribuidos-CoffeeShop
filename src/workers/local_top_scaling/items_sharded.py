#!/usr/bin/env python3

"""Sharded Items worker that aggregates per-month best sellers and profits based on item_id sharding."""

import logging
import os
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List

from message_utils import ClientId
from worker_utils import run_main, safe_float_conversion, safe_int_conversion, extract_year_month
from workers.local_top_scaling.aggregator_worker import AggregatorWorker
from workers.utils.sharding_utils import get_routing_key_for_semester, extract_semester_from_payload

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

class ShardedItemsWorker(AggregatorWorker):
    """
    Sharded version of ItemsWorker that processes transaction items based on item_id sharding.
    Each worker processes a specific shard of items.
    """
    
    def __init__(self) -> None:
        super().__init__()
        
        # Get sharding configuration from environment
        self.num_shards = int(os.getenv('NUM_SHARDS', '2'))
        self.worker_id = int(os.getenv('WORKER_ID', '0'))
        
        # Validate worker_id is within shard range
        if self.worker_id >= self.num_shards:
            raise ValueError(f"WORKER_ID {self.worker_id} must be less than NUM_SHARDS {self.num_shards}")
        
        # Configure routing key for this worker's shard
        self.expected_routing_key = f"shard_{self.worker_id}"
        
        logger.info(f"ShardedItemsWorker initialized: worker_id={self.worker_id}, num_shards={self.num_shards}, routing_key={self.expected_routing_key}")

        self._quantity_totals: QuantityTotals = defaultdict(_new_monthly_quantity_map)
        self._profit_totals: ProfitTotals = defaultdict(_new_monthly_profit_map)

    def reset_state(self, client_id: ClientId) -> None:
        self._quantity_totals[client_id] = _new_monthly_quantity_map()
        self._profit_totals[client_id] = _new_monthly_profit_map()

    def should_process_transaction(self, payload: Dict[str, Any]) -> bool:
        """
        Determine if this worker should process the transaction item based on semester sharding.
        Also handles coordinated EOF messages that don't have created_at.
        
        Args:
            payload: Transaction item data
            
        Returns:
            True if this worker should process the transaction item
        """
        # Check if this is a control message (EOF, heartbeat, etc.)
        # Control messages like EOF don't have created_at and should be processed by all workers
        semester = extract_semester_from_payload(payload)
        if semester is None:
            # This is likely a control message (EOF, etc.) - process it
            logger.debug(f"Received control message (no created_at): {payload}")
            return False
            
        # For regular transaction items, verify they belong to our shard
        expected_routing_key = get_routing_key_for_semester(payload.get('created_at'), self.num_shards)
        if expected_routing_key != self.expected_routing_key:
            logger.warning(f"Received transaction item for wrong shard: semester={semester}, expected={self.expected_routing_key}, got={expected_routing_key}")
            return False
            
        return True

    def accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        # Only process transaction items that belong to this worker's shard
        if not self.should_process_transaction(payload):
            return

        # Skip control messages (EOF, etc.) - they don't have created_at
        semester = extract_semester_from_payload(payload)
        if semester is None:
            logger.debug(f"Skipping control message in accumulate_transaction: {payload}")
            return
            
        year_month = extract_year_month(payload.get('created_at'))
        if not year_month:
            return

        item_id = safe_int_conversion(payload.get('item_id'))
        quantity = safe_int_conversion(payload.get('quantity'), 0)
        subtotal = safe_float_conversion(payload.get('subtotal'), 0.0)

        logger.info(f"Processing items transaction for item_id={item_id}, year_month={year_month}, quantity={quantity}, subtotal={subtotal}")
        self._quantity_totals[client_id][year_month][item_id] += quantity
        self._profit_totals[client_id][year_month][item_id] += subtotal

    def _build_results(
        self,
        totals: Dict[YearMonth, Dict[ItemId, int | float]],
        metric_key: str,
    ) -> list[Dict[str, Any]]:
        results: list[Dict[str, Any]] = []
        for year_month, items_map in totals.items():
            for item_id, value in items_map.items():
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

        if not quantity_totals and not profit_totals:
            return []

        # Convert defaultdict structures into plain dicts for serialization
        q_out: Dict[str, Dict[int, int]] = {}
        for ym, items_map in quantity_totals.items():
            q_out[str(ym)] = {int(item_id): int(value) for item_id, value in items_map.items()}

        p_out: Dict[str, Dict[int, float]] = {}
        for ym, items_map in profit_totals.items():
            p_out[str(ym)] = {int(item_id): float(value) for item_id, value in items_map.items()}

        payload = {
            'quantity': q_out,
            'profit': p_out,
        }

        return [payload]


if __name__ == '__main__':
    run_main(ShardedItemsWorker)
