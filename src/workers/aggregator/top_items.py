#!/usr/bin/env python3

"""Aggregate top-item partial results from multiple replicas."""

import logging
import os
import threading
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Mapping, Optional
from message_utils import ClientId # pyright: ignore[reportMissingImports]
from worker_utils import run_main, safe_int_conversion, top_items_sort_key # pyright: ignore[reportMissingImports]
from workers.metadata_store.menu_items import MenuItemsMetadataStore
from workers.sharded_process.process_worker import ProcessWorker
from workers.utils.processed_message_store import ProcessedMessageStore
from workers.utils.eof_counter_store import EOFCounterStore
from workers.utils.aggregator_state_store import AggregatorStateStore

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

class TopItemsAggregator(ProcessWorker):
    """Aggregates per-client quantity and profit rankings across replicas."""

    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload = False

        self.top_per_month = safe_int_conversion(os.getenv("TOP_ITEMS_COUNT"), default=1)

        self._quantity_totals: DefaultDict[ClientId, QuantityTotals]
        self._quantity_totals = defaultdict(_new_quantity_totals)

        self._profit_totals: DefaultDict[ClientId, ProfitTotals]
        self._profit_totals = defaultdict(_new_profit_totals)

        self.menu_items_source = MenuItemsMetadataStore(self.middleware_config)
        self.menu_items_source.start_consuming()

        # Fault tolerance: Track processed message UUIDs
        self.processed_messages = ProcessedMessageStore(worker_label="top_items_aggregator")
        
        # Fault tolerance: Track EOF counters with deduplication
        self.eof_counter_store = EOFCounterStore(worker_label="top_items_aggregator")
        
        # Fault tolerance: Persist intermediate aggregation state
        self.state_store = AggregatorStateStore(worker_label="top_items_aggregator")
        
        # Number of sharded workers we expect EOFs from (must be set before loading state)
        self.expected_eof_count = int(os.getenv('REPLICA_COUNT', '2'))
        
        # Load persisted state on startup
        self._load_persisted_state()
        
        # Track how many EOFs we've received from sharded workers per client
        # Load persisted EOF counters on startup
        self.eof_count_per_client: Dict[ClientId, int] = {}
        self._load_persisted_eof_counters()
        
        self.eof_count_lock = threading.Lock()
        
        logger.info(
            "%s configured with top_per_month=%s, expected_eof_count=%s",
            self.__class__.__name__,
            self.top_per_month,
            self.expected_eof_count,
        )
    
    def _load_persisted_state(self) -> None:
        """Load persisted aggregation state for all clients on startup."""
        logger.info("[ITEMS-AGGREGATOR] Loading persisted aggregation state...")
        
        # Restore _quantity_totals and _profit_totals from persisted state
        # BUT: Only restore if the client hasn't completed yet (EOF count < expected)
        for client_id in list(self.state_store._cache.keys()):
            # Check if client already completed (should not restore state if completed)
            eof_count = self.eof_counter_store.get_counter(client_id)
            if eof_count >= self.expected_eof_count:
                # Client already completed, don't restore state
                # BUT: Don't clean up yet - we need to keep EOF UUIDs for deduplication
                # The state will be cleaned up when we actually process and send results
                logger.info(
                    f"[ITEMS-AGGREGATOR] Client {client_id} already completed "
                    f"(EOF count: {eof_count}), skipping state restoration "
                    f"(will clean up after sending results)"
                )
                continue
            
            # Client hasn't completed, restore state
            state = self.state_store.get_state(client_id)
            if 'quantity_totals' in state:
                # Convert back from dict to nested defaultdict structure
                quantity_data = state['quantity_totals']
                for ym, items in quantity_data.items():
                    for item_id, qty in items.items():
                        self._quantity_totals[client_id][ym][int(item_id)] = int(qty)
            
            if 'profit_totals' in state:
                # Convert back from dict to nested defaultdict structure
                profit_data = state['profit_totals']
                for ym, items in profit_data.items():
                    for item_id, profit in items.items():
                        self._profit_totals[client_id][ym][int(item_id)] = float(profit)
            
            if 'quantity_totals' in state or 'profit_totals' in state:
                logger.info(
                    f"[ITEMS-AGGREGATOR] Restored aggregation state for client {client_id} "
                    f"(EOF count: {eof_count}/{self.expected_eof_count})"
                )
    
    def _load_persisted_eof_counters(self) -> None:
        """Load persisted EOF counters for all clients on startup."""
        logger.info("[ITEMS-AGGREGATOR] EOF counters will be loaded from persistence on first access")

    def reset_state(self, client_id: ClientId) -> None:
        """Reset state for a client. 
        
        NOTE: This is called after sending final results. We keep EOF counter
        and processed UUIDs to handle duplicate EOFs that may arrive later.
        """
        for store in (self._quantity_totals, self._profit_totals):
            try:
                del store[client_id]
            except KeyError:
                continue
        #self.menu_items_source.reset_state(client_id)
        # Clear processed messages and aggregation state
        self.processed_messages.clear_client(client_id)
        self.state_store.clear_client(client_id)
        # NOTE: We DON'T clear eof_counter_store here to keep UUIDs for deduplication
        # The EOF counter will be cleared when the client disconnects or after a timeout

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

    def process_transaction(self, client_id: ClientId, payload: Dict[str, Any]) -> None:
        self._merge_quantity_totals_map(client_id, payload.get('quantity'))
        self._merge_profit_totals_map(client_id, payload.get('profit'))
    
    def process_batch(self, batch: list[Dict[str, Any]], client_id: ClientId):
        """Process a batch with deduplication."""
        message_uuid = self._get_current_message_uuid()
        
        # Check for duplicate message
        if message_uuid and self.processed_messages.has_processed(client_id, message_uuid):
            logger.info(
                f"[ITEMS-AGGREGATOR] [DUPLICATE] Skipping duplicate batch {message_uuid} "
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
        quantity_data = {}
        for ym, items in self._quantity_totals.get(client_id, {}).items():
            quantity_data[ym] = {str(k): v for k, v in items.items()}
        
        profit_data = {}
        for ym, items in self._profit_totals.get(client_id, {}).items():
            profit_data[ym] = {str(k): v for k, v in items.items()}
        
        state_data = {
            'quantity_totals': quantity_data,
            'profit_totals': profit_data
        }
        self.state_store.save_state(client_id, state_data)

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

    def handle_eof(self, message: Dict[str, Any], client_id: ClientId, message_uuid: Optional[str] = None):
        """
        Handle EOF from sharded workers with deduplication.
        
        This aggregator waits for EOFs from ALL sharded workers before sending
        the final results. The mechanism works as follows:
        1. Each sharded worker sends EOF directly to this aggregator
        2. This aggregator accumulates data from all sharded workers
        3. We track how many EOFs we've received per client (with deduplication)
        4. Only when ALL EOFs are received (expected_eof_count), we send the
           final aggregated results along with our own EOF
        """
        # Use message_uuid from parameter or extract from message
        if not message_uuid:
            message_uuid = message.get('message_uuid')
        
        # Check for duplicate EOF first
        if message_uuid and self.eof_counter_store.has_processed(client_id, message_uuid):
            logger.info(
                f"[ITEMS-AGGREGATOR] [DUPLICATE-EOF] Skipping duplicate EOF {message_uuid} "
                f"for client {client_id} (already processed)"
            )
            return
        
        # Check if client already completed (before processing this EOF)
        current_eof_count = self.eof_counter_store.get_counter(client_id)
        if current_eof_count >= self.expected_eof_count:
            # Client already completed, ignore this EOF (may be a duplicate or retry)
            logger.info(
                f"[ITEMS-AGGREGATOR] [ALREADY-COMPLETED] Ignoring EOF for client {client_id} "
                f"(already completed with {current_eof_count} EOFs). "
                f"UUID: {message_uuid}"
            )
            # Mark as processed to prevent future duplicates
            if message_uuid:
                self.eof_counter_store.mark_processed(client_id, message_uuid)
            return
        
        logger.info(f"[ITEMS-AGGREGATOR] Received EOF for client {client_id} (UUID: {message_uuid})")
        
        # Mark EOF as processed and increment counter (persisted atomically)
        if message_uuid:
            self.eof_counter_store.mark_processed(client_id, message_uuid)
        
        # Increment EOF counter for this client (persisted)
        with self.eof_count_lock:
            eof_count = self.eof_counter_store.increment_counter(client_id)
            self.eof_count_per_client[client_id] = eof_count
        
        logger.info(
            f"[ITEMS-AGGREGATOR] EOF count for client {client_id}: {eof_count}/{self.expected_eof_count}"
        )
        
        with self._pause_message_processing():
            if eof_count >= self.expected_eof_count:
                # We've received EOFs from all sharded workers
                # Now send the final aggregated results along with our own EOF
                logger.info(
                    f"[ITEMS-AGGREGATOR] All EOFs received for client {client_id} "
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
                    f"[ITEMS-AGGREGATOR] Not all EOFs received yet for client {client_id} "
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
            self.menu_items_source.close()
        finally:
            super().cleanup()


if __name__ == "__main__":
    run_main(TopItemsAggregator)
