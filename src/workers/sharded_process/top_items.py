#!/usr/bin/env python3

"""Sharded Items worker that aggregates per-month best sellers and profits based on item_id sharding."""

import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional

from message_utils import ClientId # pyright: ignore[reportMissingImports]
from worker_utils import run_main, safe_float_conversion, safe_int_conversion, extract_year_month # pyright: ignore[reportMissingImports]
from workers.sharded_process.process_worker import ProcessWorker
from workers.utils.sharding_utils import get_routing_key_by_item_id, extract_item_id_from_payload
from workers.state_manager.items import ItemsStateManager

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

class ShardedItemsWorker(ProcessWorker):
    """
    Sharded version of ItemsWorker that processes transaction items based on item_id sharding.
    Each worker processes a specific shard of items (item_id 1-8 distributed across shards).
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

        # Configure state manager
        state_path_env = os.getenv('STATE_FILE')
        state_dir_env = os.getenv('STATE_DIR')
        
        state_path = None
        state_dir = None
        
        if state_path_env:
            state_path = Path(state_path_env)
        elif state_dir_env:
            state_dir = Path(state_dir_env)

        self.state_manager = ItemsStateManager(
            state_data=None,
            state_path=state_path,
            state_dir=state_dir,
            worker_id=str(self.worker_id)
        )
        
        # Connect state manager structures to worker's structures
        state_data = self.state_manager.state_data
        if state_data is None:
            quantity_totals = defaultdict(_new_monthly_quantity_map)
            profit_totals = defaultdict(_new_monthly_profit_map)
            state_data = (quantity_totals, profit_totals)
            self.state_manager.state_data = state_data
        
        self._quantity_totals = self.state_manager.quantity_totals
        self._profit_totals = self.state_manager.profit_totals
        
        # Track clients that have already received EOF to reject batches that arrive after EOF
        self._clients_with_eof: set[ClientId] = set()

    def reset_state(self, client_id: ClientId) -> None:
        self._quantity_totals[client_id] = _new_monthly_quantity_map()
        self._profit_totals[client_id] = _new_monthly_profit_map()

    def should_process_transaction(self, payload: Dict[str, Any]) -> bool:
        """
        Determine if this worker should process the transaction item based on item_id sharding.
        Also handles coordinated EOF messages that don't have item_id.
        
        Args:
            payload: Transaction item data
            
        Returns:
            True if this worker should process the transaction item
        """
        # Check if this is a control message (EOF, heartbeat, etc.)
        # Control messages like EOF don't have item_id and should not be processed
        item_id = extract_item_id_from_payload(payload)
        if item_id is None:
            # This is likely a control message (EOF, etc.) - don't process it
            logger.debug(f"Received control message (no item_id): {payload}")
            return False
        
        # For regular transaction items, verify they belong to our shard
        expected_routing_key = get_routing_key_by_item_id(item_id, self.num_shards)
        if expected_routing_key != self.expected_routing_key:
            logger.warning(f"Received transaction item for wrong shard: item_id={item_id}, expected={self.expected_routing_key}, got={expected_routing_key}")
            return False
            
        return True

    def process_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        # Only process transaction items that belong to this worker's shard
        if not self.should_process_transaction(payload):
            return

        # Skip control messages (EOF, etc.) - they don't have item_id
        item_id = extract_item_id_from_payload(payload)
        if item_id is None:
            logger.debug(f"Skipping control message in process_transaction: {payload}")
            return
            
        year_month = extract_year_month(payload.get('created_at'))
        if not year_month:
            return

        item_id_int = safe_int_conversion(item_id)
        quantity = safe_int_conversion(payload.get('quantity'), 0)
        subtotal = safe_float_conversion(payload.get('subtotal'), 0.0)

        #logger.info(f"Processing items transaction for item_id={item_id_int}, year_month={year_month}, quantity={quantity}, subtotal={subtotal}")
        self._quantity_totals[client_id][year_month][item_id_int] += quantity
        self._profit_totals[client_id][year_month][item_id_int] += subtotal

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

    def process_batch(self, batch: list[Dict[str, Any]], client_id: ClientId):
        if self.shutdown_requested:
            logger.info("[BATCH-FLOW] Shutdown requested, rejecting batch to requeue")
            raise InterruptedError("Shutdown requested before batch processing")
        
        # Reject batches that arrive after EOF for this client
        if client_id in self._clients_with_eof:
            message_uuid = self._get_current_message_uuid()
            logger.info(
                "[PROCESSING - BATCH] [PROTOCOL-ERROR] Batch %s for client %s arrived AFTER EOF (protocol violation). "
                "Discarding batch to prevent incorrect processing.",
                message_uuid,
                client_id,
            )
            raise Exception(f"Batch arrived after EOF for client {client_id} (protocol error), discarding")
            
        message_uuid = self._get_current_message_uuid()

        if message_uuid and self.state_manager.get_last_processed_message(client_id) == message_uuid:
            logger.info(
                "[PROCESSING - BATCH] [DUPLICATE] Skipping duplicate batch %s for client %s (already processed)",
                message_uuid,
                client_id,
            )
            return

        with self._state_lock:
            previous_state = self.state_manager.clone_client_state(client_id)
            previous_uuid = self.state_manager.get_last_processed_message(client_id)

            try:
                for idx, entry in enumerate(batch):
                    if self.shutdown_requested:
                        logger.info("[PROCESSING - BATCH] [INTERRUPT] Shutdown requested during batch processing, rolling back")
                        raise InterruptedError("Shutdown requested during batch processing")

                    self.process_transaction(client_id, entry)

                logger.info(f"All {len(batch)} transactions accumulated successfully")

                if message_uuid:
                    self.state_manager.set_last_processed_message(client_id, message_uuid)

                if not self.shutdown_requested:
                    logger.info(f"[BATCH-FLOW] [PERSIST] About to persist state for client {client_id}, message_uuid={message_uuid}")
                    self.state_manager.persist_state(client_id)  # Persist only this client
                    logger.info(f"[BATCH-FLOW] [PERSIST] State persisted successfully for client {client_id}, message_uuid={message_uuid}")
                else:
                    logger.info("[PROCESSING - BATCH] [INTERRUPT] Shutdown requested after batch processing, rolling back state")
                    raise InterruptedError("Shutdown requested, preventing state persistence")
                
                logger.info(f"[BATCH-FLOW] [SUCCESS] Batch processing completed successfully for client {client_id}, message_uuid={message_uuid}. Returning from process_batch.")
            except Exception as e:
                logger.error(f"[BATCH-FLOW] [ERROR] Exception during batch processing: {type(e).__name__}: {e}")
                logger.info("[BATCH-FLOW] [ROLLBACK] Restoring previous state and UUID")
                self.state_manager.restore_client_state(client_id, previous_state)
                if message_uuid:
                    if previous_uuid is None:
                        logger.info("[BATCH-FLOW] [ROLLBACK] Clearing last_processed_message (was None)")
                        self.state_manager.clear_last_processed_message(client_id)
                    else:
                        logger.info(f"[BATCH-FLOW] [ROLLBACK] Restoring previous UUID: {previous_uuid}")
                        self.state_manager.set_last_processed_message(client_id, previous_uuid)
                raise

    def handle_eof(self, message: Dict[str, Any], client_id: ClientId, message_uuid: Optional[str] = None):
        """
        Handle EOF by sending data to aggregator and then sending EOF directly to output.
        
        Sharded workers do NOT use the consensus mechanism (requeue) between them.
        Each sharded worker sends EOF directly to the Items aggregator after sending its data.
        The aggregator will wait for EOFs from all sharded workers (using REPLICA_COUNT).
        """
        if self.shutdown_requested:
            logger.info("Shutdown requested, rejecting EOF to requeue")
            raise InterruptedError("Shutdown requested before EOF handling")
        
        # Mark this client as having received EOF
        with self._state_lock:
            self._clients_with_eof.add(client_id)
            logger.info(f"[EOF] Marking client {client_id} as having received EOF. Future batches for this client will be rejected.")
        
        logger.info(f"[EOF] [SHARDED-ITEMS-WORKER] Received EOF for client {client_id}")
        
        with self._pause_message_processing():
            try:
                # Send data to aggregator
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
                
                # Send EOF directly to output WITHOUT consensus mechanism
                # Extract and propagate sequence_id from the received EOF message
                from workers.utils.message_utils import extract_sequence_id
                sequence_id = extract_sequence_id(message)
                logger.info(f"[EOF] [SHARDED-ITEMS-WORKER] Sending EOF directly to Items aggregator for client {client_id} (no consensus), sequence_id: {sequence_id}")
                self.eof_handler.output_eof(client_id=client_id, sequence_id=sequence_id)
                
            except Exception:
                raise

            with self._state_lock:
                previous_state = self.state_manager.clone_client_state(client_id)
                previous_uuid = self.state_manager.get_last_processed_message(client_id)
                message_uuid = self._get_current_message_uuid()
                try:
                    if message_uuid:
                        self.state_manager.set_last_processed_message(client_id, message_uuid)
                    else:
                        logger.warning(f"EOF message for client {client_id} has no UUID; clearing last processed message")
                        self.state_manager.clear_last_processed_message(client_id)

                    self.state_manager.drop_empty_client_state(client_id)
                    
                    if not self.shutdown_requested:
                        self.state_manager.persist_state(client_id)  # Persist only this client
                    else:
                        logger.info("Shutdown requested during EOF handling, skipping state persistence")
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

    def _get_current_message_uuid(self) -> str | None:
        metadata = self._get_current_message_metadata()
        if not metadata:
            return None
        message_uuid = metadata.get('message_uuid')
        if not message_uuid:
            logger.warning("Missing message_uuid in metadata: %s", metadata.keys())
            return None
        return str(message_uuid)

    def _clear_client_state(self, client_id: ClientId) -> None:
        with self._state_lock:
            self._quantity_totals.pop(client_id, None)
            self._profit_totals.pop(client_id, None)
            self._clients_with_eof.discard(client_id)

        self.state_manager.clear_last_processed_message(client_id)
        self.state_manager.drop_empty_client_state(client_id)
        self.state_manager.clear_client_files(client_id)
        logger.info("[CONTROL] Sharded items worker %s cleared state for client %s", self.worker_id, client_id)

    def handle_client_reset(self, client_id: ClientId) -> None:
        self._clear_client_state(client_id)

    def handle_reset_all_clients(self) -> None:
        with self._state_lock:
            self._quantity_totals.clear()
            self._profit_totals.clear()
            self._clients_with_eof.clear()

        self.state_manager.clear_last_processed_messages()
        self.state_manager.clear_all_files()
        self.state_manager.state_data = (
            defaultdict(lambda: defaultdict(lambda: defaultdict(int))),
            defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        )
        logger.info("[CONTROL] Sharded items worker %s cleared all client state", self.worker_id)


if __name__ == '__main__':
    run_main(ShardedItemsWorker)
