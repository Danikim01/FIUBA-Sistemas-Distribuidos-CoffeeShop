#!/usr/bin/env python3

"""Aggregator that enriches top clients with birthdays and re-ranks globally."""

import logging
import os
import threading
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List
from message_utils import ClientId # pyright: ignore[reportMissingImports]
from worker_utils import run_main, safe_int_conversion # pyright: ignore[reportMissingImports]
from workers.local_top_scaling.aggregator_worker import AggregatorWorker
from workers.extra_source.users import UsersExtraSource
from workers.extra_source.stores import StoresExtraSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Birthday date of the 3 customers who have made the most purchases for each branch

class TopClientsBirthdaysAggregator(AggregatorWorker):
    """Aggregates top-client partials and injects client birthdays."""

    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload = False
        
        self.top_n = safe_int_conversion(os.getenv('TOP_USERS_COUNT'), default=3)

        self.stores_source = StoresExtraSource(self.middleware_config)
        self.stores_source.start_consuming()
        self.birthdays_source = UsersExtraSource(self.middleware_config)
        self.birthdays_source.start_consuming()
        
        self.recieved_payloads: Dict[ClientId, list[dict[str, Any]]] = {}
        
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
        self.birthdays_source.reset_state(client_id)
    
    def accumulate_transaction(self, client_id: str, payload: dict[str, Any]) -> None:
        self.recieved_payloads.setdefault(client_id, []).append(payload)

    def create_payload(self, client_id: ClientId) -> list[Dict[str, Any]]:
        client_payloads = self.recieved_payloads.pop(client_id, [])
        aggregated: Dict[tuple[str, int], Dict[str, Any]] = {}

        for payload in client_payloads:
            store_id = str(payload.get("store_id", "")).strip()
            try:
                user_id = int(payload.get("user_id", 0))
            except (TypeError, ValueError):
                user_id = 0

            raw_qty = payload.get("purchases_qty") or payload.get("purchase_qty") or 0
            try:
                purchase_qty = int(raw_qty)
            except (TypeError, ValueError):
                purchase_qty = 0

            if not store_id or user_id <= 0 or purchase_qty <= 0:
                continue

            key = (store_id, user_id)

            if key not in aggregated:
                aggregated[key] = {
                    "store_id": store_id,
                    "user_id": user_id,
                    "purchases_qty": 0,
                }

            aggregated[key]["purchases_qty"] += purchase_qty

        # Enrich with birthdays and store names
        grouped_results: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        for entry in aggregated.values():
            user_id = entry["user_id"]
            store_id = entry["store_id"]

            birthdate = self.birthdays_source.get_item_when_done(
                client_id,
                str(user_id),
            )
            store_name = self.stores_source.get_item_when_done(client_id, store_id)

            grouped_results[store_id].append(
                {
                    "user_id": user_id,
                    "store_id": store_id,
                    "store_name": store_name,
                    "birthdate": birthdate,
                    "purchases_qty": entry["purchases_qty"],
                }
            )

        limited_results: List[Dict[str, Any]] = []
        for store_id, entries in grouped_results.items():
            entries.sort(
                key=lambda row: -safe_int_conversion(row.get("purchases_qty"), default=0)
            )
            limited_results.extend(entries[: self.top_n])

        limited_results.sort(
            key=lambda row: (
                row.get("store_name", ""),
                row.get("birthdate", ""),
            )
        )

        return limited_results

    def gateway_type_metadata(self) -> dict:
        return {
            "list_type": "TOP_CLIENTS_BIRTHDAYS",
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
        logger.info(f"[TOP-CLIENTS-AGGREGATOR] Received EOF for client {client_id}")
        
        # Increment EOF counter for this client
        with self.eof_count_lock:
            self.eof_count_per_client[client_id] = self.eof_count_per_client.get(client_id, 0) + 1
            eof_count = self.eof_count_per_client[client_id]
        
        logger.info(
            f"[TOP-CLIENTS-AGGREGATOR] EOF count for client {client_id}: {eof_count}/{self.expected_eof_count}"
        )
        
        with self._pause_message_processing():
            if eof_count >= self.expected_eof_count:
                # We've received EOFs from all sharded workers
                # Now send the final aggregated results along with our own EOF
                logger.info(
                    f"[TOP-CLIENTS-AGGREGATOR] All EOFs received for client {client_id} "
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
                logger.info(f"[TOP-CLIENTS-AGGREGATOR] Sending EOF to gateway for client {client_id}")
                self.eof_handler.output_eof(client_id=client_id)
                
                # Clean up EOF counter for this client
                with self.eof_count_lock:
                    if client_id in self.eof_count_per_client:
                        del self.eof_count_per_client[client_id]
            else:
                # Not all EOFs received yet, just discard this EOF
                # (we don't need requeue since each sharded worker sends its own EOF)
                logger.info(
                    f"[TOP-CLIENTS-AGGREGATOR] Not all EOFs received yet for client {client_id} "
                    f"({eof_count}/{self.expected_eof_count}). "
                    f"Waiting for more EOFs..."
                )

    def cleanup(self):
        super().cleanup()
        try:
            self.stores_source.close()
            self.birthdays_source.close()
        except Exception:  # noqa: BLE001
            logger.warning("Failed to close extra sources", exc_info=True)

    def handle_client_reset(self, client_id: ClientId) -> None:
        """Drop any partial aggregation for a disconnected client."""
        with self._state_lock:
            self.recieved_payloads.pop(client_id, None)
            with self.eof_count_lock:
                self.eof_count_per_client.pop(client_id, None)
            self.stores_source.reset_state(client_id)
            self.birthdays_source.reset_state(client_id)
        logger.info("[CONTROL] Cleared Top Clients aggregator state for client %s", client_id)

    def handle_reset_all_clients(self) -> None:
        """Drop all aggregation state."""
        with self._state_lock:
            self.recieved_payloads.clear()
            with self.eof_count_lock:
                self.eof_count_per_client.clear()
            self.stores_source.reset_all()
            self.birthdays_source.reset_all()
        logger.info("[CONTROL] Cleared Top Clients aggregator state for all clients")


if __name__ == '__main__':
    run_main(TopClientsBirthdaysAggregator)
