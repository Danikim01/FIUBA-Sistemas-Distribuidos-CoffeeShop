#!/usr/bin/env python3

"""Top clients worker that aggregates purchase quantities per branch."""

import logging
import os
from collections import defaultdict
from typing import Any, DefaultDict, Dict

from message_utils import ClientId
from worker_utils import run_main, safe_int_conversion
from workers.top.top_worker import TopWorker


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopClientsWorker(TopWorker):
    """Computes the top-N clients per store using purchase quantities."""

    def __init__(self) -> None:
        super().__init__()
        self.top_n = safe_int_conversion(os.getenv('TOP_CLIENTS_COUNT', '3'), 3)
        self.clients_data: DefaultDict[
            str, DefaultDict[int, DefaultDict[int, int]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

        logger.info("TopClientsWorker configured with top_n=%s", self.top_n)

    def reset_state(self, client_id: ClientId) -> None:
        self.clients_data[client_id] = defaultdict(lambda: defaultdict(int))

    def accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        try:
            store_id = safe_int_conversion(payload.get('store_id'))
            user_id = safe_int_conversion(payload.get('user_id'))
            raw_qty = (
                payload.get('purchase_qty')
                if 'purchase_qty' in payload
                else payload.get('purchases_qty', 1)
            )
            quantity = safe_int_conversion(raw_qty, 1)
        except Exception:  # noqa: BLE001
            logger.debug("Invalid transaction skipped: %s", payload)
            return

        if store_id <= 0 or user_id <= 0 or quantity <= 0:
            return

        self.clients_data[client_id][store_id][user_id] += quantity

    def create_payload(self, client_id: str) -> list[Dict[str, Any]]:
        counts_for_client = self.clients_data.pop(client_id, {})
        results: list[Dict[str, Any]] = []

        for store_id, user_counts in counts_for_client.items():
            ranked = sorted(
                user_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )[: self.top_n]

            for user_id, purchases_qty in ranked:
                results.append(
                    {
                        'store_id': store_id,
                        'user_id': user_id,
                        'purchases_qty': purchases_qty,
                    }
                )

        return results

if __name__ == '__main__':
    run_main(TopClientsWorker)
