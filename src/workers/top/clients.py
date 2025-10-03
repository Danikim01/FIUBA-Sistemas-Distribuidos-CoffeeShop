#!/usr/bin/env python3

"""Top clients worker that aggregates purchase quantities per branch."""

import logging
import os
from collections import defaultdict
from typing import Any, DefaultDict, Dict

try:
    from workers.worker_utils import (  # type: ignore
        run_main,
        safe_int_conversion,
    )
except ImportError:  # pragma: no cover - legacy fallback
    from worker_utils import run_main, safe_int_conversion  # type: ignore

from workers.top.top_worker import TopWorker


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopClientsWorker(TopWorker):
    """Computes the top-N clients per store using purchase quantities."""

    def __init__(self) -> None:
        super().__init__()
        self.top_n = safe_int_conversion(os.getenv('TOP_CLIENTS_COUNT', '3')) or 3
        self._client_counts: DefaultDict[
            str, DefaultDict[int, DefaultDict[int, int]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

        logger.info("TopClientsWorker configured with top_n=%s", self.top_n)

    # ------------------------------------------------------------------
    # Data accumulation
    # ------------------------------------------------------------------

    def _accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        try:
            store_id = safe_int_conversion(payload.get('store_id'))
            user_id = safe_int_conversion(payload.get('user_id'))
            quantity = safe_int_conversion(payload.get('purchase_qty'))
        except Exception:  # noqa: BLE001
            logger.debug("Invalid transaction skipped: %s", payload)
            return

        if store_id <= 0 or user_id <= 0 or quantity <= 0:
            return

        self._client_counts[client_id][store_id][user_id] += quantity

    def process_message(self, message: Any):
        if not isinstance(message, dict):
            logger.debug("Ignoring non-dict payload: %s", type(message))
            return

        client_id = self.current_client_id or ''
        if not client_id:
            logger.warning("Transaction received without client metadata")
            return

        self._accumulate_transaction(client_id, message)

    def process_batch(self, batch: Any):
        client_id = self.current_client_id or ''
        if not client_id:
            logger.warning("Batch received without client metadata")
            return

        for entry in batch:
            if isinstance(entry, dict):
                self._accumulate_transaction(client_id, entry)

    # ------------------------------------------------------------------
    # Result emission
    # ------------------------------------------------------------------

    def handle_eof(self, message: Dict[str, Any]):
        client_id = message.get('client_id') or self.current_client_id
        if not client_id:
            logger.warning("EOF received without client_id in TopClientsWorker")
            return

        counts_for_client = self._client_counts.pop(client_id, {})
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

        results.sort(key=lambda row: (row['store_id'], -row['purchases_qty'], row['user_id']))

        payload = {
            'type': 'top_clients_partial',
            'results': results,
        }

        self.send_message(payload, client_id=client_id)
        logger.info(
            "TopClientsWorker emitted %s rows for client %s",
            len(results),
            client_id,
        )

        self.send_eof(client_id=client_id, additional_data={'source': 'top_clients'})
        self.reset_client(client_id)


def main() -> None:
    run_main(TopClientsWorker)


if __name__ == '__main__':
    main()
