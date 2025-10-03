#!/usr/bin/env python3

"""TPV worker that aggregates semester totals per store."""

import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Any, DefaultDict, Dict

try:
    from workers.worker_utils import (  # type: ignore
        run_main,
        safe_float_conversion,
        safe_int_conversion,
    )
except ImportError:  # pragma: no cover - legacy fallback
    from worker_utils import run_main, safe_float_conversion, safe_int_conversion  # type: ignore

from workers.top.top_worker import TopWorker


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TPVWorker(TopWorker):
    """Computes Total Payment Value per semester for each store."""

    def __init__(self) -> None:
        super().__init__()
        self._tpv_totals: DefaultDict[
            str, DefaultDict[str, DefaultDict[int, float]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        logger.info("TPVWorker initialized")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_year_half(created_at: Any) -> str | None:
        if not created_at:
            return None

        if isinstance(created_at, str):
            try:
                dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    dt = datetime.fromisoformat(created_at)
                except ValueError:
                    return None
        else:
            try:
                dt = datetime.fromisoformat(str(created_at))
            except ValueError:
                return None

        half = '1' if dt.month <= 6 else '2'
        return f"{dt.year}-H{half}"

    def _accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        year_half = self._extract_year_half(payload.get('created_at'))
        if not year_half:
            return

        try:
            store_id = safe_int_conversion(payload.get('store_id'))
        except Exception:  # noqa: BLE001
            logger.debug("Transaction without valid store_id: %s", payload)
            return

        if store_id <= 0:
            return

        amount = safe_float_conversion(payload.get('final_amount'), 0.0)
        if amount == 0:
            return

        bucket = self._tpv_totals[client_id][year_half]
        bucket[store_id] += amount

    # ------------------------------------------------------------------
    # BaseWorker overrides
    # ------------------------------------------------------------------

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
            logger.warning("EOF received without client_id in TPVWorker")
            return

        totals = self._tpv_totals.pop(client_id, {})
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

        results.sort(key=lambda row: (row['year_half_created_at'], row['store_id']))

        payload = {
            'type': 'tpv_partial',
            'results': results,
        }

        self.send_message(payload, client_id=client_id)
        logger.info(
            "TPVWorker emitted %s rows for client %s",
            len(results),
            client_id,
        )

        self.send_eof(client_id=client_id, additional_data={'source': 'tpv'})
        self.reset_client(client_id)


def main() -> None:
    run_main(TPVWorker)


if __name__ == '__main__':
    main()
