#!/usr/bin/env python3

"""Aggregator that enriches top clients with birthdays and re-ranks globally."""

import logging
import os
import threading
from collections import defaultdict
from typing import Any, DefaultDict, Dict
from message_utils import extract_client_metadata, is_eof_message
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue
from worker_utils import run_main, safe_int_conversion
from workers.utils.base_worker import BaseWorker


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopClientsBirthdaysAggregator(BaseWorker):
    """Aggregates top-client partials and injects client birthdays."""

    def __init__(self) -> None:
        super().__init__()

        self._client_counts: DefaultDict[
            str, DefaultDict[int, DefaultDict[int, int]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        self._client_birthdays: DefaultDict[str, Dict[int, str]] = defaultdict(dict)

        self.client_data_queue_name = os.getenv('CLIENT_DATA_QUEUE', 'client_data_raw').strip()
        self.client_data_middleware: RabbitMQMiddlewareQueue | None = None
        self._client_data_thread: threading.Thread | None = None

        if self.client_data_queue_name:
            connection_params = {
                'host': self.config.rabbitmq_host,
                'port': self.config.rabbitmq_port,
                'prefetch_count': self.config.prefetch_count,
            }
            self.client_data_middleware = RabbitMQMiddlewareQueue(
                queue_name=self.client_data_queue_name,
                **connection_params,
            )
            self._client_data_thread = threading.Thread(
                target=self._consume_client_data,
                daemon=True,
            )
            self._client_data_thread.start()

        logger.info(
            "TopClientsBirthdaysAggregator configured (client_data_queue=%s)",
            self.client_data_queue_name or 'disabled',
        )

    # ------------------------------------------------------------------
    # Auxiliary source consumption
    # ------------------------------------------------------------------

    def _consume_client_data(self) -> None:
        assert self.client_data_middleware is not None

        def on_message(message: Any) -> None:
            if self.shutdown_requested and self.client_data_middleware:
                self.client_data_middleware.stop_consuming()
                return

            if is_eof_message(message):
                logger.debug("EOF received from client data source: %s", message)
                return

            client_id, actual_data = extract_client_metadata(message)
            if not client_id:
                logger.warning("Client data message without client_id ignored: %s", message)
                return

            records = actual_data if isinstance(actual_data, list) else [actual_data]
            lookup = self._client_birthdays[client_id]

            for record in records:
                if not isinstance(record, dict):
                    continue
                try:
                    user_id = safe_int_conversion(record.get('user_id'))
                except Exception:  # noqa: BLE001
                    logger.debug("Invalid client data payload skipped: %s", record)
                    continue

                if user_id <= 0:
                    continue

                birthdate = str(record.get('birthdate', '') or '').strip()
                if birthdate:
                    lookup[user_id] = birthdate

        try:
            self.client_data_middleware.start_consuming(on_message)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error consuming client data queue: %s", exc)

    # ------------------------------------------------------------------
    # Main stream processing
    # ------------------------------------------------------------------

    def _accumulate_partial(self, client_id: str, payload: Dict[str, Any]) -> None:
        results = payload.get('results')
        if not isinstance(results, list):
            logger.warning("Partial top clients payload missing results list: %s", payload)
            return

        counts = self._client_counts[client_id]

        for entry in results:
            if not isinstance(entry, dict):
                continue
            try:
                store_id = safe_int_conversion(entry.get('store_id'))
                user_id = safe_int_conversion(entry.get('user_id'))
                purchases_qty = safe_int_conversion(entry.get('purchases_qty'))
            except Exception:  # noqa: BLE001
                logger.debug("Invalid partial entry skipped: %s", entry)
                continue

            if store_id <= 0 or user_id <= 0 or purchases_qty <= 0:
                continue

            counts[store_id][user_id] += purchases_qty

    def process_message(self, message: Any):
        if not isinstance(message, dict):
            logger.debug("Ignoring non-dict payload from partial results: %s", type(message))
            return

        client_id = self.current_client_id or ''
        if not client_id:
            logger.warning("Partial results received without client metadata")
            return

        self._accumulate_partial(client_id, message)

    def process_batch(self, batch: Any):
        client_id = self.current_client_id or ''
        if not client_id:
            logger.warning("Batch of partial results received without client metadata")
            return

        for entry in batch:
            if isinstance(entry, dict):
                self._accumulate_partial(client_id, entry)

    # ------------------------------------------------------------------
    # Result emission
    # ------------------------------------------------------------------

    def handle_eof(self, message: Dict[str, Any]):
        client_id = message.get('client_id') or self.current_client_id
        if not client_id:
            logger.warning("EOF received without client_id in TopClientsBirthdaysAggregator")
            return

        counts_by_store = self._client_counts.pop(client_id, {})
        birthdays = self._client_birthdays.get(client_id, {})

        results: list[Dict[str, Any]] = []

        for store_id, user_counts in counts_by_store.items():
            ranked = sorted(
                user_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )[:3]

            for user_id, purchases_qty in ranked:
                results.append(
                    {
                        'store_id': store_id,
                        'user_id': user_id,
                        'birthdate': birthdays.get(user_id, ''),
                        'purchases_qty': purchases_qty,
                    }
                )

        results.sort(
            key=lambda row: (
                row['store_id'],
                -row['purchases_qty'],
                row['user_id'],
            )
        )

        payload = {
            'type': 'top_clients_birthdays',
            'results': results,
        }

        self.send_message(payload, client_id=client_id)
        logger.info(
            "TopClientsBirthdaysAggregator emitted %s rows for client %s",
            len(results),
            client_id,
        )

        self.send_eof(client_id=client_id, additional_data={'source': 'top_clients_birthdays'})
        self._client_birthdays.pop(client_id, None)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup(self):
        super().cleanup()
        if self.client_data_middleware:
            try:
                self.client_data_middleware.stop_consuming()
            except Exception:  # noqa: BLE001
                pass
            try:
                self.client_data_middleware.close()
            except Exception:  # noqa: BLE001
                pass

if __name__ == '__main__':
    run_main(TopClientsBirthdaysAggregator)
