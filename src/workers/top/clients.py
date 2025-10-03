#!/usr/bin/env python3

"""Worker that aggregates top clients per store."""

import logging
import os
import threading
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Tuple

from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue
from workers.worker_utils import (
    extract_client_metadata,
    is_eof_message,
    run_main,
    safe_int_conversion,
)
from workers.top.top_worker import TopWorker


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopClientsWorker(TopWorker):
    """Aggregates the top-N clients (by purchases) for every store."""

    def __init__(self) -> None:
        super().__init__()

        self.top_n = safe_int_conversion(os.getenv('TOP_LEVELS', 3)) or 3

        connection_params = {
            'host': self.config.rabbitmq_host,
            'port': self.config.rabbitmq_port,
            'prefetch_count': self.config.prefetch_count,
        }

        self.stores_queue_name = os.getenv('STORES_QUEUE', '').strip()
        self.users_queue_name = os.getenv('USERS_QUEUE', '').strip()

        self.stores_middleware = (
            RabbitMQMiddlewareQueue(
                queue_name=self.stores_queue_name,
                **connection_params,
            )
            if self.stores_queue_name
            else None
        )
        self.users_middleware = (
            RabbitMQMiddlewareQueue(
                queue_name=self.users_queue_name,
                **connection_params,
            )
            if self.users_queue_name
            else None
        )

        self._stores_thread: threading.Thread | None = None
        self._users_thread: threading.Thread | None = None

        self._client_store_lookup: Dict[str, Dict[int, str]] = defaultdict(dict)
        self._client_user_lookup: Dict[str, Dict[int, str]] = defaultdict(dict)
        self._client_counts: Dict[
            str, Dict[int, Dict[int, int]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

        logger.info(
            "TopClientsWorker configured (top_n=%s, stores_queue=%s, users_queue=%s)",
            self.top_n,
            self.stores_queue_name or 'N/A',
            self.users_queue_name or 'N/A',
        )

    # ------------------------------------------------------------------
    # Auxiliary source consumption
    # ------------------------------------------------------------------

    def _consume_source(
        self,
        middleware: RabbitMQMiddlewareQueue,
        source_name: str,
        handler,
    ) -> None:
        """Continuously consume data from a secondary source in a background thread."""

        def on_message(message: Any) -> None:
            if self.shutdown_requested:
                middleware.stop_consuming()
                return

            if is_eof_message(message):
                logger.debug("EOF received from %s source: %s", source_name, message)
                return

            client_id, actual_data = extract_client_metadata(message)
            if not client_id:
                logger.warning(
                    "Received %s message without client metadata; skipping: %s",
                    source_name,
                    message,
                )
                return

            if isinstance(actual_data, list):
                handler(client_id, actual_data)
            else:
                handler(client_id, [actual_data])

        try:
            middleware.start_consuming(on_message)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error consuming %s source: %s", source_name, exc)

    def _start_auxiliary_consumers(self) -> None:
        if self.stores_middleware:
            self._stores_thread = threading.Thread(
                target=self._consume_source,
                args=(self.stores_middleware, 'stores', self._handle_store_records),
                daemon=True,
            )
            self._stores_thread.start()

        if self.users_middleware:
            self._users_thread = threading.Thread(
                target=self._consume_source,
                args=(self.users_middleware, 'users', self._handle_user_records),
                daemon=True,
            )
            self._users_thread.start()

    # ------------------------------------------------------------------
    # Accumulators
    # ------------------------------------------------------------------

    def _handle_store_records(self, client_id: str, records: Iterable[Dict[str, Any]]) -> None:
        lookup = self._client_store_lookup[client_id]
        for store in records:
            try:
                store_id = safe_int_conversion(store.get('store_id'))
            except Exception:  # noqa: BLE001
                logger.debug("Invalid store payload skipped: %s", store)
                continue

            if store_id <= 0:
                continue

            store_name = str(store.get('store_name', '') or '').strip()
            if not store_name:
                continue

            lookup[store_id] = store_name

    def _handle_user_records(self, client_id: str, records: Iterable[Dict[str, Any]]) -> None:
        lookup = self._client_user_lookup[client_id]
        for user in records:
            try:
                user_id = safe_int_conversion(user.get('user_id'))
            except Exception:  # noqa: BLE001
                logger.debug("Invalid user payload skipped: %s", user)
                continue

            if user_id <= 0:
                continue

            birthdate = str(user.get('birthdate', '') or '').strip()
            if not birthdate:
                continue

            lookup[user_id] = birthdate

    def _increment_transaction(self, client_id: str, transaction: Dict[str, Any]) -> None:
        try:
            store_id = safe_int_conversion(transaction.get('store_id'))
            user_id = safe_int_conversion(transaction.get('user_id'))
        except Exception:  # noqa: BLE001
            logger.debug("Invalid transaction skipped: %s", transaction)
            return

        if store_id <= 0 or user_id <= 0:
            return

        self._client_counts[client_id][store_id][user_id] += 1

    # ------------------------------------------------------------------
    # BaseWorker hooks
    # ------------------------------------------------------------------

    def process_message(self, message: Any):
        if not isinstance(message, dict):
            logger.debug("Unexpected transaction payload type: %s", type(message))
            return

        client_id = self.current_client_id or ''
        if not client_id:
            logger.warning("Transaction received without client metadata")
            return

        self._increment_transaction(client_id, message)

    def process_batch(self, batch: List[Any]):
        client_id = self.current_client_id or ''
        if not client_id:
            logger.warning("Batch received without client metadata")
            return

        for transaction in batch:
            if isinstance(transaction, dict):
                self._increment_transaction(client_id, transaction)

    # ------------------------------------------------------------------
    # Result emission
    # ------------------------------------------------------------------

    def _compute_results(self, client_id: str) -> List[Dict[str, Any]]:
        store_lookup = self._client_store_lookup.get(client_id, {})
        user_lookup = self._client_user_lookup.get(client_id, {})
        counts_for_client = self._client_counts.get(client_id, {})

        results: List[Dict[str, Any]] = []

        for store_id, user_counts in counts_for_client.items():
            store_name = store_lookup.get(store_id)
            if not store_name:
                continue

            sorted_users: List[Tuple[int, int]] = sorted(
                user_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )

            for user_id, purchases_qty in sorted_users[: self.top_n]:
                birthdate = user_lookup.get(user_id, '')
                results.append(
                    {
                        'store_id': store_id,
                        'store_name': store_name,
                        'user_id': user_id,
                        'birthdate': birthdate,
                        'purchases_qty': purchases_qty,
                    }
                )

        results.sort(
            key=lambda item: (
                item['store_name'],
                -item['purchases_qty'],
                item['birthdate'],
                item['user_id'],
            )
        )

        return results

    def handle_eof(self, message: Dict[str, Any]):
        client_id = message.get('client_id') or self.current_client_id
        if not client_id:
            logger.warning("EOF received without client_id in TopClientsWorker")
            return

        results = self._compute_results(client_id)
        payload = {
            'type': 'top_clients_birthdays',
            'results': results,
        }

        self.send_message(payload, client_id=client_id)
        logger.info(
            "TopClientsWorker emitted %s rows for client %s",
            len(results),
            client_id,
        )

        self.send_eof(client_id=client_id, additional_data={'source': 'top_clients'})
        self._cleanup_client(client_id)

    def _cleanup_client(self, client_id: str) -> None:
        self._client_counts.pop(client_id, None)
        self._client_store_lookup.pop(client_id, None)
        self._client_user_lookup.pop(client_id, None)
        self.reset_client_state(client_id)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start_consuming(self):
        self._start_auxiliary_consumers()
        try:
            super().start_consuming()
        finally:
            self.cleanup()

    def cleanup(self):
        super().cleanup()
        for middleware in (self.stores_middleware, self.users_middleware):
            if middleware:
                try:
                    middleware.stop_consuming()
                except Exception:  # noqa: BLE001
                    pass
                try:
                    middleware.close()
                except Exception:  # noqa: BLE001
                    pass


def main() -> None:
    run_main(TopClientsWorker)


if __name__ == '__main__':
    main()
