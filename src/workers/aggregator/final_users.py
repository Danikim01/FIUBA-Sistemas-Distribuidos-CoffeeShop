#!/usr/bin/env python3

"""Aggregator that enriches top clients with birthdays and re-ranks globally."""

import logging
from typing import Any, Dict
from message_utils import ClientId
from worker_utils import run_main
from workers.aggregator.extra_source.users import UsersExtraSource
from workers.aggregator.extra_source.stores import StoresExtraSource
from workers.top.top_worker import TopWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TopClientsBirthdaysAggregator(TopWorker):
    """Aggregates top-client partials and injects client birthdays."""

    def __init__(self) -> None:
        super().__init__()
        self.stores_source = StoresExtraSource(self.middleware_config)
        self.stores_source.start_consuming()
        self.birthdays_source = UsersExtraSource(self.middleware_config)
        self.birthdays_source.start_consuming()
        self.recieved_payloads: Dict[ClientId, list[dict[str, Any]]] = {}

    def reset_state(self, client_id: ClientId) -> None:
        self.recieved_payloads[client_id] = []
    
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
        results: list[Dict[str, Any]] = []
        for entry in aggregated.values():
            user_id = entry["user_id"]
            store_id = entry["store_id"]

            birthdate = self.birthdays_source.get_item_when_done(
                client_id,
                str(user_id),
            )
            store_name = self.stores_source.get_item_when_done(client_id, store_id)

            results.append(
                {
                    "user_id": user_id,
                    "store_id": store_id,
                    "store_name": store_name,
                    "birthdate": birthdate,
                    "purchases_qty": entry["purchases_qty"],
                }
            )

        # Sort by store, then desc purchases, then user id for stable output
        results.sort(
            key=lambda row: (
                row["store_id"],
                -int(row.get("purchases_qty", 0) or 0),
                row["user_id"],
            )
        )

        logger.info(
            "%s aggregated %d unique client(s) for client %s",
            self.__class__.__name__,
            results,
            client_id,
        )
        return results


    def cleanup(self):
        super().cleanup()
        try:
            self.stores_source.close()
            self.birthdays_source.close()
        except Exception:  # noqa: BLE001
            logger.warning("Failed to close extra sources", exc_info=True)


if __name__ == '__main__':
    run_main(TopClientsBirthdaysAggregator)
