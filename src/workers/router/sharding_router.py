#!/usr/bin/env python3

"""Sharding router that distributes transactions to workers based on store_id or item_id."""

import logging
import os
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional

from workers.utils.worker_utils import run_main
from workers.utils.sharding_utils import (
    extract_item_id_from_payload,
    extract_store_id_from_payload,
    get_routing_key_by_store_id,
    get_routing_key_by_item_id,
)
from workers.utils.message_utils import ClientId
from workers.utils.processed_message_store import ProcessedMessageStore
from workers.base_worker import BaseWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ShardingRouter(BaseWorker):
    """
    Router that receives transactions and distributes them to sharded workers
    based on a configurable routing field (store_id by default). Incoming
    batches are immediately regrouped per shard and forwarded without retaining
    local batch state.
    """

    def __init__(self, routing_field: str = "store_id"):
        super().__init__()
        self.num_shards = int(os.getenv("NUM_SHARDS", "2"))
        if routing_field not in {"store_id", "item_id"}:
            raise ValueError(f"Unsupported routing_field: {routing_field}")
        self.routing_field = routing_field

        worker_label = f"{self.__class__.__name__}-{os.getenv('WORKER_ID', '0')}"
        self._processed_store = ProcessedMessageStore(worker_label)

        logger.info(
            "ShardingRouter initialized with %s shards (routing by %s)",
            self.num_shards,
            self.routing_field,
        )
        logger.info(
            "\033[33m[SHARDING-ROUTER] Robust deduplication enabled with two-phase commit persistence "
            "to handle batch retries\033[0m"
        )

    def _clear_client_state(self, client_id: ClientId) -> None:
        """Remove persistence for a single client."""
        logger.info(f"[CONTROL] Clearing deduplication state for client {client_id}")
        self._processed_store.clear_client(client_id)

    def _get_current_message_uuid(self) -> str | None:
        """Get the message UUID from the current message metadata."""
        metadata = self._get_current_message_metadata()
        if not metadata:
            logger.debug("[SHARDING-ROUTER] No metadata available for message UUID extraction")
            return None
        message_uuid = metadata.get("message_uuid")
        if not message_uuid:
            logger.debug(f"[SHARDING-ROUTER] No message_uuid found in metadata: {metadata.keys()}")
            return None
        return str(message_uuid)


    def _mark_processed(self, client_id: str, message_uuid: str | None) -> None:
        """Mark a message as processed."""
        if message_uuid:
            self._processed_store.mark_processed(client_id, message_uuid)

    def _compute_routing_key_by_store_id(self, message: Any) -> str | None:
        """Compute routing key using store_id."""
        store_id = extract_store_id_from_payload(message)
        if store_id is None:
            return None
        return get_routing_key_by_store_id(store_id, self.num_shards)

    def _compute_routing_key_by_item_id(self, message: Any) -> str | None:
        """Compute routing key using item_id."""
        item_id = extract_item_id_from_payload(message)
        if item_id is None:
            return None
        return get_routing_key_by_item_id(item_id, self.num_shards)

    def _compute_routing_key(self, message: Any) -> str | None:
        """Return the routing key for a message or None if it cannot be routed."""
        if self.routing_field == "item_id":
            return self._compute_routing_key_by_item_id(message)
        return self._compute_routing_key_by_store_id(message)

    def _log_missing_routing_key(self, message: Any) -> None:
        """Log why routing key could not be computed for this message."""
        logger.warning(
            "Transaction without %s, skipping. Message keys: %s",
            self.routing_field,
            list(message.keys()) if isinstance(message, dict) else "Not a dict",
        )

    def process_batch(self, batch: list, client_id: ClientId) -> None:
        """
        Process an incoming batch by grouping its messages by shard and forwarding
        each shard-specific batch immediately. No state is kept between batches.
        """
        message_uuid = self._get_current_message_uuid()
        batch_size = len(batch) if isinstance(batch, list) else 1

        logger.debug(
            "[SHARDING-ROUTER] Received batch for client %s, size: %s, message_uuid: %s",
            client_id,
            batch_size,
            message_uuid,
        )

        if message_uuid and self._processed_store.has_processed(client_id, message_uuid):
            logger.info(
                "\033[31m[SHARDING-ROUTER] Duplicate batch %s for client %s detected; skipping %s messages\033[0m",
                message_uuid,
                client_id,
                batch_size,
            )
            return

        try:
            shard_batches: Dict[str, List[Any]] = defaultdict(list)
            for message in batch:
                routing_key = self._compute_routing_key(message)
                if routing_key is None:
                    self._log_missing_routing_key(message)
                    continue
                shard_batches[routing_key].append(message)

            base_uuid = message_uuid or str(uuid.uuid4())
            for routing_key, shard_batch in shard_batches.items():
                if not shard_batch:
                    continue
                outgoing_uuid = self.add_sharding_id_to_uuid_if_missing(base_uuid, routing_key)
                self.send_message(
                    client_id,
                    shard_batch,
                    routing_key=routing_key,
                    message_uuid=outgoing_uuid,
                )

            if message_uuid:
                self._mark_processed(client_id, message_uuid)
        except Exception as e:
            logger.error(
                "\033[31m[SHARDING-ROUTER] Failed to process batch for client %s, message_uuid: %s: %s\033[0m",
                client_id,
                message_uuid,
                e,
            )
            raise

    def handle_eof(self, message: Dict[str, Any], client_id: ClientId, message_uuid: Optional[str] = None) -> None:
        """
        Handle EOF by sending it to all shards. Since batches are forwarded
        immediately, there's nothing to flush here.
        """
        logger.info(
            "\033[36m[SHARDING-ROUTER] Received EOF for client %s, sending EOF to all shards\033[0m",
            client_id,
        )

        with self._pause_message_processing():
            self._clear_client_state(client_id)

            for shard_id in range(self.num_shards):
                routing_key = f"shard_{shard_id}"
                logger.info(
                    "\033[36m[SHARDING-ROUTER] Sending EOF to shard %s for client %s\033[0m",
                    routing_key,
                    client_id,
                )
                try:
                    new_message_uuid_from_sharding_router = str(uuid.uuid4())
                    self.eof_handler.handle_eof_with_routing_key(
                        client_id=client_id,
                        routing_key=routing_key,
                        message=message,
                        exchange=self.middleware_config.output_exchange,
                        message_uuid=new_message_uuid_from_sharding_router,
                    )
                    logger.debug(
                        "[SHARDING-ROUTER] EOF sent successfully to %s for client %s",
                        routing_key,
                        client_id,
                    )
                except Exception as e:
                    logger.error(
                        "\033[31m[SHARDING-ROUTER] Failed to send EOF to %s for client %s: %s\033[0m",
                        routing_key,
                        client_id,
                        e,
                        exc_info=True,
                    )
                    raise

            logger.info(
                "\033[32m[SHARDING-ROUTER] EOF propagation completed for client %s to all %s shards\033[0m",
                client_id,
                self.num_shards,
            )

    def add_sharding_id_to_uuid_if_missing(self, message_uuid: str, routing_key: str) -> str:
        """Ensure the message UUID includes the sharding ID."""
        if f"-{routing_key}" not in message_uuid:
            return f"{message_uuid}-{routing_key}"
        return message_uuid

    def cleanup(self) -> None:
        """Clean up resources."""
        logger.info("Cleaning up ShardingRouter")
        super().cleanup()

    def _send_control_to_output(self, message: Dict[str, Any]) -> None:
        """
        Base worker method overridden to propagate control messages to every shard when the router publishes to an exchange.
        """
        for shard_id in range(self.num_shards):
            routing_key = f"shard_{shard_id}"
            shard_message = dict(message)
            logger.info(
                "[CONTROL] Propagating control message to %s with message_uuid %s",
                routing_key,
                shard_message["message_uuid"],
            )
            try:
                self.middleware_config.output_middleware.send(
                    shard_message,
                    routing_key=routing_key,
                )
            except Exception as exc:
                logger.error(
                    "[CONTROL] Failed to propagate control message to %s: %s",
                    routing_key,
                    exc,
                    exc_info=True,
                )

    def handle_client_reset(self, client_id: ClientId) -> None:
        """Drop any client-specific deduplication files when instructed."""
        logger.info(f"[CONTROL] Client reset received for {client_id} on ShardingRouter")
        self._clear_client_state(client_id)

    def handle_reset_all_clients(self) -> None:
        """Remove deduplication state for every client."""
        logger.info("[CONTROL] Global reset received on ShardingRouter, clearing all deduplication state")
        self._processed_store.clear_all()


if __name__ == '__main__':
    run_main(ShardingRouter)
