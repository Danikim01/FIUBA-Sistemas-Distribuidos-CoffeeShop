"""Shared utilities for top aggregation workers."""

from __future__ import annotations

from abc import abstractmethod
from asyncio.log import logger
import threading
from typing import Any, Dict, TypeVar

from workers.utils.message_utils import ClientId # pyright: ignore[reportMissingImports]
from workers.utils.worker_utils import get_payload_len # pyright: ignore[reportMissingImports]
from workers.base_worker import BaseWorker

StateDict = Dict[str, Any]
T = TypeVar("T")


class ProcessWorker(BaseWorker):
    """Base class for workers that process data (more than just forwarding it)."""

    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload: bool = True
        self.chunk_size: int = 1000
        self._state_lock = threading.RLock()

    @abstractmethod
    def process_transaction(self, client_id: ClientId, payload: Dict[str, Any]) -> None:
        """Accumulate data from a single transaction payload."""
        pass

    @abstractmethod
    def create_payload(self, client_id: ClientId) -> list[Dict[str, Any]]:
        """Create the payload for the output message."""
        pass

    @abstractmethod
    def reset_state(self, client_id: ClientId) -> None:
        """Reset the internal state for a given client."""
        pass

    def gateway_type_metadata(self) -> dict:
        """For aggregators to override and provide type metadata."""
        return {}

    def send_payload(self, payload: list[Dict[str, Any]], client_id: ClientId):
        """Send the payload to the output middleware."""
        type_metadata = self.gateway_type_metadata()
        if type_metadata:
            logger.info(f"[AGGREGATOR WORKER] Sending with type metadata: {type_metadata}")
        self.send_message(client_id=client_id, data=payload, type_metadata=type_metadata)
        logger.info(
            "%s emitted %s result(s) for client %s",
            self.__class__.__name__,
            get_payload_len(payload),
            client_id
        )
    
    def process_batch(self, batch: list, client_id: ClientId):
        with self._state_lock:
            for entry in batch:
                self.process_transaction(client_id, entry)

    @staticmethod
    def _chunk_payload(payload: list[Dict[str, Any]], chunk_size: int) -> list[list[Dict[str, Any]]]:
        """Chunk the payload into smaller lists of a given size."""
        return [payload[i:i + chunk_size] for i in range(0, len(payload), chunk_size)]
