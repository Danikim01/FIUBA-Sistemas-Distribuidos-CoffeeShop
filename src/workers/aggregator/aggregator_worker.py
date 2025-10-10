"""Shared utilities for top aggregation workers."""

from __future__ import annotations

from abc import abstractmethod
from asyncio.log import logger
from typing import Any, Dict, TypeVar

from message_utils import ClientId
from worker_utils import get_payload_len
from workers.base_worker import BaseWorker

StateDict = Dict[str, Any]
T = TypeVar("T")


class AggregatorWorker(BaseWorker):
    """Base class for single-source top workers with per-client state helpers."""

    def __init__(self) -> None:
        super().__init__()
        self.chunk_payload: bool = True

    @abstractmethod
    def accumulate_transaction(self, client_id: ClientId, payload: Dict[str, Any]) -> None:
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
        self.send_message(data=payload, type_metadata=type_metadata)
        logger.info(
            "%s emitted %s result(s) for client %s",
            self.__class__.__name__,
            get_payload_len(payload),
            client_id,
        )

    # @override
    def handle_eof(self, message: Dict[str, Any]):
        payload = self.create_payload(self.current_client_id)
        if payload:
            self.reset_state(self.current_client_id)
            if self.chunk_payload:
                self.send_payload(payload, self.current_client_id)
            else:
                chunked_payloads = self._chunk_payload(payload, 5000)
                for chunk in chunked_payloads:
                    self.send_payload(chunk, self.current_client_id)

        super().handle_eof(message)

    def process_message(self, message: dict):
        self.accumulate_transaction(self.current_client_id, message)

    def process_batch(self, batch: list):
        for entry in batch:
            self.accumulate_transaction(self.current_client_id, entry)

    def _chunk_payload(self, payload: list[Dict[str, Any]], chunk_size: int) -> list[list[Dict[str, Any]]]:
        """Chunk the payload into smaller lists of a given size."""
        return [payload[i:i + chunk_size] for i in range(0, len(payload), chunk_size)]