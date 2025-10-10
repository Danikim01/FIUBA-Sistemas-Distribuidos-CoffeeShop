"""Shared utilities for top aggregation workers."""

from abc import abstractmethod
from asyncio.log import logger
from typing import Any, Dict, TypeVar
from message_utils import ClientId
from worker_utils import get_payload_len
from workers.base_worker import BaseWorker

StateDict = Dict[str, Any]
T = TypeVar("T")

class TopWorker(BaseWorker):
    """Base class for single-source top workers with per-client state helpers."""

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
            "%s emitted %s result(s) for client %s\n%s",
            self.__class__.__name__,
            get_payload_len(payload),
            client_id, payload
        )
    
    # @override
    def handle_eof(self, message: Dict[str, Any]):
        payload = self.create_payload(self.current_client_id)
        if len(payload) > 0:
            self.reset_state(self.current_client_id)
            self.send_payload(payload, self.current_client_id)

        super().handle_eof(message)

    def process_message(self, message: dict):
        self.accumulate_transaction(self.current_client_id, message)

    def process_batch(self, batch: list):
        for entry in batch:
            self.accumulate_transaction(self.current_client_id, entry)
