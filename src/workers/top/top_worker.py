"""Shared utilities for top aggregation workers."""

from abc import abstractmethod
from asyncio.log import logger
from typing import Any, Dict, TypeVar
from message_utils import ClientId
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

    # @overwrite
    def handle_eof(self, message: Dict[str, Any]):
        client_id = message.get('client_id', self.current_client_id)
        self.eof_handler.handle_eof(message, client_id)

        payload = self.create_payload(client_id)
        if payload is None or len(payload) == 0:
            return

        self.reset_state(client_id)
        self.send_message(payload, client_id=client_id)
        logger.info(
            "%s emitted %s result(s) for client %s",
            self.__class__.__name__,
            len(payload) if hasattr(payload, "__len__") else 0,
            client_id,
        )
        

    def process_message(self, message: dict):
        client_id = self.current_client_id or message.get('client_id', '')
        if not client_id or client_id == '':
            logger.warning("Transaction received without client metadata")
            return

        self.accumulate_transaction(client_id, message)

    def process_batch(self, batch: Any):
        client_id = self.current_client_id or ''
        if not client_id:
            logger.warning("Batch received without client metadata")
            return

        for entry in batch:
            self.accumulate_transaction(client_id, entry)
