"""Filter worker implementation for data filtering operations."""

from __future__ import annotations

import logging
import os
from abc import abstractmethod
from typing import Any, List, Optional, Tuple

from workers.base_worker import BaseWorker
from workers.utils.processed_message_store import ProcessedMessageStore

logger = logging.getLogger(__name__)


class FilterWorker(BaseWorker):
    """Base class for filter workers that apply filtering logic to messages."""

    def __init__(self) -> None:
        super().__init__()
        worker_label = f"{self.__class__.__name__}-{os.getenv('WORKER_ID', '0')}"
        self._processed_store = ProcessedMessageStore(worker_label)  


    @abstractmethod
    def apply_filter(self, item: Any) -> bool:
        """Apply filter logic to determine if an item should pass through.
        
        Args:
            item: Data item to filter
            
        Returns:
            True if item should pass through, False otherwise
        """
        raise NotImplementedError

    def _get_current_message_uuid(self) -> str | None:
        metadata = self._get_current_message_metadata()
        if not metadata:
            return None
        message_uuid = metadata.get("message_uuid")
        if not message_uuid:
            return None
        return str(message_uuid)

    def _check_duplicate(self, client_id: str) -> Tuple[bool, str | None]:
        message_uuid = self._get_current_message_uuid()
        if message_uuid and self._processed_store.has_processed(client_id, message_uuid):
            logger.info(
                "[FILTER] Duplicate message %s for client %s detected; skipping processing",
                message_uuid,
                client_id,
            )
            return True, message_uuid
        return False, message_uuid

    def _mark_processed(self, client_id: str, message_uuid: str | None) -> None:
        if message_uuid:
            self._processed_store.mark_processed(client_id, message_uuid)

    def process_message(self, message: Any, client_id: str):
        """Process a single message by applying filter.
        
        Args:
            message: Message to process
        """
        duplicate, message_uuid = self._check_duplicate(client_id)
        if duplicate:
            return

        try:
            if self.apply_filter(message):
                self.send_message(client_id=client_id, data=message)
        finally:
            self._mark_processed(client_id, message_uuid)

    def process_batch(self, batch: List[Any], client_id: str):
        """Process a batch by filtering and sending filtered results.
        
        Args:
            batch: List of messages to process
        """
        duplicate, message_uuid = self._check_duplicate(client_id)
        if duplicate:
            return

        try:
            filtered_items = [item for item in batch if self.apply_filter(item)]
            if filtered_items:
                self.send_message(client_id=client_id, data=filtered_items)
        finally:
            self._mark_processed(client_id, message_uuid)

    def handle_eof(self, message: dict, client_id: str):
        """
        Clear processed state and send EOF to filter aggregator.
        
        The aggregator will count EOFs from all replicas and propagate when complete.
        """
        self._processed_store.clear_client(client_id)
        
        # Send EOF to filter aggregator
        #self.send_message(client_id=client_id, data=None, message_type='EOF')
        self.eof_handler.output_eof(client_id=client_id)
        logger.info(f"\033[36m[FILTER] EOF sent to aggregator for client {client_id}\033[0m")
        