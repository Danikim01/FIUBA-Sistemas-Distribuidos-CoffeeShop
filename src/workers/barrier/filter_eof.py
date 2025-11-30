#!/usr/bin/env python3

"""Filter EOF Barrier Worker that outputs single EOF after receiving all EOFs from replicas."""

import logging
import os
from typing import Any, Dict, Optional
import uuid

from workers.base_worker import BaseWorker
from workers.utils.worker_utils import run_main
from workers.utils.message_utils import ClientId, extract_message_uuid
from common.persistence.eof_counter_store import EOFCounterStore
from common.persistence.processed_message_store import ProcessedMessageStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilterEOFBarrier(BaseWorker):
    """
    Barrier worker that receives messages (data and EOFs) from filter workers
    and outputs a single EOF after receiving all EOFs from replicas.
    
    Forwards data messages immediately without accumulating them in memory.
    When EOF count reaches REPLICA_COUNT, propagates EOF to output.
    
    Includes deduplication to handle message retries when workers are killed.
    """
    
    def __init__(self):
        """Initialize Filter EOF Barrier Worker."""
        super().__init__()
        self.replica_count = int(os.getenv('REPLICA_COUNT', '3'))
        
        worker_label = f"{self.__class__.__name__}-{os.getenv('WORKER_ID', '0')}"        
        self._eof_counter_store = EOFCounterStore(worker_label)
        
        self._processed_store = ProcessedMessageStore(worker_label)
        logger.info(
            "\033[33m[FILTER-EOF-BARRIER] Batch deduplication enabled with ProcessedMessageStore "
            "to handle duplicate batches from sharding routers\033[0m"
        )
        
        self._transaction_counters: Dict[str, int] = {}
        self._duplicate_counters: Dict[str, int] = {}
        
        logger.info(
            f"Filter EOF Barrier initialized - Input: {self.middleware_config.get_input_target()}, "
            f"Output: {self.middleware_config.get_output_target()}, "
            f"Replica count: {self.replica_count}"
        )

    def _clear_client_state(self, client_id: ClientId) -> None:
        """Remove all in-memory and persisted state for a client."""
        logger.info(f"[CONTROL] Clearing FilterEOFBarrier state for client {client_id}")
        self._eof_counter_store.clear_client(client_id)
        self._processed_store.clear_client(client_id)
        self._transaction_counters.pop(client_id, None)
        self._duplicate_counters.pop(client_id, None)

    def _get_current_message_uuid(self) -> str | None:
        """Get the message UUID from the current message metadata."""
        metadata = self._get_current_message_metadata()
        if not metadata:
            logger.info(f"\033[33m[FILTER-EOF-BARRIER] No metadata available for message UUID extraction\033[0m")
            return None
        message_uuid = metadata.get("message_uuid")
        if not message_uuid:
            logger.info(f"\033[33m[FILTER-EOF-BARRIER] No message_uuid found in metadata: {metadata.keys()}\033[0m")
            return None
        return str(message_uuid)
    
    
    
    def process_batch(self, batch: list, client_id: ClientId):
        """
        Process batch of messages - forward immediately.
        
        Args:
            batch: List of messages to forward
            client_id: Client identifier
        """
        message_uuid = self._get_current_message_uuid()
        batch_size = len(batch) if isinstance(batch, list) else 1
                
        if not message_uuid:
            logger.error(
                f"\033[31m[FILTER-EOF-BARRIER] CRITICAL: Batch has no message_uuid! "
                f"Cannot deduplicate. Client: {client_id}, Batch size: {batch_size}. "
                f"This batch will be forwarded without deduplication check - potential duplicate!\033[0m"
            )
         
        
        if message_uuid:
            if self._processed_store.has_processed(client_id, message_uuid):
                self._duplicate_counters[client_id] = self._duplicate_counters.get(client_id, 0) + batch_size
                logger.warning(
                    f"\033[31m[FILTER-EOF-BARRIER] Duplicate batch detected! "
                    f"UUID: {message_uuid}, Client: {client_id}, Batch size: {batch_size} messages - SKIPPING\033[0m"
                )
                logger.info(
                    f"[FILTER-EOF-BARRIER] Duplicate stats for client {client_id}: "
                    f"{self._duplicate_counters.get(client_id, 0)} duplicate messages so far"
                )
                return
        
        try:
            if isinstance(batch, list) and batch:
                self.send_message(client_id=client_id, data=batch)
                logger.debug(
                    f"[FILTER-EOF-BARRIER] Forwarded batch of {len(batch)} messages immediately for client {client_id}"
                )
            elif batch:
                self.send_message(client_id=client_id, data=batch)
            
            self._transaction_counters[client_id] = self._transaction_counters.get(client_id, 0) + batch_size
            
            self._processed_store.mark_processed(client_id, message_uuid)

        except Exception as e:
            logger.error(
                f"\033[31m[FILTER-EOF-BARRIER] Failed to forward batch for client {client_id}, "
                f"{e}\033[0m"
            )
            raise
    
    def handle_eof(self, message: Dict[str, Any], client_id: ClientId, message_uuid: Optional[str] = None):
        """
        Handle EOF message - count and propagate when all replicas have sent EOF.        
        Args:
            message: EOF message
            client_id: Client identifier
            message_uuid: Optional message UUID from the original message
        """
        if not message_uuid:
            message_uuid = extract_message_uuid(message)
         
        if self._eof_counter_store.has_processed(client_id, message_uuid):
            logger.info(
                f"\033[33m[FILTER-EOF-BARRIER] Duplicate EOF {message_uuid} for client {client_id} detected; skipping\033[0m"
            )
            return

        self._eof_counter_store.mark_processed(client_id, message_uuid)
        
        current_count = self._eof_counter_store.increment_counter(client_id)
        
        logger.info(
            f"\033[36m[FILTER-EOF-BARRIER] Received EOF marker for client {client_id} "
            f"with message_uuid {message_uuid} - "
            f"count: {current_count}/{self.replica_count}\033[0m"
        )
        
        if current_count >= self.replica_count:
            logger.info(
                f"\033[92m[FILTER-EOF-BARRIER] All EOFs received for client {client_id} "
                f"({current_count}/{self.replica_count}), propagating EOF\033[0m"
            )
            
            new_message_uuid_from_eof_barrier = str(uuid.uuid4())
            self.eof_handler.output_eof(client_id=client_id, message_uuid=new_message_uuid_from_eof_barrier)
            logger.info(
                f"\033[32m[FILTER-EOF-BARRIER] EOF propagated for client {client_id} "
                f"with new message_uuid {new_message_uuid_from_eof_barrier}\033[0m"
            )
            
            total_forwarded = self._transaction_counters.get(client_id, 0)
            total_duplicates = self._duplicate_counters.get(client_id, 0)

            logger.info(
                f"\033[33m[FILTER-EOF-BARRIER] Final stats for client {client_id}: "
                f"Forwarded: {total_forwarded} messages, "
                f"Duplicates skipped: {total_duplicates} messages"
            )

            self._clear_client_state(client_id)

    def handle_client_reset(self, client_id: ClientId) -> None:
        """Delete any partial state for a client when instructed by the gateway."""
        logger.info(f"[CONTROL] Client reset received for {client_id}")
        self._clear_client_state(client_id)

    def handle_reset_all_clients(self) -> None:
        """Drop every cached counter when a global reset is requested."""
        logger.info("[CONTROL] Global reset received, clearing FilterEOFBarrier state")
        self._eof_counter_store.clear_all()
        self._processed_store.clear_all()
        self._transaction_counters.clear()
        self._duplicate_counters.clear()

if __name__ == "__main__":
    run_main(FilterEOFBarrier)
