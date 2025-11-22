#!/usr/bin/env python3

"""Filter EOF Barrier Worker that outputs single EOF after receiving all EOFs from replicas."""

import logging
import os
from typing import Any, Dict, Tuple, Optional
import uuid

from workers.base_worker import BaseWorker
from workers.utils.worker_utils import run_main
from workers.utils.message_utils import ClientId, is_eof_message, extract_message_uuid
from workers.utils.eof_counter_store import EOFCounterStore

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
        
        logger.info(
            f"Filter EOF Barrier initialized - Input: {self.middleware_config.get_input_target()}, "
            f"Output: {self.middleware_config.get_output_target()}, "
            f"Replica count: {self.replica_count}"
        )
        logger.info(
            f"\033[33m[FILTER-EOF-BARRIER] Forwarding messages immediately, outputing single EOF after receiving all EOFs from replicas\033[0m"
        )
        logger.info(
            f"\033[33m[FILTER-EOF-BARRIER] Robust deduplication enabled with two-phase commit persistence to handle message retries from filter workers\033[0m"
        )
    
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
        
        try:
            # Forward batch immediately without accumulating
            if isinstance(batch, list) and batch:
                self.send_message(client_id=client_id, data=batch)
                logger.debug(
                    f"[FILTER-EOF-BARRIER] Forwarded batch of {len(batch)} messages immediately for client {client_id}"
                )
            elif batch:
                # Single message in batch format
                self.send_message(client_id=client_id, data=batch)
            
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
        # Get message_uuid and replica_id from message
        if not message_uuid:
            message_uuid = extract_message_uuid(message)
         
        # Deduplicate using EOFCounterStore
        if self._eof_counter_store.has_processed(client_id, message_uuid):
            logger.info(
                f"\033[33m[FILTER-EOF-BARRIER] Duplicate EOF {message_uuid} for client {client_id} detected; skipping\033[0m"
            )
            return  # Don't increment counter if duplicate

        # Mark this EOF as processed to prevent duplicate processing
        self._eof_counter_store.mark_processed(client_id, message_uuid)
        
        # Increment persistent counter (automatically loads from disk if needed)
        current_count = self._eof_counter_store.increment_counter(client_id)
        
        # Log when EOF is received (cyan color)
        logger.info(
            f"\033[36m[FILTER-EOF-BARRIER] Received EOF marker for client {client_id} "
            f"with message_uuid {message_uuid} - "
            f"count: {current_count}/{self.replica_count}\033[0m"
        )
        
        # Check if we've received EOFs from all replicas
        if current_count >= self.replica_count:
            # Log when all EOFs are received (bright green)
            logger.info(
                f"\033[92m[FILTER-EOF-BARRIER] All EOFs received for client {client_id} "
                f"({current_count}/{self.replica_count}), propagating EOF\033[0m"
            )
            
            # Propagate EOF to output with new message_uuid
            new_message_uuid_from_eof_barrier = str(uuid.uuid4())
            self.eof_handler.output_eof(client_id=client_id, message_uuid=new_message_uuid_from_eof_barrier)
            logger.info(
                f"\033[32m[FILTER-EOF-BARRIER] EOF propagated for client {client_id} "
                f"with new message_uuid {new_message_uuid_from_eof_barrier}\033[0m"
            )
            
            # CRITICAL: Clear processed state AFTER propagating EOF
            # This ensures that if the worker crashes, the state is already cleared
            # and won't interfere with the next client processing.
            # We clear ALL processed EOF UUIDs and reset counter when all EOFs are received.
            logger.info(f"\033[32m[FILTER-EOF-BARRIER] Clearing processed EOF state for client {client_id} after propagating EOF\033[0m")
            self._eof_counter_store.clear_client(client_id)

if __name__ == "__main__":
    run_main(FilterEOFBarrier)

