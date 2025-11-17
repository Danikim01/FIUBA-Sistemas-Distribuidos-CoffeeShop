#!/usr/bin/env python3

"""Filter Aggregator Worker that aggregates messages from multiple filter worker replicas."""

import logging
import os
import threading
from typing import Any, Dict
from collections import defaultdict

from workers.base_worker import BaseWorker
from workers.utils.worker_utils import run_main
from workers.utils.message_utils import ClientId, is_eof_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilterAggregator(BaseWorker):
    """
    Aggregator worker that receives messages (data and EOFs) from filter workers
    and aggregates EOFs before propagating to output.
    
    Accumulates all data messages and sends them when all EOFs are received.
    When EOF count reaches REPLICA_COUNT, sends all accumulated messages and then EOF.
    """
    
    def __init__(self):
        """Initialize Filter Aggregator Worker."""
        super().__init__()
        self.replica_count = int(os.getenv('REPLICA_COUNT', '3'))
        self.end_of_file_received = {}  # {client_id: count}
        self.accumulated_messages = defaultdict(list)  # {client_id: [messages]}
        self.eof_lock = threading.Lock()
        self.messages_lock = threading.Lock()
        
        logger.info(
            f"Filter Aggregator initialized - Input: {self.middleware_config.get_input_target()}, "
            f"Output: {self.middleware_config.get_output_target()}, "
            f"Replica count: {self.replica_count}"
        )
        logger.info(
            f"\033[33m[FILTER-AGGREGATOR] DEBUG MODE: Accumulating messages until all EOFs received\033[0m"
        )
    
    def process_message(self, message: Any, client_id: ClientId):
        """
        Process regular data message - accumulate for later sending.
        
        Args:
            message: Message data to accumulate
            client_id: Client identifier
        """
        # Accumulate message instead of forwarding immediately
        with self.messages_lock:
            self.accumulated_messages[client_id].append(message)
        logger.debug(
            f"[FILTER-AGGREGATOR] Accumulated message for client {client_id} "
            f"(total: {len(self.accumulated_messages[client_id])})"
        )
    
    def process_batch(self, batch: list, client_id: ClientId):
        """
        Process batch of messages - accumulate for later sending.
        
        Args:
            batch: List of messages to accumulate
            client_id: Client identifier
        """
        # Accumulate batch instead of forwarding immediately
        with self.messages_lock:
            # If batch is a list of items, add each item
            if isinstance(batch, list):
                self.accumulated_messages[client_id].extend(batch)
            else:
                self.accumulated_messages[client_id].append(batch)
        logger.debug(
            f"[FILTER-AGGREGATOR] Accumulated batch of {len(batch) if isinstance(batch, list) else 1} "
            f"messages for client {client_id} "
            f"(total: {len(self.accumulated_messages[client_id])})"
        )
    
    def handle_eof(self, message: Dict[str, Any], client_id: ClientId):
        """
        Handle EOF message - count and propagate when all replicas have sent EOF.
        
        When all EOFs are received, sends all accumulated messages and then EOF.
        
        Args:
            message: EOF message
            client_id: Client identifier
        """
        # Increment EOF counter for this client
        with self.eof_lock:
            self.end_of_file_received[client_id] = self.end_of_file_received.get(client_id, 0) + 1
            current_count = self.end_of_file_received[client_id]
        
        # Log when EOF is received (cyan color)
        logger.info(
            f"\033[36m[FILTER-AGGREGATOR] Received EOF marker for client {client_id} - "
            f"count: {current_count}/{self.replica_count}\033[0m"
        )
        
        # Check if we've received EOFs from all replicas
        if current_count >= self.replica_count:
            # Log when all EOFs are received (bright green)
            logger.info(
                f"\033[92m[FILTER-AGGREGATOR] All EOFs received for client {client_id} "
                f"({current_count}/{self.replica_count}), sending accumulated messages\033[0m"
            )
            
            # Pause message processing to ensure no new messages arrive while sending
            with self._pause_message_processing():
                # Get accumulated messages for this client
                with self.messages_lock:
                    accumulated = self.accumulated_messages.pop(client_id, [])
                    message_count = len(accumulated)
                
                # Send all accumulated messages
                if accumulated:
                    logger.info(
                        f"\033[33m[FILTER-AGGREGATOR] Sending {message_count} accumulated messages "
                        f"for client {client_id}\033[0m"
                    )
                    
                    # Send messages in batches to avoid overwhelming the output
                    batch_size = 5000
                    for i in range(0, len(accumulated), batch_size):
                        batch = accumulated[i:i + batch_size]
                        self.send_message(client_id=client_id, data=batch)
                    
                    logger.info(
                        f"\033[32m[FILTER-AGGREGATOR] All {message_count} messages sent for client {client_id}\033[0m"
                    )
                else:
                    logger.warning(
                        f"\033[33m[FILTER-AGGREGATOR] No accumulated messages for client {client_id}\033[0m"
                    )
                
                # Now propagate EOF to output
                #self._propagate_eof(client_id)
                self.eof_handler.output_eof(client_id=client_id)
                
            # Reset counter for this client
            with self.eof_lock:
                self.end_of_file_received[client_id] = 0
    
    def _propagate_eof(self, client_id: ClientId):
        """
        Propagate EOF message to output (exchange or queue).
        
        Args:
            client_id: Client identifier
        """
        output_target = self.middleware_config.get_output_target()
        
        try:
            # For exchanges, use the exchange name as routing_key to ensure
            # the message reaches all queues bound to that exchange
            if self.middleware_config.has_output_exchange():
                routing_key = self.middleware_config.output_exchange
                logger.debug(
                    f"[FILTER-AGGREGATOR] Sending EOF to exchange '{routing_key}' "
                    f"with routing_key '{routing_key}' for client {client_id}"
                )
                self.send_message(
                    client_id=client_id,
                    data=None,
                    message_type='EOF',
                    routing_key=routing_key
                )
                # self.eof_handler.handle_eof_with_routing_key(client_id=client_id, routing_key=routing_key, message=None,exchange=self.middleware_config.output_exchange)
            else:
                # For queues, no routing_key needed
                self.send_message(client_id=client_id, data=None, message_type='EOF')
            
            # Log when EOF is sent (green color)
            logger.info(
                f"\033[32m[FILTER-AGGREGATOR] EOF sent to {output_target} for client {client_id}\033[0m"
            )
        except Exception as e:
            # Log error in red
            logger.error(
                f"\033[31m[FILTER-AGGREGATOR] Failed to propagate EOF to {output_target} for client {client_id}: {e}\033[0m",
                exc_info=True
            )
            raise


if __name__ == "__main__":
    run_main(FilterAggregator)

