import logging
import os
from typing import Any, Optional, Dict
from message_utils import ClientId, create_message_with_metadata, extract_eof_metadata
from middleware_config import MiddlewareConfig

logger = logging.getLogger(__name__)

Counter = Dict[int, int]  # [worker_id, count]

class EOFHandler:
    def __init__(self, middleware_config: MiddlewareConfig):
        self.max_retries: int = int(os.getenv('MAX_EOF_RETRIES', '5'))
        self.worker_id: int = int(os.getenv('WORKER_ID', '0'))
        self.replica_count: int = int(os.getenv('REPLICA_COUNT', '1'))
        self.middleware_config = middleware_config

    def handle_eof(self, message: Dict[str, Any], current_client_id: ClientId):
        """Handle EOF message. Can be overridden by subclasses.
        
        Args:
            message: EOF message dictionary
        """
        client_id = message.get('client_id', current_client_id)
        
        additional_data: Dict[str, Any] = extract_eof_metadata(message)
        counter: Counter = additional_data.get('counter', {})

        if counter.get(self.worker_id) is None:
            counter[self.worker_id] = 0
        counter[self.worker_id] += 1

        if self.should_output(counter):
            self.output_eof(client_id=client_id)
        else:
            self.requeue_eof(client_id=client_id, counter=counter)

    def should_output(self, counter: Counter) -> bool:
        """Determine if EOF should be output based on counter.

        Args:
            counter: Dictionary tracking how many workers have processed the EOF
        Returns:
            True if EOF should be output, False otherwise
        """
        if len(counter) >= self.replica_count:
            return True
        if any(count >= self.max_retries for count in counter.values()):
            return True
        return False

    def output_eof(self, client_id: ClientId):
        """Send EOF message to output with client metadata.
        
        Args:
            client_id: Client identifier
            additional_data: Additional data to include in EOF message
        """
        message = create_message_with_metadata(client_id, data=None, message_type='EOF')
        self.middleware_config.output_middleware.send(message)

    def requeue_eof(self, client_id: ClientId, counter: Counter):
        """Requeue an EOF message back to the input middleware.
        
        Args:
            message: EOF message dictionary
        """
        message = create_message_with_metadata(client_id, data=None, message_type='EOF')
        self.middleware_config.input_middleware.send(message)