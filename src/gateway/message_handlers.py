"""Message handlers module for processing different types of messages."""

import logging
import socket
from typing import Any, Dict, Tuple

from protocol import (
    DataType, send_response, parse_batch_message, parse_eof_message # type: ignore
)
from queue_manager import QueueManager

logger = logging.getLogger(__name__)


class MessageHandlers:
    """Handles different types of messages received by the gateway."""
    
    def __init__(self, queue_manager: QueueManager):
        self.queue_manager = queue_manager
    
    def handle_batch_message(self, client_socket: socket.socket, message_data: bytes, client_id: str) -> None:
        """Handle a batch of data rows with client identification."""
        try:
            data_type, rows = parse_batch_message(message_data)
            
            # Route the data to appropriate handler based on type
            if data_type == DataType.TRANSACTIONS:
                self._handle_transactions_batch(rows, client_id)
            elif data_type == DataType.STORES:
                self._handle_stores_batch(rows, client_id)
            elif data_type == DataType.USERS:
                self._handle_users_batch(rows, client_id)
            elif data_type == DataType.TRANSACTION_ITEMS:
                self._handle_transaction_items_batch(rows, client_id)
            elif data_type == DataType.MENU_ITEMS:
                self._handle_menu_items_batch(rows, client_id)
            else:
                logger.warning(f"Unknown data type: {data_type} for client {client_id}")
                send_response(client_socket, False)
                return
            
            # Send success response
            send_response(client_socket, True)
            
        except Exception as e:
            logger.error(f"Failed to process batch message for client {client_id}: {e}")
            send_response(client_socket, False)
    
    def handle_eof_message(
        self,
        client_socket: socket.socket,
        message_data: bytes,
        eof_state: Dict[DataType, bool],
        client_id: str
    ) -> Tuple[DataType | None, bool]:
        """Handle EOF message for a data type with client identification.

        Returns a tuple (data_type, is_new) where is_new indicates whether this is
        the first EOF received for the given type.
        """
        try:
            data_type = parse_eof_message(message_data)

            is_new = not eof_state[data_type]
            if is_new:
                eof_state[data_type] = True
                logger.info(f"Received EOF for {data_type.name} from client {client_id}.")
                
                # Propagate EOF to appropriate queues
                self._propagate_eof_for_data_type(data_type, client_id)
            else:
                logger.warning(f"Duplicate EOF received for {data_type.name} from client {client_id}")

            send_response(client_socket, True)
            return data_type, is_new

        except Exception as e:
            logger.error(f"Failed to process EOF message for client {client_id}: {e}")
            send_response(client_socket, False)
            return None, False
    
    def _handle_transactions_batch(self, rows: list, client_id: str) -> None:
        """Handle transactions batch processing."""
        logger.info(f"Processing {len(rows)} transactions with chunking for client {client_id}")
        self.queue_manager.send_transactions_chunks(rows, client_id)
    
    def _handle_stores_batch(self, rows: list, client_id: str) -> None:
        """Handle stores batch processing."""
        self.queue_manager.send_stores(rows, client_id)
    
    def _handle_users_batch(self, rows: list, client_id: str) -> None:
        """Handle users batch processing."""
        self.queue_manager.send_users(rows, client_id)
    
    def _handle_transaction_items_batch(self, rows: list, client_id: str) -> None:
        """Handle transaction items batch processing."""
        self.queue_manager.send_transaction_items_chunks(rows, client_id)
    
    def _handle_menu_items_batch(self, rows: list, client_id: str) -> None:
        """Handle menu items batch processing."""
        self.queue_manager.send_menu_items(rows, client_id)
    
    def _propagate_eof_for_data_type(self, data_type: DataType, client_id: str) -> None:
        """Propagate EOF message for the specified data type with client identification."""
        try:
            if data_type == DataType.TRANSACTIONS:
                self.queue_manager.propagate_transactions_eof(client_id)
            elif data_type == DataType.USERS:
                self.queue_manager.propagate_users_eof(client_id)
            elif data_type == DataType.STORES:
                self.queue_manager.propagate_stores_eof(client_id)
            elif data_type == DataType.TRANSACTION_ITEMS:
                self.queue_manager.propagate_transaction_items_eof(client_id)
            elif data_type == DataType.MENU_ITEMS:
                self.queue_manager.propagate_menu_items_eof(client_id)
            else:
                logger.warning(f"Unknown data type for EOF propagation: {data_type} for client {client_id}")
        except Exception as e:
            logger.error(f"Error propagating EOF for {data_type} for client {client_id}: {e}")
            # Re-raise to allow higher level error handling
            raise
    
    @staticmethod
    def is_eof_message(message: Any) -> bool:
        """Check if a message is an EOF message."""
        return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'