"""Message handling utilities for worker communication."""

import logging
import uuid
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

ClientId = str

RESET_CLIENT_TYPE = "CLIENT_RESET"
RESET_ALL_TYPE = "RESET_ALL"

def is_eof_message(message: Any) -> bool:
    """Check if a message is an EOF (End Of File) control message.
    
    Args:
        message: Message to check
        
    Returns:
        True if message is EOF, False otherwise
    """
    return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'


def extract_data_and_client_id(message: dict) -> tuple[ClientId, dict[str, Any]]:
    """Extract client_id and actual data from message with metadata.
    
    Args:
        message: Message that may contain client metadata
        
    Returns:
        tuple: (client_id, actual_data)
    """
    client_id = message.get('client_id', '')
    if 'data' in message and message.get('type') != 'EOF':
        return client_id, message['data']
    
    # For EOF return the whole message
    return client_id, message
    
def create_message_with_metadata(
    client_id: str,
    data: Any,
    message_type: Optional[str] = None,
    message_uuid: Optional[str] = None,
    **additional_metadata
) -> Dict[str, Any]:
    """Create a message with client metadata.
    
    Args:
        client_id: Client identifier
        data: The actual message data
        message_type: Optional message type (e.g., 'EOF')
        **additional_metadata: Additional metadata fields
    
    Returns:
        dict: Message with metadata wrapper
    """
    metadata = dict(additional_metadata)
    uuid_from_metadata = metadata.pop('message_uuid', None)
    resolved_uuid = message_uuid or uuid_from_metadata or str(uuid.uuid4())

    message = {
        'client_id': client_id,
        'data': data,
        'message_uuid': resolved_uuid,
    }

    if message_type is not None:
        message['type'] = message_type
    elif isinstance(data, dict) and 'type' in data:
        message['type'] = data['type']
        logger.debug("Inheriting message type from data: %s", data['type'])

    if metadata:
        logger.info(f"[MESSAGE UTILS] Additional metadata: {metadata}")

    message.update(metadata)
    return message


def extract_eof_metadata(message: Dict[str, Any]) -> Dict[str, Any]:
    """Extract additional metadata from EOF message, excluding standard fields.
    
    Args:
        message: EOF message dictionary
        
    Returns:
        Dictionary containing additional metadata
    """
    excluded_fields = {'client_id', 'type', 'data'}
    return {k: v for k, v in message.items() if k not in excluded_fields}


def _is_type(message: Any, expected_type: str) -> bool:
    return isinstance(message, dict) and str(message.get('type', '')).upper() == expected_type.upper()


def is_client_reset_message(message: Any) -> bool:
    """Detect a control message that instructs workers to reset a single client."""
    return _is_type(message, RESET_CLIENT_TYPE)


def is_reset_all_clients_message(message: Any) -> bool:
    """Detect a control message that instructs workers to reset all clients."""
    return _is_type(message, RESET_ALL_TYPE)
