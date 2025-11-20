"""Message handling utilities for worker communication."""

import logging
import uuid
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

ClientId = str

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


def extract_sequence_id(message: Dict[str, Any]) -> Optional[str]:
    """Extract sequence_id from message metadata.
    
    Args:
        message: Message dictionary that may contain sequence_id in metadata
        
    Returns:
        sequence_id string if found, None otherwise
    """
    return message.get('sequence_id')


def extract_batch_num_from_sequence_id(sequence_id: str) -> Optional[int]:
    """Parse batch number from sequence ID string.
    
    Sequence ID format: {client_id_sin_guiones}-{batch_num}
    Example: "abc123def456-42" -> 42
    
    Args:
        sequence_id: Sequence ID string in format {client_id}-{batch_num}
        
    Returns:
        Batch number as integer if parsing succeeds, None otherwise
    """
    if not sequence_id:
        return None
    
    try:
        # Split by last hyphen to get batch number
        parts = sequence_id.rsplit('-', 1)
        if len(parts) == 2:
            return int(parts[1])
        return None
    except (ValueError, AttributeError):
        logger.warning(f"Failed to parse batch number from sequence_id: {sequence_id}")
        return None
