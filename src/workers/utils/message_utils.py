"""Message handling utilities for worker communication."""

from typing import Any, Dict, Optional


def is_eof_message(message: Any) -> bool:
    """Check if a message is an EOF (End Of File) control message.
    
    Args:
        message: Message to check
        
    Returns:
        True if message is EOF, False otherwise
    """
    return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'


def extract_client_metadata(message: Any) -> tuple[str, Any]:
    """Extract client_id and actual data from message with metadata.
    
    Args:
        message: Message that may contain client metadata
        
    Returns:
        tuple: (client_id, actual_data)
    """
    if isinstance(message, dict) and 'client_id' in message:
        client_id = message.get('client_id', '')
        # For data messages, extract the 'data' field
        if 'data' in message and message.get('type') != 'EOF':
            return client_id, message['data']
        # For EOF and other control messages, return the whole message
        return client_id, message
    
    # Legacy format - no client metadata
    return '', message


def create_message_with_metadata(
    client_id: str,
    data: Any,
    message_type: Optional[str] = None,
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
    message = {
        'client_id': client_id,
        'data': data
    }
    
    if message_type:
        message['type'] = message_type
    
    message.update(additional_metadata)
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