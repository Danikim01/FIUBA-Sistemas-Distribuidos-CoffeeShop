import socket
from enum import IntEnum
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

def pack_uint32(value: int) -> bytes:
    """Pack 32-bit unsigned integer to bytes (big-endian)"""
    return bytes([
        (value >> 24) & 0xFF,
        (value >> 16) & 0xFF,
        (value >> 8) & 0xFF,
        value & 0xFF
    ])

def unpack_uint32(data: bytes, offset: int) -> tuple[int, int]:
    """Unpack 32-bit unsigned integer from bytes (big-endian)"""
    value = (data[offset] << 24) | (data[offset+1] << 16) | (data[offset+2] << 8) | data[offset+3]
    return value, offset + 4

def unpack_float32_from_string(data: bytes, offset: int) -> tuple[float, int]:
    """Unpack float from string representation stored in bytes"""
    length, new_offset = unpack_uint32(data, offset)
    float_str = data[new_offset:new_offset + length].decode('utf-8')
    return float(float_str), new_offset + length

def unpack_float32(data: bytes, offset: int) -> tuple[float, int]:
    """Unpack 32-bit float from bytes (big-endian)"""
    import struct
    float_bytes = data[offset:offset + 4]
    value = struct.unpack('>f', float_bytes)[0]
    return value, offset + 4

def unpack_string(data: bytes, offset: int) -> tuple[str, int]:
    """Unpack string with length prefix, return (string, new_offset)"""
    length, new_offset = unpack_uint32(data, offset)
    string = data[new_offset:new_offset + length].decode('utf-8')
    return string, new_offset + length

class MessageType(IntEnum):
    BATCH = 1
    EOF = 2  # EOF for a specific data type
    SESSION_RESET = 3  # Reset session to start a new session

class DataType(IntEnum):
    TRANSACTIONS = 1
    TRANSACTION_ITEMS = 2
    USERS = 3
    STORES = 4
    MENU_ITEMS = 5

class ResponseCode(IntEnum):
    OK = 0
    ERROR = 1

def send_all(sock: socket.socket, data: bytes) -> None:
    """Send all data through socket"""
    total_sent = 0
    while total_sent < len(data):
        sent = sock.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket connection broken")
        total_sent += sent

def recv_all(sock: socket.socket, length: int) -> bytes:
    """Receive exact amount of data from socket"""
    data = b''
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise ConnectionError("Socket connection broken")
        data += chunk
    return data

def deserialize_transaction(data: bytes, offset: int) -> tuple[Dict[str, Any], int]:
    """Deserialize transaction from bytes"""
    transaction = {}
    
    # transaction_id (string)
    transaction['transaction_id'], offset = unpack_string(data, offset)
    
    # store_id (int)
    transaction['store_id'], offset = unpack_uint32(data, offset)
    
    # payment_method_id (int)
    transaction['payment_method_id'], offset = unpack_uint32(data, offset)
    
    # voucher_id (float)
    voucher_id, offset = unpack_float32_from_string(data, offset)
    transaction['voucher_id'] = voucher_id if voucher_id != 0.0 else ''
    
    # user_id (int)
    transaction['user_id'], offset = unpack_uint32(data, offset)
    
    # original_amount (float)
    transaction['original_amount'], offset = unpack_float32_from_string(data, offset)
    
    # discount_applied (float)
    transaction['discount_applied'], offset = unpack_float32_from_string(data, offset)
    
    # final_amount (float)
    transaction['final_amount'], offset = unpack_float32_from_string(data, offset)
    
    # created_at (string)
    transaction['created_at'], offset = unpack_string(data, offset)
    
    return transaction, offset

def deserialize_transaction_item(data: bytes, offset: int) -> tuple[Dict[str, Any], int]:
    """Deserialize transaction_item from bytes"""
    item = {}
    
    # transaction_id (string)
    item['transaction_id'], offset = unpack_string(data, offset)
    
    # item_id (int)
    item['item_id'], offset = unpack_uint32(data, offset)
    
    # quantity (int)
    item['quantity'], offset = unpack_uint32(data, offset)
    
    # unit_price (float)
    item['unit_price'], offset = unpack_float32_from_string(data, offset)
    
    # subtotal (float)
    item['subtotal'], offset = unpack_float32_from_string(data, offset)
    
    # created_at (string)
    item['created_at'], offset = unpack_string(data, offset)
    
    return item, offset

def deserialize_user(data: bytes, offset: int) -> tuple[Dict[str, Any], int]:
    """Deserialize user from bytes"""
    user = {}
    
    # user_id (int)
    user['user_id'], offset = unpack_uint32(data, offset)
    
    # gender (string)
    user['gender'], offset = unpack_string(data, offset)
    
    # birthdate (string)
    user['birthdate'], offset = unpack_string(data, offset)
    
    # registered_at (string)
    user['registered_at'], offset = unpack_string(data, offset)
    
    return user, offset

def deserialize_store(data: bytes, offset: int) -> tuple[Dict[str, Any], int]:
    """Deserialize store from bytes - only relevant fields for enrichment"""
    store = {}
    
    # store_id (int)
    store['store_id'], offset = unpack_uint32(data, offset)
    
    # store_name (string)
    store['store_name'], offset = unpack_string(data, offset)
    
    return store, offset


def deserialize_menu_item(data: bytes, offset: int) -> tuple[Dict[str, Any], int]:
    """Deserialize menu item from bytes."""
    menu_item: Dict[str, Any] = {}

    menu_item['item_id'], offset = unpack_uint32(data, offset)
    menu_item['item_name'], offset = unpack_string(data, offset)
    menu_item['category'], offset = unpack_string(data, offset)
    menu_item['price'], offset = unpack_float32_from_string(data, offset)
    menu_item['is_seasonal'], offset = unpack_string(data, offset)
    menu_item['available_from'], offset = unpack_string(data, offset)
    menu_item['available_to'], offset = unpack_string(data, offset)

    return menu_item, offset

def send_response(sock: socket.socket, success: bool) -> None:
    """Send response (OK or ERROR)"""
    # Calculate total message size (total_size + response_code)
    total_message_size = 4 + 4
    
    message = b''
    message += pack_uint32(total_message_size) # Total Message Size
    response_code = ResponseCode.OK if success else ResponseCode.ERROR
    message += pack_uint32(response_code) # Response Code
    
    send_all(sock, message)

def send_session_reset_message(sock: socket.socket) -> None:
    """Send a session reset message"""
    # Calculate total message size: total_size (4) + message_type (4)
    total_message_size = 4 + 4
    
    message = b''
    message += pack_uint32(total_message_size)  # Total Message Size
    message += pack_uint32(MessageType.SESSION_RESET)  # Message Type
    
    send_all(sock, message)

def receive_message(sock: socket.socket) -> tuple[int, bytes]:
    """
    Receive a complete message from the socket
    
    New Protocol:
    - Total Message Size (4 bytes): size of entire message including this field
    - Message Type (4 bytes): BATCH or EOF
    - Rest of message...
    
    Returns: (message_type, message_data)
    """
    # Total message size
    total_size_bytes = recv_all(sock, 4)
    total_message_size, _ = unpack_uint32(total_size_bytes, 0)
    
    remaining_data = recv_all(sock, total_message_size - 4)
    
    message_type, _ = unpack_uint32(remaining_data, 0)
    
    message_data = remaining_data[4:]
    
    return message_type, message_data

def parse_batch_message(message_data: bytes) -> tuple[DataType, List[Dict[str, Any]]]:
    """Parse batch message data and return data type and rows"""
    offset = 0
    
    # Data Type (4 bytes)
    data_type_value, offset = unpack_uint32(message_data, offset)
    data_type = DataType(data_type_value)
    
    # Data Size (4 bytes) - size of serialized data in bytes
    data_size, offset = unpack_uint32(message_data, offset)
    
    # Parse rows until we've consumed all data bytes
    rows = []
    data_end = offset + data_size
    
    while offset < data_end:
        if data_type == DataType.TRANSACTIONS:
            row, offset = deserialize_transaction(message_data, offset)
        elif data_type == DataType.TRANSACTION_ITEMS:
            row, offset = deserialize_transaction_item(message_data, offset)
        elif data_type == DataType.USERS:
            row, offset = deserialize_user(message_data, offset)
        elif data_type == DataType.STORES:
            row, offset = deserialize_store(message_data, offset)
        elif data_type == DataType.MENU_ITEMS:
            row, offset = deserialize_menu_item(message_data, offset)
        else:
            raise ValueError(f"Unknown data type: {data_type}")
        
        rows.append(row)
    
    return data_type, rows

def parse_eof_message(message_data: bytes) -> DataType:
    """Parse EOF message data and return data type"""
    data_type_value, _ = unpack_uint32(message_data, 0)
    data_type = DataType(data_type_value)
    return data_type