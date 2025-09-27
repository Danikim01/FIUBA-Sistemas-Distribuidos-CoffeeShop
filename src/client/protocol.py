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

def pack_float32(value: float) -> bytes:
    """Pack 32-bit float to bytes - simplified approach using string representation"""
    float_str = f"{value:.6f}"
    return pack_string(float_str)

def pack_string(text: str) -> bytes:
    """Pack string with length prefix (4 bytes + string)"""
    encoded = text.encode('utf-8')
    return pack_uint32(len(encoded)) + encoded

def unpack_uint32(data: bytes, offset: int) -> tuple[int, int]:
    """Unpack 32-bit unsigned integer from bytes (big-endian)"""
    value = (data[offset] << 24) | (data[offset+1] << 16) | (data[offset+2] << 8) | data[offset+3]
    return value, offset + 4

def unpack_string(data: bytes, offset: int) -> tuple[str, int]:
    """Unpack string with length prefix, return (string, new_offset)"""
    length, new_offset = unpack_uint32(data, offset)
    string_data = data[new_offset:new_offset+length].decode('utf-8')
    return string_data, new_offset + length

class MessageType(IntEnum):
    BATCH = 1
    EOF = 2  # End of files for a specific data type

class DataType(IntEnum):
    TRANSACTIONS = 1
    TRANSACTION_ITEMS = 2
    USERS = 3
    STORES = 4

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

def serialize_transaction(row: Dict[str, Any]) -> bytes:
    """Serialize transaction row to bytes"""
    data = b''
    
    # transaction_id (string)
    data += pack_string(row['transaction_id'])
    
    # store_id (int)
    data += pack_uint32(int(row['store_id']))
    
    # payment_method_id (int)  
    data += pack_uint32(int(row['payment_method_id']))
    
    # voucher_id (float, can be empty)
    voucher_id = float(row['voucher_id']) if row['voucher_id'] else 0.0
    data += pack_float32(voucher_id)
    
    # user_id (int)
    user_id = int(float(row['user_id'])) if row['user_id'] else 0
    data += pack_uint32(user_id)
    
    # original_amount (float)
    original_amount = float(row['original_amount']) if row['original_amount'] else 0.0
    data += pack_float32(original_amount)
    
    # discount_applied (float)
    discount_applied = float(row['discount_applied']) if row['discount_applied'] else 0.0
    data += pack_float32(discount_applied)
    
    # final_amount (float)
    final_amount = float(row['final_amount']) if row['final_amount'] else 0.0
    data += pack_float32(final_amount)
    
    # created_at (string)
    data += pack_string(row['created_at'])
    
    return data

def serialize_transaction_item(row: Dict[str, Any]) -> bytes:
    """Serialize transaction_item row to bytes"""
    data = b''
    
    # transaction_id (string)
    data += pack_string(row['transaction_id'])
    
    # item_id (int)
    data += pack_uint32(int(row['item_id']))
    
    # quantity (int)
    data += pack_uint32(int(row['quantity']))
    
    # unit_price (float)
    data += pack_float32(float(row['unit_price']))
    
    # subtotal (float)
    data += pack_float32(float(row['subtotal']))
    
    # created_at (string)
    data += pack_string(row['created_at'])
    
    return data

def serialize_store(row: Dict[str, Any]) -> bytes:
    """Serialize store row to bytes - only relevant fields for enrichment"""
    data = b''
    
    # store_id (int)
    data += pack_uint32(int(row['store_id']))
    
    # store_name (string)
    data += pack_string(row['store_name'])
    
    return data


def serialize_user(row: Dict[str, Any]) -> bytes:
    """Serialize user row to bytes"""
    data = b''
    
    # user_id (int)
    data += pack_uint32(int(row['user_id']))
    
    # gender (string)
    data += pack_string(row['gender'])
    
    # birthdate (string)
    data += pack_string(row['birthdate'])
    
    # registered_at (string)
    data += pack_string(row['registered_at'])
    
    return data

def send_batch(sock: socket.socket, data_type: DataType, rows: List[Dict[str, Any]]) -> None:
    """
    Send a batch of data rows
    
    Protocol:
    - Total Message Size (4 bytes): size of entire message including this field
    - Message Type (4 bytes): BATCH
    - Data Type (4 bytes): TRANSACTIONS, TRANSACTION_ITEMS, USERS or STORES  
    - Data Size (4 bytes): size in bytes of serialized data
    - Rows: serialized row data
    """
    # Serialize all rows
    serialized_rows = b''
    
    for row in rows:
        if data_type == DataType.TRANSACTIONS:
            serialized_rows += serialize_transaction(row)
        elif data_type == DataType.TRANSACTION_ITEMS:
            serialized_rows += serialize_transaction_item(row)
        elif data_type == DataType.USERS:
            serialized_rows += serialize_user(row)
        elif data_type == DataType.STORES:
            serialized_rows += serialize_store(row)
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    
    # Calculate total message size (total_size + msg_type + data_type + data_size + serialized_rows)
    total_message_size = 4 + 4 + 4 + 4 + len(serialized_rows)
    
    # Build message
    message = b''
    message += pack_uint32(total_message_size)      # Total Message Size
    message += pack_uint32(MessageType.BATCH)       # Message Type
    message += pack_uint32(data_type)               # Data Type
    message += pack_uint32(len(serialized_rows))    # Data Size (bytes)
    message += serialized_rows                      # Rows
    
    send_all(sock, message)
    logger.debug(f"Sent batch: type={data_type}, rows={len(rows)}, bytes={len(serialized_rows)}")

def send_eof(sock: socket.socket, data_type: DataType) -> None:
    """
    Send EOF signal for a data type
    
    Protocol:
    - Total Message Size (4 bytes): size of entire message including this field
    - Message Type (4 bytes): EOF
    - Data Type (4 bytes): TRANSACTIONS, TRANSACTION_ITEMS, USERS or STORES
    """
    # Calculate total message size (total_size + msg_type + data_type)
    total_message_size = 4 + 4 + 4
    
    message = b''
    message += pack_uint32(total_message_size)    # Total Message Size
    message += pack_uint32(MessageType.EOF)       # Message Type
    message += pack_uint32(data_type)             # Data Type
    
    send_all(sock, message)
    logger.info(f"Sent EOF for data type: {data_type}")

def receive_response(sock: socket.socket) -> int:
    """Receive response from server"""
    # Read total message size first
    total_size_data = recv_all(sock, 4)
    total_message_size, _ = unpack_uint32(total_size_data, 0)
    
    # Read the rest of the message
    remaining_data = recv_all(sock, total_message_size - 4)
    response_code, _ = unpack_uint32(remaining_data, 0)
    
    return response_code