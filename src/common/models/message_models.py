"""Message models for typed message structures."""

from typing import Any, List, Literal, TypedDict, Union


# ============================================================================
# Data Entity Types
# ============================================================================

class Transaction(TypedDict):
    """Transaction data structure."""
    transaction_id: str
    store_id: int
    payment_method_id: int
    voucher_id: Union[float, str]  # Can be float or empty string
    user_id: int
    original_amount: float
    discount_applied: float
    final_amount: float
    created_at: str


class TransactionItem(TypedDict):
    """Transaction item data structure."""
    transaction_id: str
    item_id: int
    quantity: int
    unit_price: float
    subtotal: float
    created_at: str


class User(TypedDict):
    """User data structure."""
    user_id: int
    gender: str
    birthdate: str
    registered_at: str


class Store(TypedDict):
    """Store data structure."""
    store_id: int
    store_name: str


class MenuItem(TypedDict):
    """Menu item data structure."""
    item_id: int
    item_name: str
    category: str
    price: float
    is_seasonal: str
    available_from: str
    available_to: str


# ============================================================================
# Message Types with Metadata
# ============================================================================

class DataMessage(TypedDict, total=False):
    """Message structure for data without chunking.
    
    Used for stores, users, and menu_items.
    """
    client_id: str
    data: List[Union[Store, User, MenuItem]]
    sequence_id: str


class ChunkedDataMessage(TypedDict, total=False):
    """Message structure for chunked data.
    
    Used for transactions and transaction_items that are sent in chunks.
    """
    client_id: str
    data: List[Union[Transaction, TransactionItem]]
    chunk_index: int
    total_chunks: int
    sequence_id: str


class EOFMessage(TypedDict, total=False):
    """Message structure for EOF (End Of File) control messages."""
    client_id: str
    type: Literal['EOF']
    data_type: str  # TRANSACTIONS, USERS, STORES, TRANSACTION_ITEMS, MENU_ITEMS
    sequence_id: str


# Union type for all message types
MessageWithMetadata = DataMessage | ChunkedDataMessage | EOFMessage

