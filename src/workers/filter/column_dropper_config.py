#!/usr/bin/env python3
"""
Column Dropper Configuration Module

This module defines the mapping between input queues and their corresponding
data transformation configurations. The column_dropper acts as the data
transformation layer between RAW data from the gateway and typed data for
subsequent workers.

Architecture:
Gateway -> RAW Queues -> ColumnDropper -> Typed Queues -> Processing Workers

Configuration Structure:
- output_columns: Set of column names to keep from raw data
- type_converters: Dictionary mapping column names to conversion functions
- filters: Dictionary mapping column names to validation functions
"""

from typing import Any, Dict, Set, Callable, List
from worker_utils import safe_int_conversion, safe_float_conversion


def validate_positive_int(value: int) -> bool:
    """Validate that an integer is positive."""
    return value > 0


def validate_non_negative_float(value: float) -> bool:
    """Validate that a float is non-negative."""
    return value >= 0.0


def validate_non_empty_string(value: str) -> bool:
    """Validate that a string is not empty."""
    return bool(value and value.strip())


# Main configuration mapping: input_queue -> transformation_config
QUEUE_TRANSFORMATION_CONFIG: Dict[str, Dict[str, Any]] = {
    
    # TRANSACTIONS DATA TRANSFORMATIONS
    
    # Minimal transaction data for top clients analysis
    'transactions_raw_compact': {
        'description': 'Extract minimal transaction data for top clients analysis',
        'output_columns': {'store_id', 'user_id'},
        'type_converters': {
            'store_id': safe_int_conversion,
            'user_id': safe_int_conversion
        },
        'filters': {
            'store_id': validate_positive_int,
            'user_id': validate_positive_int
        }
    },
    
    # Full transaction data for amount and time filtering
    'transactions_raw_full': {
        'description': 'Extract full transaction data for amount/time analysis',
        'output_columns': {
            'transaction_id', 'store_id', 'user_id', 'payment_method_id', 
            'amount_total', 'created_at'
        },
        'type_converters': {
            'transaction_id': str,
            'store_id': safe_int_conversion,
            'user_id': safe_int_conversion,
            'payment_method_id': safe_int_conversion,
            'amount_total': safe_float_conversion,
            'created_at': str  # Keep as string for datetime parsing later
        },
        'filters': {
            'store_id': validate_positive_int,
            'user_id': validate_positive_int,
            'payment_method_id': validate_positive_int,
            'amount_total': validate_non_negative_float,
            'transaction_id': validate_non_empty_string,
            'created_at': validate_non_empty_string
        }
    },
    
    # Transaction data for TPV analysis
    'transactions_raw_tpv': {
        'description': 'Extract transaction data for TPV (Total Payment Value) analysis',
        'output_columns': {
            'store_id', 'amount_total', 'created_at'
        },
        'type_converters': {
            'store_id': safe_int_conversion,
            'amount_total': safe_float_conversion,
            'created_at': str
        },
        'filters': {
            'store_id': validate_positive_int,
            'amount_total': validate_non_negative_float,
            'created_at': validate_non_empty_string
        }
    },
    
    # TRANSACTION ITEMS DATA TRANSFORMATIONS
    
    # Transaction items for item analysis
    'transaction_items_raw': {
        'description': 'Extract transaction items data for item analysis',
        'output_columns': {
            'transaction_id', 'item_id', 'quantity', 'unit_price', 'subtotal', 'created_at'
        },
        'type_converters': {
            'transaction_id': str,
            'item_id': safe_int_conversion,
            'quantity': safe_int_conversion,
            'unit_price': safe_float_conversion,
            'subtotal': safe_float_conversion,
            'created_at': str
        },
        'filters': {
            'transaction_id': validate_non_empty_string,
            'item_id': validate_positive_int,
            'quantity': validate_positive_int,
            'unit_price': validate_non_negative_float,
            'subtotal': validate_non_negative_float,
            'created_at': validate_non_empty_string
        }
    },
    
    # USERS DATA TRANSFORMATIONS
    
    # User demographics for client analysis
    'users_raw': {
        'description': 'Extract user demographics for client analysis',
        'output_columns': {'user_id', 'gender', 'birthdate'},
        'type_converters': {
            'user_id': safe_int_conversion,
            'gender': str,
            'birthdate': str
        },
        'filters': {
            'user_id': validate_positive_int,
            'gender': validate_non_empty_string,
            'birthdate': validate_non_empty_string
        }
    },
    
    # STORES DATA TRANSFORMATIONS
    
    # Store information for store-based analysis
    'stores_raw': {
        'description': 'Extract store information for store-based analysis',
        'output_columns': {'store_id', 'store_name'},
        'type_converters': {
            'store_id': safe_int_conversion,
            'store_name': str
        },
        'filters': {
            'store_id': validate_positive_int,
            'store_name': validate_non_empty_string
        }
    },
    
    # MENU ITEMS DATA TRANSFORMATIONS
    
    # Menu items for item metadata
    'menu_items_raw': {
        'description': 'Extract menu items metadata for item analysis',
        'output_columns': {'item_id', 'item_name', 'category', 'price'},
        'type_converters': {
            'item_id': safe_int_conversion,
            'item_name': str,
            'category': str,
            'price': safe_float_conversion
        },
        'filters': {
            'item_id': validate_positive_int,
            'item_name': validate_non_empty_string,
            'category': validate_non_empty_string,
            'price': validate_non_negative_float
        }
    },
    
    # CUSTOM TRANSFORMATIONS
    
    # Example of a custom transformation for specific analysis
    'transactions_custom_analysis': {
        'description': 'Custom transaction transformation for specific analysis',
        'output_columns': {'store_id', 'user_id', 'amount_total', 'payment_method_id'},
        'type_converters': {
            'store_id': safe_int_conversion,
            'user_id': safe_int_conversion,
            'amount_total': safe_float_conversion,
            'payment_method_id': safe_int_conversion
        },
        'filters': {
            'store_id': validate_positive_int,
            'user_id': validate_positive_int,
            'amount_total': lambda x: x >= 50.0,  # Custom filter: minimum $50
            'payment_method_id': validate_positive_int
        }
    }
}


def get_queue_config(queue_name: str) -> Dict[str, Any]:
    """
    Get configuration for a specific queue.
    
    Args:
        queue_name: Name of the input queue
        
    Returns:
        Configuration dictionary for the queue
        
    Raises:
        KeyError: If queue configuration is not found
    """
    if queue_name not in QUEUE_TRANSFORMATION_CONFIG:
        raise KeyError(f"No configuration found for queue: {queue_name}")
    
    return QUEUE_TRANSFORMATION_CONFIG[queue_name]


def list_available_queues() -> List[str]:
    """Get list of all available queue configurations."""
    return list(QUEUE_TRANSFORMATION_CONFIG.keys())


def validate_queue_config(config: Dict[str, Any]) -> bool:
    """
    Validate that a queue configuration has all required fields.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        True if valid, False otherwise
    """
    required_keys = {'output_columns', 'type_converters', 'filters'}
    return all(key in config for key in required_keys)


def print_queue_configurations():
    """Print all available queue configurations for debugging."""
    print("Available Queue Configurations:")
    print("=" * 50)
    
    for queue_name, config in QUEUE_TRANSFORMATION_CONFIG.items():
        print(f"\nQueue: {queue_name}")
        print(f"Description: {config.get('description', 'No description')}")
        print(f"Output Columns: {list(config['output_columns'])}")
        print(f"Type Converters: {list(config['type_converters'].keys())}")
        print(f"Filters: {list(config['filters'].keys())}")


if __name__ == '__main__':
    # Print configurations when run directly
    print_queue_configurations()