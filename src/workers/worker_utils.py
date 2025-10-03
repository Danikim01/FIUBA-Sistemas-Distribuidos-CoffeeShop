#!/usr/bin/env python3
"""
Worker utilities module for common functionality across all workers.
This module provides backward compatibility by re-exporting all functionality
from the separated modules.
"""

import logging

# Re-export message utilities
from message_utils import (
    is_eof_message,
    extract_client_metadata,
    create_message_with_metadata,
    extract_eof_metadata
)

# Re-export worker configuration
from worker_config import WorkerConfig

# Re-export base worker
from workers.utils.base_worker import BaseWorker

# Re-export specialized workers
from workers.filter.filter_worker import FilterWorker
from workers.top.top_worker import TopWorker
from workers.aggregator.aggregator_worker import AggregatorWorker

# Re-export utility functions
from utils import (
    run_main,
    safe_float_conversion,
    safe_int_conversion,
    parse_datetime_safe,
    extract_year_safe,
    extract_time_safe,
    validate_positive_number,
    format_bytes,
    truncate_string
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# All exports for backward compatibility
__all__ = [
    # Message utilities
    'is_eof_message',
    'extract_client_metadata', 
    'create_message_with_metadata',
    'extract_eof_metadata',
    
    # Configuration
    'WorkerConfig',
    
    # Worker classes
    'BaseWorker',
    'FilterWorker',
    'TopWorker',
    'AggregatorWorker',
    
    # Utility functions
    'run_main',
    'safe_float_conversion',
    'safe_int_conversion',
    'parse_datetime_safe',
    'extract_year_safe',
    'extract_time_safe',
    'validate_positive_number',
    'format_bytes',
    'truncate_string'
]