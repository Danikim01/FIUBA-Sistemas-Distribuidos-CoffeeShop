"""Utility functions for worker operations."""

import sys
import logging
from typing import Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def run_main(worker_class, *args, **kwargs):
    try:
        worker = worker_class(*args, **kwargs)
        worker.start_consuming()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)


def safe_float_conversion(value: Any, default: float = 0.0) -> float:
    """Safely convert a value to float.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Converted float value or default
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int_conversion(value: Any, default: int = 0) -> int:
    """Safely convert a value to int.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Converted int value or default
    """
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def parse_datetime_safe(datetime_str: str, format_str: str = '%Y-%m-%d %H:%M:%S') -> Optional[datetime]:
    """Safely parse a datetime string.
    
    Args:
        datetime_str: String to parse
        format_str: Format string for parsing
        
    Returns:
        Parsed datetime object or None if parsing fails
    """
    try:
        return datetime.strptime(datetime_str, format_str)
    except (ValueError, TypeError):
        return None


def extract_year_safe(datetime_str: str) -> Optional[int]:
    """Safely extract year from datetime string.
    
    Args:
        datetime_str: Datetime string to extract year from
        
    Returns:
        Extracted year or None if extraction fails
    """
    dt = parse_datetime_safe(datetime_str)
    return dt.year if dt else None


def extract_time_safe(datetime_str: str) -> Optional[Any]:
    """Safely extract time from datetime string.
    
    Args:
        datetime_str: Datetime string to extract time from
        
    Returns:
        Extracted time object or None if extraction fails
    """
    dt = parse_datetime_safe(datetime_str)
    return dt.time() if dt else None


def validate_positive_number(value: Any, name: str) -> float:
    """Validate that a value is a positive number.
    
    Args:
        value: Value to validate
        name: Name of the value for error messages
        
    Returns:
        Validated positive number
        
    Raises:
        ValueError: If value is not a positive number
    """
    try:
        num = float(value)
        if num <= 0:
            raise ValueError(f"{name} must be positive, got: {num}")
        return num
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid {name}: {value}") from e


def format_bytes(bytes_value: int) -> str:
    """Format bytes value in human-readable format.
    
    Args:
        bytes_value: Number of bytes
        
    Returns:
        Formatted string (e.g., "1.2 MB")
    """
    value = float(bytes_value)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if value < 1024.0:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} PB"


def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate string to maximum length with suffix.
    
    Args:
        text: String to truncate
        max_length: Maximum length including suffix
        suffix: Suffix to add when truncating
        
    Returns:
        Truncated string
    """
    if len(text) <= max_length:
        return text
    
    if len(suffix) >= max_length:
        return suffix[:max_length]
    
    return text[:max_length - len(suffix)] + suffix