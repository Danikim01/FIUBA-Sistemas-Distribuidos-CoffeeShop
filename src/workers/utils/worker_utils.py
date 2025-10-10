"""Utility functions for worker operations."""

import sys
import logging
from typing import Any, List, Optional, Tuple, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

def run_main(worker_class, *args, **kwargs):
    try:
        worker = worker_class(*args, **kwargs)
        worker.start_consuming()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)

YearHalfSortKey = Tuple[int, int]

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


def safe_int_conversion(value: Any, minimum: int | None = None, default: int = 0) -> int:
    """Safely convert a value to int.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Converted int value or default
    """
    try:
        if minimum is not None and int(value) < minimum:
            return default
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

def extract_year_half(created_at: Any) -> str | None:
    if not created_at:
        return None

    if isinstance(created_at, str):
        try:
            dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                dt = datetime.fromisoformat(created_at)
            except ValueError:
                return None
    else:
        try:
            dt = datetime.fromisoformat(str(created_at))
        except ValueError:
            return None

    half = '1' if dt.month <= 6 else '2'
    return f"{dt.year}-H{half}"

def extract_year_month(created_at: Any) -> str | None:
    if not created_at:
        return None

    if isinstance(created_at, str):
        try:
            dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return created_at[:7] if len(created_at) >= 7 else None
    else:
        try:
            dt = datetime.fromisoformat(str(created_at))
        except ValueError:
            return None

    return dt.strftime('%Y-%m')


def _parse_year(value: Any) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _parse_half(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    text = str(value).strip().upper()
    if not text:
        return 0
    if text.startswith('H'):
        text = text[1:]
    if text.endswith('H'):
        text = text[:-1]
    return int(text) if text.isdigit() else 0


def normalize_year_half(
    raw_period: Any,
    *,
    year: Any | None = None,
    semester: Any | None = None,
) -> Tuple[str, int, int]:
    """Normalize year/half information and return display value plus numeric tuple."""

    normalized_year = 0
    normalized_half = 0

    if isinstance(raw_period, str) and raw_period:
        period = raw_period.strip()
        upper = period.upper()
        if '-H' in upper:
            year_part, half_part = upper.split('-H', 1)
            normalized_year = _parse_year(year_part)
            normalized_half = _parse_half(half_part)
        elif '-' in upper:
            year_part, half_part = upper.split('-', 1)
            normalized_year = _parse_year(year_part)
            normalized_half = _parse_half(half_part)
        else:
            normalized_year = _parse_year(upper)
            normalized_half = _parse_half(upper)

    if not normalized_year and year is not None:
        normalized_year = _parse_year(year)
    if not normalized_half and semester is not None:
        normalized_half = _parse_half(semester)

    if normalized_year and normalized_half:
        normalized_str = f"{normalized_year}-H{normalized_half}"
    elif isinstance(raw_period, str) and raw_period.strip():
        normalized_str = raw_period.strip()
    else:
        normalized_str = ''

    return normalized_str, normalized_year, normalized_half


def normalize_tpv_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize TPV entry fields in-place and return the entry."""

    period, year_value, half_value = normalize_year_half(
        entry.get('year_half_created_at'),
        year=entry.get('year'),
        semester=entry.get('semester'),
    )

    entry['year_half_created_at'] = period
    if year_value:
        entry['year'] = year_value
    else:
        entry.pop('year', None)

    if half_value:
        entry['semester'] = f'H{half_value}'
    else:
        entry.pop('semester', None)

    entry['store_name'] = (entry.get('store_name') or '').strip()
    entry['store_id'] = str(entry.get('store_id', '')).strip()

    try:
        entry['tpv'] = float(entry.get('tpv', 0.0) or 0.0)
    except (TypeError, ValueError):
        entry['tpv'] = 0.0

    return entry


def store_id_sort_key(value: Any) -> Tuple[int, str]:
    text = str(value or '').strip()
    if text.isdigit():
        return int(text), text
    try:
        numeric = int(float(text))
        return numeric, text
    except (ValueError, TypeError):
        return 0, text


def tpv_sort_key(entry: Dict[str, Any]) -> Tuple[YearHalfSortKey, str, Tuple[int, str]]:
    """Return a composite key to sort TPV rows by period, store name, then store id."""

    _, year_value, half_value = normalize_year_half(
        entry.get('year_half_created_at'),
        year=entry.get('year'),
        semester=entry.get('semester'),
    )

    store_name = entry.get('store_name') or ''
    store_key = store_id_sort_key(entry.get('store_id'))
    return (year_value, half_value), store_name, store_key


def normalize_year_month(value: Any) -> Tuple[str, int, int]:
    if isinstance(value, str) and value:
        parts = value.split('-')
        if len(parts) >= 2:
            year = _parse_year(parts[0])
            month = _parse_half(parts[1])
            return f"{year:04d}-{month:02d}" if year and month else value, year, month
        year = _parse_year(value)
        return str(year) if year else value, year, 0
    return '', 0, 0


def top_items_sort_key(entry: Dict[str, Any], metric_key: str) -> Tuple[YearHalfSortKey, float, Tuple[int, str]]:
    normalized, year, month = normalize_year_month(entry.get('year_month_created_at'))
    if normalized:
        entry['year_month_created_at'] = normalized

    metric_value = entry.get(metric_key, 0)
    try:
        metric_numeric = float(metric_value)
    except (TypeError, ValueError):
        metric_numeric = 0.0

    item_key = store_id_sort_key(entry.get('item_id'))
    return (year, month), -metric_numeric, item_key

# def get_top_number(env_name: str, default: int) -> int:
#     wanted_top_items = safe_int_conversion(os.getenv(env_name), default=default)
#     replica_count = safe_int_conversion(os.getenv('REPLICA_COUNT'), default=1)
#     return wanted_top_items * replica_count

# Pequeña trampa
def get_payload_len(payload: List[Dict[str, Any]]) -> int:
    if not payload:
        return 0

    item0 = payload[0]
    qty = item0.get("quantity_totals")
    prof = item0.get("profit_totals")

    is_items_payload: bool = (
        len(payload) == 1
        and isinstance(item0, dict)
        and (isinstance(qty, dict) or isinstance(prof, dict))
    )

    if is_items_payload:
        return len(qty or {}) + len(prof or {})

    return len(payload)