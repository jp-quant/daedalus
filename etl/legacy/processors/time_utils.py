"""Timestamp parsing utilities for ETL processors."""
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Try to import dateutil as fallback for edge cases
try:
    from dateutil import parser as dateutil_parser
    HAS_DATEUTIL = True
except ImportError:
    HAS_DATEUTIL = False
    logger.warning("dateutil not installed - some timestamp formats may fail. Install with: pip install python-dateutil")

# Note: We use Python's built-in datetime over alternatives like dateutil.parser or pandas
# because:
# 1. datetime.fromisoformat() is FAST (C-optimized, ~10x faster than dateutil.parser)
# 2. No external dependencies (dateutil is heavy, pandas overkill for just parsing)
# 3. ISO8601 is the standard for financial data (all major exchanges use it)
# 4. We handle edge cases manually (nanoseconds, Z vs +00:00) for specific needs
#
# For truly arbitrary formats (rare in production financial data), consider:
# - dateutil.parser.parse() - slower but handles almost any format
# - pandas.to_datetime() - good for vectorized operations on DataFrames
#
# Current approach: Fast, minimal dependencies, handles 99.9% of real-world cases


def parse_timestamp_fields(timestamp_str: Any) -> Dict[str, Any]:
    """
    Parse timestamp and extract all time components.
    
    Handles multiple formats:
    - ISO8601 strings (e.g., 2025-11-26T00:02:40Z)
    - Unix timestamps (int/float, seconds or milliseconds)
    - pandas.Timestamp objects
    - datetime objects
    
    Args:
        timestamp_str: Timestamp input (string, int, float, datetime, etc.)
        
    Returns:
        Dictionary with time fields: year, month, day, hour, minute, second, 
        microsecond, date, datetime_obj. Returns empty dict on parse failure.
    """
    if timestamp_str is None:
        return {}
    
    try:
        # Handle pandas Timestamp objects (from parquet files)
        if hasattr(timestamp_str, 'to_pydatetime'):
            dt = timestamp_str.to_pydatetime()
        # Handle datetime objects directly
        elif isinstance(timestamp_str, datetime):
            dt = timestamp_str
        # Handle numeric timestamps (Unix epoch)
        elif isinstance(timestamp_str, (int, float)):
            # Heuristic to detect milliseconds vs seconds
            # 30000000000 is roughly year 2920 in seconds, but 1970 in ms
            # Current timestamp ~1.7e9 (sec) or ~1.7e12 (ms)
            ts = float(timestamp_str)
            if ts > 1e11: # Likely milliseconds
                ts = ts / 1000.0
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        # Handle string timestamps
        elif isinstance(timestamp_str, str):
            # Normalize timezone format (Z -> +00:00)
            normalized = timestamp_str.replace('Z', '+00:00')
            
            # Handle fractional seconds (Python datetime supports up to 6 digits - microseconds)
            # Examples:
            # - 2025-11-27T00:20:02.069533736+00:00 (9 digits) -> truncate to 6
            # - 2025-11-27T11:41:54.58958+00:00 (5 digits) -> pad to 6
            # - 2025-11-27T00:20:02+00:00 (0 digits) -> keep as is
            if '.' in normalized:
                # Split on decimal point
                date_part, frac_and_tz = normalized.split('.', 1)
                
                # Extract fractional seconds and timezone
                if '+' in frac_and_tz:
                    frac, tz = frac_and_tz.split('+', 1)
                    tz_part = f"+{tz}"
                elif '-' in frac_and_tz[1:]:  # Skip first char in case of negative timezone
                    frac, tz = frac_and_tz.split('-', 1)
                    tz_part = f"-{tz}"
                else:
                    # No timezone in fractional part (shouldn't happen with normalized input)
                    frac = frac_and_tz
                    tz_part = ""
                
                # Normalize fractional seconds to exactly 6 digits
                if len(frac) > 6:
                    # Truncate to 6 digits (e.g., nanoseconds -> microseconds)
                    frac = frac[:6]
                elif len(frac) < 6:
                    # Pad with zeros to 6 digits (e.g., 58958 -> 589580)
                    frac = frac.ljust(6, '0')
                
                normalized = f"{date_part}.{frac}{tz_part}"
            
            dt = datetime.fromisoformat(normalized)
        else:
            logger.warning(f"Unsupported timestamp type: {type(timestamp_str)}")
            return {}
        
        return {
            "year": dt.year,
            "month": dt.month,
            "day": dt.day,
            "hour": dt.hour,
            "minute": dt.minute,
            "second": dt.second,
            "microsecond": dt.microsecond,
            "date": dt.strftime("%Y-%m-%d"),  # For partitioning
            "datetime": dt.isoformat(),  # Full ISO8601 string
            "day_of_week": dt.weekday(),  # 0=Monday, 6=Sunday
            "is_weekend": dt.weekday() >= 5,  # True if Saturday/Sunday
        }
    
    except (ValueError, AttributeError) as e:
        logger.warning(f"Primary parsing failed for '{timestamp_str}': {e}")
        
        # FALLBACK: Try dateutil.parser (handles almost any format)
        if HAS_DATEUTIL and isinstance(timestamp_str, str):
            try:
                logger.info(f"Attempting dateutil fallback for: {timestamp_str}")
                dt = dateutil_parser.parse(timestamp_str)
                return {
                    "year": dt.year,
                    "month": dt.month,
                    "day": dt.day,
                    "hour": dt.hour,
                    "minute": dt.minute,
                    "second": dt.second,
                    "microsecond": dt.microsecond,
                    "date": dt.strftime("%Y-%m-%d"),
                    "datetime": dt.isoformat(),
                    "day_of_week": dt.weekday(),  # 0=Monday, 6=Sunday
                    "is_weekend": dt.weekday() >= 5,  # True if Saturday/Sunday
                }
            except Exception as fallback_error:
                logger.error(f"Dateutil fallback also failed for '{timestamp_str}': {fallback_error}")
        else:
            if not HAS_DATEUTIL:
                logger.error(f"No fallback available - install dateutil: pip install python-dateutil")
        
        return {}


def add_time_fields(
    record: Dict[str, Any],
    timestamp_field: str,
    fallback_field: Optional[str] = None,
    prefix: str = "",
) -> Dict[str, Any]:
    """
    Add derived time fields to a record based on timestamp field.
    
    Args:
        record: Record to enhance with time fields
        timestamp_field: Primary timestamp field name to parse
        fallback_field: Optional fallback timestamp field if primary is missing
        prefix: Optional prefix for generated field names (e.g., "event_")
        
    Returns:
        Record with added time fields (year, month, day, hour, minute, second, etc.)
    
    Example:
        record = {"server_timestamp": "2025-11-26T00:02:40.498898Z"}
        add_time_fields(record, "server_timestamp")
        # Adds: year, month, day, hour, minute, second, microsecond, date
    """
    # Get timestamp value from primary or fallback field
    timestamp_str = record.get(timestamp_field)
    if not timestamp_str and fallback_field:
        timestamp_str = record.get(fallback_field)
    
    if not timestamp_str:
        logger.info(f"No timestamp found in fields: {timestamp_field}, {fallback_field}")
        return record
    
    # Parse and add time fields
    time_fields = parse_timestamp_fields(timestamp_str)
    
    # Add with optional prefix
    for key, value in time_fields.items():
        field_name = f"{prefix}{key}" if prefix else key
        record[field_name] = value
    
    return record
