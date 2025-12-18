"""
Timestamp parsing utilities for ETL processing.

Provides fast, dependency-minimal timestamp parsing for financial data.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Try to import dateutil as fallback for edge cases
try:
    from dateutil import parser as dateutil_parser
    HAS_DATEUTIL = True
except ImportError:
    HAS_DATEUTIL = False
    logger.warning(
        "dateutil not installed - some timestamp formats may fail. "
        "Install with: pip install python-dateutil"
    )


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
        dt: Optional[datetime] = None
        
        # Handle datetime objects directly
        if isinstance(timestamp_str, datetime):
            dt = timestamp_str
            
        # Handle numeric timestamps (Unix epoch)
        elif isinstance(timestamp_str, (int, float)):
            ts_value = float(timestamp_str)
            # Detect if milliseconds (13+ digits) or seconds
            if ts_value > 1e12:  # Likely milliseconds
                ts_value = ts_value / 1000
            dt = datetime.fromtimestamp(ts_value, tz=timezone.utc)
            
        # Handle string timestamps
        elif isinstance(timestamp_str, str):
            dt = _parse_string_timestamp(timestamp_str)
            
        # Try to handle pandas Timestamp
        else:
            try:
                # pandas.Timestamp has .to_pydatetime()
                if hasattr(timestamp_str, 'to_pydatetime'):
                    dt = timestamp_str.to_pydatetime()
            except Exception:
                pass
        
        if dt is None:
            return {}
            
        # Ensure UTC timezone
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        return {
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'hour': dt.hour,
            'minute': dt.minute,
            'second': dt.second,
            'microsecond': dt.microsecond,
            'date': dt.strftime('%Y-%m-%d'),
            'datetime_obj': dt,
            'day_of_week': dt.weekday(),  # 0=Monday, 6=Sunday
            'is_weekend': dt.weekday() >= 5,
        }
        
    except Exception as e:
        logger.warning(f"Failed to parse timestamp: {timestamp_str} ({type(timestamp_str).__name__}): {e}")
        return {}


def _parse_string_timestamp(ts_str: str) -> Optional[datetime]:
    """
    Parse string timestamp with multiple format support.
    
    Optimized for ISO8601 (most common in financial data).
    Falls back to dateutil for edge cases.
    """
    # Handle common ISO8601 variations
    clean = ts_str.strip()
    
    # Replace Z with +00:00 for fromisoformat compatibility
    if clean.endswith('Z'):
        clean = clean[:-1] + '+00:00'
    
    # Handle nanoseconds (truncate to microseconds for datetime)
    # ISO format: YYYY-MM-DDTHH:MM:SS.nnnnnn+00:00
    # Some exchanges provide nanoseconds: YYYY-MM-DDTHH:MM:SS.nnnnnnnnn
    if '.' in clean:
        parts = clean.split('.')
        if len(parts) == 2:
            frac_and_tz = parts[1]
            # Find where timezone starts (+ or - after fraction)
            tz_start = -1
            for i, c in enumerate(frac_and_tz):
                if c in '+-' and i > 0:
                    tz_start = i
                    break
            
            if tz_start > 0:
                frac = frac_and_tz[:tz_start]
                tz = frac_and_tz[tz_start:]
                # Truncate to 6 digits (microseconds)
                frac = frac[:6].ljust(6, '0')
                clean = f"{parts[0]}.{frac}{tz}"
            else:
                # No timezone in string
                frac = frac_and_tz[:6].ljust(6, '0')
                clean = f"{parts[0]}.{frac}"
    
    try:
        return datetime.fromisoformat(clean)
    except ValueError:
        pass
    
    # Fallback to dateutil if available
    if HAS_DATEUTIL:
        try:
            return dateutil_parser.parse(ts_str)
        except Exception:
            pass
    
    return None


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
        logger.debug(f"No timestamp found in fields: {timestamp_field}, {fallback_field}")
        return record
    
    # Parse and add time fields
    time_fields = parse_timestamp_fields(timestamp_str)
    
    # Add with optional prefix
    for key, value in time_fields.items():
        field_name = f"{prefix}{key}" if prefix else key
        record[field_name] = value
    
    return record
