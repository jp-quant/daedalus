"""
Partitioning utilities for Daedalus market data pipeline.

Provides unified partitioning logic used by both ingestion and ETL layers.
This ensures consistent directory-aligned partitioning across the entire pipeline.

Directory-Aligned Partitioning Approach:
    Unlike traditional Hive partitioning where partition columns are derived
    from the directory path and NOT stored in the data files, our approach
    requires partition column values to EXIST in the Parquet data AND MATCH
    the directory partition values exactly.
    
    This means:
    - ALL partition columns exist in data with matching values
    - ALL partition values are sanitized for filesystem compatibility
    - Sanitization replaces prohibited characters (/, \\, :, etc.) with -
    - No ambiguity between path and data - they are always consistent

Default Partition Columns:
    ["exchange", "symbol", "year", "month", "day", "hour"]
    
    Creates paths like:
    orderbook/exchange=binanceus/symbol=BTC-USD/year=2025/month=6/day=19/hour=14/

Usage:
    from shared.partitioning import (
        sanitize_partition_value,
        build_partition_path,
        partition_dataframe,
        DEFAULT_PARTITION_COLUMNS,
    )
    
    # Sanitize a single value
    safe_symbol = sanitize_partition_value("BTC/USD")  # -> "BTC-USD"
    
    # Build partition path from values
    path = build_partition_path(
        base_path="/data/processed",
        partition_cols=["exchange", "symbol", "year", "month", "day", "hour"],
        partition_values=("binanceus", "BTC-USD", 2025, 6, 19, 14),
    )
    # -> "/data/processed/exchange=binanceus/symbol=BTC-USD/year=2025/month=06/day=19/hour=14"
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

# Polars import with fallback for environments without polars
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None  # type: ignore


# =============================================================================
# Constants
# =============================================================================

# Default partition columns for directory-aligned partitioning
DEFAULT_PARTITION_COLUMNS: List[str] = [
    "exchange", "symbol", "year", "month", "day", "hour"
]

# Characters that are prohibited in filesystem paths (replaced with -)
# Covers Windows, Linux, macOS, and S3 key restrictions
PROHIBITED_PATH_CHARS = re.compile(r'[/\\:*?"<>|]')

# Datetime partition columns that need zero-padding
DATETIME_PARTITION_COLS = {"year", "month", "day", "hour"}


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class PartitionConfig:
    """
    Configuration for partitioned data writing.
    
    Attributes:
        partition_cols: List of columns to partition by.
        sanitize_values: Whether to sanitize values for filesystem safety.
        zero_pad_datetime: Whether to zero-pad datetime columns (month -> '06').
        drop_partition_cols_from_data: Whether to drop partition columns from
            the written data files (Hive-style) or keep them (Directory-Aligned).
            Default: False (Directory-Aligned - keeps columns in data).
    """
    partition_cols: List[str] = field(default_factory=lambda: DEFAULT_PARTITION_COLUMNS.copy())
    sanitize_values: bool = True
    zero_pad_datetime: bool = False
    drop_partition_cols_from_data: bool = False
    
    def validate(self) -> None:
        """Validate configuration."""
        if not self.partition_cols:
            raise ValueError("partition_cols cannot be empty")
        if len(self.partition_cols) != len(set(self.partition_cols)):
            raise ValueError("partition_cols contains duplicates")


# =============================================================================
# Sanitization Functions
# =============================================================================

def sanitize_partition_value(value: Any) -> str:
    """
    Sanitize a partition value for filesystem compatibility.
    
    Replaces prohibited characters (/, \\, :, *, ?, ", <, >, |) with -.
    This sanitization is applied to BOTH the partition path AND the actual
    data to ensure they always match (Directory-Aligned Partitioning).
    
    Args:
        value: The raw partition value (will be converted to string)
        
    Returns:
        Sanitized string safe for filesystem paths
        
    Examples:
        >>> sanitize_partition_value("BTC/USD")
        'BTC-USD'
        >>> sanitize_partition_value("exchange:test")
        'exchange-test'
        >>> sanitize_partition_value(2024)
        '2024'
        >>> sanitize_partition_value(None)
        'unknown'
    """
    str_value = str(value) if value is not None else "unknown"
    return PROHIBITED_PATH_CHARS.sub("-", str_value)


def format_partition_value(col: str, value: Any, zero_pad: bool = True) -> str:
    """
    Format a partition value with optional zero-padding for datetime columns.
    
    Args:
        col: Column name (used to determine if datetime padding needed)
        value: The partition value
        zero_pad: Whether to zero-pad datetime columns
        
    Returns:
        Formatted string value
        
    Examples:
        >>> format_partition_value("month", 6, zero_pad=True)
        '06'
        >>> format_partition_value("month", 6, zero_pad=False)
        '6'
        >>> format_partition_value("symbol", "BTC/USD", zero_pad=True)
        'BTC-USD'
    """
    # Sanitize first
    sanitized = sanitize_partition_value(value)
    
    # Apply zero-padding for datetime columns
    if zero_pad and col in DATETIME_PARTITION_COLS:
        try:
            num_value = int(sanitized)
            if col == "year":
                return f"{num_value:04d}"
            else:  # month, day, hour
                return f"{num_value:02d}"
        except (ValueError, TypeError):
            pass
    
    return sanitized


# =============================================================================
# DateTime Extraction
# =============================================================================

def extract_datetime_components(
    record: Dict[str, Any],
    fallback_to_now: bool = True,
) -> Tuple[int, int, int, int]:
    """
    Extract year, month, day, hour from record timestamp.
    
    Precedence: capture_ts > timestamp > current time
    
    Args:
        record: Record dictionary with timestamp fields
        fallback_to_now: If True, use current time when no timestamp found
        
    Returns:
        Tuple of (year, month, day, hour) as integers
        
    Raises:
        ValueError: If no timestamp found and fallback_to_now is False
    """
    capture_ts = record.get("capture_ts")
    timestamp = record.get("timestamp")
    
    dt = None
    
    # Try capture_ts first (ISO format string or datetime)
    if capture_ts:
        if isinstance(capture_ts, datetime):
            dt = capture_ts
        elif isinstance(capture_ts, str):
            try:
                # Handle ISO format with Z suffix
                if capture_ts.endswith("Z"):
                    capture_ts = capture_ts[:-1] + "+00:00"
                dt = datetime.fromisoformat(capture_ts)
            except ValueError:
                pass
    
    # Fall back to timestamp (epoch milliseconds)
    if dt is None and timestamp:
        try:
            ts_value = int(timestamp)
            # Detect if milliseconds or seconds
            if ts_value > 1e12:  # Likely milliseconds
                ts_value = ts_value / 1000
            dt = datetime.fromtimestamp(ts_value, tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            pass
    
    # Fall back to current time
    if dt is None:
        if fallback_to_now:
            dt = datetime.now(tz=timezone.utc)
        else:
            raise ValueError("No timestamp found in record and fallback_to_now=False")
    
    return (dt.year, dt.month, dt.day, dt.hour)


def datetime_to_partition_values(
    dt: datetime,
    include_hour: bool = True,
) -> Dict[str, int]:
    """
    Convert datetime to partition value dictionary.
    
    Args:
        dt: Datetime to convert
        include_hour: Whether to include hour in output
        
    Returns:
        Dictionary with year, month, day, and optionally hour
    """
    result = {
        "year": dt.year,
        "month": dt.month,
        "day": dt.day,
    }
    if include_hour:
        result["hour"] = dt.hour
    return result


# =============================================================================
# Path Building
# =============================================================================

def build_partition_path(
    base_path: Union[str, Path],
    partition_cols: List[str],
    partition_values: Union[Tuple[Any, ...], Dict[str, Any]],
    zero_pad_datetime: bool = False,
) -> Path:
    """
    Build a Hive-style partition path from column names and values.
    
    Args:
        base_path: Base directory path
        partition_cols: List of partition column names in order
        partition_values: Tuple of values (same order as cols) or dict
        zero_pad_datetime: Whether to zero-pad datetime columns
        
    Returns:
        Full partition path as Path object
        
    Examples:
        >>> build_partition_path(
        ...     "/data",
        ...     ["exchange", "symbol", "year", "month", "day", "hour"],
        ...     ("binanceus", "BTC/USD", 2025, 6, 19, 14),
        ... )
        PosixPath('/data/exchange=binanceus/symbol=BTC-USD/year=2025/month=06/day=19/hour=14')
    """
    base = Path(base_path)
    
    # Convert dict to tuple if needed
    if isinstance(partition_values, dict):
        values = tuple(partition_values.get(col) for col in partition_cols)
    else:
        values = partition_values
    
    if len(partition_cols) != len(values):
        raise ValueError(
            f"Mismatch: {len(partition_cols)} columns but {len(values)} values"
        )
    
    # Build partition path parts
    parts = []
    for col, val in zip(partition_cols, values):
        formatted_val = format_partition_value(col, val, zero_pad=zero_pad_datetime)
        parts.append(f"{col}={formatted_val}")
    
    return base / "/".join(parts)


def parse_partition_path(
    path: Union[str, Path],
    partition_cols: Optional[List[str]] = None,
) -> Dict[str, str]:
    """
    Parse partition values from a Hive-style path.
    
    Args:
        path: Path containing partition directories
        partition_cols: Expected partition columns (for validation)
        
    Returns:
        Dictionary of partition column -> value
        
    Examples:
        >>> parse_partition_path("data/exchange=binanceus/symbol=BTC-USD/year=2025")
        {'exchange': 'binanceus', 'symbol': 'BTC-USD', 'year': '2025'}
    """
    path_str = str(path)
    parts = path_str.replace("\\", "/").split("/")
    
    result = {}
    for part in parts:
        if "=" in part:
            col, val = part.split("=", 1)
            result[col] = val
    
    # Validate expected columns if provided
    if partition_cols:
        missing = set(partition_cols) - set(result.keys())
        if missing:
            raise ValueError(f"Missing partition columns in path: {missing}")
    
    return result


# =============================================================================
# DataFrame Partitioning (Polars)
# =============================================================================

if HAS_POLARS:
    def partition_dataframe(
        df: "pl.DataFrame",
        partition_cols: List[str],
        sanitize: bool = True,
    ) -> Generator[Tuple[Tuple[Any, ...], "pl.DataFrame"], None, None]:
        """
        Partition a DataFrame by specified columns.
        
        Yields (partition_values, partition_df) tuples for each unique
        combination of partition column values.
        
        Args:
            df: DataFrame to partition
            partition_cols: Columns to partition by
            sanitize: Whether to sanitize partition values for filesystem safety
            
        Yields:
            Tuple of (partition_values, partition_dataframe)
            
        Examples:
            for partition_vals, partition_df in partition_dataframe(df, ["exchange", "symbol"]):
                print(f"Partition {partition_vals}: {len(partition_df)} rows")
        """
        # Validate columns exist
        missing = set(partition_cols) - set(df.columns)
        if missing:
            raise ValueError(f"Partition columns not in DataFrame: {missing}")
        
        # Sanitize partition column values if needed
        if sanitize:
            df = sanitize_dataframe_partition_values(df, partition_cols)
        
        # Group by partition columns
        for partition_values, group_df in df.group_by(partition_cols):
            # Ensure partition_values is always a tuple
            if not isinstance(partition_values, tuple):
                partition_values = (partition_values,)
            yield partition_values, group_df


    def sanitize_dataframe_partition_values(
        df: "pl.DataFrame",
        partition_cols: List[str],
    ) -> "pl.DataFrame":
        """
        Sanitize partition column values in a DataFrame for filesystem compatibility.
        
        This ensures partition values in the data match the directory paths.
        
        Args:
            df: Input DataFrame
            partition_cols: Columns to sanitize
            
        Returns:
            DataFrame with sanitized partition columns
        """
        for col in partition_cols:
            if col in df.columns:
                # Only sanitize string columns
                if df[col].dtype == pl.Utf8 or df[col].dtype == pl.String:
                    df = df.with_columns(
                        pl.col(col).str.replace_all(r'[/\\:*?"<>|]', "-").alias(col)
                    )
        return df


    def add_datetime_partition_columns(
        df: "pl.DataFrame",
        timestamp_col: str = "timestamp",
        add_cols: Optional[List[str]] = None,
    ) -> "pl.DataFrame":
        """
        Add year, month, day, hour columns derived from a timestamp column.
        
        Args:
            df: Input DataFrame
            timestamp_col: Name of timestamp column (epoch ms or datetime)
            add_cols: Which columns to add (default: all datetime partition cols)
            
        Returns:
            DataFrame with added datetime partition columns
        """
        add_cols = add_cols or ["year", "month", "day", "hour"]
        
        # Handle different timestamp formats
        ts_dtype = df[timestamp_col].dtype
        
        if ts_dtype in (pl.Int64, pl.UInt64, pl.Int32, pl.UInt32):
            # Epoch milliseconds - convert to datetime
            dt_col = pl.col(timestamp_col).cast(pl.Int64)
            # Detect if ms or seconds based on magnitude
            # If > 1e12, assume milliseconds
            dt_col = pl.when(pl.col(timestamp_col) > 1e12).then(
                (pl.col(timestamp_col) / 1000).cast(pl.Int64)
            ).otherwise(
                pl.col(timestamp_col).cast(pl.Int64)
            )
            dt_expr = dt_col.cast(pl.Datetime("ms")).dt.replace_time_zone("UTC")
        elif ts_dtype == pl.Datetime:
            dt_expr = pl.col(timestamp_col)
        else:
            raise ValueError(f"Unsupported timestamp dtype: {ts_dtype}")
        
        # Add requested columns
        exprs = []
        if "year" in add_cols and "year" not in df.columns:
            exprs.append(dt_expr.dt.year().alias("year"))
        if "month" in add_cols and "month" not in df.columns:
            exprs.append(dt_expr.dt.month().alias("month"))
        if "day" in add_cols and "day" not in df.columns:
            exprs.append(dt_expr.dt.day().alias("day"))
        if "hour" in add_cols and "hour" not in df.columns:
            exprs.append(dt_expr.dt.hour().alias("hour"))
        
        if exprs:
            df = df.with_columns(exprs)
        
        return df


    def ensure_partition_columns_exist(
        df: "pl.DataFrame",
        partition_cols: List[str],
        timestamp_col: str = "timestamp",
    ) -> "pl.DataFrame":
        """
        Ensure all partition columns exist in the DataFrame.
        
        - For datetime columns (year, month, day, hour): derives from timestamp
        - For other columns: raises error if missing
        
        Args:
            df: Input DataFrame
            partition_cols: Required partition columns
            timestamp_col: Column to derive datetime partitions from
            
        Returns:
            DataFrame with all partition columns present
            
        Raises:
            ValueError: If non-datetime partition column is missing
        """
        missing = set(partition_cols) - set(df.columns)
        
        if not missing:
            return df
        
        # Check which missing columns are datetime-derivable
        datetime_cols = missing & DATETIME_PARTITION_COLS
        other_missing = missing - datetime_cols
        
        if other_missing:
            raise ValueError(
                f"Partition columns missing and cannot be derived: {other_missing}"
            )
        
        # Add datetime columns from timestamp
        if datetime_cols and timestamp_col in df.columns:
            df = add_datetime_partition_columns(
                df, timestamp_col, add_cols=list(datetime_cols)
            )
        elif datetime_cols:
            raise ValueError(
                f"Cannot derive datetime columns {datetime_cols} - "
                f"'{timestamp_col}' column not found"
            )
        
        return df


# =============================================================================
# Utility Functions
# =============================================================================

def get_partition_cols_for_granularity(
    granularity: str,
    include_identity: bool = True,
) -> List[str]:
    """
    Get partition columns for a given time granularity.
    
    Args:
        granularity: One of "year", "month", "day", "hour"
        include_identity: Whether to include exchange/symbol columns
        
    Returns:
        List of partition column names
        
    Examples:
        >>> get_partition_cols_for_granularity("day")
        ['exchange', 'symbol', 'year', 'month', 'day']
        >>> get_partition_cols_for_granularity("hour", include_identity=False)
        ['year', 'month', 'day', 'hour']
    """
    identity_cols = ["exchange", "symbol"] if include_identity else []
    
    if granularity == "year":
        return identity_cols + ["year"]
    elif granularity == "month":
        return identity_cols + ["year", "month"]
    elif granularity == "day":
        return identity_cols + ["year", "month", "day"]
    elif granularity == "hour":
        return identity_cols + ["year", "month", "day", "hour"]
    else:
        raise ValueError(f"Unknown granularity: {granularity}")
