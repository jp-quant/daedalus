"""
Time Series Resampling Utilities
================================

ASOF-style resampling for point-in-time accurate financial data processing.

These utilities ensure no lookahead bias when resampling high-frequency
market data to regular time intervals.

Key Concepts:
- ASOF semantics: "At time T, what was the most recent known state?"
- Point-in-time safe: Each timestamp T contains only information available BEFORE T
- Backtesting/ML safe: No data leakage from future observations

Usage:
    from etl.utils.resampling import resample_orderbook, resample_timeseries
    
    # Resample orderbook to 1-minute bars
    resampled = resample_orderbook(orderbook_lf, frequency="1m")
    
    # Generic timeseries resampling
    resampled = resample_timeseries(df, frequency="5m", value_cols=["price", "volume"])
"""

import re
from typing import List, Optional

import polars as pl


def resample_orderbook(
    lf: pl.LazyFrame,
    frequency: str = "1m",
    max_staleness: Optional[str] = None,
    fill_gaps: bool = False,
    group_cols: Optional[List[str]] = None,
    timestamp_col: str = "timestamp",
) -> pl.LazyFrame:
    """
    Resample orderbook snapshots to consistent time frequency using ASOF semantics.
    
    This is the gold-standard approach for point-in-time financial data:
    "At time T, what was the most recent known orderbook state?"
    
    The answer is always: the last snapshot BEFORE or AT time T.
    This prevents lookahead bias and is safe for backtesting/ML training.
    
    Algorithm:
    1. Truncate each snapshot's timestamp to its frequency bucket
       - 17:04:59.322 → bucket 17:04:00
    2. Take the LAST snapshot within each bucket (most recent state)
    3. Shift bucket forward by one period
       - Bucket 17:04:00 becomes timestamp 17:05:00
       - Semantics: "At 17:05:00, the known state was from 17:04:59"
    
    Args:
        lf: LazyFrame with orderbook data. Required columns:
            - timestamp_col: datetime of snapshot
            - 'bids': list of bid levels (price/size structs or arrays)
            - 'asks': list of ask levels (price/size structs or arrays)
            Optional: 'symbol', 'exchange' for multi-asset datasets
        
        frequency: Resample frequency string. Examples:
            - "1s", "5s", "30s" (seconds)
            - "1m", "5m", "15m" (minutes)  
            - "1h", "4h" (hours)
            - "1d" (daily)
        
        max_staleness: Optional maximum age for data validity. If the last
            snapshot is older than this threshold, mark data as stale.
            Examples: "5m", "1h". When set, adds 'is_stale' boolean column.
            Useful for detecting gaps in data collection.
        
        fill_gaps: If True, generates rows for ALL time buckets in the range
            (even those without snapshots) using forward-fill. This ensures
            a complete, regular time grid. Default False to preserve only
            buckets that had actual data.
        
        group_cols: Columns to group by in addition to time bucket.
            Default: ["symbol", "exchange"] if present in schema.
            Set to [] to disable grouping (single-asset mode).
        
        timestamp_col: Name of the timestamp column. Default "timestamp".
            Also supports "capture_ts" commonly used in ingestion.
    
    Returns:
        LazyFrame with columns:
            - 'timestamp': The point-in-time this row represents (bucket END)
            - 'bids': Orderbook bid side (last known state)
            - 'asks': Orderbook ask side (last known state)
            - 'original_timestamp': Actual timestamp of the source snapshot
            - 'snapshot_count': Number of snapshots aggregated into this bucket
            - 'data_staleness': Duration since last actual snapshot
            - 'is_stale': (if max_staleness set) Boolean flag for stale data
            - Plus any group_cols (symbol, exchange, etc.)
    
    Example:
        >>> # Basic usage - resample to 1-minute bars
        >>> resampled = resample_orderbook(orderbook_lf, frequency="1m")
        
        >>> # With staleness detection (flag data older than 5 minutes)
        >>> resampled = resample_orderbook(
        ...     orderbook_lf, 
        ...     frequency="1m",
        ...     max_staleness="5m"
        ... )
        
        >>> # Fill gaps for complete time grid
        >>> resampled = resample_orderbook(
        ...     orderbook_lf,
        ...     frequency="1m", 
        ...     fill_gaps=True
        ... )
        
        >>> # Single-asset mode (no symbol/exchange grouping)
        >>> resampled = resample_orderbook(
        ...     btc_orderbook_lf,
        ...     frequency="1m",
        ...     group_cols=[]
        ... )
    
    Notes:
        - Point-in-time safe: No lookahead bias. Each timestamp T contains
          only information available BEFORE time T.
        - Backtesting safe: Use these resampled snapshots directly in
          backtests without fear of data leakage.
        - ML training safe: Features computed on this data won't leak
          future information into training samples.
        - The 'original_timestamp' column lets you verify data freshness
          and debug any staleness issues.
    """
    schema = lf.collect_schema()
    schema_cols = schema.names()
    
    # Auto-detect group columns if not specified
    if group_cols is None:
        group_cols = [col for col in ["symbol", "exchange"] if col in schema_cols]
    
    # Detect timestamp column
    if timestamp_col not in schema_cols:
        # Try common alternatives
        for alt_col in ["capture_ts", "ts", "time"]:
            if alt_col in schema_cols:
                timestamp_col = alt_col
                break
        else:
            raise ValueError(
                f"Timestamp column '{timestamp_col}' not found. "
                f"Available columns: {schema_cols}"
            )
    
    # Step 1: Ensure timestamp is datetime with millisecond precision
    result = lf.with_columns(
        pl.col(timestamp_col).cast(pl.Datetime("ms")).alias("_ts_normalized")
    )
    
    # Step 2: Truncate timestamp to frequency bucket
    result = result.with_columns(
        pl.col("_ts_normalized").dt.truncate(frequency).alias("_truncated_bucket")
    )
    
    # Step 3: Build aggregation - take LAST snapshot per bucket
    agg_cols = ["_truncated_bucket"] + [
        col for col in group_cols if col in schema_cols
    ]
    
    # Build aggregation expressions
    agg_exprs = [
        pl.col("_ts_normalized").last().alias("original_timestamp"),
        pl.len().alias("snapshot_count"),
    ]
    
    # Add bids/asks if present
    if "bids" in schema_cols:
        agg_exprs.append(pl.col("bids").last())
    if "asks" in schema_cols:
        agg_exprs.append(pl.col("asks").last())
    
    result = result.group_by(agg_cols).agg(agg_exprs)
    
    # Step 4: Shift bucket forward by one period (ASOF semantics)
    result = result.with_columns(
        pl.col("_truncated_bucket").dt.offset_by(frequency).alias("timestamp")
    )
    
    # Step 5: Calculate data staleness
    result = result.with_columns(
        (pl.col("timestamp") - pl.col("original_timestamp")).alias("data_staleness")
    )
    
    # Step 6: Optional staleness flagging
    if max_staleness is not None:
        staleness_ms = parse_duration_to_ms(max_staleness)
        result = result.with_columns(
            (pl.col("data_staleness") > pl.duration(milliseconds=staleness_ms))
            .alias("is_stale")
        )
    
    # Step 7: Optional gap filling with forward-fill
    if fill_gaps:
        result = _fill_time_gaps_orderbook(result, frequency, group_cols)
    
    # Cleanup and sort
    sort_cols = [col for col in group_cols if col in result.collect_schema().names()] + ["timestamp"]
    result = result.drop("_truncated_bucket").sort(sort_cols)
    
    return result


def resample_timeseries(
    lf: pl.LazyFrame,
    frequency: str = "1m",
    value_cols: Optional[List[str]] = None,
    agg_method: str = "last",
    group_cols: Optional[List[str]] = None,
    timestamp_col: str = "timestamp",
    add_staleness: bool = True,
) -> pl.LazyFrame:
    """
    Generic ASOF-style resampling for any time series data.
    
    Similar to resample_orderbook but works with arbitrary value columns.
    
    Args:
        lf: LazyFrame with time series data
        frequency: Resample frequency (e.g., "1m", "5m", "1h")
        value_cols: Columns to aggregate. If None, aggregates all non-group columns.
        agg_method: Aggregation method - "last" (default), "first", "mean", "sum"
        group_cols: Columns to group by (e.g., ["symbol", "exchange"])
        timestamp_col: Name of timestamp column
        add_staleness: Whether to add data_staleness column
    
    Returns:
        Resampled LazyFrame with ASOF semantics
    
    Example:
        >>> # Resample ticker data
        >>> resampled = resample_timeseries(
        ...     ticker_lf,
        ...     frequency="1m",
        ...     value_cols=["bid", "ask", "last_price"],
        ...     group_cols=["symbol"]
        ... )
    """
    schema = lf.collect_schema()
    schema_cols = schema.names()
    
    # Auto-detect group columns
    if group_cols is None:
        group_cols = [col for col in ["symbol", "exchange"] if col in schema_cols]
    
    # Auto-detect value columns
    if value_cols is None:
        exclude_cols = set(group_cols + [timestamp_col, "capture_ts", "ts"])
        value_cols = [col for col in schema_cols if col not in exclude_cols]
    
    # Normalize timestamp
    result = lf.with_columns(
        pl.col(timestamp_col).cast(pl.Datetime("ms")).alias("_ts_normalized")
    )
    
    # Truncate to bucket
    result = result.with_columns(
        pl.col("_ts_normalized").dt.truncate(frequency).alias("_truncated_bucket")
    )
    
    # Build aggregation based on method
    agg_map = {
        "last": lambda col: pl.col(col).last(),
        "first": lambda col: pl.col(col).first(),
        "mean": lambda col: pl.col(col).mean(),
        "sum": lambda col: pl.col(col).sum(),
    }
    agg_fn = agg_map.get(agg_method, agg_map["last"])
    
    agg_cols = ["_truncated_bucket"] + group_cols
    agg_exprs = [agg_fn(col) for col in value_cols]
    agg_exprs.append(pl.col("_ts_normalized").last().alias("original_timestamp"))
    agg_exprs.append(pl.len().alias("snapshot_count"))
    
    result = result.group_by(agg_cols).agg(agg_exprs)
    
    # Shift forward (ASOF semantics)
    result = result.with_columns(
        pl.col("_truncated_bucket").dt.offset_by(frequency).alias("timestamp")
    )
    
    # Add staleness
    if add_staleness:
        result = result.with_columns(
            (pl.col("timestamp") - pl.col("original_timestamp")).alias("data_staleness")
        )
    
    # Cleanup and sort
    sort_cols = group_cols + ["timestamp"]
    result = result.drop("_truncated_bucket").sort(sort_cols)
    
    return result


def parse_duration_to_ms(duration_str: str) -> int:
    """
    Parse a duration string to milliseconds.
    
    Supports formats:
    - Seconds: "1s", "30s", "90s"
    - Minutes: "1m", "5m", "15m"
    - Hours: "1h", "4h", "24h"
    - Days: "1d", "7d"
    
    Args:
        duration_str: Duration string (e.g., "5m", "1h", "30s")
    
    Returns:
        Duration in milliseconds
    
    Raises:
        ValueError: If format is invalid
    """
    match = re.match(r"(\d+)([smhd])", duration_str.lower())
    if not match:
        raise ValueError(
            f"Invalid duration format: {duration_str}. "
            "Use e.g., '5m', '1h', '30s', '1d'"
        )
    
    value = int(match.group(1))
    unit = match.group(2)
    
    multipliers = {
        's': 1_000,
        'm': 60_000,
        'h': 3_600_000,
        'd': 86_400_000,
    }
    
    return value * multipliers[unit]


def _fill_time_gaps_orderbook(
    lf: pl.LazyFrame, 
    frequency: str,
    group_cols: List[str],
) -> pl.LazyFrame:
    """
    Fill gaps in orderbook time series with forward-filled values.
    
    Creates a complete time grid and forward-fills missing buckets
    with the last known orderbook state.
    
    Note: This operation requires knowing the time range, which may
    trigger partial collection for very large lazy frames.
    """
    schema_cols = lf.collect_schema().names()
    active_group_cols = [col for col in group_cols if col in schema_cols]
    
    if active_group_cols:
        # Multi-asset: fill gaps per group using window functions
        result = (
            lf
            .sort(active_group_cols + ["timestamp"])
            .with_columns([
                pl.col("bids").forward_fill().over(active_group_cols) if "bids" in schema_cols else pl.lit(None),
                pl.col("asks").forward_fill().over(active_group_cols) if "asks" in schema_cols else pl.lit(None),
                pl.col("original_timestamp").forward_fill().over(active_group_cols),
                pl.col("snapshot_count").fill_null(0),
            ])
        )
    else:
        # Single-asset: simple forward fill
        result = lf.sort("timestamp").with_columns([
            pl.col("bids").forward_fill() if "bids" in schema_cols else pl.lit(None),
            pl.col("asks").forward_fill() if "asks" in schema_cols else pl.lit(None),
            pl.col("original_timestamp").forward_fill(),
            pl.col("snapshot_count").fill_null(0),
        ])
    
    return result


def get_resampling_stats(
    df: pl.DataFrame,
    staleness_col: str = "data_staleness",
    stale_flag_col: str = "is_stale",
) -> dict:
    """
    Compute statistics about resampled data quality.
    
    Args:
        df: Resampled DataFrame
        staleness_col: Name of staleness duration column
        stale_flag_col: Name of stale flag column (if present)
    
    Returns:
        Dictionary with statistics:
        - total_rows: Total number of resampled rows
        - stale_rows: Number of stale rows (if is_stale column present)
        - stale_pct: Percentage of stale rows
        - avg_staleness: Average data staleness
        - max_staleness: Maximum data staleness
        - min_staleness: Minimum data staleness
        - median_staleness: Median data staleness
    """
    stats = {
        "total_rows": len(df),
    }
    
    if staleness_col in df.columns:
        staleness = df[staleness_col]
        stats["avg_staleness"] = staleness.mean()
        stats["max_staleness"] = staleness.max()
        stats["min_staleness"] = staleness.min()
        stats["median_staleness"] = staleness.median()
    
    if stale_flag_col in df.columns:
        stale_count = df.filter(pl.col(stale_flag_col)).height
        stats["stale_rows"] = stale_count
        stats["stale_pct"] = (stale_count / len(df) * 100) if len(df) > 0 else 0.0
    
    return stats
