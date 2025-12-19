"""
Bar Aggregation Transform
=========================

Transform that aggregates high-frequency features into time bars (OHLCV).

Supports multiple bar durations (1m, 5m, 15m, 1h) and provides
comprehensive bar statistics for research and strategy development.

This transform operates on silver-tier data with features computed
and produces gold-tier aggregated bars.
"""

import logging
from typing import Any, Dict, List

import polars as pl

from etl.core.base import BaseTransform, TransformContext
from etl.core.config import TransformConfig
from etl.core.registry import register_transform

logger = logging.getLogger(__name__)


@register_transform("bar_aggregation")
class BarAggregationTransform(BaseTransform):
    """
    Transform silver HF features to gold-tier bars.
    
    Aggregates tick-level or snapshot-level data into time bars
    with comprehensive statistics for quantitative research.
    
    Output columns include:
    - OHLC: open, high, low, close (mid price based)
    - Spread stats: mean_spread, min_spread, max_spread
    - Imbalance: mean_l1_imbalance
    - OFI: sum_ofi (if available)
    - Volatility: realized_variance
    - Count: tick/snapshot count per bar
    
    Example:
        config = TransformConfig(
            name="bar_aggregation",
            inputs={"hf": InputConfig(name="hf", path="silver/orderbook")},
            outputs={"bars": OutputConfig(name="bars", path="gold/bars")},
        )
        transform = BarAggregationTransform(config)
    """
    
    def __init__(self, config: TransformConfig):
        """Initialize with config."""
        super().__init__(config)
        # Default bar durations if not specified in feature_config
        self._default_durations = [60, 300, 900, 3600]
    
    def transform(
        self,
        inputs: Dict[str, pl.LazyFrame],
        context: TransformContext,
    ) -> Dict[str, pl.LazyFrame]:
        """
        Aggregate HF data into time bars.
        
        Args:
            inputs: Dictionary with input LazyFrame containing HF features
            context: Execution context with feature_config
        
        Returns:
            Dictionary with bar data for each duration
        """
        # Get the HF input
        input_key = list(self.config.inputs.keys())[0]
        hf_lf = inputs[input_key]
        
        # Get bar durations from config or use defaults
        durations = getattr(context.feature_config, 'bar_durations', None)
        if durations is None:
            durations = self._default_durations
        
        logger.info(f"Aggregating to bars with durations: {durations}")
        
        # Aggregate to bars
        bars = aggregate_bars_vectorized(hf_lf, durations)
        
        # Return bars keyed by duration
        return {f"bars_{d}s": lf for d, lf in bars.items()}


def aggregate_bars_vectorized(
    df: pl.LazyFrame,
    durations: List[int] = [60, 300, 900, 3600],
    group_cols: List[str] = ["exchange", "symbol"],
    time_col: str = "capture_ts",
) -> Dict[int, pl.LazyFrame]:
    """
    Aggregate high-frequency features into time bars using Polars.
    
    This is a fully vectorized implementation using group_by_dynamic.
    Use this after structural/stateful features are computed.
    
    Args:
        df: LazyFrame with HF features including:
            - capture_ts: Timestamp column
            - mid_price: Mid price (required)
            - spread: Spread (optional)
            - relative_spread: Relative spread (optional)
            - imbalance_L1: L1 imbalance (optional)
            - ofi: Order flow imbalance (optional)
            - log_return: Log return (optional)
        durations: Bar durations in seconds (e.g., [60, 300, 900])
        group_cols: Columns to group by (default: exchange, symbol)
        time_col: Name of timestamp column
        
    Returns:
        Dict mapping duration -> aggregated LazyFrame with bars
    """
    bars = {}
    
    for duration in durations:
        window = f"{duration}s"
        
        # Core aggregations (always computed)
        agg_exprs = [
            # OHLC from mid_price
            pl.col("mid_price").first().alias("open"),
            pl.col("mid_price").max().alias("high"),
            pl.col("mid_price").min().alias("low"),
            pl.col("mid_price").last().alias("close"),
            
            # Count
            pl.len().alias("tick_count"),
        ]
        
        # Optional aggregations - only add if column exists
        # We need to check the schema for available columns
        # For now, add spread stats which are commonly available
        optional_cols = {
            "spread": [
                pl.col("spread").fill_null(0.0).mean().alias("mean_spread"),
                pl.col("spread").fill_null(0.0).min().alias("min_spread"),
                pl.col("spread").fill_null(0.0).max().alias("max_spread"),
            ],
            "relative_spread": [
                pl.col("relative_spread").fill_null(0.0).mean().alias("mean_relative_spread"),
            ],
            "imbalance_L1": [
                pl.col("imbalance_L1").fill_null(0.0).mean().alias("mean_l1_imbalance"),
            ],
            "total_imbalance": [
                pl.col("total_imbalance").fill_null(0.0).mean().alias("mean_total_imbalance"),
            ],
            "log_return": [
                pl.col("log_return").fill_null(0.0).var().alias("realized_variance"),
            ],
        }
        
        # Try to collect schema to check columns
        # LazyFrame doesn't have columns attribute, so we use a fallback approach
        try:
            schema = df.collect_schema()
            available_cols = set(schema.names())
        except Exception:
            # If we can't get schema, try all optional aggs
            available_cols = set(optional_cols.keys())
        
        for col_name, exprs in optional_cols.items():
            if col_name in available_cols:
                agg_exprs.extend(exprs)
        
        # Aggregate
        agg_df = df.group_by_dynamic(
            time_col,
            every=window,
            period=window,
            closed="left",
            label="left",
            group_by=group_cols,
        ).agg(agg_exprs)
        
        # Add bar metadata
        agg_df = agg_df.with_columns([
            pl.lit(duration).alias("bar_duration_sec"),
            
            # Derived bar features
            ((pl.col("close") - pl.col("open")) / pl.col("open")).alias("return"),
            ((pl.col("close") / pl.col("open")).log()).alias("log_return"),
            ((pl.col("high") - pl.col("low")) / pl.col("open")).alias("range"),
            
            # Partition columns for Hive-style partitioning
            pl.col(time_col).dt.year().alias("year"),
            pl.col(time_col).dt.month().alias("month"),
            pl.col(time_col).dt.day().alias("day"),
        ])
        
        bars[duration] = agg_df
    
    return bars


def aggregate_orderbook_bars(
    df: pl.LazyFrame,
    durations: List[int] = [60, 300, 900],
) -> Dict[int, pl.LazyFrame]:
    """
    Specialized bar aggregation for orderbook data.
    
    Includes orderbook-specific features like depth, microprice, etc.
    
    Args:
        df: LazyFrame with orderbook features
        durations: Bar durations in seconds
        
    Returns:
        Dict mapping duration -> aggregated LazyFrame
    """
    bars = {}
    
    for duration in durations:
        window = f"{duration}s"
        
        agg_exprs = [
            # OHLC (mid price)
            pl.col("mid_price").first().alias("open"),
            pl.col("mid_price").max().alias("high"),
            pl.col("mid_price").min().alias("low"),
            pl.col("mid_price").last().alias("close"),
            
            # Spread
            pl.col("spread").mean().alias("mean_spread"),
            pl.col("relative_spread").mean().alias("mean_relative_spread"),
            
            # Microprice
            pl.col("microprice").mean().alias("mean_microprice"),
            (pl.col("microprice") - pl.col("mid_price")).mean().alias("mean_micro_minus_mid"),
            
            # Imbalances
            pl.col("imbalance_L1").mean().alias("mean_l1_imbalance"),
            pl.col("total_imbalance").mean().alias("mean_total_imbalance"),
            
            # Depth
            pl.col("total_bid_depth").mean().alias("mean_bid_depth"),
            pl.col("total_ask_depth").mean().alias("mean_ask_depth"),
            
            # Smart depth imbalance
            pl.col("smart_depth_imbalance").mean().alias("mean_smart_depth_imbalance"),
            
            # OFI features
            pl.col("ofi").sum().alias("sum_ofi"),
            pl.col("mlofi").sum().alias("sum_mlofi"),
            
            # Volatility
            pl.col("log_return").var().alias("realized_variance"),
            
            # Count
            pl.len().alias("snapshot_count"),
        ]
        
        agg_df = df.group_by_dynamic(
            "capture_ts",
            every=window,
            period=window,
            closed="left",
            label="left",
            group_by=["exchange", "symbol"],
        ).agg(agg_exprs)
        
        # Add derived features
        agg_df = agg_df.with_columns([
            pl.lit(duration).alias("bar_duration_sec"),
            ((pl.col("close") - pl.col("open")) / pl.col("open")).alias("return"),
            (pl.col("realized_variance").sqrt() * pl.lit((86400 / duration) ** 0.5)).alias("annualized_vol"),
            
            # Partition columns for Hive-style partitioning
            pl.col("capture_ts").dt.year().alias("year"),
            pl.col("capture_ts").dt.month().alias("month"),
            pl.col("capture_ts").dt.day().alias("day"),
        ])
        
        bars[duration] = agg_df
    
    return bars
