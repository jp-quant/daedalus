"""
Ticker Feature Transform
========================

Transform that computes ticker features from bronze to silver tier.

Ticker data is fully vectorized - no cross-channel dependencies.

Features computed:
- Spread (absolute and relative)
- Mid price
- Volume imbalance
- Time features (hour, day_of_week, is_weekend)
"""

import logging
from typing import Any, Dict

import polars as pl

from etl.core.base import BaseTransform, TransformContext
from etl.core.config import TransformConfig
from etl.core.registry import register_transform

logger = logging.getLogger(__name__)


@register_transform("ticker_features")
class TickerFeatureTransform(BaseTransform):
    """
    Transform bronze ticker data to silver feature data.
    
    Ticker data has no cross-channel dependencies, so this is
    fully vectorized and very fast.
    
    Features computed:
    - spread: Absolute spread (ask - bid)
    - relative_spread: Spread / mid_price
    - mid_price: (bid + ask) / 2
    - volume_imbalance: (bid_vol - ask_vol) / (bid_vol + ask_vol)
    - hour: Hour of day (0-23)
    - day_of_week: Day of week (0=Monday, 6=Sunday)
    - is_weekend: Boolean flag for weekend
    
    Example:
        config = TransformConfig(
            name="ticker_features",
            inputs={"bronze": InputConfig(name="bronze", path="bronze/ticker")},
            outputs={"silver": OutputConfig(name="silver", path="silver/ticker")},
        )
        transform = TickerFeatureTransform(config)
        executor.execute(transform)
    """
    
    def transform(
        self,
        inputs: Dict[str, pl.LazyFrame],
        context: TransformContext,
    ) -> Dict[str, pl.LazyFrame]:
        """
        Transform bronze ticker data to silver features.
        
        Args:
            inputs: Dictionary with input LazyFrame containing ticker data
            context: Execution context
        
        Returns:
            Dictionary with "silver" key containing feature LazyFrame
        """
        # Get the bronze input
        bronze_key = list(self.config.inputs.keys())[0]
        bronze_lf = inputs[bronze_key]
        
        logger.info(f"Transforming ticker data with {self.name}")
        
        # Apply ticker feature engineering
        silver_lf = compute_ticker_features(bronze_lf)
        
        # Return with output key matching config
        output_key = list(self.config.outputs.keys())[0]
        return {output_key: silver_lf}


def compute_ticker_features(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute ticker features - fully vectorized.
    
    Args:
        df: LazyFrame with ticker data. Expected columns:
            - bid: Best bid price
            - ask: Best ask price
            - bid_volume: Bid volume at best bid
            - ask_volume: Ask volume at best ask
            - capture_ts: Timestamp (datetime)
        
    Returns:
        LazyFrame with ticker features added
    """
    # Add derived features
    df = df.with_columns([
        # Spread from ticker
        (pl.col("ask") - pl.col("bid")).alias("spread"),
        ((pl.col("ask") - pl.col("bid")) / ((pl.col("ask") + pl.col("bid")) / 2)).alias("relative_spread"),
        
        # Mid price
        ((pl.col("bid") + pl.col("ask")) / 2).alias("mid_price"),
        
        # Volume imbalance (if volumes exist)
        pl.when(pl.col("bid_volume") + pl.col("ask_volume") > 0)
            .then((pl.col("bid_volume") - pl.col("ask_volume")) / (pl.col("bid_volume") + pl.col("ask_volume")))
            .otherwise(0.0)
            .alias("volume_imbalance"),
    ])
    
    # Add time features (only if capture_ts exists and is datetime)
    df = df.with_columns([
        pl.col("capture_ts").dt.hour().alias("hour"),
        pl.col("capture_ts").dt.weekday().alias("day_of_week"),
        (pl.col("capture_ts").dt.weekday() >= 5).alias("is_weekend"),
    ])
    
    return df


def compute_ticker_rolling_features(
    df: pl.LazyFrame,
    windows: list[int] = [60, 300, 900],
    group_cols: list[str] = ["exchange", "symbol"],
    time_col: str = "capture_ts",
) -> pl.LazyFrame:
    """
    Compute rolling features on ticker data using group_by_dynamic.
    
    Args:
        df: LazyFrame with ticker features (must have mid_price, spread)
        windows: Rolling window sizes in seconds
        group_cols: Columns to group by (default: exchange, symbol)
        time_col: Name of timestamp column
        
    Returns:
        LazyFrame with rolling features added
    """
    # For each window, we compute rolling stats using group_by_dynamic
    # and then join back to the original dataframe
    result = df
    
    for window in windows:
        window_str = f"{window}s"
        suffix = f"_{window}s"
        
        # Compute rolling aggregates using group_by_dynamic
        rolling_stats = df.sort(time_col).group_by_dynamic(
            time_col,
            every="1s",  # Emit a result for each second
            period=window_str,  # Look back window seconds
            closed="left",
            label="right",
            group_by=group_cols,
        ).agg([
            pl.col("mid_price").mean().alias(f"mid_price_mean{suffix}"),
            pl.col("mid_price").std().alias(f"mid_price_std{suffix}"),
            pl.col("spread").mean().alias(f"spread_mean{suffix}"),
            pl.col("volume_imbalance").mean().alias(f"vol_imbalance_mean{suffix}"),
        ])
        
        # Join back to original - this is a left join on time and group cols
        result = result.join(
            rolling_stats,
            on=[time_col] + group_cols,
            how="left",
        )
    
    return result
