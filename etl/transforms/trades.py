"""
Trades Feature Transform
========================

Transform that computes trades features from bronze to silver tier.

Trades data is fully vectorized for basic features.
More advanced features (like TFI) require orderbook context.

Features computed:
- Trade direction encoding
- Dollar volume
- Time features
- Log returns
- Rolling VWAP (optional aggregates)
"""

import logging
from typing import Any, Dict, List, Optional

import polars as pl

from etl.core.base import BaseTransform, TransformContext
from etl.core.config import TransformConfig, FeatureConfig
from etl.core.registry import register_transform
from etl.core.enums import FeatureCategory

logger = logging.getLogger(__name__)


@register_transform("trades_features")
class TradesFeatureTransform(BaseTransform):
    """
    Transform bronze trades data to silver feature data.
    
    Trades data is processed vectorized for basic features.
    Optionally computes rolling aggregates if feature_config contains ROLLING category.
    
    Features computed:
    - is_buy: Boolean flag (1 if buy, 0 if sell)
    - dollar_volume: price * amount
    - signed_volume: positive for buys, negative for sells
    - hour: Hour of day
    - day_of_week: Day of week
    - is_weekend: Weekend indicator
    - log_return: Log return from previous trade price
    
    Optional rolling features (if ROLLING in categories):
    - vwap_{window}s: Rolling VWAP
    - volume_{window}s: Rolling volume
    - dollar_volume_{window}s: Rolling dollar volume
    - trade_count_{window}s: Rolling trade count
    - buy_ratio_{window}s: Ratio of buy trades
    - realized_var_{window}s: Rolling realized variance
    
    Example:
        config = TransformConfig(
            name="trades_features",
            inputs={"bronze": InputConfig(name="bronze", path="bronze/trades")},
            outputs={"silver": OutputConfig(name="silver", path="silver/trades")},
        )
        transform = TradesFeatureTransform(config)
        executor.execute(transform)
    """
    
    def transform(
        self,
        inputs: Dict[str, pl.LazyFrame],
        context: TransformContext,
    ) -> Dict[str, pl.LazyFrame]:
        """
        Transform bronze trades data to silver features.
        
        Args:
            inputs: Dictionary with input LazyFrame containing trades data
            context: Execution context (contains feature_config for rolling windows)
        
        Returns:
            Dictionary with output key containing feature LazyFrame
        """
        # Get the bronze input
        bronze_key = list(self.config.inputs.keys())[0]
        bronze_lf = inputs[bronze_key]
        
        logger.info(f"Transforming trades data with {self.name}")
        
        # Apply base trades feature engineering
        silver_lf = compute_trades_features(bronze_lf)
        
        # Optionally add rolling aggregates based on feature_config
        if context.feature_config:
            feature_config = context.feature_config
            
            # Check if ROLLING category is enabled
            if FeatureCategory.ROLLING in feature_config.categories:
                windows = feature_config.rolling_windows
                if windows:
                    logger.info(f"Computing rolling aggregates: {windows}")
                    silver_lf = compute_trades_aggregates(
                        silver_lf,
                        windows=windows,
                    )
        
        # Return with output key matching config
        output_key = list(self.config.outputs.keys())[0]
        return {output_key: silver_lf}


def compute_trades_features(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute trades features - fully vectorized.
    
    Args:
        df: LazyFrame with trades data. Expected columns:
            - price: Trade price
            - amount: Trade size/amount
            - side: Trade side ('buy' or 'sell')
            - capture_ts: Timestamp (datetime)
            - exchange: Exchange name
            - symbol: Trading pair symbol
        
    Returns:
        LazyFrame with trades features added
    """
    # Add derived features
    df = df.with_columns([
        # Trade direction indicator
        (pl.col("side").str.to_lowercase() == "buy").cast(pl.Int8).alias("is_buy"),
        
        # Dollar volume
        (pl.col("price") * pl.col("amount")).alias("dollar_volume"),
        
        # Signed volume (positive for buys, negative for sells)
        pl.when(pl.col("side").str.to_lowercase() == "buy")
            .then(pl.col("amount"))
            .otherwise(-pl.col("amount"))
            .alias("signed_volume"),
    ])
    
    # Add time features
    df = df.with_columns([
        pl.col("capture_ts").dt.hour().alias("hour"),
        pl.col("capture_ts").dt.weekday().alias("day_of_week"),
        (pl.col("capture_ts").dt.weekday() >= 5).alias("is_weekend"),
        # Partition columns for Hive-style partitioning
        pl.col("capture_ts").dt.year().alias("year"),
        pl.col("capture_ts").dt.month().alias("month"),
        pl.col("capture_ts").dt.day().alias("day"),
    ])
    
    # Add log returns (sorted within each symbol)
    df = df.sort("capture_ts").with_columns([
        (pl.col("price") / pl.col("price").shift(1)).log().over(["exchange", "symbol"]).alias("log_return"),
    ])
    
    return df


def compute_trades_aggregates(
    df: pl.LazyFrame,
    windows: List[int] = [60, 300, 900],
    group_cols: List[str] = ["exchange", "symbol"],
    time_col: str = "capture_ts",
) -> pl.LazyFrame:
    """
    Compute rolling aggregate features on trades data using group_by_dynamic.
    
    Args:
        df: LazyFrame with trades features
        windows: Rolling window sizes in seconds
        group_cols: Columns to group by (default: exchange, symbol)
        time_col: Name of timestamp column
        
    Returns:
        LazyFrame with rolling aggregates added
    """
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
            # Rolling VWAP = sum(dollar_volume) / sum(amount)
            (pl.col("dollar_volume").sum() / pl.col("amount").sum()).alias(f"vwap{suffix}"),
            
            # Rolling volume
            pl.col("amount").sum().alias(f"volume{suffix}"),
            
            # Rolling dollar volume
            pl.col("dollar_volume").sum().alias(f"dollar_volume{suffix}"),
            
            # Rolling trade count
            pl.len().alias(f"trade_count{suffix}"),
            
            # Buy ratio = sum(is_buy) / count
            (pl.col("is_buy").sum() / pl.len()).alias(f"buy_ratio{suffix}"),
            
            # Rolling realized variance
            pl.col("log_return").var().alias(f"realized_var{suffix}"),
        ])
        
        # Join back to original
        result = result.join(
            rolling_stats,
            on=[time_col] + group_cols,
            how="left",
        )
    
    return result


def aggregate_trades_to_bars(
    df: pl.LazyFrame,
    durations: List[int] = [60, 300, 900, 3600],
) -> Dict[int, pl.LazyFrame]:
    """
    Aggregate trades into time bars.
    
    Args:
        df: LazyFrame with trades data
        durations: Bar durations in seconds
        
    Returns:
        Dict mapping duration -> aggregated LazyFrame
    """
    bars = {}
    
    for duration in durations:
        window = f"{duration}s"
        
        agg_df = df.group_by_dynamic(
            "capture_ts",
            every=window,
            period=window,
            closed="left",
            label="left",
            group_by=["exchange", "symbol"],
        ).agg([
            # OHLC
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            
            # Volume stats
            pl.col("amount").sum().alias("volume"),
            pl.col("dollar_volume").sum().alias("dollar_volume"),
            pl.col("price").count().alias("trade_count"),
            
            # Buy/sell split
            pl.col("amount").filter(pl.col("is_buy") == 1).sum().alias("buy_volume"),
            pl.col("amount").filter(pl.col("is_buy") == 0).sum().alias("sell_volume"),
            
            # VWAP
            (pl.col("dollar_volume").sum() / pl.col("amount").sum()).alias("vwap"),
            
            # Realized variance
            pl.col("log_return").var().alias("realized_variance"),
        ])
        
        # Add derived bar features
        agg_df = agg_df.with_columns([
            pl.lit(duration).alias("bar_duration"),
            
            # Returns
            ((pl.col("close") - pl.col("open")) / pl.col("open")).alias("return"),
            ((pl.col("close") / pl.col("open")).log()).alias("log_return"),
            
            # Range
            ((pl.col("high") - pl.col("low")) / pl.col("open")).alias("range"),
            
            # Trade imbalance
            pl.when(pl.col("buy_volume") + pl.col("sell_volume") > 0)
                .then((pl.col("buy_volume") - pl.col("sell_volume")) / 
                      (pl.col("buy_volume") + pl.col("sell_volume")))
                .otherwise(0.0)
                .alias("trade_imbalance"),
            
            # Partition columns for Hive-style partitioning
            pl.col("capture_ts").dt.year().alias("year"),
            pl.col("capture_ts").dt.month().alias("month"),
            pl.col("capture_ts").dt.day().alias("day"),
        ])
        
        bars[duration] = agg_df
    
    return bars


def create_trades_feature_config(
    input_path: str,
    output_path: str,
    partition_cols: Optional[List[str]] = None,
) -> "TransformConfig":
    """
    Create a standard configuration for trades feature transform.
    
    Args:
        input_path: Path to bronze trades data
        output_path: Path to write silver trades features
        partition_cols: Partition columns (default: exchange, symbol, year, month, day)
    
    Returns:
        TransformConfig ready to use with TradesFeatureTransform
    
    Example:
        config = create_trades_feature_config(
            input_path="bronze/ccxt/trades",
            output_path="silver/ccxt/trades",
        )
        transform = TradesFeatureTransform(config)
        executor.execute(transform)
    """
    from etl.core.config import InputConfig, OutputConfig, TransformConfig
    from etl.core.enums import CompressionCodec, DataFormat, WriteMode
    
    if partition_cols is None:
        partition_cols = ["exchange", "symbol", "year", "month", "day"]
    
    return TransformConfig(
        name="trades_features",
        description="Extract features from bronze trades data",
        inputs={
            "trades": InputConfig(
                name="trades",
                path=input_path,
                format=DataFormat.PARQUET,
                partition_cols=partition_cols,
            ),
        },
        outputs={
            "silver": OutputConfig(
                name="silver",
                path=output_path,
                format=DataFormat.PARQUET,
                partition_cols=partition_cols,
                mode=WriteMode.OVERWRITE_PARTITION,
                compression=CompressionCodec.ZSTD,
                compression_level=3,
            ),
        },
    )