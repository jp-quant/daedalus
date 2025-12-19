"""
Orderbook Feature Transform
===========================

Transform that computes orderbook features from bronze to silver tier.

This transform:
1. Reads bronze orderbook data (raw snapshots)
2. Optionally reads bronze trades data (for TFI calculation)
3. Extracts structural features (vectorized)
4. Computes rolling features
5. Computes stateful features (OFI, MLOFI, TFI, etc.)
6. Writes silver feature data

The transform supports both:
- Fully vectorized processing (faster, for static features)
- Stateful processing (for path-dependent features like OFI)
- Hybrid orderbook+trades processing (for TFI)
"""

import logging
from typing import Any, Dict, List, Optional

import polars as pl

from etl.core.base import BaseTransform, StatefulTransform, TransformContext
from etl.core.config import (
    FeatureConfig,
    FilterSpec,
    InputConfig,
    OutputConfig,
    TransformConfig,
)
from etl.core.enums import (
    CompressionCodec,
    DataFormat,
    FeatureCategory,
    WriteMode,
)
from etl.core.registry import register_transform
from etl.features.orderbook import (
    compute_rolling_features,
    extract_structural_features,
)
from etl.features.stateful import StatefulFeatureProcessor, StatefulProcessorConfig

logger = logging.getLogger(__name__)


@register_transform("orderbook_features")
class OrderbookFeatureTransform(StatefulTransform):
    """
    Transform bronze orderbook data to silver feature data.
    
    This transform computes comprehensive orderbook features including:
    
    Structural (vectorized):
    - Level prices and sizes (L0-LN)
    - Mid price, spread, relative spread
    - Microprice, imbalance
    - VWAP, smart depth
    - Book slope, center of gravity
    - Concentration, liquidity proxies
    
    Dynamic (stateful):
    - Order Flow Imbalance (OFI)
    - Multi-level OFI with decay (MLOFI)
    - Trade Flow Imbalance (TFI) - requires trades input
    - Kyle's Lambda
    - Velocity, acceleration
    - Spread regime
    
    Rolling:
    - Realized volatility
    - Mean return
    - Mean spread
    
    Configuration:
        Accepts FeatureConfig through TransformContext to control:
        - depth_levels: Number of orderbook levels to process
        - rolling_windows: Time windows for rolling stats
        - categories: Which feature categories to compute
    
    Inputs:
        - "orderbook" (required): Bronze orderbook data
        - "trades" (optional): Bronze trades data for TFI calculation
    
    Example:
        config = TransformConfig(
            name="orderbook_features",
            inputs={
                "orderbook": InputConfig(name="orderbook", path="bronze/orderbook"),
                "trades": InputConfig(name="trades", path="bronze/trades"),  # Optional
            },
            outputs={"silver": OutputConfig(name="silver", path="silver/features")},
        )
        transform = OrderbookFeatureTransform(config)
        executor.execute(transform)
    """
    
    # Input keys
    ORDERBOOK_INPUT = "orderbook"
    TRADES_INPUT = "trades"
    
    def __init__(self, config: TransformConfig):
        """
        Initialize transform.
        
        Args:
            config: Transform configuration with input/output declarations
        """
        super().__init__(config)
        self._processor: Optional[StatefulFeatureProcessor] = None
    
    def initialize(self, context: TransformContext) -> None:
        """Initialize stateful processor."""
        super().initialize(context)
        
        # Create processor config from feature config
        feature_config = context.feature_config
        processor_config = StatefulProcessorConfig(
            horizons=feature_config.rolling_windows,
            ofi_levels=feature_config.depth_levels,
            ofi_decay_alpha=feature_config.ofi_decay_alpha,
            spread_tight_percentile=feature_config.spread_tight_threshold_bps / 100,
            spread_wide_percentile=feature_config.spread_wide_threshold_bps / 100,
            vpin_bucket_volume=feature_config.vpin_bucket_size,
        )
        
        self._processor = StatefulFeatureProcessor(processor_config)
    
    def transform(
        self,
        inputs: Dict[str, pl.LazyFrame],
        context: TransformContext,
    ) -> Dict[str, pl.LazyFrame]:
        """
        Transform bronze orderbook data to silver features.
        
        Args:
            inputs: Dictionary with:
                - "orderbook" (required): Orderbook LazyFrame
                - "trades" (optional): Trades LazyFrame for TFI calculation
            context: Execution context with feature configuration
        
        Returns:
            Dictionary with "silver" key containing feature LazyFrame
        """
        # Get the orderbook input (use first key if "orderbook" not found)
        if self.ORDERBOOK_INPUT in inputs:
            orderbook_lf = inputs[self.ORDERBOOK_INPUT]
        else:
            # Fallback: use first input
            first_key = list(self.config.inputs.keys())[0]
            orderbook_lf = inputs[first_key]
        
        # Get trades input if available
        trades_lf = inputs.get(self.TRADES_INPUT)
        
        feature_config = context.feature_config
        
        # Step 1: Extract structural features (vectorized)
        logger.info("Extracting structural features")
        silver_lf = extract_structural_features(
            orderbook_lf,
            max_levels=feature_config.depth_levels,
        )
        
        # Step 2: Compute rolling features (vectorized)
        if FeatureCategory.ROLLING in feature_config.categories:
            logger.info("Computing rolling features")
            silver_lf = compute_rolling_features(
                silver_lf,
                horizons=feature_config.rolling_windows,
            )
        
        # Step 3: Compute stateful features (requires collect for row-by-row)
        if FeatureCategory.DYNAMIC in feature_config.categories:
            logger.info("Computing stateful features (OFI, MLOFI, TFI, etc.)")
            silver_lf = self._compute_stateful_features(silver_lf, trades_lf, context)
        
        # Step 4: Add partition columns for Hive-style partitioning
        # Use capture_ts if available, otherwise timestamp
        time_col = "capture_ts" if "capture_ts" in orderbook_lf.collect_schema().names() else "timestamp"
        silver_lf = silver_lf.with_columns([
            pl.col(time_col).dt.year().alias("year"),
            pl.col(time_col).dt.month().alias("month"),
            pl.col(time_col).dt.day().alias("day"),
        ])
        
        # Step 5: Optionally drop raw bid/ask arrays to reduce storage
        # Default is True - drops raw arrays since structural features are extracted
        if feature_config.drop_raw_book_arrays:
            cols_to_drop = [
                col for col in ["bids", "asks"] 
                if col in silver_lf.collect_schema().names()
            ]
            if cols_to_drop:
                logger.info(f"Dropping raw orderbook arrays: {cols_to_drop}")
                silver_lf = silver_lf.drop(cols_to_drop)
        
        # Return with output key from config
        output_key = list(self.config.outputs.keys())[0]
        return {output_key: silver_lf}
    
    def _compute_stateful_features(
        self,
        lf: pl.LazyFrame,
        trades_lf: Optional[pl.LazyFrame],
        context: TransformContext,
    ) -> pl.LazyFrame:
        """
        Compute stateful features that require row-by-row processing.
        
        This is less efficient than vectorized but necessary for
        path-dependent features like OFI.
        
        If trades are provided, they are merged chronologically with orderbook
        snapshots to compute TFI correctly.
        
        Args:
            lf: Orderbook LazyFrame with structural features
            trades_lf: Optional trades LazyFrame for TFI
            context: Execution context
        
        Returns:
            LazyFrame with stateful features added
        """
        # Collect to DataFrame for row-by-row processing
        df = lf.collect()
        
        if self._processor is None:
            return df.lazy()
        
        # Sort by timestamp
        df = df.sort("timestamp")
        
        # Prepare trades if provided
        trades_df = None
        if trades_lf is not None:
            trades_df = trades_lf.collect()
            # Normalize timestamp column for trades
            if "timestamp" not in trades_df.columns:
                if "capture_ts" in trades_df.columns:
                    trades_df = trades_df.with_columns(
                        pl.col("capture_ts").alias("timestamp")
                    )
            trades_df = trades_df.sort("timestamp")
        
        # Process with or without trades
        if trades_df is not None and len(trades_df) > 0:
            stateful_features = self._process_with_trades(df, trades_df, context)
        else:
            stateful_features = self._process_orderbook_only(df, context)
        
        # Create DataFrame from stateful features
        if stateful_features:
            stateful_df = pl.DataFrame(stateful_features)
            
            # Join with original data using row index
            # with_row_index replaces deprecated with_row_count
            df = df.with_row_index("_idx")
            stateful_df = stateful_df.with_row_index("_idx")
            
            df = df.join(stateful_df, on="_idx", how="left")
            df = df.drop("_idx")
        
        return df.lazy()
    
    def _process_orderbook_only(
        self,
        df: pl.DataFrame,
        context: TransformContext,
    ) -> List[Dict[str, Any]]:
        """Process orderbook-only mode (no TFI)."""
        stateful_features: List[Dict[str, Any]] = []
        
        # Get partition columns to reset state between symbols
        partition_values = context.partition_values or {}
        current_symbol = partition_values.get("symbol")
        
        for row in df.iter_rows(named=True):
            # Reset processor state if symbol changes
            row_symbol = row.get("symbol")
            if current_symbol is not None and row_symbol != current_symbol:
                self._processor.reset()
                current_symbol = row_symbol
            
            # Process orderbook
            features = self._processor.process_orderbook(row)
            stateful_features.append(features)
        
        return stateful_features
    
    def _process_with_trades(
        self,
        orderbook_df: pl.DataFrame,
        trades_df: pl.DataFrame,
        context: TransformContext,
    ) -> List[Dict[str, Any]]:
        """
        Process orderbook and trades chronologically for TFI calculation.
        
        This merges orderbook snapshots and trades by timestamp, then processes
        them in order. Trades between orderbook snapshots accumulate buy/sell
        volume for TFI, which is computed at each orderbook snapshot.
        
        Args:
            orderbook_df: Orderbook DataFrame sorted by timestamp
            trades_df: Trades DataFrame sorted by timestamp
            context: Execution context
        
        Returns:
            List of feature dictionaries (one per orderbook snapshot)
        """
        stateful_features: List[Dict[str, Any]] = []
        
        # Get partition columns to reset state between symbols
        partition_values = context.partition_values or {}
        current_symbol = partition_values.get("symbol")
        
        # Convert to lists of dicts for iteration
        orderbook_rows = orderbook_df.to_dicts()
        trade_rows = trades_df.to_dicts()
        
        # Pointers for merge
        trade_idx = 0
        num_trades = len(trade_rows)
        
        for ob_row in orderbook_rows:
            # Reset processor state if symbol changes
            row_symbol = ob_row.get("symbol")
            if current_symbol is not None and row_symbol != current_symbol:
                self._processor.reset()
                current_symbol = row_symbol
            
            ob_ts = ob_row.get("timestamp", 0)
            
            # Process all trades that occurred before this orderbook snapshot
            while trade_idx < num_trades:
                trade = trade_rows[trade_idx]
                trade_ts = trade.get("timestamp", 0)
                
                # If trade is after orderbook, stop
                if trade_ts > ob_ts:
                    break
                
                # Only process trades for same symbol
                trade_symbol = trade.get("symbol")
                if trade_symbol == row_symbol or current_symbol is None:
                    self._processor.process_trade(trade)
                
                trade_idx += 1
            
            # Process orderbook snapshot (TFI uses accumulated trades)
            features = self._processor.process_orderbook(ob_row)
            stateful_features.append(features)
        
        return stateful_features
    
    def get_state(self) -> Dict[str, Any]:
        """Get state including processor state."""
        state = super().get_state()
        if self._processor:
            state["processor"] = self._processor.get_state()
        return state
    
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restore state including processor state."""
        processor_state = state.pop("processor", None)
        super().set_state(state)
        if processor_state and self._processor:
            self._processor.set_state(processor_state)


def create_orderbook_feature_config(
    orderbook_path: str,
    silver_path: str,
    trades_path: Optional[str] = None,
    partition_cols: Optional[List[str]] = None,
) -> TransformConfig:
    """
    Create a standard configuration for orderbook feature transform.
    
    This is a convenience factory for common use cases.
    
    Args:
        orderbook_path: Path to bronze orderbook data
        silver_path: Path to write silver feature data
        trades_path: Optional path to bronze trades data (for TFI calculation)
        partition_cols: Partition columns (default: exchange, symbol, year, month, day)
    
    Returns:
        TransformConfig ready to use with OrderbookFeatureTransform
    
    Example:
        # Without trades (TFI will be zero)
        config = create_orderbook_feature_config(
            orderbook_path="bronze/orderbook",
            silver_path="silver/features",
        )
        
        # With trades (full TFI support)
        config = create_orderbook_feature_config(
            orderbook_path="bronze/orderbook",
            silver_path="silver/features",
            trades_path="bronze/trades",
        )
    """
    if partition_cols is None:
        partition_cols = ["exchange", "symbol", "year", "month", "day"]
    
    inputs = {
        "orderbook": InputConfig(
            name="orderbook",
            path=orderbook_path,
            format=DataFormat.PARQUET,
            partition_cols=partition_cols,
        ),
    }
    
    # Add trades input if provided
    if trades_path:
        inputs["trades"] = InputConfig(
            name="trades",
            path=trades_path,
            format=DataFormat.PARQUET,
            partition_cols=partition_cols,
        )
    
    return TransformConfig(
        name="orderbook_features",
        description="Extract comprehensive features from bronze orderbook data",
        inputs=inputs,
        outputs={
            "silver": OutputConfig(
                name="silver",
                path=silver_path,
                format=DataFormat.PARQUET,
                partition_cols=partition_cols,
                mode=WriteMode.OVERWRITE_PARTITION,
                compression=CompressionCodec.ZSTD,
                compression_level=3,
            ),
        },
    )
