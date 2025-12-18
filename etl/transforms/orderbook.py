"""
Orderbook Feature Transform
===========================

Transform that computes orderbook features from bronze to silver tier.

This transform:
1. Reads bronze orderbook data (raw snapshots)
2. Extracts structural features (vectorized)
3. Computes rolling features
4. Computes stateful features (OFI, MLOFI, TFI, etc.)
5. Writes silver feature data

The transform supports both:
- Fully vectorized processing (faster, for static features)
- Stateful processing (for path-dependent features like OFI)
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
    - Trade Flow Imbalance (TFI)
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
    
    Example:
        config = TransformConfig(
            name="orderbook_features",
            inputs={"bronze": InputConfig(name="bronze", path="bronze/orderbook")},
            outputs={"silver": OutputConfig(name="silver", path="silver/features")},
        )
        transform = OrderbookFeatureTransform(config)
        executor.execute(transform)
    """
    
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
            inputs: Dictionary with "bronze" key containing orderbook LazyFrame
            context: Execution context with feature configuration
        
        Returns:
            Dictionary with "silver" key containing feature LazyFrame
        """
        # Get the bronze input
        bronze_key = list(self.config.inputs.keys())[0]
        bronze_lf = inputs[bronze_key]
        
        feature_config = context.feature_config
        
        # Step 1: Extract structural features (vectorized)
        logger.info("Extracting structural features")
        silver_lf = extract_structural_features(
            bronze_lf,
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
            logger.info("Computing stateful features (OFI, MLOFI, etc.)")
            silver_lf = self._compute_stateful_features(silver_lf, context)
        
        # Return with output key from config
        output_key = list(self.config.outputs.keys())[0]
        return {output_key: silver_lf}
    
    def _compute_stateful_features(
        self,
        lf: pl.LazyFrame,
        context: TransformContext,
    ) -> pl.LazyFrame:
        """
        Compute stateful features that require row-by-row processing.
        
        This is less efficient than vectorized but necessary for
        path-dependent features like OFI.
        """
        # Collect to DataFrame for row-by-row processing
        df = lf.collect()
        
        if self._processor is None:
            return df.lazy()
        
        # Sort by timestamp
        df = df.sort("timestamp")
        
        # Process each row and collect stateful features
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
        
        # Create DataFrame from stateful features
        if stateful_features:
            stateful_df = pl.DataFrame(stateful_features)
            
            # Join with original data
            # Add index column for joining
            df = df.with_row_count("_idx")
            stateful_df = stateful_df.with_row_count("_idx")
            
            df = df.join(stateful_df, on="_idx", how="left")
            df = df.drop("_idx")
        
        return df.lazy()
    
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
    bronze_path: str,
    silver_path: str,
    partition_cols: Optional[List[str]] = None,
) -> TransformConfig:
    """
    Create a standard configuration for orderbook feature transform.
    
    This is a convenience factory for common use cases.
    
    Args:
        bronze_path: Path to bronze orderbook data
        silver_path: Path to write silver feature data
        partition_cols: Partition columns (default: exchange, symbol, year, month, day)
    
    Returns:
        TransformConfig ready to use with OrderbookFeatureTransform
    """
    if partition_cols is None:
        partition_cols = ["exchange", "symbol", "year", "month", "day"]
    
    return TransformConfig(
        name="orderbook_features",
        description="Extract comprehensive features from bronze orderbook data",
        inputs={
            "bronze": InputConfig(
                name="bronze",
                path=bronze_path,
                format=DataFormat.PARQUET,
                partition_cols=partition_cols,
            ),
        },
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
