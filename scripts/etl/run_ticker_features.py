#!/usr/bin/env python3
"""
Ticker Features ETL Script
==========================

Transform raw ticker data into comprehensive features using TransformExecutor.

This script properly delegates I/O to the executor:
- Input scanning and filtering via FilterSpec
- Transform execution via TickerFeatureTransform
- Output writing with proper partitioning

Ticker data is stateless and fully vectorized - simpler than orderbook.

Features computed (in TickerFeatureTransform):
- Spread (absolute and relative)
- Mid price
- Volume imbalance
- Time features (hour, day_of_week, is_weekend)
- Log returns
- Rolling statistics (if ROLLING category enabled)

Note: Bar aggregation (silver → gold) is handled separately by run_bars.py.
This script focuses on bronze → silver feature engineering.

Usage:
    # Basic usage - process all ticker data
    python scripts/etl/run_ticker_features.py

    # Process specific exchange/symbol partition
    python scripts/etl/run_ticker_features.py --exchange binanceus --symbol BTC/USDT

    # Enable rolling features
    python scripts/etl/run_ticker_features.py --rolling-windows 60,300,900

    # Dry run (preview only)
    python scripts/etl/run_ticker_features.py --dry-run

    # Limit rows for testing
    python scripts/etl/run_ticker_features.py --limit 10000
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl

from config.config import load_config, DaedalusConfig
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
from etl.core.executor import TransformExecutor
from etl.transforms.ticker import TickerFeatureTransform

logger = logging.getLogger(__name__)


def create_transform_config(
    input_path: str,
    output_path: str,
) -> TransformConfig:
    """
    Create the transform configuration for ticker features.
    
    Args:
        input_path: Path to bronze ticker data
        output_path: Path for silver features output
    
    Returns:
        TransformConfig ready for execution
    """
    return TransformConfig(
        name="ticker_features",
        description="Extract features from raw ticker data",
        inputs={
            "ticker": InputConfig(
                name="ticker",
                path=input_path,
                format=DataFormat.PARQUET,
            ),
        },
        outputs={
            "silver": OutputConfig(
                name="silver",
                path=output_path,
                format=DataFormat.PARQUET,
                partition_cols=["exchange", "symbol", "year", "month", "day"],
                mode=WriteMode.OVERWRITE_PARTITION,
                compression=CompressionCodec.ZSTD,
                compression_level=3,
            ),
        },
    )


def create_feature_config(
    rolling_windows: Optional[List[int]] = None,
    enable_rolling: bool = False,
) -> FeatureConfig:
    """
    Create feature configuration.
    
    Args:
        rolling_windows: Optional list of rolling window sizes in seconds
        enable_rolling: Whether to enable rolling features
    
    Returns:
        FeatureConfig with appropriate categories
    """
    categories = {FeatureCategory.STRUCTURAL}
    
    if enable_rolling or rolling_windows:
        categories.add(FeatureCategory.ROLLING)
    
    return FeatureConfig(
        categories=categories,
        depth_levels=1,  # Ticker only has best bid/ask
        rolling_windows=rolling_windows or [60, 300, 900],
    )


def run_ticker_etl(
    input_path: Path,
    output_path: Path,
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    rolling_windows: Optional[List[int]] = None,
    dry_run: bool = False,
    limit_rows: Optional[int] = None,
    config: Optional[DaedalusConfig] = None,
    normalize_quotes: bool = True,
) -> Dict[str, Any]:
    """
    Run the ticker feature ETL pipeline using TransformExecutor.
    
    Args:
        input_path: Path to bronze ticker parquet files
        output_path: Base output path for silver outputs
        exchange: Optional exchange filter
        symbol: Optional symbol filter
        rolling_windows: Optional rolling window sizes in seconds
        dry_run: If True, don't write outputs
        limit_rows: Optional limit on rows to process
        config: Optional Daedalus configuration
    
    Returns:
        Execution statistics dictionary
    """
    execution_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info(f"[{execution_id}] Ticker Features ETL Pipeline")
    logger.info("=" * 70)
    
    # Load config if not provided
    if config is None:
        try:
            config = load_config()
        except Exception as e:
            logger.warning(f"Could not load config: {e}. Using defaults.")
    
    # Get tier names from config
    features_tier = "silver"
    if config and hasattr(config, "storage"):
        features_tier = config.storage.paths.tier_features
    
    # Build FilterSpec for partition pruning
    filter_spec = None
    if exchange or symbol:
        filter_spec = FilterSpec(
            exchange=exchange,
            symbol=symbol,
            normalize_quotes=normalize_quotes,
        )
        logger.info(f"Filter: {filter_spec}")
    
    # Create feature config
    enable_rolling = rolling_windows is not None
    feature_config = create_feature_config(
        rolling_windows=rolling_windows,
        enable_rolling=enable_rolling,
    )
    # Propagate normalize_quotes to feature config
    feature_config.normalize_quotes = normalize_quotes
    
    # Log configuration
    logger.info(f"Input:  {input_path}")
    logger.info(f"Output: {output_path}")
    logger.info(f"Categories: {[c.value for c in feature_config.categories]}")
    if rolling_windows:
        logger.info(f"Rolling windows: {rolling_windows}")
    if limit_rows:
        logger.info(f"Row limit: {limit_rows:,}")
    if dry_run:
        logger.info("[DRY RUN] No outputs will be written")
    
    # Build full output path with tier
    full_output_path = str(output_path / features_tier / "ticker")
    
    # Create transform configuration
    transform_config = create_transform_config(
        input_path=str(input_path),
        output_path=full_output_path,
    )
    
    # Create transform and executor
    transform = TickerFeatureTransform(transform_config)
    executor = TransformExecutor(feature_config=feature_config)
    
    # Execute transform via executor
    logger.info("Executing transform via executor...")
    result = executor.execute(
        transform=transform,
        filter_spec=filter_spec,
        dry_run=dry_run,
        limit_rows=limit_rows,
    )
    
    # Calculate duration
    duration = (datetime.now() - start_time).total_seconds()
    result["duration_seconds"] = duration
    result["execution_id"] = execution_id
    
    # Log summary
    logger.info("=" * 70)
    logger.info(f"[{execution_id}] COMPLETE in {duration:.2f}s")
    if result.get("outputs"):
        for name, stats in result["outputs"].items():
            logger.info(f"  {name}: {stats.get('rows', 0):,} rows, {stats.get('files', 0)} files")
    logger.info("=" * 70)
    
    return result


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Ticker Features ETL - Transform raw ticker data to features"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/raw/ready/ccxt/ticker",
        help="Input directory with bronze ticker parquet files",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed",
        help="Output directory for silver outputs",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        help="Filter by exchange (e.g., binanceus)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="Filter by symbol (e.g., BTC/USDT)",
    )
    parser.add_argument(
        "--rolling-windows",
        type=str,
        help="Comma-separated rolling window sizes in seconds (e.g., 60,300,900)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of rows to process (for testing)",
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config.yaml",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without writing outputs",
    )
    parser.add_argument(
        "--normalize-quotes",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Normalize quote currencies (USD/USDC/USDT treated as equivalent). "
             "Default: enabled. Use --no-normalize-quotes to disable.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse rolling windows
    rolling_windows = None
    if args.rolling_windows:
        rolling_windows = [int(x.strip()) for x in args.rolling_windows.split(",")]
    
    # Load config
    config = None
    if args.config:
        try:
            config = load_config(args.config)
        except Exception as e:
            logger.warning(f"Could not load config from {args.config}: {e}")
    
    # Run ETL
    result = run_ticker_etl(
        input_path=Path(args.input),
        output_path=Path(args.output),
        exchange=args.exchange,
        symbol=args.symbol,
        rolling_windows=rolling_windows,
        dry_run=args.dry_run,
        limit_rows=args.limit,
        config=config,
        normalize_quotes=args.normalize_quotes,
    )
    
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
