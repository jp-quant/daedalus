#!/usr/bin/env python3
"""
Bars ETL Script (Silver → Gold)
===============================

Transform silver-tier feature data into gold-tier time bars using TransformExecutor.

This script handles the silver → gold transformation, aggregating high-frequency
feature data into time bars (OHLCV + microstructure statistics).

Separation of Concerns:
- Feature ETL (run_*_features.py): bronze → silver (feature engineering)
- Bars ETL (this script): silver → gold (time aggregation)

Supported Data Sources:
- orderbook: Uses specialized orderbook bar aggregation with depth/OFI features
- ticker: Uses generic bar aggregation with mid_price
- trades: Uses trades-specific bar aggregation (already has aggregate_trades_to_bars)

Usage:
    # Aggregate orderbook features to bars
    python scripts/etl/run_bars.py --channel orderbook

    # Aggregate trades features to bars
    python scripts/etl/run_bars.py --channel trades

    # Aggregate ticker features to bars
    python scripts/etl/run_bars.py --channel ticker

    # Custom bar durations
    python scripts/etl/run_bars.py --channel orderbook --durations 60,300,900,3600

    # Process specific exchange/symbol
    python scripts/etl/run_bars.py --channel orderbook --exchange binanceus --symbol BTC/USDT

    # Dry run
    python scripts/etl/run_bars.py --channel orderbook --dry-run
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
from etl.core.base import TransformContext
from etl.core.config import (
    FeatureConfig,
    FilterSpec,
    InputConfig,
    OutputConfig,
    StorageConfig,
    TransformConfig,
)
from etl.core.enums import (
    CompressionCodec,
    DataFormat,
    FeatureCategory,
    WriteMode,
)
from etl.core.executor import TransformExecutor
from etl.transforms.bars import (
    BarAggregationTransform,
    aggregate_bars_vectorized,
    aggregate_orderbook_bars,
)
from etl.transforms.trades import aggregate_trades_to_bars

logger = logging.getLogger(__name__)


# Channel-specific bar aggregation functions
BAR_AGGREGATORS = {
    "orderbook": aggregate_orderbook_bars,
    "ticker": aggregate_bars_vectorized,
    "trades": aggregate_trades_to_bars,
}


def create_transform_config(
    channel: str,
    input_path: str,
    output_path: str,
    durations: List[int],
    storage_config: Optional[StorageConfig] = None,
) -> TransformConfig:
    """
    Create the transform configuration for bar aggregation.
    
    Args:
        channel: Data channel (orderbook, ticker, trades)
        input_path: Path to silver feature data
        output_path: Base output path for gold bars
        durations: List of bar durations in seconds
        storage_config: Optional storage configuration
    
    Returns:
        TransformConfig ready for execution
    """
    # Create outputs for each bar duration
    outputs = {}
    for duration in durations:
        output_name = f"bars_{duration}s"
        outputs[output_name] = OutputConfig(
            name=output_name,
            path=f"{output_path}/{channel}_bars/{duration}s",
            format=DataFormat.PARQUET,
            partition_cols=["exchange", "symbol", "year", "month", "day"],
            mode=WriteMode.OVERWRITE_PARTITION,
            compression=CompressionCodec.ZSTD,
            compression_level=3,
        )
    
    return TransformConfig(
        name=f"{channel}_bar_aggregation",
        description=f"Aggregate silver {channel} features to gold bars",
        inputs={
            "silver": InputConfig(
                name="silver",
                path=input_path,
                format=DataFormat.PARQUET,
            ),
        },
        outputs=outputs,
        storage=storage_config,
    )


def create_feature_config(
    durations: List[int],
) -> FeatureConfig:
    """
    Create feature configuration for bar aggregation.
    
    Args:
        durations: List of bar durations in seconds
    
    Returns:
        FeatureConfig with bar-specific settings
    """
    return FeatureConfig(
        categories={FeatureCategory.BARS},
        bar_interval_seconds=min(durations) if durations else 60,
    )


class BarsAggregationTransform(BarAggregationTransform):
    """
    Custom bar aggregation transform that uses channel-specific aggregators.
    
    Extends BarAggregationTransform to support different aggregation
    strategies for orderbook, ticker, and trades data.
    """
    
    def __init__(self, config: TransformConfig, channel: str):
        """
        Initialize with config and channel.
        
        Args:
            config: Transform configuration
            channel: Data channel (orderbook, ticker, trades)
        """
        super().__init__(config)
        self.channel = channel
        self._aggregator = BAR_AGGREGATORS.get(channel, aggregate_bars_vectorized)
    
    def transform(
        self,
        inputs: Dict[str, pl.LazyFrame],
        context: TransformContext,
    ) -> Dict[str, pl.LazyFrame]:
        """
        Aggregate silver features to gold bars.
        
        Uses the channel-specific aggregator function.
        
        Args:
            inputs: Dictionary with silver feature LazyFrame
            context: Execution context with feature_config
        
        Returns:
            Dictionary with bars for each duration
        """
        # Get the silver input
        input_key = list(self.config.inputs.keys())[0]
        silver_lf = inputs[input_key]
        
        # Get bar durations from output config names
        durations = []
        for output_name in self.config.outputs.keys():
            # Parse duration from "bars_60s" -> 60
            if output_name.startswith("bars_") and output_name.endswith("s"):
                try:
                    duration = int(output_name[5:-1])
                    durations.append(duration)
                except ValueError:
                    pass
        
        if not durations:
            durations = self._default_durations
        
        logger.info(f"Aggregating {self.channel} to bars: {durations}")
        
        # Use channel-specific aggregator
        bars = self._aggregator(silver_lf, durations)
        
        # Return bars keyed to match output config
        return {f"bars_{d}s": lf for d, lf in bars.items()}


def run_bars_etl(
    channel: str,
    input_path: Path,
    output_path: Path,
    durations: List[int],
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    dry_run: bool = False,
    limit_rows: Optional[int] = None,
    config: Optional[DaedalusConfig] = None,
) -> Dict[str, Any]:
    """
    Run the bars aggregation ETL pipeline using TransformExecutor.
    
    This transforms silver-tier feature data into gold-tier time bars.
    
    Args:
        channel: Data channel (orderbook, ticker, trades)
        input_path: Path to silver feature parquet files
        output_path: Base output path for gold bars
        durations: List of bar durations in seconds
        exchange: Optional exchange filter
        symbol: Optional symbol filter
        dry_run: If True, don't write outputs
        limit_rows: Optional limit on rows to process
        config: Optional Daedalus configuration
    
    Returns:
        Execution statistics dictionary
    """
    execution_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info(f"[{execution_id}] Bars ETL Pipeline (Silver → Gold)")
    logger.info(f"  Channel: {channel}")
    logger.info("=" * 70)
    
    # Validate channel
    if channel not in BAR_AGGREGATORS:
        logger.error(f"Unknown channel: {channel}. Supported: {list(BAR_AGGREGATORS.keys())}")
        return {"success": False, "reason": f"unknown_channel: {channel}"}
    
    # Load config if not provided
    if config is None:
        try:
            config = load_config()
        except Exception as e:
            logger.warning(f"Could not load config: {e}. Using defaults.")
    
    # Get tier names from config
    aggregates_tier = "gold"
    if config and hasattr(config, "storage"):
        aggregates_tier = config.storage.paths.tier_aggregates
    
    # Build FilterSpec for partition pruning
    filter_spec = None
    if exchange or symbol:
        filter_spec = FilterSpec(
            exchange=exchange,
            symbol=symbol,
        )
        logger.info(f"Filter: {filter_spec}")
    
    # Create feature config
    feature_config = create_feature_config(durations)
    
    # Log configuration
    logger.info(f"Input:  {input_path}")
    logger.info(f"Output: {output_path}")
    logger.info(f"Durations: {durations}")
    if limit_rows:
        logger.info(f"Row limit: {limit_rows:,}")
    if dry_run:
        logger.info("[DRY RUN] No outputs will be written")
    
    # Build full output path with tier
    full_output_path = str(output_path / aggregates_tier)
    
    # Create transform configuration
    transform_config = create_transform_config(
        channel=channel,
        input_path=str(input_path),
        output_path=full_output_path,
        durations=durations,
    )
    
    # Create transform and executor
    transform = BarsAggregationTransform(transform_config, channel=channel)
    executor = TransformExecutor(feature_config=feature_config)
    
    # Execute transform via executor
    logger.info("Executing bar aggregation via executor...")
    result = executor.execute(
        transform=transform,
        filter_spec=filter_spec,
        dry_run=dry_run,
        limit_rows=limit_rows,
    )
    
    # Calculate duration
    duration_sec = (datetime.now() - start_time).total_seconds()
    result["duration_seconds"] = duration_sec
    result["execution_id"] = execution_id
    
    # Log summary
    logger.info("=" * 70)
    logger.info(f"[{execution_id}] COMPLETE in {duration_sec:.2f}s")
    if result.get("outputs"):
        for name, stats in result["outputs"].items():
            logger.info(f"  {name}: {stats.get('rows', 0):,} rows, {stats.get('files', 0)} files")
    logger.info("=" * 70)
    
    return result


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Bars ETL - Aggregate silver features to gold time bars"
    )
    parser.add_argument(
        "--channel",
        type=str,
        required=True,
        choices=["orderbook", "ticker", "trades"],
        help="Data channel to process",
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Input directory with silver feature parquet files (default: data/processed/silver/<channel>)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed",
        help="Base output directory for gold bars",
    )
    parser.add_argument(
        "--durations",
        type=str,
        default="60,300,900,3600",
        help="Comma-separated bar durations in seconds (default: 60,300,900,3600)",
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
    
    # Parse durations
    durations = [int(x.strip()) for x in args.durations.split(",")]
    
    # Determine input path
    if args.input:
        input_path = Path(args.input)
    else:
        # Default: silver tier data for the channel
        input_path = Path(args.output) / "silver" / args.channel
    
    # Load config
    config = None
    if args.config:
        try:
            config = load_config(args.config)
        except Exception as e:
            logger.warning(f"Could not load config from {args.config}: {e}")
    
    # Run ETL
    result = run_bars_etl(
        channel=args.channel,
        input_path=input_path,
        output_path=Path(args.output),
        durations=durations,
        exchange=args.exchange,
        symbol=args.symbol,
        dry_run=args.dry_run,
        limit_rows=args.limit,
        config=config,
    )
    
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
