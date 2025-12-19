#!/usr/bin/env python3
"""
Run ETL Using New Framework
===========================

This script processes Parquet files using the new modular ETL framework.
Supports orderbook, ticker, and trades data with comprehensive feature engineering.

Uses research-optimized parameters from config.yaml based on:
- Cont et al. (2014): OFI price impact
- Kyle & Obizhaeva (2016): Microstructure invariance
- López de Prado (2018): Volume clocks
- Zhang et al. (2019): DeepLOB deep book features
- Xu et al. (2019): Multi-level OFI

Usage:
    # Process orderbook data (uses config.yaml settings)
    python scripts/run_etl_v2.py --channel orderbook
    
    # Process all channels
    python scripts/run_etl_v2.py --channel all
    
    # Process with custom paths
    python scripts/run_etl_v2.py --input data/raw/ready/ccxt --output data/processed/v2
    
    # Dry run
    python scripts/run_etl_v2.py --dry-run
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl

# Config imports
from config.config import load_config, DaedalusConfig

from etl.features.orderbook import extract_structural_features, compute_rolling_features
from etl.features.stateful import StatefulFeatureProcessor, StatefulProcessorConfig
from etl.transforms.ticker import compute_ticker_features
from etl.transforms.trades import compute_trades_features
from etl.transforms.bars import aggregate_bars_vectorized

logger = logging.getLogger(__name__)


def get_processor_options(config: Optional[DaedalusConfig], channel: str) -> Dict[str, Any]:
    """
    Get processor options from config.
    
    Args:
        config: Loaded DaedalusConfig or None
        channel: Channel name (orderbook, ticker, trades)
        
    Returns:
        Dict of processor options
    """
    if not config or not getattr(config, "etl", None):
        return {}

    channel_config = config.etl.channels.get(channel)
    if not channel_config or not channel_config.processor_options:
        return {}

    return dict(channel_config.processor_options)


def _ensure_capture_ts(df: pl.LazyFrame) -> pl.LazyFrame:
    """Ensure a usable capture_ts column exists as Polars Datetime."""
    schema_names = set(df.collect_schema().names())

    if "capture_ts" in schema_names:
        return df.with_columns([pl.col("capture_ts").cast(pl.Datetime).alias("capture_ts")])

    if "timestamp" in schema_names:
        return df.with_columns([
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").alias("capture_ts")
        ])

    if "collected_at" in schema_names:
        return df.with_columns([
            pl.from_epoch(pl.col("collected_at"), time_unit="ms").alias("capture_ts")
        ])

    raise ValueError("Input data is missing capture_ts/timestamp/collected_at; cannot compute rolling features")


def _add_stateful_features(df: pl.DataFrame, options: Dict[str, Any]) -> pl.DataFrame:
    """Compute stateful features row-by-row and hstack onto an orderbook DataFrame."""
    config_kwargs: Dict[str, Any] = {}
    for key in StatefulProcessorConfig.__dataclass_fields__.keys():
        if key in options:
            config_kwargs[key] = options[key]

    processor = StatefulFeatureProcessor(StatefulProcessorConfig(**config_kwargs))

    features_rows = []
    for record in df.iter_rows(named=True):
        features_rows.append(processor.process_orderbook(record))

    if not features_rows:
        return df

    features_df = pl.DataFrame(features_rows)
    # Avoid accidental collisions: only add truly new columns
    existing = set(df.columns)
    new_cols = [c for c in features_df.columns if c not in existing]
    if not new_cols:
        return df

    return df.hstack(features_df.select(new_cols))


def process_orderbook(
    input_path: Path,
    output_path: Path,
    options: Dict[str, Any],
    dry_run: bool = False,
) -> dict:
    """
    Process orderbook data using the new framework.
    
    Pipeline:
    1. Load bronze parquet files
    2. Extract structural features (vectorized)
    3. Compute stateful features (OFI, MLOFI, TFI) if enabled
    4. Compute rolling features
    5. Aggregate to bars
    6. Write silver/gold outputs
    
    Args:
        input_path: Path to input parquet files
        output_path: Base output path
        options: Processor options from config (max_levels, horizons, etc.)
        dry_run: If True, don't write output
        
    Returns:
        Dict with processing statistics
    """
    max_levels = int(options.get("max_levels", 20))
    horizons = list(options.get("horizons", [5, 15, 60, 300, 900]))
    bar_durations = list(options.get("bar_durations", [60, 300, 900, 3600]))
    bands_bps = list(options.get("bands_bps", [5, 10, 25, 50, 100]))
    enable_stateful = bool(options.get("enable_stateful", True))
    
    stats = {"rows_processed": 0, "features_computed": 0, "bars_computed": {}}
    
    # Log configuration
    logger.info(f"Configuration:")
    logger.info(f"  max_levels: {max_levels}")
    logger.info(f"  horizons: {horizons}")
    logger.info(f"  bar_durations: {bar_durations}")
    logger.info(f"  bands_bps: {bands_bps}")
    logger.info(f"  enable_stateful: {enable_stateful}")
    
    # Discover input files
    parquet_files = list(input_path.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No parquet files found in {input_path}")
        return stats
    
    logger.info(f"Found {len(parquet_files)} parquet files")
    
    if dry_run:
        logger.info("[DRY RUN] Would process files:")
        for f in parquet_files[:5]:
            logger.info(f"  - {f.name}")
        if len(parquet_files) > 5:
            logger.info(f"  ... and {len(parquet_files) - 5} more")
        return stats
    
    # Load and process
    logger.info("Loading data...")
    df = pl.scan_parquet([str(f) for f in parquet_files])

    # Ensure capture_ts exists for rolling features
    df = _ensure_capture_ts(df)
    
    # Step 1: Extract structural features
    logger.info(f"Extracting structural features (max_levels={max_levels})...")
    df = extract_structural_features(df, max_levels=max_levels, bands_bps=bands_bps)

    # Optional stateful features (requires sequential processing)
    if enable_stateful:
        logger.info("Collecting for stateful feature computation...")
        base_df = df.collect()
        logger.info("Computing stateful features...")
        base_df = _add_stateful_features(base_df, options)
        df = base_df.lazy()

    # Compute rolling features (vectorized)
    logger.info(f"Computing rolling features (horizons={horizons})...")
    df = compute_rolling_features(df, horizons=horizons)

    # Collect silver tier
    logger.info("Collecting silver tier data...")
    silver_df = df.collect()
    stats["rows_processed"] = len(silver_df)
    stats["features_computed"] = len(silver_df.columns)
    
    # Write silver output
    silver_path = output_path / "silver" / "orderbook"
    silver_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    silver_file = silver_path / f"features_{timestamp}.parquet"
    silver_df.write_parquet(silver_file, compression="zstd")
    logger.info(f"Wrote silver output: {silver_file} ({len(silver_df)} rows)")
    
    # Step 4: Aggregate to bars
    logger.info(f"Aggregating to bars (durations={bar_durations})...")
    bars = aggregate_bars_vectorized(silver_df.lazy(), durations=bar_durations)
    
    # Write gold tier (bars)
    gold_path = output_path / "gold" / "bars"
    gold_path.mkdir(parents=True, exist_ok=True)
    
    for duration, bar_lf in bars.items():
        bar_df = bar_lf.collect()
        stats["bars_computed"][duration] = len(bar_df)
        
        bar_file = gold_path / f"bars_{duration}s_{timestamp}.parquet"
        bar_df.write_parquet(bar_file, compression="zstd")
        logger.info(f"Wrote {duration}s bars: {bar_file} ({len(bar_df)} rows)")
    
    return stats


def process_ticker(
    input_path: Path,
    output_path: Path,
    dry_run: bool = False,
) -> dict:
    """
    Process ticker data using the new framework.
    """
    stats = {"rows_processed": 0, "features_computed": 0}
    
    parquet_files = list(input_path.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No parquet files found in {input_path}")
        return stats
    
    logger.info(f"Found {len(parquet_files)} ticker parquet files")
    
    if dry_run:
        logger.info(f"[DRY RUN] Would process {len(parquet_files)} files")
        return stats
    
    # Load and process
    df = pl.scan_parquet([str(f) for f in parquet_files])
    
    # Compute features
    logger.info("Computing ticker features...")
    df = compute_ticker_features(df)
    
    # Collect and write
    result_df = df.collect()
    stats["rows_processed"] = len(result_df)
    stats["features_computed"] = len(result_df.columns)
    
    # Write output
    silver_path = output_path / "silver" / "ticker"
    silver_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = silver_path / f"features_{timestamp}.parquet"
    result_df.write_parquet(output_file, compression="zstd")
    logger.info(f"Wrote ticker output: {output_file} ({len(result_df)} rows)")
    
    return stats


def process_trades(
    input_path: Path,
    output_path: Path,
    dry_run: bool = False,
) -> dict:
    """
    Process trades data using the new framework.
    """
    stats = {"rows_processed": 0, "features_computed": 0}
    
    parquet_files = list(input_path.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No parquet files found in {input_path}")
        return stats
    
    logger.info(f"Found {len(parquet_files)} trades parquet files")
    
    if dry_run:
        logger.info(f"[DRY RUN] Would process {len(parquet_files)} files")
        return stats
    
    # Load and process
    df = pl.scan_parquet([str(f) for f in parquet_files])
    
    # Compute features
    logger.info("Computing trades features...")
    df = compute_trades_features(df)
    
    # Collect and write
    result_df = df.collect()
    stats["rows_processed"] = len(result_df)
    stats["features_computed"] = len(result_df.columns)
    
    # Write output
    silver_path = output_path / "silver" / "trades"
    silver_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = silver_path / f"features_{timestamp}.parquet"
    result_df.write_parquet(output_file, compression="zstd")
    logger.info(f"Wrote trades output: {output_file} ({len(result_df)} rows)")
    
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="ETL Pipeline v2 - New Modular Framework (Research-Optimized)"
    )
    parser.add_argument(
        "--channel",
        type=str,
        default="all",
        choices=["all", "orderbook", "ticker", "trades"],
        help="Channel to process (default: all)"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/raw/ready/ccxt",
        help="Input directory path"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed",
        help="Output directory path"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config.yaml (auto-detected if not specified)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write output, just show what would be processed"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Load configuration
    config: Optional[DaedalusConfig] = None
    try:
        config = load_config(args.config)
        logger.info(f"Loaded config from: {args.config or 'auto-detected'}")
    except FileNotFoundError as e:
        logger.warning(f"Config not found ({e}), using built-in defaults")
        config = DaedalusConfig()
    except Exception as e:
        logger.warning(f"Error loading config ({e}), using built-in defaults")
        config = DaedalusConfig()
    
    # Parse paths
    input_base = Path(args.input)
    output_base = Path(args.output)
    
    logger.info("=" * 60)
    logger.info("ETL Pipeline v2 - Research-Optimized Framework")
    logger.info("=" * 60)
    logger.info(f"Input:  {input_base}")
    logger.info(f"Output: {output_base}")
    logger.info(f"Channel: {args.channel}")
    logger.info(f"Config:  {'Loaded from config.yaml' if args.config else 'Auto-detected or defaults'}")
    if args.dry_run:
        logger.info("MODE: DRY RUN")
    logger.info("=" * 60)
    
    all_stats = {}
    
    # Process channels
    channels = (
        ["orderbook", "ticker", "trades"]
        if args.channel == "all"
        else [args.channel]
    )
    
    for channel in channels:
        channel_input = input_base / channel
        
        if not channel_input.exists():
            logger.warning(f"Channel directory not found: {channel_input}")
            continue
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing channel: {channel.upper()}")
        logger.info(f"{'='*60}")
        
        # Get processor options from config or use research defaults
        options = get_processor_options(config, channel)
        
        if channel == "orderbook":
            stats = process_orderbook(
                channel_input,
                output_base,
                options=options,
                dry_run=args.dry_run,
            )
        elif channel == "ticker":
            stats = process_ticker(
                channel_input,
                output_base,
                dry_run=args.dry_run,
            )
        elif channel == "trades":
            stats = process_trades(
                channel_input,
                output_base,
                dry_run=args.dry_run,
            )
        else:
            stats = {}
        
        all_stats[channel] = stats
        logger.info(f"Channel {channel} stats: {stats}")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 60)
    
    total_rows = sum(s.get("rows_processed", 0) for s in all_stats.values())
    logger.info(f"Total rows processed: {total_rows:,}")
    
    for channel, stats in all_stats.items():
        logger.info(f"  {channel}: {stats.get('rows_processed', 0):,} rows, {stats.get('features_computed', 0)} features")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
