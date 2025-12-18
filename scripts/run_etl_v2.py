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
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl

# Config imports
from config.config import load_config, DaedalusConfig

# New framework imports
from etl.core.enums import DataTier, FeatureCategory
from etl.core.config import FeatureConfig
from etl.features.orderbook import extract_structural_features, compute_rolling_features
from etl.features.stateful import StatefulFeatureProcessor, StatefulProcessorConfig
from etl.transforms.ticker import compute_ticker_features
from etl.transforms.trades import compute_trades_features
from etl.transforms.bars import aggregate_bars_vectorized

logger = logging.getLogger(__name__)


# =============================================================================
# RESEARCH-OPTIMIZED DEFAULTS (from orderbook research)
# These are used if config.yaml doesn't specify processor_options
# =============================================================================
RESEARCH_DEFAULTS = {
    # Orderbook depth: 20 levels captures institutional "walls"
    "max_levels": 20,
    
    # Rolling horizons: [5s micro, 15s short, 60s standard, 300s medium, 900s trend]
    "horizons": [5, 15, 60, 300, 900],
    
    # Bar durations: Skip sub-minute (redundant), focus on 1min+ for patterns
    "bar_durations": [60, 300, 900, 3600],
    
    # OFI: 10 levels with decay reduces forecast error by 15-70%
    "ofi_levels": 10,
    "ofi_decay_alpha": 0.5,
    
    # Liquidity bands: Wider bands for crypto volatility
    "bands_bps": [5, 10, 25, 50, 100],
    
    # Spread regime: Percentile-based is more robust
    "use_dynamic_spread_regime": True,
    "spread_regime_window": 300,
    "spread_tight_percentile": 0.2,
    "spread_wide_percentile": 0.8,
    
    # Kyle's Lambda window
    "kyle_lambda_window": 300,
}


def get_processor_options(config: Optional[DaedalusConfig], channel: str) -> Dict[str, Any]:
    """
    Get processor options from config, falling back to research defaults.
    
    Args:
        config: Loaded DaedalusConfig or None
        channel: Channel name (orderbook, ticker, trades)
        
    Returns:
        Dict of processor options
    """
    options = RESEARCH_DEFAULTS.copy()
    
    if config and hasattr(config, 'etl') and config.etl:
        channel_config = config.etl.channels.get(channel)
        if channel_config and channel_config.processor_options:
            # Merge config options over defaults
            options.update(channel_config.processor_options)
            logger.info(f"Loaded {channel} processor_options from config.yaml")
    
    return options


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
    max_levels = options.get("max_levels", RESEARCH_DEFAULTS["max_levels"])
    horizons = options.get("horizons", RESEARCH_DEFAULTS["horizons"])
    bar_durations = options.get("bar_durations", RESEARCH_DEFAULTS["bar_durations"])
    bands_bps = options.get("bands_bps", RESEARCH_DEFAULTS["bands_bps"])
    
    stats = {"rows_processed": 0, "features_computed": 0, "bars_computed": {}}
    
    # Log configuration
    logger.info(f"Configuration:")
    logger.info(f"  max_levels: {max_levels}")
    logger.info(f"  horizons: {horizons}")
    logger.info(f"  bar_durations: {bar_durations}")
    logger.info(f"  bands_bps: {bands_bps}")
    
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
    
    # Step 1: Extract structural features
    logger.info(f"Extracting structural features (max_levels={max_levels})...")
    df = extract_structural_features(df, max_levels=max_levels, bands_bps=bands_bps)
    
    # Step 2: Add log return
    df = df.with_columns([
        (pl.col("mid_price") / pl.col("mid_price").shift(1)).log().alias("log_return"),
    ])
    
    # Step 3: Compute rolling features
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
    config = None
    try:
        config = load_config(args.config)
        logger.info(f"Loaded config from: {args.config or 'auto-detected'}")
    except FileNotFoundError as e:
        logger.warning(f"Config not found ({e}), using research defaults")
    except Exception as e:
        logger.warning(f"Error loading config ({e}), using research defaults")
    
    # Parse paths
    input_base = Path(args.input)
    output_base = Path(args.output)
    
    logger.info("=" * 60)
    logger.info("ETL Pipeline v2 - Research-Optimized Framework")
    logger.info("=" * 60)
    logger.info(f"Input:  {input_base}")
    logger.info(f"Output: {output_base}")
    logger.info(f"Channel: {args.channel}")
    logger.info(f"Config:  {'Loaded from config.yaml' if config else 'Using research defaults'}")
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
