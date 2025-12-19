#!/usr/bin/env python3
"""
Run Daedalus Parquet ETL Pipeline.

This script processes channel-separated Parquet files (orderbook, trades, ticker)
using the hybrid Polars-based pipeline for feature engineering.

Usage:
    # Process all channels from default config
    python scripts/run_parquet_etl.py
    
    # Process specific source
    python scripts/run_parquet_etl.py --source ccxt
    
    # Process specific channel only
    python scripts/run_parquet_etl.py --channel orderbook
    
    # Use custom config
    python scripts/run_parquet_etl.py --config config/production.yaml
    
    # Process specific date range
    python scripts/run_parquet_etl.py --start-date 2025-01-01 --end-date 2025-01-15
    
    # Dry run (show what would be processed)
    python scripts/run_parquet_etl.py --dry-run
"""
import logging
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import argparse

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
os.chdir(Path(__file__).parent.parent)

from config import load_config, DaedalusConfig
from storage.factory import (
    create_etl_storage_input,
    create_etl_storage_output,
    get_etl_input_path,
    get_etl_output_path,
)
from etl.parquet_etl_pipeline import (
    ParquetETLPipeline,
    ParquetETLConfig,
)

logger = logging.getLogger(__name__)


def create_etl_config_from_daedalus(config: DaedalusConfig) -> ParquetETLConfig:
    """
    Create ParquetETLConfig from DaedalusConfig.
    
    Maps ETL-specific settings from the main config.
    """
    # Cast compression to proper type (validated at config load)
    from etl.parquet_etl_pipeline import CompressionType
    compression: CompressionType = config.etl.compression  # type: ignore[assignment]
    
    etl_config = ParquetETLConfig(
        compression=compression,
    )
    
    # Map channel-specific partition columns if configured
    if config.etl.channels:
        if "ticker" in config.etl.channels and config.etl.channels["ticker"].partition_cols:
            etl_config.ticker_partition_cols = config.etl.channels["ticker"].partition_cols
        if "trades" in config.etl.channels and config.etl.channels["trades"].partition_cols:
            etl_config.trades_partition_cols = config.etl.channels["trades"].partition_cols
        if "orderbook" in config.etl.channels and config.etl.channels["orderbook"].partition_cols:
            etl_config.orderbook_partition_cols = config.etl.channels["orderbook"].partition_cols
    
    return etl_config


def discover_parquet_files(
    storage_input,
    base_path: str,
    channel: str,
) -> List[str]:
    """
    Discover Parquet files for a given channel.
    
    Args:
        storage_input: Storage backend for input
        base_path: Base path for raw data
        channel: Channel name (ticker, trades, orderbook)
        
    Returns:
        List of paths to Parquet files
    """
    channel_path = f"{base_path}/{channel}"
    
    try:
        files = storage_input.list_files(
            channel_path,
            pattern="*.parquet",
            recursive=True,
        )
        return [f["path"] for f in files]
    except Exception as e:
        logger.warning(f"Error listing files in {channel_path}: {e}")
        return []


def run_etl_pipeline(
    config: DaedalusConfig,
    source: str = "ccxt",
    channels: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    exchange_filter: Optional[str] = None,
    symbol_filter: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run the Parquet ETL pipeline.
    
    Args:
        config: Daedalus configuration
        source: Data source (ccxt, coinbase)
        channels: Channels to process (default: all)
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        exchange_filter: Optional exchange filter
        symbol_filter: Optional symbol filter
        dry_run: If True, just show what would be processed
        
    Returns:
        Dict with processing statistics
    """
    # Create storage backends
    storage_input = create_etl_storage_input(config)
    storage_output = create_etl_storage_output(config)
    
    # Get paths
    input_base = get_etl_input_path(config, source)
    output_base = get_etl_output_path(config, source)
    
    logger.info("=" * 80)
    logger.info("Daedalus Parquet ETL Pipeline")
    logger.info("=" * 80)
    logger.info(f"Source: {source}")
    logger.info(f"Input Storage:  {storage_input.backend_type} @ {storage_input.base_path}")
    logger.info(f"Output Storage: {storage_output.backend_type} @ {storage_output.base_path}")
    logger.info(f"Input Path:  {input_base}")
    logger.info(f"Output Path: {output_base}")
    
    if start_date:
        logger.info(f"Start Date: {start_date}")
    if end_date:
        logger.info(f"End Date: {end_date}")
    if exchange_filter:
        logger.info(f"Exchange Filter: {exchange_filter}")
    if symbol_filter:
        logger.info(f"Symbol Filter: {symbol_filter}")
    
    # Determine channels to process
    available_channels = ["ticker", "trades", "orderbook"]
    if channels:
        channels_to_process = [c for c in channels if c in available_channels]
    else:
        # Check which channels are enabled in config
        channels_to_process = []
        for ch in available_channels:
            if ch in config.etl.channels and config.etl.channels[ch].enabled:
                channels_to_process.append(ch)
        
        # If no config, default to all
        if not channels_to_process:
            channels_to_process = available_channels
    
    logger.info(f"Channels to process: {channels_to_process}")
    
    # Discover input files
    channel_files = {}
    for channel in channels_to_process:
        files = discover_parquet_files(storage_input, input_base, channel)
        channel_files[channel] = files
        logger.info(f"Found {len(files)} files for {channel}")
    
    if dry_run:
        logger.info("")
        logger.info("DRY RUN - No processing will occur")
        logger.info("")
        for channel, files in channel_files.items():
            logger.info(f"  {channel}: {len(files)} files")
            for f in files[:5]:  # Show first 5
                logger.info(f"    - {f}")
            if len(files) > 5:
                logger.info(f"    ... and {len(files) - 5} more")
        return {"dry_run": True, "files": channel_files}
    
    # Create ETL config and pipeline
    etl_config = create_etl_config_from_daedalus(config)
    pipeline = ParquetETLPipeline(
        config=etl_config,
        storage_input=storage_input,
        storage_output=storage_output,
    )
    
    stats = {
        "channels_processed": [],
        "records_per_channel": {},
        "errors": [],
    }
    
    # Process each channel
    for channel in channels_to_process:
        if not channel_files.get(channel):
            logger.warning(f"No files found for {channel}, skipping")
            continue
        
        logger.info("")
        logger.info(f"Processing {channel}...")
        logger.info("-" * 40)
        
        input_path = f"{input_base}/{channel}"
        output_path = f"{output_base}/{channel}"
        
        try:
            if channel == "ticker":
                result = pipeline.process_ticker(
                    input_path=input_path,
                    output_path=output_path,
                )
                stats["records_per_channel"]["ticker"] = len(result)
                
            elif channel == "trades":
                result = pipeline.process_trades(
                    input_path=input_path,
                    output_path=output_path,
                )
                stats["records_per_channel"]["trades"] = len(result)
                
            elif channel == "orderbook":
                # Orderbook needs trades for TFI calculation
                trades_path = f"{input_base}/trades"
                hf_df, bars_df = pipeline.process_orderbook_with_trades(
                    orderbook_path=input_path,
                    trades_path=trades_path,
                    output_hf_path=f"{output_path}/hf",
                    output_bars_path=f"{output_path}/bars",
                    exchange=exchange_filter,
                    symbol=symbol_filter,
                )
                stats["records_per_channel"]["orderbook_hf"] = len(hf_df)
                stats["records_per_channel"]["orderbook_bars"] = len(bars_df)
            
            stats["channels_processed"].append(channel)
            logger.info(f"Completed {channel}")
            
        except Exception as e:
            logger.error(f"Error processing {channel}: {e}", exc_info=True)
            stats["errors"].append({"channel": channel, "error": str(e)})
    
    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("ETL Complete")
    logger.info("=" * 80)
    logger.info(f"Channels processed: {stats['channels_processed']}")
    for channel, count in stats["records_per_channel"].items():
        logger.info(f"  {channel}: {count:,} records")
    if stats["errors"]:
        logger.warning(f"Errors: {len(stats['errors'])}")
        for err in stats["errors"]:
            logger.warning(f"  {err['channel']}: {err['error']}")
    
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Daedalus Parquet ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (default: config/config.yaml)"
    )
    parser.add_argument(
        "--source",
        type=str,
        default="ccxt",
        choices=["coinbase", "ccxt"],
        help="Data source to process (default: ccxt)"
    )
    parser.add_argument(
        "--channel",
        type=str,
        action="append",
        dest="channels",
        choices=["ticker", "trades", "orderbook"],
        help="Channel to process (can be specified multiple times, default: all)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date filter (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date filter (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--exchange",
        type=str,
        help="Filter by exchange (e.g., binance)"
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="Filter by symbol (e.g., BTC/USDT)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without executing"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Load config
    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please create a config file or set DAEDALUS_CONFIG environment variable")
        sys.exit(1)
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else getattr(logging, config.log_level)
    logging.basicConfig(
        level=log_level,
        format=config.log_format,
    )
    
    # Run ETL
    try:
        stats = run_etl_pipeline(
            config=config,
            source=args.source,
            channels=args.channels,
            start_date=args.start_date,
            end_date=args.end_date,
            exchange_filter=args.exchange,
            symbol_filter=args.symbol,
            dry_run=args.dry_run,
        )
        
        if stats.get("errors"):
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
