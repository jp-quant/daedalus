#!/usr/bin/env python3
"""
Orderbook Features ETL Script - Batched Processing
==================================================

Memory-efficient orderbook features ETL that processes data by partition.

This script processes orderbook data partition-by-partition (exchange/symbol/date)
to avoid loading the entire dataset into memory. Essential for large datasets.

Key differences from run_orderbook_features.py:
- Processes each (exchange, symbol, date) partition separately
- Uses MUCH less memory (~100x reduction for large datasets)
- Enables parallel processing across partitions
- Supports resume from checkpoint

Features computed:
- Structural: L0-LN prices/sizes, mid, spread, microprice, imbalances, depth bands
- Dynamic: OFI, MLOFI, TFI*, velocity, acceleration, log returns
- Rolling: Realized volatility, mean spread, regime fraction
- Advanced: Kyle's Lambda, VPIN

*TFI requires trades data. If --trades is not provided, TFI will be zero.

Usage:
    # Process all partitions
    python scripts/etl/run_orderbook_features_batched.py

    # Process with trades for TFI calculation
    python scripts/etl/run_orderbook_features_batched.py --trades data/raw/ready/ccxt/trades

    # Process specific exchange/symbol
    python scripts/etl/run_orderbook_features_batched.py --exchange binanceus --symbol BTC/USDT

    # Parallel processing with 4 workers
    python scripts/etl/run_orderbook_features_batched.py --workers 4

    # Resume from checkpoint
    python scripts/etl/run_orderbook_features_batched.py --checkpoint-path state/orderbook_checkpoint.json
"""
import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
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
from etl.transforms.orderbook import OrderbookFeatureTransform

logger = logging.getLogger(__name__)


def discover_partitions(
    input_path: Path,
    exchange_filter: Optional[str] = None,
    symbol_filter: Optional[str] = None,
) -> List[Tuple[str, str, str]]:
    """
    Discover all (exchange, symbol, date) partitions in the input directory.
    
    Args:
        input_path: Base path to scan for partitions
        exchange_filter: Optional exchange to filter
        symbol_filter: Optional symbol to filter
    
    Returns:
        List of (exchange, symbol, date) tuples
    """
    partitions = []
    
    # Pattern: {base}/exchange={ex}/symbol={sym}/year={y}/month={m}/day={d}/...
    for exchange_dir in input_path.glob("exchange=*"):
        exchange = exchange_dir.name.split("=")[1]
        
        if exchange_filter and exchange != exchange_filter:
            continue
        
        for symbol_dir in exchange_dir.glob("symbol=*"):
            symbol = symbol_dir.name.split("=")[1]
            
            if symbol_filter and symbol != symbol_filter:
                continue
            
            # Collect all dates for this exchange/symbol
            for year_dir in symbol_dir.glob("year=*"):
                year = year_dir.name.split("=")[1]
                for month_dir in year_dir.glob("month=*"):
                    month = month_dir.name.split("=")[1]
                    for day_dir in month_dir.glob("day=*"):
                        day = day_dir.name.split("=")[1]
                        date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        partitions.append((exchange, symbol, date_str))
    
    return partitions


def load_checkpoint(checkpoint_path: Path) -> Dict[str, Any]:
    """Load checkpoint state from disk."""
    if not checkpoint_path.exists():
        return {"processed": set(), "failed": set(), "stats": {}}
    
    try:
        with open(checkpoint_path, 'r') as f:
            data = json.load(f)
        # Convert lists back to sets
        data["processed"] = set(tuple(p) for p in data.get("processed", []))
        data["failed"] = set(tuple(p) for p in data.get("failed", []))
        return data
    except Exception as e:
        logger.warning(f"Could not load checkpoint: {e}")
        return {"processed": set(), "failed": set(), "stats": {}}


def save_checkpoint(checkpoint_path: Path, state: Dict[str, Any]):
    """Save checkpoint state to disk."""
    try:
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        # Convert sets to lists for JSON serialization
        data = {
            "processed": [list(p) for p in state["processed"]],
            "failed": [list(p) for p in state["failed"]],
            "stats": state["stats"],
            "last_update": datetime.now().isoformat(),
        }
        with open(checkpoint_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    except Exception as e:
        logger.error(f"Could not save checkpoint: {e}")


def process_partition(
    exchange: str,
    symbol: str,
    date: str,
    input_path: Path,
    output_path: Path,
    trades_path: Optional[Path],
    feature_config: FeatureConfig,
    transform_config_template: TransformConfig,
) -> Dict[str, Any]:
    """
    Process a single (exchange, symbol, date) partition.
    
    Args:
        exchange: Exchange identifier
        symbol: Trading pair symbol
        date: Date string (YYYY-MM-DD)
        input_path: Base input path
        output_path: Base output path
        trades_path: Optional trades path
        feature_config: Feature configuration
        transform_config_template: Transform config template
    
    Returns:
        Result dictionary with statistics
    """
    partition_key = f"{exchange}/{symbol}/{date}"
    logger.info(f"Processing partition: {partition_key}")
    
    try:
        # Parse date components
        year, month, day = date.split("-")
        
        # Create filter spec for this exact partition
        filter_spec = FilterSpec(
            exchange=exchange,
            symbol=symbol,
            year=int(year),
            month=int(month),
            day=int(day),
        )
        
        # Create transform instance (fresh for each partition to reset state)
        transform = OrderbookFeatureTransform(transform_config_template)
        
        # Create executor
        executor = TransformExecutor(feature_config=feature_config)
        
        # Initialize transform
        context = TransformContext(
            execution_id=str(uuid.uuid4())[:8],
            partition_values={
                "exchange": exchange,
                "symbol": symbol,
                "date": date,
            },
            feature_config=feature_config,
        )
        transform.initialize(context)
        
        # Execute transform for this partition only
        result = executor.execute(
            transform=transform,
            context=context,
            filter_spec=filter_spec,
            dry_run=False,
        )
        
        logger.info(f"✓ Completed partition: {partition_key} - "
                   f"{result.get('write_stats', {}).get('silver', {}).get('rows', 0):,} rows")
        
        return {
            "success": True,
            "partition": (exchange, symbol, date),
            "stats": result.get("write_stats", {}),
        }
        
    except Exception as e:
        logger.error(f"✗ Failed partition: {partition_key} - {e}")
        return {
            "success": False,
            "partition": (exchange, symbol, date),
            "error": str(e),
        }


def run_batched_etl(
    input_path: Path,
    output_path: Path,
    trades_path: Optional[Path] = None,
    exchange_filter: Optional[str] = None,
    symbol_filter: Optional[str] = None,
    workers: int = 1,
    checkpoint_path: Optional[Path] = None,
    config: Optional[DaedalusConfig] = None,
) -> Dict[str, Any]:
    """
    Run orderbook ETL in batched mode (partition-by-partition).
    
    Args:
        input_path: Base input path
        output_path: Base output path
        trades_path: Optional trades path
        exchange_filter: Optional exchange filter
        symbol_filter: Optional symbol filter
        workers: Number of parallel workers
        checkpoint_path: Optional checkpoint file path
        config: Optional Daedalus configuration
    
    Returns:
        Summary statistics
    """
    start_time = datetime.now()
    execution_id = str(uuid.uuid4())[:8]
    
    logger.info("=" * 70)
    logger.info(f"[{execution_id}] Orderbook Features ETL - BATCHED MODE")
    logger.info("=" * 70)
    
    # Load checkpoint if provided
    checkpoint = load_checkpoint(checkpoint_path) if checkpoint_path else {
        "processed": set(),
        "failed": set(),
        "stats": {},
    }
    
    # Discover all partitions
    logger.info("Discovering partitions...")
    all_partitions = discover_partitions(input_path, exchange_filter, symbol_filter)
    logger.info(f"Found {len(all_partitions)} total partitions")
    
    # Filter out already processed partitions
    pending_partitions = [
        p for p in all_partitions
        if p not in checkpoint["processed"] and p not in checkpoint["failed"]
    ]
    
    if not pending_partitions:
        logger.info("All partitions already processed!")
        return {
            "success": True,
            "total_partitions": len(all_partitions),
            "processed": len(checkpoint["processed"]),
            "failed": len(checkpoint["failed"]),
        }
    
    logger.info(f"Processing {len(pending_partitions)} pending partitions")
    if checkpoint["processed"]:
        logger.info(f"Resuming: {len(checkpoint['processed'])} already processed")
    
    # Create feature config
    from scripts.etl.run_orderbook_features import create_feature_config_from_options
    feature_config = create_feature_config_from_options(config)
    
    logger.info(f"Features: {[c.value for c in feature_config.categories]}")
    logger.info(f"Depth levels: {feature_config.depth_levels}")
    logger.info(f"Workers: {workers}")
    
    # Build transform config template
    features_tier = "silver"
    if config and hasattr(config, "storage"):
        features_tier = config.storage.paths.tier_features
    
    full_output_path = str(output_path / features_tier / "orderbook")
    
    from scripts.etl.run_orderbook_features import create_transform_config
    transform_config_template = create_transform_config(
        input_path=str(input_path),
        output_path=full_output_path,
        trades_path=str(trades_path) if trades_path else None,
    )
    
    # Process partitions
    processed_count = len(checkpoint["processed"])
    failed_count = len(checkpoint["failed"])
    total_rows = 0
    
    if workers == 1:
        # Sequential processing
        for exchange, symbol, date in pending_partitions:
            result = process_partition(
                exchange, symbol, date,
                input_path, output_path, trades_path,
                feature_config, transform_config_template,
            )
            
            partition_tuple = (exchange, symbol, date)
            if result["success"]:
                checkpoint["processed"].add(partition_tuple)
                processed_count += 1
                rows = result.get("stats", {}).get("silver", {}).get("rows", 0)
                total_rows += rows
            else:
                checkpoint["failed"].add(partition_tuple)
                failed_count += 1
            
            # Save checkpoint after each partition
            if checkpoint_path:
                save_checkpoint(checkpoint_path, checkpoint)
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    process_partition,
                    exchange, symbol, date,
                    input_path, output_path, trades_path,
                    feature_config, transform_config_template,
                ): (exchange, symbol, date)
                for exchange, symbol, date in pending_partitions
            }
            
            for future in as_completed(futures):
                partition_tuple = futures[future]
                try:
                    result = future.result()
                    if result["success"]:
                        checkpoint["processed"].add(partition_tuple)
                        processed_count += 1
                        rows = result.get("stats", {}).get("silver", {}).get("rows", 0)
                        total_rows += rows
                    else:
                        checkpoint["failed"].add(partition_tuple)
                        failed_count += 1
                except Exception as e:
                    logger.error(f"Exception in partition {partition_tuple}: {e}")
                    checkpoint["failed"].add(partition_tuple)
                    failed_count += 1
                
                # Save checkpoint periodically
                if checkpoint_path and (processed_count + failed_count) % 10 == 0:
                    save_checkpoint(checkpoint_path, checkpoint)
    
    # Final checkpoint save
    if checkpoint_path:
        save_checkpoint(checkpoint_path, checkpoint)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    logger.info("=" * 70)
    logger.info(f"[{execution_id}] BATCHED ETL COMPLETE in {duration:.2f}s")
    logger.info(f"Total partitions: {len(all_partitions)}")
    logger.info(f"Processed: {processed_count} ✓")
    logger.info(f"Failed: {failed_count} ✗")
    logger.info(f"Total rows: {total_rows:,}")
    logger.info("=" * 70)
    
    return {
        "success": failed_count == 0,
        "execution_id": execution_id,
        "duration_seconds": duration,
        "total_partitions": len(all_partitions),
        "processed": processed_count,
        "failed": failed_count,
        "total_rows": total_rows,
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Orderbook Features ETL - Batched partition-by-partition processing"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/raw/ready/ccxt/orderbook",
        help="Input directory with bronze orderbook parquet files",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed",
        help="Output directory for silver outputs",
    )
    parser.add_argument(
        "--trades",
        type=str,
        help="Path to bronze trades parquet files (enables TFI calculation)",
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
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1)",
    )
    parser.add_argument(
        "--checkpoint-path",
        type=str,
        default="state/orderbook_checkpoint.json",
        help="Path to checkpoint file for resume support",
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config.yaml",
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
    
    # Load config
    config = None
    if args.config:
        try:
            config = load_config(args.config)
        except Exception as e:
            logger.warning(f"Could not load config from {args.config}: {e}")
    
    # Run batched ETL
    result = run_batched_etl(
        input_path=Path(args.input),
        output_path=Path(args.output),
        trades_path=Path(args.trades) if args.trades else None,
        exchange_filter=args.exchange,
        symbol_filter=args.symbol,
        workers=args.workers,
        checkpoint_path=Path(args.checkpoint_path) if args.checkpoint_path else None,
        config=config,
    )
    
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
