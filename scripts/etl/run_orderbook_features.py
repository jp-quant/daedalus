#!/usr/bin/env python3
"""
Orderbook Features ETL Script
=============================

Transform raw orderbook data into comprehensive features using TransformExecutor.

This script properly delegates I/O to the executor while supporting:
- Stateful feature computation with checkpoint/resume
- Optional trades data for TFI calculation
- FilterSpec for efficient partition pruning

Features computed (in OrderbookFeatureTransform):
- Structural: L0-LN prices/sizes, mid, spread, microprice, imbalances, depth bands
- Dynamic: OFI, MLOFI, TFI*, velocity, acceleration, log returns
- Rolling: Realized volatility, mean spread, regime fraction
- Advanced: Kyle's Lambda, VPIN

*TFI requires trades data. If --trades is not provided, TFI will be zero.

Note: Bar aggregation (silver → gold) is handled separately by run_bars.py.
This script focuses on bronze → silver feature engineering.

Usage:
    # Process orderbook only (TFI will be zero)
    python scripts/etl/run_orderbook_features.py

    # Process with trades for TFI calculation (recommended)
    python scripts/etl/run_orderbook_features.py --trades data/raw/ready/ccxt/trades

    # Process specific exchange/symbol partition
    python scripts/etl/run_orderbook_features.py --exchange binanceus --symbol BTC/USDT

    # Dry run (preview only)
    python scripts/etl/run_orderbook_features.py --dry-run

    # With state persistence for checkpoint/resume
    python scripts/etl/run_orderbook_features.py --state-path state/orderbook_state.json

    # Limit rows for testing
    python scripts/etl/run_orderbook_features.py --limit 10000
"""
import argparse
import json
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
from etl.transforms.orderbook import OrderbookFeatureTransform

logger = logging.getLogger(__name__)


def create_transform_config(
    input_path: str,
    output_path: str,
    trades_path: Optional[str] = None,
    storage_config: Optional[StorageConfig] = None,
) -> TransformConfig:
    """
    Create the transform configuration for orderbook features.
    
    Args:
        input_path: Path to bronze orderbook data
        output_path: Path for silver features output
        trades_path: Optional path to trades data for TFI calculation
        storage_config: Optional storage configuration
    
    Returns:
        TransformConfig ready for execution
    """
    inputs = {
        "orderbook": InputConfig(
            name="orderbook",
            path=input_path,
            format=DataFormat.PARQUET,
        ),
    }
    
    # Add trades input if provided (enables TFI calculation)
    if trades_path:
        inputs["trades"] = InputConfig(
            name="trades",
            path=trades_path,
            format=DataFormat.PARQUET,
        )
    
    return TransformConfig(
        name="orderbook_features",
        description="Extract comprehensive features from raw orderbook data",
        inputs=inputs,
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
        storage=storage_config,
    )


def create_feature_config_from_options(
    config: Optional[DaedalusConfig] = None,
) -> FeatureConfig:
    """
    Create feature configuration from Daedalus config options.
    
    Args:
        config: Optional Daedalus configuration
    
    Returns:
        FeatureConfig with appropriate settings
    """
    # Default values
    depth_levels = 20
    rolling_windows = [5, 15, 60, 300, 900]
    vpin_bucket_size = 1.0
    ofi_decay_alpha = 0.5
    enable_vpin = True
    enable_stateful = True
    
    # Override from config if available
    if config and hasattr(config, "etl"):
        channel_config = config.etl.channels.get("orderbook")
        if channel_config and channel_config.processor_options:
            opts = dict(channel_config.processor_options)
            depth_levels = opts.get("max_levels", depth_levels)
            rolling_windows = opts.get("horizons", rolling_windows)
            vpin_bucket_size = opts.get("vpin_bucket_volume", vpin_bucket_size)
            ofi_decay_alpha = opts.get("ofi_decay_alpha", ofi_decay_alpha)
            enable_vpin = opts.get("enable_vpin", enable_vpin)
            enable_stateful = opts.get("enable_stateful", enable_stateful)
    
    # Build categories
    categories = {FeatureCategory.STRUCTURAL, FeatureCategory.DYNAMIC}
    
    if enable_stateful:
        categories.add(FeatureCategory.ROLLING)
    
    if enable_vpin:
        categories.add(FeatureCategory.ADVANCED)
    
    return FeatureConfig(
        categories=categories,
        depth_levels=depth_levels,
        rolling_windows=rolling_windows,
        vpin_bucket_size=vpin_bucket_size,
        ofi_decay_alpha=ofi_decay_alpha,
    )


class StatefulOrderbookRunner:
    """
    Runner for orderbook feature ETL with state management.
    
    Supports checkpoint/resume for long-running batch jobs.
    """
    
    def __init__(
        self,
        config: Optional[DaedalusConfig] = None,
        state_path: Optional[Path] = None,
    ):
        """
        Initialize runner.
        
        Args:
            config: Daedalus configuration
            state_path: Path for state persistence
        """
        self.config = config
        self.state_path = state_path
        self._state: Dict[str, Any] = {}
        
        if self.state_path and self.state_path.exists():
            self._load_state()
    
    def _load_state(self):
        """Load state from disk."""
        try:
            with open(self.state_path, 'r') as f:
                self._state = json.load(f)
            logger.info(f"Loaded state from {self.state_path}")
        except Exception as e:
            logger.warning(f"Could not load state: {e}")
            self._state = {}
    
    def _save_state(self):
        """Save state to disk."""
        if not self.state_path:
            return
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_path, 'w') as f:
                json.dump(self._state, f, indent=2, default=str)
            logger.debug(f"Saved state to {self.state_path}")
        except Exception as e:
            logger.error(f"Could not save state: {e}")
    
    def get_transform_state(self) -> Optional[Dict[str, Any]]:
        """Get saved transform state for resume."""
        return self._state.get("transform_state")
    
    def save_transform_state(
        self,
        transform: OrderbookFeatureTransform,
        execution_id: str,
        stats: Dict[str, Any],
    ):
        """Save transform state after execution."""
        self._state["transform_state"] = transform.get_state()
        self._state["last_execution"] = {
            "id": execution_id,
            "time": datetime.now().isoformat(),
            "stats": stats,
        }
        self._save_state()


def run_orderbook_etl(
    input_path: Path,
    output_path: Path,
    trades_path: Optional[Path] = None,
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    dry_run: bool = False,
    limit_rows: Optional[int] = None,
    state_path: Optional[Path] = None,
    config: Optional[DaedalusConfig] = None,
) -> Dict[str, Any]:
    """
    Run the orderbook feature ETL pipeline using TransformExecutor.
    
    Args:
        input_path: Path to bronze orderbook parquet files
        output_path: Base output path for silver outputs
        trades_path: Optional path to trades data for TFI calculation
        exchange: Optional exchange filter
        symbol: Optional symbol filter
        dry_run: If True, don't write outputs
        limit_rows: Optional limit on rows to process
        state_path: Optional path for state persistence
        config: Optional Daedalus configuration
    
    Returns:
        Execution statistics dictionary
    """
    execution_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info(f"[{execution_id}] Orderbook Features ETL Pipeline")
    logger.info("=" * 70)
    
    # Load config if not provided
    if config is None:
        try:
            config = load_config()
        except Exception as e:
            logger.warning(f"Could not load config: {e}. Using defaults.")
    
    # Initialize state runner if state persistence enabled
    state_runner = None
    if state_path:
        state_runner = StatefulOrderbookRunner(config=config, state_path=state_path)
    
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
        )
        logger.info(f"Filter: {filter_spec}")
    
    # Create feature config from options
    feature_config = create_feature_config_from_options(config)
    
    # Log configuration
    logger.info(f"Input:  {input_path}")
    logger.info(f"Output: {output_path}")
    if trades_path:
        logger.info(f"Trades: {trades_path} (TFI enabled)")
    else:
        logger.info("Trades: not provided (TFI disabled)")
    logger.info(f"Categories: {[c.value for c in feature_config.categories]}")
    logger.info(f"Depth levels: {feature_config.depth_levels}")
    logger.info(f"Rolling windows: {feature_config.rolling_windows}")
    if limit_rows:
        logger.info(f"Row limit: {limit_rows:,}")
    if dry_run:
        logger.info("[DRY RUN] No outputs will be written")
    
    # Build full output path with tier
    full_output_path = str(output_path / features_tier / "orderbook")
    
    # Check if trades path exists and has data
    actual_trades_path = None
    if trades_path and trades_path.exists():
        trades_files = list(trades_path.glob("**/*.parquet"))
        if trades_files:
            actual_trades_path = str(trades_path)
            logger.info(f"Found {len(trades_files)} trades files for TFI")
        else:
            logger.warning(f"No parquet files found in {trades_path} - TFI disabled")
    
    # Create transform configuration
    transform_config = create_transform_config(
        input_path=str(input_path),
        output_path=full_output_path,
        trades_path=actual_trades_path,
    )
    
    # Create transform and executor
    transform = OrderbookFeatureTransform(transform_config)
    executor = TransformExecutor(feature_config=feature_config)
    
    # Restore transform state if resuming
    if state_runner:
        saved_state = state_runner.get_transform_state()
        if saved_state:
            transform.set_state(saved_state)
            logger.info("Restored transform state from checkpoint")
    
    # Execute transform via executor
    logger.info("Executing transform via executor...")
    result = executor.execute(
        transform=transform,
        filter_spec=filter_spec,
        dry_run=dry_run,
        limit_rows=limit_rows,
    )
    
    # Save state for future resume
    if state_runner and not dry_run:
        state_runner.save_transform_state(transform, execution_id, result)
    
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
        description="Orderbook Features ETL - Transform raw orderbook data to features"
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
        "--limit",
        type=int,
        help="Limit number of rows to process (for testing)",
    )
    parser.add_argument(
        "--state-path",
        type=str,
        help="Path to persist/load state for checkpoint/resume",
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
    
    # Load config
    config = None
    if args.config:
        try:
            config = load_config(args.config)
        except Exception as e:
            logger.warning(f"Could not load config from {args.config}: {e}")
    
    # Run ETL
    result = run_orderbook_etl(
        input_path=Path(args.input),
        output_path=Path(args.output),
        trades_path=Path(args.trades) if args.trades else None,
        exchange=args.exchange,
        symbol=args.symbol,
        dry_run=args.dry_run,
        limit_rows=args.limit,
        state_path=Path(args.state_path) if args.state_path else None,
        config=config,
    )
    
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
