#!/usr/bin/env python3
"""
Orderbook Features ETL Script
=============================

Transform raw orderbook data into comprehensive features using the new ETL framework.
Properly uses the abstracted transform classes and executor pattern.

This script implements stateful feature computation with proper state management
for both batch processing and future streaming support.

Features computed:
- Structural: L0-LN prices/sizes, mid, spread, microprice, imbalances, depth bands
- Dynamic: OFI, MLOFI, TFI*, velocity, acceleration, log returns
- Rolling: Realized volatility, mean spread, regime fraction
- Advanced: Kyle's Lambda, VPIN
- Bars: Multi-timeframe OHLCV with microstructure stats

*TFI requires trades data. If --trades is not provided, TFI will be zero.

Usage:
    # Process orderbook only (TFI will be zero)
    python scripts/etl/run_orderbook_features.py

    # Process with trades for TFI calculation (recommended)
    python scripts/etl/run_orderbook_features.py --trades data/raw/ready/ccxt/trades

    # Process specific input/output paths
    python scripts/etl/run_orderbook_features.py --input data/raw/ready/ccxt/orderbook --output data/processed

    # Process specific exchange/symbol partition
    python scripts/etl/run_orderbook_features.py --exchange binanceus --symbol BTC/USDT

    # Dry run (preview only)
    python scripts/etl/run_orderbook_features.py --dry-run

    # Load state from previous run (for batch continuity)
    python scripts/etl/run_orderbook_features.py --state-path state/orderbook_state.json
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
from etl.transforms.orderbook import OrderbookFeatureTransform
from etl.transforms.bars import aggregate_bars_vectorized
from etl.utils.state_manager import StateManager

logger = logging.getLogger(__name__)


class OrderbookETLRunner:
    """
    Runner for orderbook feature ETL pipeline.
    
    Manages stateful processing across batches and symbols.
    Supports checkpoint/resume for long-running batch jobs with
    automatic state persistence on graceful shutdown.
    """
    
    def __init__(
        self,
        config: Optional[DaedalusConfig] = None,
        state_path: Optional[Path] = None,
    ):
        """
        Initialize runner.
        
        Args:
            config: Daedalus configuration (loads default if None)
            state_path: Path to persist state for checkpoint/resume
                       If None, uses config default (data/temp/state/)
        """
        self.config = config or self._load_config()
        
        # Determine state directory from config
        state_config = self.config.etl.state
        storage_config = self.config.storage.etl_storage_input
        
        if state_path:
            self.state_path = state_path
        elif state_config.enabled:
            base_path = Path(storage_config.base_dir)
            state_dir = base_path / state_config.state_dir
            state_dir.mkdir(parents=True, exist_ok=True)
            self.state_path = state_dir / "orderbook_features_state.json"
        else:
            self.state_path = None
        
        self._state: Dict[str, Any] = {}
        
        # Initialize state manager for automatic persistence
        if state_config.enabled and self.state_path:
            self._state_manager = StateManager(
                state_dir=self.state_path.parent,
                checkpoint_interval=state_config.checkpoint_interval_seconds,
                max_state_files=state_config.max_state_files,
                auto_save_on_shutdown=state_config.auto_save_on_shutdown,
            )
        else:
            self._state_manager = None
        
        # Load existing state if available
        if self.state_path and self.state_path.exists():
            self._load_state()
    
    def _load_config(self) -> DaedalusConfig:
        """Load configuration with fallback to defaults."""
        try:
            return load_config()
        except Exception as e:
            logger.warning(f"Could not load config: {e}. Using defaults.")
            return DaedalusConfig()
    
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
    
    def _get_processor_options(self) -> Dict[str, Any]:
        """Get processor options from config."""
        if not self.config or not getattr(self.config, "etl", None):
            return {}
        
        channel_config = self.config.etl.channels.get("orderbook")
        if not channel_config or not channel_config.processor_options:
            return {}
        
        return dict(channel_config.processor_options)
    
    def _create_feature_config(self, options: Dict[str, Any]) -> FeatureConfig:
        """Create FeatureConfig from processor options."""
        categories = {FeatureCategory.STRUCTURAL, FeatureCategory.DYNAMIC}
        
        if options.get("enable_stateful", True):
            categories.add(FeatureCategory.ROLLING)
        
        if options.get("enable_vpin", True):
            categories.add(FeatureCategory.ADVANCED)
        
        return FeatureConfig(
            categories=categories,
            depth_levels=options.get("max_levels", 20),
            rolling_windows=options.get("horizons", [5, 15, 60, 300, 900]),
            bar_interval_seconds=60,
            vpin_bucket_size=options.get("vpin_bucket_volume", 1.0),
            ofi_decay_alpha=options.get("ofi_decay_alpha", 0.5),
            spread_tight_threshold_bps=options.get("spread_tight_percentile", 0.2) * 100,
            spread_wide_threshold_bps=options.get("spread_wide_percentile", 0.8) * 100,
        )
    
    def _create_transform_config(
        self,
        input_path: str,
        output_path: str,
        partition_cols: List[str],
        trades_path: Optional[str] = None,
    ) -> TransformConfig:
        """Create transform configuration.
        
        Args:
            input_path: Path to bronze orderbook data
            output_path: Base output path
            partition_cols: Partition columns for input
            trades_path: Optional path to trades data for TFI calculation
        """
        inputs = {
            "orderbook": InputConfig(
                name="orderbook",
                path=input_path,
                format=DataFormat.PARQUET,
                partition_cols=partition_cols,
            ),
        }
        
        # Add trades input if provided (enables TFI calculation)
        if trades_path:
            inputs["trades"] = InputConfig(
                name="trades",
                path=trades_path,
                format=DataFormat.PARQUET,
                partition_cols=partition_cols,
            )
        
        return TransformConfig(
            name="orderbook_features_batch",
            description="Extract comprehensive features from raw orderbook data",
            inputs=inputs,
            outputs={
                "silver_features": OutputConfig(
                    name="silver_features",
                    path=f"{output_path}/silver/orderbook",
                    format=DataFormat.PARQUET,
                    partition_cols=["exchange", "symbol", "year", "month", "day"],
                    mode=WriteMode.OVERWRITE_PARTITION,
                    compression=CompressionCodec.ZSTD,
                    compression_level=3,
                ),
            },
        )
    
    def run(
        self,
        input_path: Path,
        output_path: Path,
        trades_path: Optional[Path] = None,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Run the orderbook feature ETL pipeline.
        
        Args:
            input_path: Path to bronze orderbook parquet files
            output_path: Base output path for silver/gold outputs
            trades_path: Optional path to bronze trades parquet files (for TFI)
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            dry_run: If True, don't write outputs
            
        Returns:
            Statistics dictionary
        """
        execution_id = str(uuid.uuid4())[:8]
        start_time = datetime.now()
        
        logger.info("=" * 70)
        logger.info(f"[{execution_id}] Orderbook Features ETL Pipeline")
        logger.info("=" * 70)
        
        # Get options from config
        options = self._get_processor_options()
        feature_config = self._create_feature_config(options)
        
        # Log configuration
        self._log_config(input_path, output_path, options, feature_config, trades_path)
        
        # Discover input files
        parquet_files = self._discover_files(input_path)
        if not parquet_files:
            logger.warning(f"No parquet files found in {input_path}")
            return {"success": False, "reason": "no_files"}
        
        logger.info(f"Found {len(parquet_files)} orderbook parquet files")
        
        # Discover trades files if provided
        trades_files = []
        if trades_path:
            trades_files = self._discover_files(trades_path)
            if trades_files:
                logger.info(f"Found {len(trades_files)} trades parquet files (TFI enabled)")
            else:
                logger.warning(f"No trades files found in {trades_path} - TFI will be zero")
        else:
            logger.info("No trades path provided - TFI will be zero")
        
        if dry_run:
            self._log_dry_run(parquet_files)
            return {"success": True, "dry_run": True, "files": len(parquet_files)}
        
        # Create transform config
        transform_config = self._create_transform_config(
            input_path=str(input_path),
            output_path=str(output_path),
            partition_cols=options.get("partition_cols", []),
            trades_path=str(trades_path) if trades_path and trades_files else None,
        )
        
        # Create transform and executor
        transform = OrderbookFeatureTransform(transform_config)
        executor = TransformExecutor(feature_config=feature_config)
        
        # Create context with filters
        context = TransformContext(
            execution_id=execution_id,
            execution_time=start_time,
            feature_config=feature_config,
            partition_values=self._build_partition_values(exchange, symbol),
        )
        
        # Load existing transform state if continuing from checkpoint
        if self._state.get("transform_state"):
            transform.set_state(self._state["transform_state"])
            logger.info("Restored transform state from checkpoint")
        
        # Initialize transform
        transform.initialize(context)
        
        # Load data
        logger.info("Loading bronze orderbook data...")
        bronze_lf = pl.scan_parquet([str(f) for f in parquet_files])
        
        # Apply filters
        if exchange:
            bronze_lf = bronze_lf.filter(pl.col("exchange") == exchange)
        if symbol:
            bronze_lf = bronze_lf.filter(pl.col("symbol") == symbol)
        
        # Load trades if available
        trades_lf = None
        if trades_files:
            logger.info("Loading bronze trades data...")
            trades_lf = pl.scan_parquet([str(f) for f in trades_files])
            if exchange:
                trades_lf = trades_lf.filter(pl.col("exchange") == exchange)
            if symbol:
                trades_lf = trades_lf.filter(pl.col("symbol") == symbol)
        
        # Execute transform
        logger.info("Executing transform...")
        inputs = {"orderbook": bronze_lf}
        if trades_lf is not None:
            inputs["trades"] = trades_lf
        outputs = transform.transform(inputs, context)
        
        # Collect and write outputs
        stats = self._write_outputs(outputs, output_path, options)
        
        # Finalize transform
        transform.finalize(context)
        
        # Save state for potential resume
        self._state["transform_state"] = transform.get_state()
        self._state["last_execution"] = {
            "id": execution_id,
            "time": start_time.isoformat(),
            "stats": stats,
        }
        self._save_state()
        
        # Final stats
        duration = (datetime.now() - start_time).total_seconds()
        stats["duration_seconds"] = duration
        stats["success"] = True
        
        logger.info("=" * 70)
        logger.info(f"[{execution_id}] COMPLETE in {duration:.2f}s")
        logger.info(f"  Rows processed: {stats.get('rows_processed', 0):,}")
        logger.info(f"  Features: {stats.get('feature_count', 0)}")
        if stats.get("bars"):
            for dur, count in stats["bars"].items():
                logger.info(f"  Bars {dur}s: {count:,}")
        logger.info("=" * 70)
        
        return stats
    
    def _discover_files(self, input_path: Path) -> List[Path]:
        """Discover parquet files in input path."""
        if input_path.is_file():
            return [input_path]
        return list(input_path.glob("**/*.parquet"))
    
    def _build_partition_values(
        self,
        exchange: Optional[str],
        symbol: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        """Build partition values dict for context."""
        if not exchange and not symbol:
            return None
        values = {}
        if exchange:
            values["exchange"] = exchange
        if symbol:
            values["symbol"] = symbol
        return values
    
    def _log_config(
        self,
        input_path: Path,
        output_path: Path,
        options: Dict[str, Any],
        feature_config: FeatureConfig,
        trades_path: Optional[Path] = None,
    ):
        """Log configuration details."""
        logger.info(f"Input:  {input_path}")
        logger.info(f"Output: {output_path}")
        if trades_path:
            logger.info(f"Trades: {trades_path}")
        else:
            logger.info(f"Trades: (not provided - TFI disabled)")
        logger.info(f"Configuration:")
        logger.info(f"  depth_levels: {feature_config.depth_levels}")
        logger.info(f"  rolling_windows: {feature_config.rolling_windows}")
        logger.info(f"  categories: {[c.value for c in feature_config.categories]}")
        logger.info(f"  vpin_bucket_size: {feature_config.vpin_bucket_size}")
        logger.info(f"  ofi_decay_alpha: {feature_config.ofi_decay_alpha}")
    
    def _log_dry_run(self, parquet_files: List[Path]):
        """Log dry run information."""
        logger.info("[DRY RUN] Would process files:")
        for f in parquet_files[:5]:
            logger.info(f"  - {f.name}")
        if len(parquet_files) > 5:
            logger.info(f"  ... and {len(parquet_files) - 5} more")
    
    def _write_outputs(
        self,
        outputs: Dict[str, pl.LazyFrame],
        output_path: Path,
        options: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Write transform outputs to disk using configurable tier names."""
        stats = {"rows_processed": 0, "feature_count": 0, "bars": {}}
        
        # Get tier names from config
        path_config = self.config.storage.paths
        features_tier = path_config.tier_features  # Default: "silver"
        aggregates_tier = path_config.tier_aggregates  # Default: "gold"
        
        # Write features (silver tier)
        if "silver_features" in outputs:
            silver_df = outputs["silver_features"].collect()
            stats["rows_processed"] = len(silver_df)
            stats["feature_count"] = len(silver_df.columns)
            
            features_path = output_path / features_tier / "orderbook"
            features_path.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            features_file = features_path / f"features_{timestamp}.parquet"
            silver_df.write_parquet(str(features_file), compression="zstd")
            logger.info(f"Wrote {features_tier}: {features_file} ({len(silver_df):,} rows)")
            
            # Generate bars from features data
            bar_durations = options.get("bar_durations", [60, 300, 900, 3600])
            bars_lfs = self._aggregate_bars(silver_df.lazy(), bar_durations)
            
            bars_path = output_path / aggregates_tier / "bars"
            bars_path.mkdir(parents=True, exist_ok=True)
            
            for duration, bar_lf in bars_lfs.items():
                bar_df = bar_lf.collect()
                stats["bars"][duration] = len(bar_df)
                
                bar_file = bars_path / f"bars_{duration}s_{timestamp}.parquet"
                bar_df.write_parquet(str(bar_file), compression="zstd")
                logger.info(f"Wrote bars {duration}s ({aggregates_tier}): {bar_file} ({len(bar_df):,} rows)")
        
        return stats
    
    def _aggregate_bars(
        self,
        df: pl.LazyFrame,
        durations: List[int],
    ) -> Dict[int, pl.LazyFrame]:
        """Aggregate features into time bars."""
        return aggregate_bars_vectorized(df, durations=durations)


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
        help="Output directory for silver/gold outputs",
    )
    parser.add_argument(
        "--trades",
        type=str,
        default=None,
        help="Path to bronze trades parquet files (required for TFI calculation)",
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
        "--config",
        type=str,
        help="Path to config.yaml",
    )
    parser.add_argument(
        "--state-path",
        type=str,
        help="Path to persist/load state for checkpoint/resume",
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
    
    # Create runner
    state_path = Path(args.state_path) if args.state_path else None
    runner = OrderbookETLRunner(config=config, state_path=state_path)
    
    # Run pipeline
    stats = runner.run(
        input_path=Path(args.input),
        output_path=Path(args.output),
        trades_path=Path(args.trades) if args.trades else None,
        exchange=args.exchange,
        symbol=args.symbol,
        dry_run=args.dry_run,
    )
    
    return 0 if stats.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
