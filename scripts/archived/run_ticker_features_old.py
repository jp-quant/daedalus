#!/usr/bin/env python3
"""
Ticker Features ETL Script
==========================

Transform raw ticker data into comprehensive features using the new ETL framework.
Ticker data is fully vectorized (no cross-channel dependencies).

Features computed:
- Spread (absolute and relative)
- Mid price
- Volume imbalance
- Time features (hour, day_of_week, is_weekend)
- Rolling statistics (optional): mean, std, spread

Usage:
    # Basic usage
    python scripts/etl/run_ticker_features.py

    # Process specific input/output paths
    python scripts/etl/run_ticker_features.py --input data/raw/ready/ccxt/ticker --output data/processed

    # Process specific exchange/symbol partition
    python scripts/etl/run_ticker_features.py --exchange binanceus --symbol BTC/USDT

    # Enable rolling features
    python scripts/etl/run_ticker_features.py --rolling-windows 60,300,900

    # Dry run (preview only)
    python scripts/etl/run_ticker_features.py --dry-run
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
from etl.core.config import FeatureConfig, InputConfig, OutputConfig, TransformConfig
from etl.core.enums import CompressionCodec, DataFormat, FeatureCategory, WriteMode
from etl.core.executor import TransformExecutor
from etl.transforms.ticker import (
    TickerFeatureTransform,
    compute_ticker_features,
    compute_ticker_rolling_features,
    create_ticker_feature_config,
)

logger = logging.getLogger(__name__)


class TickerETLRunner:
    """
    Runner for ticker feature ETL pipeline.
    
    Ticker data is stateless and fully vectorized - much simpler
    than orderbook processing.
    """
    
    def __init__(
        self,
        config: Optional[DaedalusConfig] = None,
    ):
        """
        Initialize runner.
        
        Args:
            config: Daedalus configuration (loads default if None)
        """
        self.config = config or self._load_config()
    
    def _load_config(self) -> DaedalusConfig:
        """Load configuration with fallback to defaults."""
        try:
            return load_config()
        except Exception as e:
            logger.warning(f"Could not load config: {e}. Using defaults.")
            return DaedalusConfig()
    
    def _get_processor_options(self) -> Dict[str, Any]:
        """Get processor options from config."""
        if not self.config or not getattr(self.config, "etl", None):
            return {}
        
        channel_config = self.config.etl.channels.get("ticker")
        if not channel_config or not channel_config.processor_options:
            return {}
        
        return dict(channel_config.processor_options)
    
    def _create_feature_config(
        self,
        options: Dict[str, Any],
        rolling_windows: Optional[List[int]] = None,
    ) -> FeatureConfig:
        """Create FeatureConfig from options."""
        categories = {FeatureCategory.STRUCTURAL}
        
        # Add rolling if windows specified
        if rolling_windows:
            categories.add(FeatureCategory.ROLLING)
        
        return FeatureConfig(
            categories=categories,
            depth_levels=1,  # Ticker only has best bid/ask
            rolling_windows=rolling_windows or [],
        )
    
    def _create_transform_config(
        self,
        input_path: str,
        output_path: str,
        partition_cols: List[str],
    ) -> TransformConfig:
        """Create transform configuration."""
        return TransformConfig(
            name="ticker_features_batch",
            description="Extract features from raw ticker data",
            inputs={
                "ticker": InputConfig(
                    name="ticker",
                    path=input_path,
                    format=DataFormat.PARQUET,
                    partition_cols=partition_cols,
                ),
            },
            outputs={
                "silver_features": OutputConfig(
                    name="silver_features",
                    path=f"{output_path}/silver/ticker",
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
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        rolling_windows: Optional[List[int]] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Run the ticker feature ETL pipeline.
        
        Args:
            input_path: Path to bronze ticker parquet files
            output_path: Base output path for silver outputs
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            rolling_windows: Optional rolling window sizes in seconds
            dry_run: If True, don't write outputs
            
        Returns:
            Statistics dictionary
        """
        execution_id = str(uuid.uuid4())[:8]
        start_time = datetime.now()
        
        logger.info("=" * 70)
        logger.info(f"[{execution_id}] Ticker Features ETL Pipeline")
        logger.info("=" * 70)
        
        # Get options from config
        options = self._get_processor_options()
        feature_config = self._create_feature_config(options, rolling_windows)
        
        # Log configuration
        self._log_config(input_path, output_path, feature_config, rolling_windows)
        
        # Discover input files
        parquet_files = self._discover_files(input_path)
        if not parquet_files:
            logger.warning(f"No parquet files found in {input_path}")
            return {"success": False, "reason": "no_files"}
        
        logger.info(f"Found {len(parquet_files)} ticker parquet files")
        
        if dry_run:
            self._log_dry_run(parquet_files)
            return {"success": True, "dry_run": True, "files": len(parquet_files)}
        
        # Create transform config
        transform_config = self._create_transform_config(
            input_path=str(input_path),
            output_path=str(output_path),
            partition_cols=options.get("partition_cols", []),
        )
        
        # Create transform and executor
        transform = TickerFeatureTransform(transform_config)
        executor = TransformExecutor(feature_config=feature_config)
        
        # Create context with filters
        context = TransformContext(
            execution_id=execution_id,
            execution_time=start_time,
            feature_config=feature_config,
            partition_values=self._build_partition_values(exchange, symbol),
        )
        
        # Initialize transform
        transform.initialize(context)
        
        # Load data
        logger.info("Loading bronze ticker data...")
        bronze_lf = pl.scan_parquet([str(f) for f in parquet_files])
        
        # Apply filters
        if exchange:
            bronze_lf = bronze_lf.filter(pl.col("exchange") == exchange)
        if symbol:
            bronze_lf = bronze_lf.filter(pl.col("symbol") == symbol)
        
        # Execute transform
        logger.info("Executing transform...")
        outputs = transform.transform({"ticker": bronze_lf}, context)
        
        # Apply rolling features if requested
        if rolling_windows:
            silver_lf = outputs.get("silver") or outputs.get("silver_features")
            if silver_lf is not None:
                logger.info(f"Computing rolling features: {rolling_windows}")
                silver_lf = compute_ticker_rolling_features(
                    silver_lf,
                    windows=rolling_windows,
                )
                output_key = "silver" if "silver" in outputs else "silver_features"
                outputs[output_key] = silver_lf
        
        # Collect and write outputs
        stats = self._write_outputs(outputs, output_path)
        
        # Finalize transform
        transform.finalize(context)
        
        # Final stats
        duration = (datetime.now() - start_time).total_seconds()
        stats["duration_seconds"] = duration
        stats["success"] = True
        
        logger.info("=" * 70)
        logger.info(f"[{execution_id}] COMPLETE in {duration:.2f}s")
        logger.info(f"  Rows processed: {stats.get('rows_processed', 0):,}")
        logger.info(f"  Features: {stats.get('feature_count', 0)}")
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
        feature_config: FeatureConfig,
        rolling_windows: Optional[List[int]],
    ):
        """Log configuration details."""
        logger.info(f"Input:  {input_path}")
        logger.info(f"Output: {output_path}")
        logger.info(f"Configuration:")
        logger.info(f"  categories: {[c.value for c in feature_config.categories]}")
        if rolling_windows:
            logger.info(f"  rolling_windows: {rolling_windows}")
        else:
            logger.info(f"  rolling_windows: (disabled)")
    
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
    ) -> Dict[str, Any]:
        """Write transform outputs to disk using configurable tier names."""
        stats = {"rows_processed": 0, "feature_count": 0}
        
        # Get tier names from config
        path_config = self.config.storage.paths
        features_tier = path_config.tier_features  # Default: "silver"
        
        # Write features (silver tier)
        output_key = "silver" if "silver" in outputs else "silver_features"
        if output_key in outputs:
            silver_df = outputs[output_key].collect()
            stats["rows_processed"] = len(silver_df)
            stats["feature_count"] = len(silver_df.columns)
            
            features_path = output_path / features_tier / "ticker"
            features_path.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            features_file = features_path / f"features_{timestamp}.parquet"
            silver_df.write_parquet(str(features_file), compression="zstd")
            logger.info(f"Wrote {features_tier}: {features_file} ({len(silver_df):,} rows)")
        
        return stats


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
    
    # Create runner
    runner = TickerETLRunner(config=config)
    
    # Run pipeline
    stats = runner.run(
        input_path=Path(args.input),
        output_path=Path(args.output),
        exchange=args.exchange,
        symbol=args.symbol,
        rolling_windows=rolling_windows,
        dry_run=args.dry_run,
    )
    
    return 0 if stats.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
