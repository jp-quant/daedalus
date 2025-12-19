#!/usr/bin/env python3
"""
ETL Watcher Script
==================

Continuously watches for new raw data segments and processes them through the ETL pipeline.
Supports stateful processing with checkpoint/resume for reliability.

This replaces the old run_etl_watcher.py with proper framework usage.

Features:
- Watches ready/ directory for new parquet segments
- Processes through OrderbookFeatureTransform (uses framework)
- Maintains state across batches for stateful features
- Checkpoint support for crash recovery
- Configurable poll interval

Usage:
    # Start watcher with defaults
    python scripts/etl/run_watcher.py

    # Custom poll interval (seconds)
    python scripts/etl/run_watcher.py --poll-interval 60

    # Watch specific source
    python scripts/etl/run_watcher.py --source ccxt

    # Enable state persistence
    python scripts/etl/run_watcher.py --state-path state/watcher_state.json
"""

import asyncio
import argparse
import json
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import uuid

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl

from config.config import load_config, DaedalusConfig
from etl.core.base import TransformContext
from etl.core.config import FeatureConfig, TransformConfig, InputConfig, OutputConfig
from etl.core.enums import (
    CompressionCodec,
    DataFormat,
    FeatureCategory,
    WriteMode,
)
from etl.transforms.orderbook import OrderbookFeatureTransform
from etl.transforms.ticker import TickerFeatureTransform
from etl.transforms.trades import TradesFeatureTransform
from etl.transforms.bars import aggregate_bars_vectorized

logger = logging.getLogger(__name__)


class ETLWatcher:
    """
    Watches for new data segments and processes them through ETL transforms.
    
    Maintains stateful processors for each symbol to ensure continuity
    of features like OFI, VPIN, Kyle's Lambda across segments.
    """
    
    def __init__(
        self,
        config: Optional[DaedalusConfig] = None,
        poll_interval: int = 30,
        state_path: Optional[Path] = None,
    ):
        """
        Initialize watcher.
        
        Args:
            config: Daedalus configuration
            poll_interval: Seconds between directory scans
            state_path: Path for persisting state
        """
        self.config = config or self._load_config()
        self.poll_interval = poll_interval
        self.state_path = state_path
        
        self._shutdown_event = asyncio.Event()
        self._processed_files: Set[str] = set()
        self._symbol_transforms: Dict[str, OrderbookFeatureTransform] = {}
        self._state: Dict[str, Any] = {}
        
        # Load existing state
        if state_path and state_path.exists():
            self._load_state()
    
    def _load_config(self) -> DaedalusConfig:
        """Load configuration."""
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
            self._processed_files = set(self._state.get("processed_files", []))
            logger.info(f"Loaded state: {len(self._processed_files)} files already processed")
        except Exception as e:
            logger.warning(f"Could not load state: {e}")
    
    def _save_state(self):
        """Save state to disk."""
        if not self.state_path:
            return
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self._state["processed_files"] = list(self._processed_files)
            self._state["last_update"] = datetime.now().isoformat()
            with open(self.state_path, 'w') as f:
                json.dump(self._state, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save state: {e}")
    
    def _get_processor_options(self, channel: str) -> Dict[str, Any]:
        """Get processor options from config."""
        if not self.config or not getattr(self.config, "etl", None):
            return {}
        channel_config = self.config.etl.channels.get(channel)
        if not channel_config or not channel_config.processor_options:
            return {}
        return dict(channel_config.processor_options)
    
    def _create_feature_config(self, options: Dict[str, Any]) -> FeatureConfig:
        """Create FeatureConfig from options."""
        categories = {FeatureCategory.STRUCTURAL, FeatureCategory.DYNAMIC}
        if options.get("enable_stateful", True):
            categories.add(FeatureCategory.ROLLING)
        if options.get("enable_vpin", True):
            categories.add(FeatureCategory.ADVANCED)
        
        return FeatureConfig(
            categories=categories,
            depth_levels=options.get("max_levels", 20),
            rolling_windows=options.get("horizons", [5, 15, 60, 300, 900]),
            vpin_bucket_size=options.get("vpin_bucket_volume", 1.0),
            ofi_decay_alpha=options.get("ofi_decay_alpha", 0.5),
        )
    
    def _get_or_create_transform(
        self,
        symbol_key: str,
        transform_config: TransformConfig,
        feature_config: FeatureConfig,
    ) -> OrderbookFeatureTransform:
        """Get existing transform for symbol or create new one."""
        if symbol_key not in self._symbol_transforms:
            transform = OrderbookFeatureTransform(transform_config)
            context = TransformContext(
                execution_id=str(uuid.uuid4())[:8],
                feature_config=feature_config,
            )
            transform.initialize(context)
            self._symbol_transforms[symbol_key] = transform
            logger.debug(f"Created new transform for {symbol_key}")
        return self._symbol_transforms[symbol_key]
    
    async def _discover_new_files(
        self,
        base_path: Path,
        channel: str,
    ) -> List[Path]:
        """Discover new unprocessed parquet files."""
        channel_path = base_path / channel
        if not channel_path.exists():
            return []
        
        all_files = list(channel_path.glob("*.parquet"))
        new_files = [f for f in all_files if str(f) not in self._processed_files]
        return sorted(new_files)  # Process in order
    
    async def _process_orderbook_file(
        self,
        file_path: Path,
        output_base: Path,
        options: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Process a single orderbook parquet file."""
        logger.info(f"Processing: {file_path.name}")
        
        feature_config = self._create_feature_config(options)
        
        # Load data
        df = pl.scan_parquet(str(file_path))
        
        # Get unique exchange/symbol combinations
        collected = df.select(["exchange", "symbol"]).collect()
        unique_pairs = collected.unique().to_dicts()
        
        stats = {"rows": 0, "symbols": 0}
        all_results = []
        
        for pair in unique_pairs:
            exchange = pair["exchange"]
            symbol = pair["symbol"]
            symbol_key = f"{exchange}:{symbol}"
            
            # Filter for this symbol
            symbol_df = df.filter(
                (pl.col("exchange") == exchange) & (pl.col("symbol") == symbol)
            )
            
            # Get or create transform
            transform_config = TransformConfig(
                name=f"orderbook_{symbol_key}",
                inputs={"bronze_orderbook": InputConfig(name="bronze_orderbook", path=str(file_path))},
                outputs={"silver_features": OutputConfig(name="silver_features", path=str(output_base / "silver" / "orderbook"))},
            )
            transform = self._get_or_create_transform(symbol_key, transform_config, feature_config)
            
            # Create context
            context = TransformContext(
                execution_id=str(uuid.uuid4())[:8],
                feature_config=feature_config,
                partition_values={"exchange": exchange, "symbol": symbol},
            )
            
            # Transform
            inputs = {"bronze_orderbook": symbol_df}
            outputs = transform.transform(inputs, context)
            
            if "silver_features" in outputs:
                result_df = outputs["silver_features"].collect()
                all_results.append(result_df)
                stats["rows"] += len(result_df)
                stats["symbols"] += 1
        
        # Write combined output
        if all_results:
            combined = pl.concat(all_results)
            output_path = output_base / "silver" / "orderbook"
            output_path.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = output_path / f"features_{file_path.stem}_{timestamp}.parquet"
            combined.write_parquet(str(output_file), compression="zstd")
            logger.info(f"Wrote: {output_file} ({len(combined)} rows)")
            
            # Also aggregate bars
            bar_durations = options.get("bar_durations", [60, 300, 900])
            bars = aggregate_bars_vectorized(combined.lazy(), durations=bar_durations)
            
            bars_path = output_base / "gold" / "bars"
            bars_path.mkdir(parents=True, exist_ok=True)
            
            for duration, bar_lf in bars.items():
                bar_df = bar_lf.collect()
                bar_file = bars_path / f"bars_{duration}s_{file_path.stem}_{timestamp}.parquet"
                bar_df.write_parquet(str(bar_file), compression="zstd")
                stats[f"bars_{duration}s"] = len(bar_df)
        
        return stats
    
    async def _process_cycle(
        self,
        source: str,
        input_base: Path,
        output_base: Path,
    ):
        """Run one processing cycle."""
        channels = ["orderbook", "ticker", "trades"]
        
        for channel in channels:
            new_files = await self._discover_new_files(input_base / source, channel)
            
            if not new_files:
                continue
            
            logger.info(f"Found {len(new_files)} new {channel} files")
            options = self._get_processor_options(channel)
            
            for file_path in new_files:
                try:
                    if channel == "orderbook":
                        stats = await self._process_orderbook_file(
                            file_path, output_base, options
                        )
                        logger.info(f"Processed {file_path.name}: {stats}")
                    # ticker and trades use simpler vectorized processing
                    # (can add similar methods if needed)
                    
                    # Mark as processed
                    self._processed_files.add(str(file_path))
                    
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
                    continue
        
        # Save state after each cycle
        self._save_state()
    
    async def run(
        self,
        source: str = "ccxt",
        input_base: Optional[Path] = None,
        output_base: Optional[Path] = None,
    ):
        """Main watcher loop."""
        input_base = input_base or Path("data/raw/ready")
        output_base = output_base or Path("data/processed")
        
        logger.info("=" * 70)
        logger.info("ETL Watcher Starting")
        logger.info("=" * 70)
        logger.info(f"Source: {source}")
        logger.info(f"Input:  {input_base}")
        logger.info(f"Output: {output_base}")
        logger.info(f"Poll interval: {self.poll_interval}s")
        logger.info("=" * 70)
        
        while not self._shutdown_event.is_set():
            try:
                await self._process_cycle(source, input_base, output_base)
            except Exception as e:
                logger.error(f"Error in process cycle: {e}")
            
            # Wait for next cycle or shutdown
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.poll_interval
                )
            except asyncio.TimeoutError:
                continue
        
        logger.info("ETL Watcher stopped")
    
    def request_shutdown(self):
        """Request graceful shutdown."""
        logger.info("Shutdown requested")
        self._shutdown_event.set()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="ETL Watcher - Continuously process new data segments"
    )
    parser.add_argument(
        "--source",
        type=str,
        default="ccxt",
        help="Data source to watch (default: ccxt)",
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/raw/ready",
        help="Base input directory",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed",
        help="Base output directory",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between directory scans (default: 30)",
    )
    parser.add_argument(
        "--state-path",
        type=str,
        help="Path to persist watcher state",
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
            logger.warning(f"Could not load config: {e}")
    
    # Create watcher
    state_path = Path(args.state_path) if args.state_path else None
    watcher = ETLWatcher(
        config=config,
        poll_interval=args.poll_interval,
        state_path=state_path,
    )
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        watcher.request_shutdown()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run
    asyncio.run(watcher.run(
        source=args.source,
        input_base=Path(args.input),
        output_base=Path(args.output),
    ))
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
