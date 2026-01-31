#!/usr/bin/env python3
"""
Batch Orderbook Features ETL with Parallel Processing
=====================================================

A high-performance batch ETL script that processes orderbook data with trades
across multiple exchanges and symbols in parallel, while maintaining temporal
order within each symbol for stateful features (OFI, MLOFI, TFI, etc.).

Architecture:
=============

    ┌─────────────────────────────────────────────────────────────────────┐
    │                     MANIFEST BUILDER                                 │
    │  Parse Hive directories → {exchange: {symbol: [hour_partitions]}}   │
    │  Zero data reads - just filesystem listing                          │
    └─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                    PARALLEL JOB DISPATCHER                          │
    │  ProcessPoolExecutor (CPU-bound) → N workers per (exchange, symbol) │
    │  Each worker: sequential hourly processing with state persistence   │
    └─────────────────────────────────────────────────────────────────────┘
                    │           │           │           │
                    ▼           ▼           ▼           ▼
              ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
              │ Worker1 │ │ Worker2 │ │ Worker3 │ │ Worker4 │
              │ BTC-USD │ │ ETH-USD │ │ SOL-USD │ │ AAVE-USD│
              │ h0→h1→  │ │ h0→h1→  │ │ h0→h1→  │ │ h0→h1→  │
              │ h2→...  │ │ h2→...  │ │ h2→...  │ │ h2→...  │
              └─────────┘ └─────────┘ └─────────┘ └─────────┘

Constraints & Design Decisions:
================================
1. **Trades Data Ordering**: TFI, toxicity, and OFI features require trades 
   chronologically merged with orderbook snapshots. We cannot use lazy 
   scan_parquet + sink_parquet because stateful computation needs row-by-row
   processing in timestamp order.

2. **Memory Management**: Each worker loads ONE hour of data at a time.
   State is persisted to JSON between hours, so memory is bounded.

3. **Parallelism**: CPU-bound (feature computation), so we use ProcessPoolExecutor
   with workers = min(cpu_count, num_symbols). Each (exchange, symbol) is independent.

4. **Fault Tolerance**: State checkpointing per (exchange, symbol). If interrupted,
   resume from last checkpoint.

5. **Progress Tracking**: Rich progress bars with per-symbol and overall tracking.

Usage:
======
    # Process all data (auto-detect partitions)
    python scripts/etl/run_orderbook_features_batch.py

    # Process specific exchange
    python scripts/etl/run_orderbook_features_batch.py --exchange coinbaseadvanced

    # Limit parallel workers
    python scripts/etl/run_orderbook_features_batch.py --workers 4

    # Dry run (show what would be processed)
    python scripts/etl/run_orderbook_features_batch.py --dry-run

    # Debug mode (single-threaded, verbose)
    python scripts/etl/run_orderbook_features_batch.py --debug
"""

import argparse
import json
import logging
import multiprocessing as mp
import os
import sys
import traceback
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Imports after path setup
import polars as pl
from tqdm import tqdm

from config.config import load_config
from etl.core.base import TransformContext
from etl.core.config import FeatureConfig, FilterSpec
from etl.core.enums import FeatureCategory

logger = logging.getLogger(__name__)


# =============================================================================
# DATA STRUCTURES
# =============================================================================


class DatetimePartition(TypedDict):
    """Represents a single hour partition."""
    year: int
    month: int
    day: int
    hour: int


@dataclass
class JobConfig:
    """Configuration for a single (exchange, symbol) ETL job."""
    exchange: str
    symbol: str
    partitions: List[DatetimePartition]
    orderbook_base_path: Path
    trades_base_path: Path
    output_base_path: Path
    state_dir: Path
    dry_run: bool = False
    debug: bool = False


@dataclass
class JobResult:
    """Result from a single job execution."""
    exchange: str
    symbol: str
    processed: int = 0
    skipped: int = 0
    failed: int = 0
    error: Optional[str] = None
    duration_seconds: float = 0.0
    
    @property
    def success(self) -> bool:
        return self.error is None and self.failed == 0


@dataclass
class BatchStats:
    """Aggregate statistics for the entire batch."""
    total_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    total_partitions: int = 0
    processed_partitions: int = 0
    skipped_partitions: int = 0
    failed_partitions: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


# =============================================================================
# MANIFEST BUILDER (Zero data reads - filesystem only)
# =============================================================================


def build_partition_manifest(
    base_path: Path,
    exchange_filter: Optional[str] = None,
    symbol_filter: Optional[str] = None,
) -> Dict[str, Dict[str, List[DatetimePartition]]]:
    """
    Parse Hive-partitioned directory structure to build ETL manifest.
    
    EFFICIENT: Only reads directory names, never opens parquet files.
    
    Directory structure expected:
        base_path/exchange=X/symbol=Y/year=YYYY/month=MM/day=DD/hour=HH/*.parquet
    
    Args:
        base_path: Root path (e.g., data/raw/ready/ccxt/orderbook)
        exchange_filter: Optional filter for specific exchange
        symbol_filter: Optional filter for specific symbol
    
    Returns:
        Nested dict: {exchange: {symbol: [sorted_datetime_partitions]}}
    """
    base = Path(base_path)
    if not base.exists():
        logger.warning(f"Base path does not exist: {base}")
        return {}
    
    # Use defaultdict for efficient building
    result: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))
    
    # Single pass through all parquet files
    for parquet_file in base.rglob("*.parquet"):
        try:
            rel_path = parquet_file.relative_to(base)
            parts = {}
            for part in rel_path.parts[:-1]:  # Exclude filename
                if "=" in part:
                    k, v = part.split("=", 1)
                    parts[k] = v
            
            exchange = parts.get("exchange")
            symbol = parts.get("symbol")
            
            # Apply filters early
            if exchange_filter and exchange != exchange_filter:
                continue
            if symbol_filter and symbol != symbol_filter:
                continue
            
            year = int(parts.get("year", 0))
            month = int(parts.get("month", 0))
            day = int(parts.get("day", 0))
            hour = int(parts.get("hour", 0))
            
            if exchange and symbol and year:
                # Tuple for deduplication (multiple files per partition)
                result[exchange][symbol].add((year, month, day, hour))
                
        except (ValueError, KeyError) as e:
            logger.debug(f"Skipping malformed path: {parquet_file} - {e}")
            continue
    
    # Convert to final sorted structure
    manifest: Dict[str, Dict[str, List[DatetimePartition]]] = {}
    
    for exchange in sorted(result.keys()):
        manifest[exchange] = {}
        for symbol in sorted(result[exchange].keys()):
            sorted_tuples = sorted(result[exchange][symbol])
            manifest[exchange][symbol] = [
                DatetimePartition(year=y, month=m, day=d, hour=h)
                for y, m, d, h in sorted_tuples
            ]
    
    return manifest


def analyze_manifest(manifest: Dict[str, Dict[str, List[DatetimePartition]]]) -> Dict[str, Any]:
    """
    Quick analysis of manifest for logging/display.
    
    Returns summary stats without loading any data.
    """
    total_exchanges = len(manifest)
    total_symbols = sum(len(symbols) for symbols in manifest.values())
    total_partitions = sum(
        len(partitions)
        for symbols in manifest.values()
        for partitions in symbols.values()
    )
    
    # Estimate data range
    min_dt = None
    max_dt = None
    for symbols in manifest.values():
        for partitions in symbols.values():
            if partitions:
                first = partitions[0]
                last = partitions[-1]
                first_dt = datetime(first["year"], first["month"], first["day"], first["hour"])
                last_dt = datetime(last["year"], last["month"], last["day"], last["hour"])
                if min_dt is None or first_dt < min_dt:
                    min_dt = first_dt
                if max_dt is None or last_dt > max_dt:
                    max_dt = last_dt
    
    return {
        "exchanges": total_exchanges,
        "symbols": total_symbols,
        "partitions": total_partitions,
        "date_range": {
            "start": min_dt.isoformat() if min_dt else None,
            "end": max_dt.isoformat() if max_dt else None,
        },
        "estimated_hours": total_partitions,
    }


def filter_already_processed(
    manifest: Dict[str, Dict[str, List[DatetimePartition]]],
    output_path: Path,
) -> Dict[str, Dict[str, List[DatetimePartition]]]:
    """
    Filter out partitions that have already been processed.
    
    Checks for existing output files and removes them from manifest.
    This enables efficient resumption after interruption.
    
    Args:
        manifest: Full partition manifest
        output_path: Base output path to check for existing files
    
    Returns:
        Filtered manifest with only unprocessed partitions
    """
    filtered: Dict[str, Dict[str, List[DatetimePartition]]] = {}
    skipped_count = 0
    
    # Expected output structure: output_path/silver/orderbook/exchange=X/symbol=Y/year=.../...
    silver_path = output_path / "silver" / "orderbook"
    
    for exchange, symbols in manifest.items():
        filtered[exchange] = {}
        for symbol, partitions in symbols.items():
            remaining = []
            for p in partitions:
                # Check if output partition exists
                partition_path = (
                    silver_path
                    / f"exchange={exchange}"
                    / f"symbol={symbol}"
                    / f"year={p['year']}"
                    / f"month={p['month']}"
                    / f"day={p['day']}"
                    / f"hour={p['hour']}"
                )
                if partition_path.exists() and any(partition_path.glob("*.parquet")):
                    skipped_count += 1
                else:
                    remaining.append(p)
            
            if remaining:
                filtered[exchange][symbol] = remaining
        
        # Clean up empty exchanges
        if not filtered[exchange]:
            del filtered[exchange]
    
    if skipped_count > 0:
        logger.info(f"Skipping {skipped_count} already-processed partitions (--resume mode)")
    
    return filtered


# =============================================================================
# SINGLE JOB PROCESSOR (runs in worker process)
# =============================================================================


def process_single_job(job: JobConfig) -> JobResult:
    """
    Process all partitions for a single (exchange, symbol) sequentially.
    
    This function runs in a worker process. It:
    1. Loads/initializes state from checkpoint
    2. Iterates through partitions in chronological order
    3. Runs ETL for each partition
    4. Saves state after each partition
    5. Returns aggregate result
    
    Args:
        job: Job configuration with paths and partitions
    
    Returns:
        JobResult with statistics
    """
    start_time = datetime.now()
    result = JobResult(exchange=job.exchange, symbol=job.symbol)
    
    # Configure logging for worker process
    log_level = logging.DEBUG if job.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format=f'%(asctime)s - [{job.exchange}/{job.symbol}] - %(levelname)s - %(message)s'
    )
    worker_logger = logging.getLogger(f"{job.exchange}.{job.symbol}")
    
    try:
        # Import here to avoid issues with multiprocessing
        from scripts.etl.run_orderbook_features import run_orderbook_etl
        
        # State file path (unique per exchange/symbol)
        state_file = job.state_dir / f"{job.exchange}_{job.symbol.replace('/', '-')}_state.json"
        
        worker_logger.info(f"Starting job: {len(job.partitions)} partitions to process")
        worker_logger.info(f"State file: {state_file}")
        
        if job.dry_run:
            worker_logger.info("[DRY RUN] Would process partitions:")
            for p in job.partitions[:5]:
                worker_logger.info(f"  {p['year']}-{p['month']:02d}-{p['day']:02d} H{p['hour']:02d}")
            if len(job.partitions) > 5:
                worker_logger.info(f"  ... and {len(job.partitions) - 5} more")
            result.processed = len(job.partitions)
            return result
        
        # Process each partition sequentially (chronological order required)
        for partition in job.partitions:
            try:
                partition_result = run_orderbook_etl(
                    input_path=job.orderbook_base_path,
                    output_path=job.output_base_path,
                    trades_path=job.trades_base_path,
                    exchange=job.exchange,
                    symbol=job.symbol,
                    year=str(partition["year"]),
                    month=str(partition["month"]),
                    day=str(partition["day"]),
                    hour=str(partition["hour"]),
                    dry_run=False,
                    state_path=state_file,
                )
                
                if partition_result.get("success"):
                    result.processed += 1
                else:
                    # Check for "no data" scenarios
                    error_msg = str(partition_result.get("error", "")).lower()
                    if "no matching files" in error_msg or "empty" in error_msg or "0 rows" in error_msg:
                        result.skipped += 1
                        worker_logger.debug(f"No data for {partition}")
                    else:
                        result.failed += 1
                        worker_logger.warning(f"Failed partition {partition}: {partition_result}")
                        
            except Exception as e:
                error_str = str(e).lower()
                # Handle expected "no data" errors gracefully
                if "no matching files" in error_str or "glob" in error_str:
                    result.skipped += 1
                    worker_logger.debug(f"No data for {partition}")
                else:
                    result.failed += 1
                    worker_logger.error(f"Error processing {partition}: {e}")
                    if job.debug:
                        traceback.print_exc()
        
        worker_logger.info(
            f"Job complete: processed={result.processed}, "
            f"skipped={result.skipped}, failed={result.failed}"
        )
        
    except Exception as e:
        result.error = str(e)
        worker_logger.error(f"Job failed: {e}")
        if job.debug:
            traceback.print_exc()
    
    result.duration_seconds = (datetime.now() - start_time).total_seconds()
    return result


# =============================================================================
# BATCH ORCHESTRATOR
# =============================================================================


class BatchOrchestrator:
    """
    Orchestrates parallel ETL processing across multiple (exchange, symbol) pairs.
    
    Design:
    - Uses ProcessPoolExecutor for CPU-bound parallelism
    - Each worker processes one (exchange, symbol) fully before picking up another
    - Within a worker, partitions are processed sequentially for state consistency
    - Progress tracked with tqdm bars
    """
    
    def __init__(
        self,
        orderbook_path: Path,
        trades_path: Path,
        output_path: Path,
        state_dir: Path,
        max_workers: Optional[int] = None,
        dry_run: bool = False,
        debug: bool = False,
    ):
        """
        Initialize orchestrator.
        
        Args:
            orderbook_path: Base path to orderbook parquet data
            trades_path: Base path to trades parquet data
            output_path: Output directory for processed data
            state_dir: Directory for state checkpoints
            max_workers: Max parallel workers (default: CPU count)
            dry_run: If True, don't actually process
            debug: If True, single-threaded with verbose logging
        """
        self.orderbook_path = Path(orderbook_path)
        self.trades_path = Path(trades_path)
        self.output_path = Path(output_path)
        self.state_dir = Path(state_dir)
        self.max_workers = max_workers or mp.cpu_count()
        self.dry_run = dry_run
        self.debug = debug
        
        # Ensure directories exist
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.output_path.mkdir(parents=True, exist_ok=True)
    
    def run(
        self,
        exchange_filter: Optional[str] = None,
        symbol_filter: Optional[str] = None,
        resume: bool = False,
    ) -> BatchStats:
        """
        Run the batch ETL process.
        
        Args:
            exchange_filter: Optional filter for specific exchange
            symbol_filter: Optional filter for specific symbol
            resume: If True, skip already-processed partitions
        
        Returns:
            BatchStats with aggregate statistics
        """
        stats = BatchStats(start_time=datetime.now())
        
        # Step 1: Build manifest (zero data reads)
        logger.info("=" * 70)
        logger.info("STEP 1: Building partition manifest")
        logger.info("=" * 70)
        
        manifest = build_partition_manifest(
            self.orderbook_path,
            exchange_filter=exchange_filter,
            symbol_filter=symbol_filter,
        )
        
        if not manifest:
            logger.warning("No partitions found matching filters")
            return stats
        
        analysis = analyze_manifest(manifest)
        logger.info(f"Found: {analysis['exchanges']} exchanges, {analysis['symbols']} symbols")
        logger.info(f"Total partitions: {analysis['partitions']} hours")
        logger.info(f"Date range: {analysis['date_range']['start']} → {analysis['date_range']['end']}")
        
        # Step 1.5: Filter already processed if resume mode
        if resume:
            logger.info("")
            logger.info("Checking for already-processed partitions...")
            manifest = filter_already_processed(manifest, self.output_path)
            
            if not manifest:
                logger.info("All partitions already processed!")
                return stats
            
            analysis = analyze_manifest(manifest)
            logger.info(f"Remaining: {analysis['partitions']} partitions to process")
        
        # Step 2: Build job list
        logger.info("")
        logger.info("=" * 70)
        logger.info("STEP 2: Creating job queue")
        logger.info("=" * 70)
        
        jobs: List[JobConfig] = []
        for exchange, symbols in manifest.items():
            for symbol, partitions in symbols.items():
                jobs.append(JobConfig(
                    exchange=exchange,
                    symbol=symbol,
                    partitions=partitions,
                    orderbook_base_path=self.orderbook_path,
                    trades_base_path=self.trades_path,
                    output_base_path=self.output_path,
                    state_dir=self.state_dir,
                    dry_run=self.dry_run,
                    debug=self.debug,
                ))
                stats.total_partitions += len(partitions)
        
        stats.total_jobs = len(jobs)
        logger.info(f"Created {len(jobs)} jobs")
        
        # Step 3: Execute jobs
        logger.info("")
        logger.info("=" * 70)
        logger.info("STEP 3: Executing jobs")
        logger.info("=" * 70)
        
        if self.debug:
            # Single-threaded for debugging
            logger.info("DEBUG MODE: Single-threaded execution")
            results = []
            for job in tqdm(jobs, desc="Processing symbols"):
                results.append(process_single_job(job))
        else:
            # Parallel execution
            actual_workers = min(self.max_workers, len(jobs))
            logger.info(f"Using {actual_workers} parallel workers")
            
            results = []
            with ProcessPoolExecutor(max_workers=actual_workers) as executor:
                # Submit all jobs
                future_to_job = {
                    executor.submit(process_single_job, job): job
                    for job in jobs
                }
                
                # Process results with progress bar
                with tqdm(total=len(jobs), desc="Jobs completed") as pbar:
                    for future in as_completed(future_to_job):
                        job = future_to_job[future]
                        try:
                            result = future.result()
                            results.append(result)
                            
                            # Update progress description
                            status = "✓" if result.success else "✗"
                            pbar.set_postfix_str(
                                f"{status} {job.exchange}/{job.symbol}: "
                                f"p={result.processed} s={result.skipped} f={result.failed}"
                            )
                        except Exception as e:
                            logger.error(f"Job {job.exchange}/{job.symbol} crashed: {e}")
                            results.append(JobResult(
                                exchange=job.exchange,
                                symbol=job.symbol,
                                error=str(e),
                            ))
                        
                        pbar.update(1)
        
        # Step 4: Aggregate statistics
        for r in results:
            if r.success:
                stats.completed_jobs += 1
            else:
                stats.failed_jobs += 1
            stats.processed_partitions += r.processed
            stats.skipped_partitions += r.skipped
            stats.failed_partitions += r.failed
        
        stats.end_time = datetime.now()
        
        # Step 5: Report
        logger.info("")
        logger.info("=" * 70)
        logger.info("BATCH COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Duration: {stats.duration_seconds:.1f}s")
        logger.info(f"Jobs: {stats.completed_jobs}/{stats.total_jobs} completed")
        logger.info(f"Partitions: {stats.processed_partitions} processed, "
                   f"{stats.skipped_partitions} skipped, {stats.failed_partitions} failed")
        
        if stats.failed_jobs > 0:
            logger.warning(f"\nFailed jobs ({stats.failed_jobs}):")
            for r in results:
                if not r.success:
                    logger.warning(f"  {r.exchange}/{r.symbol}: {r.error}")
        
        return stats


# =============================================================================
# CLI ENTRY POINT
# =============================================================================


def main():
    """Main entry point for batch ETL."""
    parser = argparse.ArgumentParser(
        description="Batch Orderbook Features ETL with Parallel Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all data
  python scripts/etl/run_orderbook_features_batch.py
  
  # Process specific exchange
  python scripts/etl/run_orderbook_features_batch.py --exchange coinbaseadvanced
  
  # Limit workers
  python scripts/etl/run_orderbook_features_batch.py --workers 4
  
  # Dry run
  python scripts/etl/run_orderbook_features_batch.py --dry-run
        """,
    )
    
    # Paths
    parser.add_argument(
        "--orderbook",
        type=str,
        default="data/raw/ready/ccxt/orderbook",
        help="Input path for orderbook parquet data",
    )
    parser.add_argument(
        "--trades",
        type=str,
        default="data/raw/ready/ccxt/trades",
        help="Input path for trades parquet data",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed",
        help="Output directory for processed features",
    )
    parser.add_argument(
        "--state-dir",
        type=str,
        default="data/state",
        help="Directory for state checkpoints",
    )
    
    # Filters
    parser.add_argument(
        "--exchange",
        type=str,
        help="Filter by exchange (e.g., coinbaseadvanced)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="Filter by symbol (e.g., BTC-USD)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip already-processed partitions (check output directory)",
    )
    
    # Execution options
    parser.add_argument(
        "--workers",
        type=int,
        help=f"Max parallel workers (default: CPU count = {mp.cpu_count()})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without executing",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Single-threaded execution with verbose logging",
    )
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Print banner
    print()
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║       ORDERBOOK FEATURES BATCH ETL - Parallel Processing          ║")
    print("╠════════════════════════════════════════════════════════════════════╣")
    print(f"║  Orderbook: {args.orderbook:<52} ║")
    print(f"║  Trades:    {args.trades:<52} ║")
    print(f"║  Output:    {args.output:<52} ║")
    print(f"║  Workers:   {args.workers or mp.cpu_count():<52} ║")
    if args.exchange:
        print(f"║  Exchange:  {args.exchange:<52} ║")
    if args.symbol:
        print(f"║  Symbol:    {args.symbol:<52} ║")
    if args.dry_run:
        print("║  Mode:      DRY RUN                                                ║")
    if args.resume:
        print("║  Resume:    YES (skip processed)                                   ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    print()
    
    # Run orchestrator
    orchestrator = BatchOrchestrator(
        orderbook_path=Path(args.orderbook),
        trades_path=Path(args.trades),
        output_path=Path(args.output),
        state_dir=Path(args.state_dir),
        max_workers=args.workers,
        dry_run=args.dry_run,
        debug=args.debug,
    )
    
    try:
        stats = orchestrator.run(
            exchange_filter=args.exchange,
            symbol_filter=args.symbol,
            resume=args.resume,
        )
        
        # Exit code based on results
        if stats.failed_jobs > 0:
            sys.exit(1)
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        if args.debug:
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
