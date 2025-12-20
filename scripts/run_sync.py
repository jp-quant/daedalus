"""
Run storage sync job - compacts and uploads local data to S3.

This script is designed to run alongside ingestion and ETL:
1. Ingestion writes raw data to local disk (fast)
2. ETL processes data to local Parquet (fast)
3. This sync job compacts and uploads to S3 (durable, cost-effective)

Storage Configuration:
    Sync storage is configured separately from ETL storage in config.yaml:
    
    storage:
      sync:
        enabled: true
        source:
          backend: "local"
          base_dir: "./data"
        destination:
          backend: "s3"
          base_dir: "market-data-vault"
          s3:
            bucket: "market-data-vault"
            region: "us-east-1"
        compact_before_upload: true
        delete_after_transfer: true
        target_file_size_mb: 100

Usage:
    # One-time sync
    python scripts/run_sync.py --mode once
    
    # Continuous sync (every 5 minutes)
    python scripts/run_sync.py --mode continuous --interval 300
    
    # Dry run (preview without changes)
    python scripts/run_sync.py --mode once --dry-run
    
    # Sync specific source
    python scripts/run_sync.py --mode once --source ccxt
    
    # Download from S3 to local (reverse sync)
    python scripts/run_sync.py --mode once --direction download
"""
import asyncio
import logging
import signal
import sys
import os
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
os.chdir(Path(__file__).parent.parent)

from config import load_config
from storage.factory import create_sync_source_storage, create_sync_destination_storage
from storage.sync import StorageSync, StorageSyncJob

logger = logging.getLogger(__name__)


def get_default_sync_paths(source: Optional[str] = None, include_raw: bool = False) -> list:
    """
    Get default paths to sync based on source.
    
    The paths support Directory-Aligned Partitioning structure:
    raw/ready/{source}/{channel}/exchange={ex}/symbol={sym}/year={y}/month={m}/day={d}/hour={h}/
    
    Args:
        source: Optional source filter (ccxt, coinbase, etc.)
        include_raw: Include raw data paths (default: False, only processed)
    
    Returns:
        List of path configurations
    """
    all_paths = []
    
    # Raw data paths (Bronze layer - already compressed and sized correctly)
    # With Directory-Aligned Partitioning, data is organized:
    #   raw/ready/ccxt/ticker/exchange=binanceus/symbol=BTC-USD/year=2025/month=6/day=19/hour=14/
    # We sync at the channel level to capture all partitioned data
    if include_raw:
        all_paths.extend([
            # CCXT raw paths - NO compaction (already rotated at optimal size)
            # Pattern matches all Parquet files under the partitioned structure
            {"source": "raw/ready/ccxt/ticker/", "compact": False, "data_source": "ccxt", "layer": "raw"},
            {"source": "raw/ready/ccxt/trades/", "compact": False, "data_source": "ccxt", "layer": "raw"},
            {"source": "raw/ready/ccxt/orderbook/", "compact": False, "data_source": "ccxt", "layer": "raw"},
            
            # Coinbase raw paths (same partitioned structure)
            {"source": "raw/ready/coinbase/ticker/", "compact": False, "data_source": "coinbase", "layer": "raw"},
            {"source": "raw/ready/coinbase/market_trades/", "compact": False, "data_source": "coinbase", "layer": "raw"},
            {"source": "raw/ready/coinbase/level2/", "compact": False, "data_source": "coinbase", "layer": "raw"},
        ])
    
    # Processed data paths (Gold layer - may benefit from compaction)
    all_paths.extend([
        # CCXT processed paths
        {"source": "processed/ccxt/ticker/", "compact": True, "data_source": "ccxt", "layer": "processed"},
        {"source": "processed/ccxt/trades/", "compact": True, "data_source": "ccxt", "layer": "processed"},
        {"source": "processed/ccxt/orderbook/hf/", "compact": True, "data_source": "ccxt", "layer": "processed"},
        {"source": "processed/ccxt/orderbook/bars/", "compact": True, "data_source": "ccxt", "layer": "processed"},
        
        # Coinbase processed paths
        {"source": "processed/coinbase/ticker/", "compact": True, "data_source": "coinbase", "layer": "processed"},
        {"source": "processed/coinbase/market_trades/", "compact": True, "data_source": "coinbase", "layer": "processed"},
        {"source": "processed/coinbase/level2/", "compact": True, "data_source": "coinbase", "layer": "processed"},
    ])
    
    if source:
        return [p for p in all_paths if p.get("data_source") == source]
    
    return all_paths


def discover_raw_sync_paths(storage: "StorageBackend", base_path: str = "raw/ready") -> list:
    """
    Dynamically discover raw data paths that need syncing.
    
    This is useful for continuous sync on a Pi where we want to automatically
    discover new partitions as they are created by ingestion.
    
    Args:
        storage: Storage backend to scan
        base_path: Base path to scan for data sources
        
    Returns:
        List of path configurations for found channels
    """
    paths = []
    
    try:
        # List source directories (ccxt, coinbase, etc.)
        # list_dirs returns list of relative path strings
        sources = storage.list_dirs(base_path)
        
        for source_path in sources:
            # Normalize path separators for cross-platform compatibility
            source_path = source_path.replace("\\", "/")
            # Extract source name from path (e.g., "raw/ready/ccxt" -> "ccxt")
            source_name = Path(source_path).name
            
            # List channels under source (ticker, trades, orderbook, etc.)
            channels = storage.list_dirs(source_path)
            
            for channel_path in channels:
                # Normalize path separators
                channel_path = channel_path.replace("\\", "/")
                # Extract channel name from path
                channel_name = Path(channel_path).name
                
                # Add trailing slash to match partitioned structure
                sync_path = f"{channel_path}/"
                
                paths.append({
                    "source": sync_path,
                    "compact": False,  # Raw data should not be compacted
                    "data_source": source_name,
                    "layer": "raw",
                    "channel": channel_name,
                })
                
    except Exception as e:
        logger.warning(f"Error discovering paths: {e}")
    
    return paths


async def run_continuous(
    job: "StorageSyncJob",
    interval: int,
    shutdown_event: asyncio.Event,
    rediscover_paths: bool = False,
    storage: Optional["StorageBackend"] = None,
):
    """
    Run sync job continuously until shutdown.
    
    Args:
        job: The sync job to run
        interval: Seconds between sync runs
        shutdown_event: Event to signal shutdown
        rediscover_paths: If True, rediscover paths before each sync
        storage: Storage backend for path discovery (required if rediscover_paths)
    """
    logger.info(f"Starting continuous sync (interval={interval}s)")
    run_count = 0
    
    while not shutdown_event.is_set():
        try:
            run_count += 1
            logger.info(f"[Sync Run #{run_count}] Starting...")
            
            # Optionally rediscover paths for new partitions
            if rediscover_paths and storage:
                new_paths = discover_raw_sync_paths(storage)
                if new_paths:
                    job.update_paths(new_paths)
            
            # Run sync in executor
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, job.run)
            
            logger.info(f"[Sync Run #{run_count}] Complete")
            
            # Wait for interval or shutdown
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(),
                    timeout=interval
                )
            except asyncio.TimeoutError:
                pass  # Normal timeout, continue
            
        except Exception as e:
            logger.error(f"Error in sync job: {e}", exc_info=True)
            await asyncio.sleep(30)  # Brief pause on error


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Daedalus Storage Sync - Compact and upload to S3"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (default: config/config.yaml)"
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="once",
        choices=["once", "continuous"],
        help="Sync mode (default: once)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Seconds between syncs in continuous mode (default: 300)"
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["ccxt", "coinbase"],
        help="Only sync specific data source"
    )
    parser.add_argument(
        "--direction",
        type=str,
        default="upload",
        choices=["upload", "download"],
        help="Sync direction: upload (local→S3) or download (S3→local)"
    )
    parser.add_argument(
        "--no-compact",
        action="store_true",
        help="Skip compaction before upload"
    )
    parser.add_argument(
        "--no-delete",
        action="store_true",
        help="Don't delete source files after transfer"
    )
    parser.add_argument(
        "--target-size-mb",
        type=int,
        default=100,
        help="Target file size for compaction (default: 100MB)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Parallel transfer threads (default: 5)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without making changes"
    )
    parser.add_argument(
        "--paths",
        type=str,
        nargs="+",
        help="Custom paths to sync (overrides defaults)"
    )
    parser.add_argument(
        "--include-raw",
        action="store_true",
        help="Include raw data paths (Bronze layer) in sync"
    )
    
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format=config.log_format
    )
    
    logger.info("=" * 80)
    logger.info("Daedalus Storage Sync")
    logger.info("=" * 80)
    
    # Check if sync is configured/enabled
    sync_config = config.storage.sync
    if not sync_config.enabled:
        logger.warning("Sync is not enabled in config. Set storage.sync.enabled = true")
        logger.info("Using sync configuration from config.yaml anyway...")
    
    # Create storage backends from sync-specific config
    sync_source = create_sync_source_storage(config)
    sync_destination = create_sync_destination_storage(config)
    
    # Determine source and destination based on direction
    if args.direction == "upload":
        source_storage = sync_source
        dest_storage = sync_destination
        logger.info(f"Direction: UPLOAD ({sync_source.backend_type} → {sync_destination.backend_type})")
    else:
        source_storage = sync_destination
        dest_storage = sync_source
        logger.info(f"Direction: DOWNLOAD ({sync_destination.backend_type} → {sync_source.backend_type})")
    
    logger.info(f"Source:      {source_storage.backend_type} @ {source_storage.base_path}")
    logger.info(f"Destination: {dest_storage.backend_type} @ {dest_storage.base_path}")
    
    # Validate storage types make sense
    if source_storage.backend_type == dest_storage.backend_type:
        if source_storage.base_path == dest_storage.base_path:
            logger.error("Source and destination are identical! Check your sync configuration.")
            logger.error("Ensure storage.sync.source and storage.sync.destination are different.")
            sys.exit(1)
        else:
            logger.warning(f"Both source and destination are {source_storage.backend_type} (different paths)")
    
    # Validate storage types for upload with compaction
    # Use config defaults unless overridden by CLI args
    compact = sync_config.compact_before_upload if not args.no_compact else False
    delete_after = sync_config.delete_after_transfer if not args.no_delete else False
    target_size_mb = args.target_size_mb or sync_config.target_file_size_mb
    max_workers = args.max_workers or sync_config.max_workers
    interval = args.interval or sync_config.interval_seconds
    
    if args.direction == "upload" and compact:
        if source_storage.backend_type != "local":
            logger.warning("Compaction requires local source storage, disabling compaction")
            compact = False
    
    # Get paths to sync
    if args.paths:
        sync_paths = [{"source": p, "compact": compact} for p in args.paths]
    elif sync_config.paths:
        # Use paths from config
        sync_paths = [{"source": p, "compact": compact, "delete_after": delete_after} for p in sync_config.paths]
    else:
        # Use default paths
        sync_paths = get_default_sync_paths(args.source, include_raw=args.include_raw)
        for p in sync_paths:
            # Override compact setting if specified, otherwise use path default
            if args.no_compact:
                p["compact"] = False
            elif "compact" not in p:
                p["compact"] = compact
            p["delete_after"] = delete_after
    
    logger.info(f"Paths to sync: {len(sync_paths)}")
    for p in sync_paths:
        logger.info(f"  - {p['source']} (compact={p.get('compact', True)})")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")
    
    logger.info("=" * 80)
    
    # Create sync job
    job = StorageSyncJob(
        source=source_storage,
        destination=dest_storage,
        paths=sync_paths,
        default_compact=compact,
        default_delete_after=delete_after,
        target_file_size_mb=target_size_mb,
        max_workers=max_workers,
    )
    
    # Setup shutdown handling
    shutdown_event = asyncio.Event()
    
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run based on mode
    if args.mode == "once":
        # Single run
        results = job.run(dry_run=args.dry_run)
        
        # Print summary
        total_transferred = sum(
            r.get('sync_stats', {}).files_transferred if hasattr(r.get('sync_stats', {}), 'files_transferred') 
            else r.get('sync_stats', {}).get('files_transferred', 0) if isinstance(r.get('sync_stats'), dict)
            else 0
            for r in results if r.get('success')
        )
        total_failed = sum(1 for r in results if not r.get('success'))
        
        logger.info("\n" + "=" * 80)
        logger.info("SYNC SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Paths processed: {len(results)}")
        logger.info(f"Successful: {len(results) - total_failed}")
        logger.info(f"Failed: {total_failed}")
        logger.info(f"Files transferred: {total_transferred}")
        
        if total_failed > 0:
            logger.error("Failed paths:")
            for r in results:
                if not r.get('success'):
                    logger.error(f"  - {r['source_path']}: {r.get('error', 'Unknown error')}")
        
        sys.exit(0 if total_failed == 0 else 1)
    
    else:
        # Continuous mode - optionally rediscover paths for new partitions
        rediscover = args.include_raw  # Rediscover when syncing raw data
        await run_continuous(
            job=job,
            interval=interval,
            shutdown_event=shutdown_event,
            rediscover_paths=rediscover,
            storage=source_storage if rediscover else None,
        )
        logger.info("Sync job stopped")


if __name__ == "__main__":
    asyncio.run(main())
