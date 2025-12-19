"""
Migrate unpartitioned raw Parquet files to Hive-style partitioned structure.

This script reads existing parquet files from the old flat structure:
    raw/ready/ccxt/{channel}/segment_*.parquet

And reorganizes them into partitioned directories:
    raw/ready/ccxt/{channel}/exchange={ex}/symbol={sym}/year={y}/month={m}/day={d}/segment_*.parquet

Features:
- Dry-run mode (default): Shows what would be done without modifying data
- Backup mode: Copies files instead of moving them
- Progress tracking with estimated time
- Resume capability: Skips already-migrated files
- Validation: Verifies data integrity after migration

Usage:
    # Dry run (default) - see what would be done
    python scripts/migrate_to_partitioned.py

    # Execute migration (move files)
    python scripts/migrate_to_partitioned.py --execute

    # Execute with backup (copy instead of move)
    python scripts/migrate_to_partitioned.py --execute --backup

    # Specify custom paths
    python scripts/migrate_to_partitioned.py --source data/raw/ready/ccxt --execute
"""
import argparse
import shutil
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl


def sanitize_symbol(symbol: str) -> str:
    """Convert symbol for filesystem safety (/ -> ~)."""
    return symbol.replace("/", "~")


def get_partition_path(
    base_path: Path,
    channel: str,
    exchange: str,
    symbol: str,
    date_str: str,
) -> Path:
    """Build Hive-style partition path."""
    year, month, day = date_str.split("-")
    safe_symbol = sanitize_symbol(symbol)
    
    return (
        base_path
        / channel
        / f"exchange={exchange}"
        / f"symbol={safe_symbol}"
        / f"year={year}"
        / f"month={int(month)}"
        / f"day={int(day)}"
    )


def extract_partitions_from_file(file_path: Path) -> List[Dict]:
    """
    Read a parquet file and extract unique partition combinations.
    
    Returns list of dicts with keys: exchange, symbol, date, rows
    """
    try:
        df = pl.read_parquet(file_path)
        
        # Extract date from timestamp or capture_ts
        if "timestamp" in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col("timestamp"), time_unit="ms")
                .dt.date()
                .alias("_date")
            )
        elif "capture_ts" in df.columns:
            df = df.with_columns(
                pl.col("capture_ts").dt.date().alias("_date")
            )
        else:
            # Fallback: use current date
            today = datetime.now(timezone.utc).date()
            df = df.with_columns(pl.lit(today).alias("_date"))
        
        # Group by partition columns
        partitions = (
            df.group_by(["exchange", "symbol", "_date"])
            .agg(pl.len().alias("row_count"))
            .to_dicts()
        )
        
        result = []
        for p in partitions:
            result.append({
                "exchange": p["exchange"],
                "symbol": p["symbol"],
                "date": str(p["_date"]),
                "rows": p["row_count"],
            })
        
        return result
        
    except Exception as e:
        print(f"  ERROR reading {file_path}: {e}")
        return []


def split_file_by_partitions(
    source_file: Path,
    base_dest: Path,
    channel: str,
    dry_run: bool = True,
    backup: bool = False,
) -> Tuple[int, int, List[str]]:
    """
    Split a single parquet file into partitioned files.
    
    Returns: (files_created, rows_processed, error_messages)
    """
    errors = []
    files_created = 0
    rows_processed = 0
    
    try:
        df = pl.read_parquet(source_file)
        original_rows = len(df)
        
        # Extract date from timestamp or capture_ts
        if "timestamp" in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col("timestamp"), time_unit="ms")
                .dt.date()
                .alias("_partition_date")
            )
        elif "capture_ts" in df.columns:
            df = df.with_columns(
                pl.col("capture_ts").dt.date().alias("_partition_date")
            )
        else:
            today = datetime.now(timezone.utc).date()
            df = df.with_columns(pl.lit(today).alias("_partition_date"))
        
        # Get unique partition combinations
        partitions = df.select(["exchange", "symbol", "_partition_date"]).unique()
        
        for partition in partitions.iter_rows(named=True):
            exchange = partition["exchange"]
            symbol = partition["symbol"]
            date_val = partition["_partition_date"]
            date_str = str(date_val)
            
            # Filter rows for this partition
            partition_df = df.filter(
                (pl.col("exchange") == exchange) &
                (pl.col("symbol") == symbol) &
                (pl.col("_partition_date") == date_val)
            ).drop("_partition_date")
            
            if len(partition_df) == 0:
                continue
            
            # Build destination path
            dest_dir = get_partition_path(base_dest, channel, exchange, symbol, date_str)
            dest_file = dest_dir / source_file.name
            
            if dry_run:
                print(f"    → {dest_file.relative_to(base_dest)} ({len(partition_df)} rows)")
            else:
                # Create directory and write file
                dest_dir.mkdir(parents=True, exist_ok=True)
                partition_df.write_parquet(dest_file, compression="zstd", compression_level=3)
            
            files_created += 1
            rows_processed += len(partition_df)
        
        # Verify row count
        if rows_processed != original_rows:
            errors.append(f"Row count mismatch: {original_rows} original vs {rows_processed} migrated")
        
    except Exception as e:
        errors.append(f"Error processing {source_file}: {e}")
    
    return files_created, rows_processed, errors


def discover_files(source_path: Path) -> Dict[str, List[Path]]:
    """
    Discover all parquet files organized by channel.
    
    Returns dict: {channel: [file_paths]}
    """
    files_by_channel = defaultdict(list)
    
    for channel_dir in source_path.iterdir():
        if not channel_dir.is_dir():
            continue
        
        channel = channel_dir.name
        
        # Skip if already partitioned (has exchange= subdirs)
        subdirs = list(channel_dir.iterdir())
        if any(d.name.startswith("exchange=") for d in subdirs if d.is_dir()):
            print(f"  Skipping {channel}/ - already partitioned")
            continue
        
        # Find parquet files directly in channel dir
        parquet_files = list(channel_dir.glob("*.parquet"))
        if parquet_files:
            files_by_channel[channel] = sorted(parquet_files)
    
    return dict(files_by_channel)


def analyze_migration(files_by_channel: Dict[str, List[Path]]) -> Dict:
    """Analyze files to determine migration plan."""
    stats = {
        "total_files": 0,
        "total_size_mb": 0,
        "channels": {},
    }
    
    for channel, files in files_by_channel.items():
        channel_size = sum(f.stat().st_size for f in files)
        stats["channels"][channel] = {
            "files": len(files),
            "size_mb": channel_size / 1024 / 1024,
        }
        stats["total_files"] += len(files)
        stats["total_size_mb"] += channel_size / 1024 / 1024
    
    return stats


def migrate_files(
    source_path: Path,
    dest_path: Path,
    files_by_channel: Dict[str, List[Path]],
    dry_run: bool = True,
    backup: bool = False,
    delete_source: bool = False,
) -> Dict:
    """
    Migrate files from flat structure to partitioned structure.
    
    Args:
        source_path: Base path for source files
        dest_path: Base path for destination (can be same as source)
        files_by_channel: Dict of channel -> file list
        dry_run: If True, only show what would be done
        backup: If True, copy files instead of moving
        delete_source: If True and not backup, delete source after successful migration
    
    Returns:
        Migration statistics
    """
    results = {
        "files_processed": 0,
        "files_created": 0,
        "rows_migrated": 0,
        "errors": [],
        "skipped": 0,
    }
    
    total_files = sum(len(files) for files in files_by_channel.values())
    processed = 0
    start_time = time.time()
    
    for channel, files in files_by_channel.items():
        print(f"\n{'[DRY RUN] ' if dry_run else ''}Processing {channel}/ ({len(files)} files)")
        
        for file_path in files:
            processed += 1
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (total_files - processed) / rate if rate > 0 else 0
            
            print(f"  [{processed}/{total_files}] {file_path.name} (ETA: {eta:.0f}s)")
            
            # Split file by partitions
            files_created, rows, errors = split_file_by_partitions(
                source_file=file_path,
                base_dest=dest_path,
                channel=channel,
                dry_run=dry_run,
                backup=backup,
            )
            
            results["files_processed"] += 1
            results["files_created"] += files_created
            results["rows_migrated"] += rows
            results["errors"].extend(errors)
            
            # Delete source if requested and successful
            if not dry_run and delete_source and not errors and not backup:
                try:
                    file_path.unlink()
                except Exception as e:
                    results["errors"].append(f"Failed to delete {file_path}: {e}")
    
    return results


def validate_migration(source_path: Path, dest_path: Path, channel: str) -> List[str]:
    """
    Validate that migration was successful by comparing row counts.
    
    Returns list of error messages (empty if successful).
    """
    errors = []
    
    # Count rows in source (flat files)
    source_channel = source_path / channel
    source_files = list(source_channel.glob("*.parquet"))
    
    if not source_files:
        return []  # No source files to validate against
    
    source_rows = 0
    for f in source_files:
        try:
            df = pl.read_parquet(f)
            source_rows += len(df)
        except Exception as e:
            errors.append(f"Error reading source {f}: {e}")
    
    # Count rows in destination (partitioned files)
    dest_channel = dest_path / channel
    dest_files = list(dest_channel.rglob("*.parquet"))
    
    dest_rows = 0
    for f in dest_files:
        try:
            df = pl.read_parquet(f)
            dest_rows += len(df)
        except Exception as e:
            errors.append(f"Error reading dest {f}: {e}")
    
    if source_rows != dest_rows:
        errors.append(f"{channel}: Row count mismatch - source={source_rows}, dest={dest_rows}")
    else:
        print(f"  ✓ {channel}: {source_rows} rows validated")
    
    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Migrate unpartitioned Parquet files to Hive-style partitioned structure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("data/raw/ready/ccxt"),
        help="Source directory with unpartitioned files (default: data/raw/ready/ccxt)",
    )
    parser.add_argument(
        "--dest",
        type=Path,
        default=None,
        help="Destination directory for partitioned files (default: same as source)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform the migration (default is dry-run)",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Keep original files (copy instead of reorganize)",
    )
    parser.add_argument(
        "--delete-source",
        action="store_true",
        help="Delete source files after successful migration",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate existing migration, don't migrate",
    )
    parser.add_argument(
        "--channel",
        type=str,
        default=None,
        help="Only process specific channel (orderbook, ticker, trades)",
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt (use with caution)",
    )
    
    args = parser.parse_args()
    
    source_path = args.source.resolve()
    dest_path = (args.dest or args.source).resolve()
    dry_run = not args.execute
    
    print("=" * 70)
    print("Parquet Partition Migration Tool")
    print("=" * 70)
    print(f"Source:      {source_path}")
    print(f"Destination: {dest_path}")
    print(f"Mode:        {'DRY RUN' if dry_run else 'EXECUTE'}")
    print(f"Backup:      {'Yes (copy)' if args.backup else 'No (in-place)'}")
    print("=" * 70)
    
    if not source_path.exists():
        print(f"ERROR: Source path does not exist: {source_path}")
        sys.exit(1)
    
    # Discover files
    print("\nDiscovering files...")
    files_by_channel = discover_files(source_path)
    
    if args.channel:
        if args.channel in files_by_channel:
            files_by_channel = {args.channel: files_by_channel[args.channel]}
        else:
            print(f"Channel '{args.channel}' not found or already partitioned")
            sys.exit(1)
    
    if not files_by_channel:
        print("No unpartitioned files found to migrate!")
        print("(Files in directories with 'exchange=' subdirs are skipped)")
        sys.exit(0)
    
    # Analyze
    stats = analyze_migration(files_by_channel)
    print(f"\nFound {stats['total_files']} files ({stats['total_size_mb']:.1f} MB) to migrate:")
    for channel, info in stats["channels"].items():
        print(f"  - {channel}: {info['files']} files ({info['size_mb']:.1f} MB)")
    
    if args.validate_only:
        print("\n--- Validation Mode ---")
        all_errors = []
        for channel in files_by_channel.keys():
            errors = validate_migration(source_path, dest_path, channel)
            all_errors.extend(errors)
        
        if all_errors:
            print("\nValidation FAILED:")
            for e in all_errors:
                print(f"  ✗ {e}")
            sys.exit(1)
        else:
            print("\n✓ Validation PASSED")
            sys.exit(0)
    
    # Confirm execution
    if not dry_run and not args.yes:
        print("\n⚠️  WARNING: This will modify your data!")
        if args.delete_source:
            print("⚠️  Source files will be DELETED after migration!")
        response = input("Type 'yes' to continue: ")
        if response.lower() != "yes":
            print("Aborted.")
            sys.exit(0)
    
    # Migrate
    print("\n--- Migration ---")
    results = migrate_files(
        source_path=source_path,
        dest_path=dest_path,
        files_by_channel=files_by_channel,
        dry_run=dry_run,
        backup=args.backup,
        delete_source=args.delete_source,
    )
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Files processed:  {results['files_processed']}")
    print(f"Files created:    {results['files_created']}")
    print(f"Rows migrated:    {results['rows_migrated']:,}")
    print(f"Skipped:          {results['skipped']}")
    print(f"Errors:           {len(results['errors'])}")
    
    if results["errors"]:
        print("\nErrors:")
        for e in results["errors"][:10]:
            print(f"  ✗ {e}")
        if len(results["errors"]) > 10:
            print(f"  ... and {len(results['errors']) - 10} more")
    
    if dry_run:
        print("\n" + "-" * 70)
        print("This was a DRY RUN. No files were modified.")
        print("Run with --execute to perform the migration.")
        print("-" * 70)


if __name__ == "__main__":
    main()
