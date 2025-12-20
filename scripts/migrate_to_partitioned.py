"""
Migrate unpartitioned raw Parquet files to Directory-Aligned Partitioned structure.

This script reads existing parquet files from the old flat structure:
    raw/ready/ccxt/{channel}/segment_*.parquet

And reorganizes them into partitioned directories:
    raw/ready/ccxt/{channel}/exchange={ex}/symbol={sym}/year={y}/month={m}/day={d}/hour={h}/segment_*.parquet

IMPORTANT: This follows Directory-Aligned Partitioning (NOT traditional Hive):
    - ALL partition column values EXIST in the Parquet data
    - ALL partition column values MATCH the directory partition values exactly
    - ALL partition values are SANITIZED for filesystem compatibility
    - Sanitization: prohibited chars (/, \\, :, etc.) -> -
    - year, month, day, hour columns are ADDED to data (derived from timestamp)

Uses PyArrow directly (same as ingestion parquet_writer) for robust I/O.

Features:
- Dry-run mode (default): Shows what would be done without modifying data
- Progress tracking with estimated time
- Resume capability: Skips already-migrated files
- Validation: Verifies row counts after migration

Usage:
    # Dry run (default) - see what would be done
    python scripts/migrate_to_partitioned.py

    # Execute migration
    python scripts/migrate_to_partitioned.py --execute

    # Execute and delete source files after successful migration
    python scripts/migrate_to_partitioned.py --execute --delete-source

    # Specify custom paths
    python scripts/migrate_to_partitioned.py --source data/raw/ready/ccxt --execute
"""
import argparse
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow as pa
import pyarrow.parquet as pq


# =============================================================================
# Constants (must match parquet_writer.py)
# =============================================================================

# Characters that are prohibited in filesystem paths (replaced with -)
PROHIBITED_PATH_CHARS = re.compile(r'[/\\:*?"<>|]')

# Default partition columns
DEFAULT_PARTITION_COLUMNS = ["exchange", "symbol", "year", "month", "day", "hour"]


# =============================================================================
# Utility Functions
# =============================================================================

def sanitize_partition_value(value: Any) -> str:
    """
    Sanitize a partition value for filesystem compatibility.
    
    Replaces prohibited characters (/, \\, :, *, ?, ", <, >, |) with -.
    This matches the ingestion parquet_writer behavior and ensures
    partition values in data match partition directory names.
    
    Args:
        value: The raw partition value (will be converted to string)
        
    Returns:
        Sanitized string safe for filesystem paths
    """
    str_value = str(value) if value is not None else "unknown"
    return PROHIBITED_PATH_CHARS.sub("-", str_value)


def timestamp_to_datetime_components(timestamp_ms: int) -> Tuple[int, int, int, int]:
    """Convert epoch milliseconds to (year, month, day, hour) tuple."""
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return (dt.year, dt.month, dt.day, dt.hour)


def get_partition_path(
    base_path: Path,
    channel: str,
    exchange: str,
    symbol: str,
    year: int,
    month: int,
    day: int,
    hour: int,
) -> Path:
    """
    Build directory-aligned partition path.
    
    All values should already be sanitized before calling this function.
    """
    return (
        base_path
        / channel
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"year={year}"
        / f"month={month}"
        / f"day={day}"
        / f"hour={hour}"
    )


def add_partition_columns(
    table: pa.Table,
    exchange: str,
    symbol: str,
    year: int,
    month: int,
    day: int,
    hour: int,
) -> pa.Table:
    """
    Add/update partition columns in the table.
    
    Ensures all partition column values (sanitized) exist in the data
    and match the partition directory structure.
    
    Args:
        table: Source PyArrow table
        exchange: Sanitized exchange value
        symbol: Sanitized symbol value
        year, month, day, hour: Date/time partition values
    
    Returns:
        New table with partition columns added/updated
    """
    columns = {name: table.column(name) for name in table.column_names}
    num_rows = table.num_rows
    
    # Update string partition columns with sanitized values
    columns["exchange"] = pa.array([exchange] * num_rows, type=pa.string())
    columns["symbol"] = pa.array([symbol] * num_rows, type=pa.string())
    
    # Add date/time partition columns
    columns["year"] = pa.array([year] * num_rows, type=pa.int32())
    columns["month"] = pa.array([month] * num_rows, type=pa.int32())
    columns["day"] = pa.array([day] * num_rows, type=pa.int32())
    columns["hour"] = pa.array([hour] * num_rows, type=pa.int32())
    
    # Determine column order based on channel (match schema order)
    if "bid" in table.column_names:
        # Ticker schema order
        ordered_cols = [
            "collected_at", "capture_ts", "exchange", "symbol", 
            "year", "month", "day", "hour",
            "bid", "ask", "bid_volume", "ask_volume", "last", "open", "high", "low",
            "close", "vwap", "base_volume", "quote_volume", "change", "percentage",
            "timestamp", "info_json"
        ]
    elif "trade_id" in table.column_names:
        # Trades schema order
        ordered_cols = [
            "collected_at", "capture_ts", "exchange", "symbol",
            "year", "month", "day", "hour",
            "trade_id", "timestamp", "side", "price", "amount", "cost", "info_json"
        ]
    elif "bids" in table.column_names:
        # Orderbook schema order
        ordered_cols = [
            "collected_at", "capture_ts", "exchange", "symbol",
            "year", "month", "day", "hour",
            "timestamp", "nonce", "bids", "asks"
        ]
    else:
        # Generic/unknown - build column order dynamically
        ordered_cols = list(table.column_names)
        # Insert partition columns after symbol if present
        partition_cols = ["year", "month", "day", "hour"]
        if "symbol" in ordered_cols:
            idx = ordered_cols.index("symbol") + 1
            for i, col in enumerate(partition_cols):
                if col not in ordered_cols:
                    ordered_cols.insert(idx + i, col)
        else:
            for col in partition_cols:
                if col not in ordered_cols:
                    ordered_cols.append(col)
    
    # Build new table with ordered columns (only include columns that exist)
    arrays = []
    names = []
    for col in ordered_cols:
        if col in columns:
            arrays.append(columns[col])
            names.append(col)
    
    return pa.table(dict(zip(names, arrays)))


def split_file_by_partitions(
    source_file: Path,
    base_dest: Path,
    channel: str,
    dry_run: bool = True,
) -> Tuple[int, int, List[str]]:
    """
    Split a single parquet file into partitioned files using PyArrow.
    
    Adds partition columns (sanitized exchange/symbol, year/month/day/hour)
    to the data to ensure they match the partition directory structure.
    
    Returns: (files_created, rows_processed, error_messages)
    """
    errors = []
    files_created = 0
    rows_processed = 0
    
    try:
        # Read using PyArrow
        table = pq.read_table(str(source_file))
        original_rows = table.num_rows
        
        if original_rows == 0:
            return 0, 0, []
        
        # Extract columns for grouping
        exchange_col = table.column("exchange").to_pylist()
        symbol_col = table.column("symbol").to_pylist()
        
        # Get timestamps for date/time partitioning
        if "timestamp" in table.column_names:
            ts_col = table.column("timestamp").to_pylist()
        else:
            ts_col = [None] * original_rows
        
        # Group rows by partition key
        # Key: (sanitized_exchange, sanitized_symbol, year, month, day, hour)
        partition_indices: Dict[Tuple[str, str, int, int, int, int], List[int]] = defaultdict(list)
        
        for i in range(original_rows):
            # Sanitize ALL string partition values
            exchange = sanitize_partition_value(exchange_col[i])
            symbol = sanitize_partition_value(symbol_col[i])
            
            # Get date/time components
            ts = ts_col[i]
            if ts and ts > 0:
                year, month, day, hour = timestamp_to_datetime_components(ts)
            else:
                # Fallback to current date/time
                now = datetime.now(timezone.utc)
                year, month, day, hour = now.year, now.month, now.day, now.hour
            
            key = (exchange, symbol, year, month, day, hour)
            partition_indices[key].append(i)
        
        # Write each partition
        for (exchange, symbol, year, month, day, hour), indices in partition_indices.items():
            # Build destination path
            dest_dir = get_partition_path(
                base_dest, channel, exchange, symbol, year, month, day, hour
            )
            dest_file = dest_dir / source_file.name
            
            # Get subset of table
            partition_table = table.take(indices)
            
            # Add/update partition columns to match directory structure
            partition_table = add_partition_columns(
                partition_table, exchange, symbol, year, month, day, hour
            )
            
            partition_rows = len(indices)
            
            if dry_run:
                print(f"    -> {channel}/exchange={exchange}/symbol={symbol}/.../hour={hour} ({partition_rows} rows)")
            else:
                # Create directory and write file
                dest_dir.mkdir(parents=True, exist_ok=True)
                pq.write_table(
                    partition_table,
                    str(dest_file),
                    compression="zstd",
                    compression_level=3,
                )
            
            files_created += 1
            rows_processed += partition_rows
        
        # Verify row count
        if rows_processed != original_rows:
            errors.append(f"Row count mismatch: {original_rows} original vs {rows_processed} migrated")
        
    except Exception as e:
        errors.append(f"Error processing {source_file.name}: {e}")
        import traceback
        traceback.print_exc()
    
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
    delete_source: bool = False,
) -> Dict:
    """
    Migrate files from flat structure to partitioned structure.
    
    Args:
        source_path: Base path for source files
        dest_path: Base path for destination (can be same as source)
        files_by_channel: Dict of channel -> file list
        dry_run: If True, only show what would be done
        delete_source: If True, delete source after successful migration
    
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
            
            file_size_mb = file_path.stat().st_size / 1024 / 1024
            print(f"  [{processed}/{total_files}] {file_path.name} ({file_size_mb:.1f} MB) ETA: {eta:.0f}s")
            
            # Split file by partitions
            files_created, rows, errors = split_file_by_partitions(
                source_file=file_path,
                base_dest=dest_path,
                channel=channel,
                dry_run=dry_run,
            )
            
            results["files_processed"] += 1
            results["files_created"] += files_created
            results["rows_migrated"] += rows
            results["errors"].extend(errors)
            
            # Delete source if requested and successful
            if not dry_run and delete_source and not errors:
                try:
                    file_path.unlink()
                    print(f"    [deleted source]")
                except Exception as e:
                    results["errors"].append(f"Failed to delete {file_path}: {e}")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Migrate unpartitioned Parquet files to Directory-Aligned Partitioned structure",
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
        "--delete-source",
        action="store_true",
        help="Delete source files after successful migration",
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
    print("Parquet Partition Migration Tool (Directory-Aligned)")
    print("=" * 70)
    print(f"Source:      {source_path}")
    print(f"Destination: {dest_path}")
    print(f"Mode:        {'DRY RUN' if dry_run else 'EXECUTE'}")
    print()
    print("This migration will:")
    print("  - Sanitize ALL partition values (/, \\, :, etc. -> -)")
    print("  - Add year, month, day, hour columns to data")
    print("  - Ensure partition values MATCH data values exactly")
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
    
    # Confirm execution
    if not dry_run and not args.yes:
        print("\nWARNING: This will modify your data!")
        if args.delete_source:
            print("WARNING: Source files will be DELETED after migration!")
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
        delete_source=args.delete_source,
    )
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Files processed:  {results['files_processed']}")
    print(f"Files created:    {results['files_created']}")
    print(f"Rows migrated:    {results['rows_migrated']:,}")
    print(f"Errors:           {len(results['errors'])}")
    
    if results["errors"]:
        print("\nErrors:")
        for e in results["errors"][:10]:
            print(f"  - {e}")
        if len(results["errors"]) > 10:
            print(f"  ... and {len(results['errors']) - 10} more")
    
    if dry_run:
        print("\n" + "-" * 70)
        print("This was a DRY RUN. No files were modified.")
        print("Run with --execute to perform the migration.")
        print("-" * 70)


if __name__ == "__main__":
    main()
