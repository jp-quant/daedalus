"""
Convert legacy NDJSON raw data to Parquet format.

This script reads existing NDJSON segment files from the raw/ready directory
and converts them to Parquet format using the same schemas as StreamingParquetWriter.

Usage:
    # Convert all NDJSON files from a source
    python scripts/convert_ndjson_to_parquet.py --source ccxt
    
    # Convert specific directory
    python scripts/convert_ndjson_to_parquet.py --input F:/raw/ready/ccxt --output F:/raw/ready/ccxt_parquet
    
    # Dry run (show what would be converted)
    python scripts/convert_ndjson_to_parquet.py --source ccxt --dry-run
    
    # Delete NDJSON after successful conversion
    python scripts/convert_ndjson_to_parquet.py --source ccxt --delete-after
"""
import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import load_config

logger = logging.getLogger(__name__)


# =============================================================================
# SCHEMA DEFINITIONS (same as StreamingParquetWriter)
# =============================================================================

def _get_ticker_schema() -> pa.Schema:
    """Schema for ticker data - preserves all CCXT unified ticker fields."""
    return pa.schema([
        pa.field("collected_at", pa.int64()),
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("bid", pa.float64()),
        pa.field("ask", pa.float64()),
        pa.field("bid_volume", pa.float64()),
        pa.field("ask_volume", pa.float64()),
        pa.field("last", pa.float64()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("vwap", pa.float64()),
        pa.field("base_volume", pa.float64()),
        pa.field("quote_volume", pa.float64()),
        pa.field("change", pa.float64()),
        pa.field("percentage", pa.float64()),
        pa.field("timestamp", pa.int64()),
        pa.field("info_json", pa.string()),
    ])


def _get_trades_schema() -> pa.Schema:
    """Schema for trade data - one row per trade."""
    return pa.schema([
        pa.field("collected_at", pa.int64()),
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("trade_id", pa.string()),
        pa.field("timestamp", pa.int64()),
        pa.field("side", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("amount", pa.float64()),
        pa.field("cost", pa.float64()),
        pa.field("info_json", pa.string()),
    ])


def _get_orderbook_schema() -> pa.Schema:
    """Schema for orderbook data - stores bids/asks as nested lists."""
    level_type = pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("size", pa.float64()),
    ]))
    
    return pa.schema([
        pa.field("collected_at", pa.int64()),
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("timestamp", pa.int64()),
        pa.field("nonce", pa.int64()),
        pa.field("bids", level_type),
        pa.field("asks", level_type),
    ])


def _get_generic_schema() -> pa.Schema:
    """Fallback schema for unknown channel types."""
    return pa.schema([
        pa.field("collected_at", pa.int64()),
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("type", pa.string()),
        pa.field("method", pa.string()),
        pa.field("data_json", pa.string()),
    ])


CHANNEL_SCHEMAS = {
    "ticker": _get_ticker_schema(),
    "trades": _get_trades_schema(),
    "orderbook": _get_orderbook_schema(),
}


# =============================================================================
# RECORD CONVERSION (same logic as StreamingParquetWriter)
# =============================================================================

def parse_timestamp(ts_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp string to datetime."""
    if not ts_str:
        return None
    try:
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        return datetime.fromisoformat(ts_str)
    except Exception:
        return None


def convert_ticker_records(records: List[Dict]) -> pa.Table:
    """Convert ticker records to PyArrow table."""
    rows = []
    for r in records:
        data = r.get("data", {})
        rows.append({
            "collected_at": r.get("collected_at", 0),
            "capture_ts": parse_timestamp(r.get("capture_ts")),
            "exchange": r.get("exchange", ""),
            "symbol": r.get("symbol", ""),
            "bid": data.get("bid"),
            "ask": data.get("ask"),
            "bid_volume": data.get("bidVolume"),
            "ask_volume": data.get("askVolume"),
            "last": data.get("last"),
            "open": data.get("open"),
            "high": data.get("high"),
            "low": data.get("low"),
            "close": data.get("close"),
            "vwap": data.get("vwap"),
            "base_volume": data.get("baseVolume"),
            "quote_volume": data.get("quoteVolume"),
            "change": data.get("change"),
            "percentage": data.get("percentage"),
            "timestamp": data.get("timestamp", 0),
            "info_json": json.dumps(data.get("info", {}), separators=(",", ":")),
        })
    return pa.Table.from_pylist(rows, schema=CHANNEL_SCHEMAS["ticker"])


def convert_trades_records(records: List[Dict]) -> pa.Table:
    """Convert trades records to PyArrow table (one row per trade)."""
    rows = []
    for r in records:
        collected_at = r.get("collected_at", 0)
        capture_ts = parse_timestamp(r.get("capture_ts"))
        exchange = r.get("exchange", "")
        symbol = r.get("symbol", "")
        
        # trades can be a list
        trades = r.get("data", [])
        if isinstance(trades, dict):
            trades = [trades]
        
        for trade in trades:
            rows.append({
                "collected_at": collected_at,
                "capture_ts": capture_ts,
                "exchange": exchange,
                "symbol": symbol,
                "trade_id": str(trade.get("id", "")),
                "timestamp": trade.get("timestamp", 0),
                "side": trade.get("side", ""),
                "price": trade.get("price"),
                "amount": trade.get("amount"),
                "cost": trade.get("cost"),
                "info_json": json.dumps(trade.get("info", {}), separators=(",", ":")),
            })
    return pa.Table.from_pylist(rows, schema=CHANNEL_SCHEMAS["trades"])


def convert_orderbook_records(records: List[Dict]) -> pa.Table:
    """Convert orderbook records to PyArrow table with nested bids/asks."""
    rows = []
    for r in records:
        data = r.get("data", {})
        
        # Convert bids/asks to list of dicts for struct array
        bids = [{"price": b[0], "size": b[1]} for b in data.get("bids", [])]
        asks = [{"price": a[0], "size": a[1]} for a in data.get("asks", [])]
        
        rows.append({
            "collected_at": r.get("collected_at", 0),
            "capture_ts": parse_timestamp(r.get("capture_ts")),
            "exchange": r.get("exchange", ""),
            "symbol": r.get("symbol", ""),
            "timestamp": data.get("timestamp", 0),
            "nonce": data.get("nonce", 0),
            "bids": bids,
            "asks": asks,
        })
    return pa.Table.from_pylist(rows, schema=CHANNEL_SCHEMAS["orderbook"])


def convert_generic_records(records: List[Dict]) -> pa.Table:
    """Fallback conversion - stores full data as JSON."""
    rows = []
    for r in records:
        rows.append({
            "collected_at": r.get("collected_at", 0),
            "capture_ts": parse_timestamp(r.get("capture_ts")),
            "exchange": r.get("exchange", ""),
            "symbol": r.get("symbol", ""),
            "type": r.get("type", ""),
            "method": r.get("method", ""),
            "data_json": json.dumps(r.get("data", {}), separators=(",", ":")),
        })
    return pa.Table.from_pylist(rows, schema=_get_generic_schema())


def records_to_table(channel: str, records: List[Dict]) -> Optional[pa.Table]:
    """Convert raw records to PyArrow table for the given channel."""
    if not records:
        return None
    
    try:
        if channel == "ticker":
            return convert_ticker_records(records)
        elif channel == "trades":
            return convert_trades_records(records)
        elif channel == "orderbook":
            return convert_orderbook_records(records)
        else:
            return convert_generic_records(records)
    except Exception as e:
        logger.error(f"Error converting {channel}: {e}")
        return convert_generic_records(records)


# =============================================================================
# NDJSON READER
# =============================================================================

def read_ndjson_file(file_path: Path) -> Tuple[Dict[str, List[Dict]], int, int]:
    """
    Read NDJSON file and group records by channel type.
    
    Returns:
        Tuple of (channel_records dict, total_lines, error_count)
    """
    channel_records: Dict[str, List[Dict]] = defaultdict(list)
    total_lines = 0
    errors = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            total_lines += 1
            line = line.strip()
            if not line:
                continue
            
            try:
                record = json.loads(line)
                
                # Determine channel from record type
                msg_type = record.get("type", "unknown")
                
                # Map message types to channels
                if msg_type in ("ticker", "watchTicker"):
                    channel = "ticker"
                elif msg_type in ("trades", "watchTrades"):
                    channel = "trades"
                elif msg_type in ("orderbook", "watchOrderBook"):
                    channel = "orderbook"
                else:
                    print("Unknown message type:", msg_type, "defaulting to 'generic'")
                    channel = "generic"
                
                channel_records[channel].append(record)
                
            except json.JSONDecodeError as e:
                errors += 1
                if errors <= 5:
                    logger.warning(f"JSON parse error at line {total_lines}: {e}")
            except Exception as e:
                errors += 1
                if errors <= 5:
                    logger.warning(f"Error processing line {total_lines}: {e}")
    
    return dict(channel_records), total_lines, errors


# =============================================================================
# PARQUET WRITER
# =============================================================================

def write_channel_parquet(
    channel: str,
    records: List[Dict],
    output_dir: Path,
    segment_name: str,
    compression: str = "zstd",
    compression_level: int = 4,
) -> Tuple[int, int]:
    """
    Write records for a single channel to Parquet.
    
    Returns:
        Tuple of (records_written, bytes_written)
    """
    if not records:
        return 0, 0
    
    # Convert to table
    table = records_to_table(channel, records)
    if table is None or table.num_rows == 0:
        return 0, 0
    
    # Create channel subdirectory
    channel_dir = output_dir / channel
    channel_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate output filename (same base name, .parquet extension)
    output_file = channel_dir / segment_name.replace(".ndjson", ".parquet")
    
    # Write Parquet file
    pq.write_table(
        table,
        output_file,
        compression=compression,
        compression_level=compression_level,
    )
    
    bytes_written = output_file.stat().st_size
    
    return table.num_rows, bytes_written


# =============================================================================
# MAIN CONVERSION LOGIC
# =============================================================================

def convert_ndjson_file(
    ndjson_path: Path,
    output_dir: Path,
    compression: str = "zstd",
    compression_level: int = 4,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Convert a single NDJSON file to channel-separated Parquet files.
    
    Returns:
        Statistics dict with conversion results
    """
    stats = {
        "input_file": str(ndjson_path),
        "input_size_mb": ndjson_path.stat().st_size / 1024 / 1024,
        "total_lines": 0,
        "parse_errors": 0,
        "channels": {},
        "output_size_mb": 0,
        "compression_ratio": 0,
    }
    
    logger.info(f"Reading: {ndjson_path.name} ({stats['input_size_mb']:.2f} MB)")
    
    # Read and group records by channel
    channel_records, total_lines, errors = read_ndjson_file(ndjson_path)
    stats["total_lines"] = total_lines
    stats["parse_errors"] = errors
    
    if dry_run:
        for channel, records in channel_records.items():
            stats["channels"][channel] = {"records": len(records)}
        logger.info(f"  [DRY RUN] Would convert {total_lines} lines across {len(channel_records)} channels")
        return stats
    
    # Write each channel to Parquet
    total_output_bytes = 0
    segment_name = ndjson_path.name
    
    for channel, records in channel_records.items():
        records_written, bytes_written = write_channel_parquet(
            channel=channel,
            records=records,
            output_dir=output_dir,
            segment_name=segment_name,
            compression=compression,
            compression_level=compression_level,
        )
        
        stats["channels"][channel] = {
            "records": records_written,
            "bytes": bytes_written,
        }
        total_output_bytes += bytes_written
        
        if records_written > 0:
            logger.info(f"  {channel}: {records_written:,} records → {bytes_written / 1024:.1f} KB")
    
    stats["output_size_mb"] = total_output_bytes / 1024 / 1024
    if stats["input_size_mb"] > 0:
        stats["compression_ratio"] = stats["input_size_mb"] / stats["output_size_mb"]
    
    return stats


def find_ndjson_files(input_dir: Path, pattern: str = "*.ndjson") -> List[Path]:
    """Find all NDJSON files in directory (recursively)."""
    return sorted(input_dir.rglob(pattern))


def convert_directory(
    input_dir: Path,
    output_dir: Path,
    compression: str = "zstd",
    compression_level: int = 4,
    dry_run: bool = False,
    delete_after: bool = False,
) -> Dict[str, Any]:
    """
    Convert all NDJSON files in a directory to Parquet.
    
    Returns:
        Aggregate statistics
    """
    ndjson_files = find_ndjson_files(input_dir)
    
    if not ndjson_files:
        logger.warning(f"No NDJSON files found in {input_dir}")
        return {"files_found": 0}
    
    logger.info(f"Found {len(ndjson_files)} NDJSON file(s) in {input_dir}")
    
    aggregate_stats = {
        "files_processed": 0,
        "files_failed": 0,
        "total_input_mb": 0,
        "total_output_mb": 0,
        "total_records": 0,
        "total_errors": 0,
        "channels": defaultdict(int),
        "deleted_files": 0,
    }
    
    for ndjson_path in ndjson_files:
        try:
            stats = convert_ndjson_file(
                ndjson_path=ndjson_path,
                output_dir=output_dir,
                compression=compression,
                compression_level=compression_level,
                dry_run=dry_run,
            )
            
            aggregate_stats["files_processed"] += 1
            aggregate_stats["total_input_mb"] += stats["input_size_mb"]
            aggregate_stats["total_output_mb"] += stats["output_size_mb"]
            aggregate_stats["total_errors"] += stats["parse_errors"]
            
            for channel, channel_stats in stats["channels"].items():
                records = channel_stats.get("records", 0)
                aggregate_stats["total_records"] += records
                aggregate_stats["channels"][channel] += records
            
            # Delete original NDJSON if requested and conversion successful
            if delete_after and not dry_run and stats["output_size_mb"] > 0:
                ndjson_path.unlink()
                aggregate_stats["deleted_files"] += 1
                logger.debug(f"  Deleted: {ndjson_path.name}")
        
        except Exception as e:
            logger.error(f"Failed to convert {ndjson_path}: {e}")
            aggregate_stats["files_failed"] += 1
    
    return dict(aggregate_stats)


def print_summary(stats: Dict[str, Any]):
    """Print conversion summary."""
    print("\n" + "=" * 70)
    print("NDJSON → PARQUET CONVERSION SUMMARY")
    print("=" * 70)
    
    print(f"\nFiles processed: {stats.get('files_processed', 0)}")
    print(f"Files failed:    {stats.get('files_failed', 0)}")
    
    input_mb = stats.get('total_input_mb', 0)
    output_mb = stats.get('total_output_mb', 0)
    
    print(f"\nInput size:  {input_mb:.2f} MB")
    print(f"Output size: {output_mb:.2f} MB")
    
    if output_mb > 0:
        ratio = input_mb / output_mb
        savings = (1 - output_mb / input_mb) * 100 if input_mb > 0 else 0
        print(f"Compression: {ratio:.1f}x ({savings:.1f}% smaller)")
    
    print(f"\nTotal records: {stats.get('total_records', 0):,}")
    print(f"Parse errors:  {stats.get('total_errors', 0):,}")
    
    channels = stats.get('channels', {})
    if channels:
        print("\nRecords by channel:")
        for channel, count in sorted(channels.items()):
            print(f"  {channel}: {count:,}")
    
    if stats.get('deleted_files', 0) > 0:
        print(f"\nDeleted {stats['deleted_files']} original NDJSON file(s)")
    
    print("=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Convert legacy NDJSON raw data to Parquet format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Convert CCXT data using config paths
    python scripts/convert_ndjson_to_parquet.py --source ccxt
    
    # Convert specific directory
    python scripts/convert_ndjson_to_parquet.py --input F:/raw/ready/ccxt --output F:/raw/parquet/ccxt
    
    # Dry run to see what would be converted
    python scripts/convert_ndjson_to_parquet.py --source ccxt --dry-run
    
    # Delete NDJSON files after successful conversion
    python scripts/convert_ndjson_to_parquet.py --source ccxt --delete-after
        """,
    )
    
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (default: config/config.yaml)"
    )
    
    parser.add_argument(
        "--source",
        type=str,
        choices=["ccxt", "coinbase"],
        help="Source name to convert (uses config paths)"
    )
    
    parser.add_argument(
        "--input",
        type=str,
        help="Input directory containing NDJSON files (overrides --source)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        help="Output directory for Parquet files (overrides --source)"
    )
    
    parser.add_argument(
        "--compression",
        type=str,
        default="zstd",
        choices=["zstd", "snappy", "lz4", "gzip", "none"],
        help="Parquet compression (default: zstd)"
    )
    
    parser.add_argument(
        "--compression-level",
        type=int,
        default=4,
        help="Compression level for zstd (1-22, default: 4)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be converted without writing"
    )
    
    parser.add_argument(
        "--delete-after",
        action="store_true",
        help="Delete NDJSON files after successful conversion"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    # Determine input/output directories
    if args.input and args.output:
        input_dir = Path(args.input)
        output_dir = Path(args.output)
    elif args.source:
        # Load config to get paths
        config = load_config(config_path=args.config)
        
        # Build paths based on config
        base_dir = Path(config.storage.ingestion_storage.base_dir)
        raw_dir = config.storage.paths.raw_dir
        ready_subdir = config.storage.paths.ready_subdir
        
        input_dir = base_dir / raw_dir / ready_subdir / args.source
        
        # Output to same location but Parquet files will be in channel subdirs
        if args.output:
            output_dir = Path(args.output)
        else:
            output_dir = input_dir  # Write channel subdirs alongside NDJSON
        
        logger.info(f"Using config paths for source '{args.source}'")
    else:
        parser.error("Either --source or both --input and --output are required")
    
    # Validate input directory
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Input:  {input_dir}")
    logger.info(f"Output: {output_dir}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - no files will be written")
    
    if args.delete_after:
        logger.warning("DELETE AFTER MODE - original NDJSON files will be deleted!")
    
    # Run conversion
    stats = convert_directory(
        input_dir=input_dir,
        output_dir=output_dir,
        compression=args.compression,
        compression_level=args.compression_level,
        dry_run=args.dry_run,
        delete_after=args.delete_after,
    )
    
    # Print summary
    print_summary(stats)
    
    # Exit with error code if failures
    if stats.get("files_failed", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
