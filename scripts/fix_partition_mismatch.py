#!/usr/bin/env python
"""
Fix partition schema mismatch in existing Parquet datasets.

Problem: Directory names use 'symbol=BTC-USD' but data contains 'symbol=BTC/USD'
Solution: Rewrite Parquet files with normalized symbol values to match directory names

Usage:
    # Dry run (preview changes)
    python scripts/fix_partition_mismatch.py data/processed/ccxt/orderbook/bars/ --dry-run
    
    # Fix a specific dataset
    python scripts/fix_partition_mismatch.py data/processed/ccxt/orderbook/bars/
    
    # Fix all CCXT datasets
    python scripts/fix_partition_mismatch.py data/processed/ccxt/ticker/
    python scripts/fix_partition_mismatch.py data/processed/ccxt/trades/
    python scripts/fix_partition_mismatch.py data/processed/ccxt/orderbook/hf/
    python scripts/fix_partition_mismatch.py data/processed/ccxt/orderbook/bars/
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
import tempfile
import shutil

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

import polars as pl
import pyarrow.parquet as pq
import pyarrow as pa
from tqdm import tqdm

logger = logging.getLogger(__name__)


def normalize_value(value: str) -> str:
    """Normalize partition value (replace / with -)."""
    return str(value).replace("/", "-")


def find_parquet_files(dataset_dir: Path) -> List[Path]:
    """Find all Parquet files in dataset."""
    return list(dataset_dir.rglob("*.parquet"))


def extract_partition_from_path(file_path: Path, dataset_dir: Path) -> Dict[str, str]:
    """
    Extract partition values from Hive-style path.
    
    Example:
        Path: data/processed/ccxt/ticker/exchange=binanceus/symbol=BTC-USD/date=2025-12-12/part_xxx.parquet
        Returns: {'exchange': 'binanceus', 'symbol': 'BTC-USD', 'date': '2025-12-12'}
    """
    partitions = {}
    relative_path = file_path.relative_to(dataset_dir)
    
    for part in relative_path.parts[:-1]:  # Exclude filename
        if "=" in part:
            key, value = part.split("=", 1)
            partitions[key] = value
    
    return partitions


def check_file_needs_fix(file_path: Path, dataset_dir: Path) -> bool:
    """
    Check if a Parquet file has partition mismatch.
    
    Returns True if data values don't match directory partition values.
    """
    try:
        # Read partition info from path
        path_partitions = extract_partition_from_path(file_path, dataset_dir)
        
        if not path_partitions:
            # No partitions in path, skip
            return False
        
        # Read a small sample of the file
        table = pq.read_table(file_path, columns=list(path_partitions.keys()))
        df_pl = pl.from_arrow(table)  # type: ignore
        
        # Check if any partition column has mismatched values
        for col, expected_value in path_partitions.items():
            if col in df_pl.columns:
                unique_values = df_pl.get_column(col).unique().to_list()
                
                # Check if any value doesn't match expected
                for actual_value in unique_values:
                    if normalize_value(actual_value) != expected_value:
                        logger.debug(
                            f"Mismatch in {file_path.name}: "
                            f"{col} path={expected_value} data={actual_value}"
                        )
                        return True
        
        return False
    
    except Exception as e:
        logger.error(f"Error checking {file_path}: {e}")
        return False


def fix_file(file_path: Path, dataset_dir: Path, dry_run: bool = False) -> bool:
    """
    Fix partition mismatch in a single Parquet file.
    
    Reads the file, normalizes partition column values, and rewrites it.
    """
    try:
        # Read partition info from path
        path_partitions = extract_partition_from_path(file_path, dataset_dir)
        
        if not path_partitions:
            return True  # No partitions, nothing to fix
        
        # Read the Parquet file
        table = pq.read_table(file_path)
        df = pl.from_arrow(table)
        
        # Normalize partition columns
        modified = False
        for col, expected_value in path_partitions.items():
            if col in df.columns:
                # Apply normalization
                original_values = df[col].unique().to_list()
                df = df.with_columns(
                    pl.col(col).str.replace_all("/", "-").alias(col)
                )
                new_values = df[col].unique().to_list()
                
                if original_values != new_values:
                    modified = True
                    logger.info(f"  Normalized {col}: {original_values} -> {new_values}")
        
        if not modified:
            logger.debug(f"No changes needed for {file_path.name}")
            return True
        
        if dry_run:
            logger.info(f"[DRY RUN] Would rewrite {file_path}")
            return True
        
        # Write to temporary file first (atomic operation)
        temp_file = file_path.with_suffix('.parquet.tmp')
        
        try:
            # Convert back to Arrow and write
            fixed_table = df.to_arrow()
            pq.write_table(
                fixed_table,
                temp_file,
                compression='zstd'
            )
            
            # Atomic replace
            shutil.move(str(temp_file), str(file_path))
            logger.info(f"✓ Fixed {file_path.name}")
            return True
        
        except Exception as e:
            # Cleanup temp file on error
            if temp_file.exists():
                temp_file.unlink()
            raise e
    
    except Exception as e:
        logger.error(f"✗ Failed to fix {file_path}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Fix partition schema mismatch in Parquet datasets"
    )
    
    parser.add_argument(
        "dataset_dir",
        type=str,
        help="Path to dataset directory (e.g., data/processed/ccxt/ticker/)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without modifying files"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    dataset_dir = Path(args.dataset_dir).resolve()
    
    if not dataset_dir.exists():
        logger.error(f"Dataset directory not found: {dataset_dir}")
        sys.exit(1)
    
    logger.info("=" * 80)
    logger.info("Partition Mismatch Fix Tool")
    logger.info("=" * 80)
    logger.info(f"Dataset: {dataset_dir}")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE FIX'}")
    logger.info("=" * 80)
    
    # Find all Parquet files
    logger.info("\nScanning for Parquet files...")
    parquet_files = find_parquet_files(dataset_dir)
    logger.info(f"Found {len(parquet_files)} Parquet files")
    
    if not parquet_files:
        logger.warning("No Parquet files found")
        return
    
    # Check which files need fixing
    logger.info("\nChecking for partition mismatches...")
    files_to_fix = []
    
    for file_path in tqdm(parquet_files, desc="Checking files", unit="file"):
        if check_file_needs_fix(file_path, dataset_dir):
            files_to_fix.append(file_path)
    
    logger.info(f"\nFiles needing fix: {len(files_to_fix)} / {len(parquet_files)}")
    
    if not files_to_fix:
        logger.info("✓ All files are consistent! No fixes needed.")
        return
    
    if args.dry_run:
        logger.info("\n[DRY RUN] Files that would be fixed:")
        for file_path in files_to_fix:
            partitions = extract_partition_from_path(file_path, dataset_dir)
            logger.info(f"  - {file_path.relative_to(dataset_dir)} {partitions}")
        logger.info("\nRun without --dry-run to apply fixes")
        return
    
    # Fix files
    logger.info("\nApplying fixes...")
    success_count = 0
    failed_count = 0
    
    for file_path in tqdm(files_to_fix, desc="Fixing files", unit="file"):
        if fix_file(file_path, dataset_dir, dry_run=False):
            success_count += 1
        else:
            failed_count += 1
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total files: {len(parquet_files)}")
    logger.info(f"Files fixed: {success_count}")
    logger.info(f"Files failed: {failed_count}")
    logger.info(f"Files unchanged: {len(parquet_files) - len(files_to_fix)}")
    
    if failed_count > 0:
        logger.error(f"\n⚠️  {failed_count} files failed to fix. Check logs above.")
        sys.exit(1)
    else:
        logger.info("\n✓ All files fixed successfully!")


if __name__ == "__main__":
    main()
