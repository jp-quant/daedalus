#!/usr/bin/env python
"""
Fix partition schema mismatch in existing Parquet datasets (Local + S3).

Problem: Directory names use 'symbol=BTC-USD' but data contains 'symbol=BTC/USD'
Solution: Rewrite Parquet files with normalized symbol values to match directory names

Features:
- Works with both local filesystem and S3 storage
- Optimized for Raspberry Pi (memory-efficient)
- Single-pass mode: Check and fix in one operation (saves memory)
- Simple pandas-based approach (handles all encoding issues)

Usage - Local Storage:
    # Dry run (preview changes)
    python scripts/fix_partition_mismatch.py data/processed/ccxt/ticker/ --dry-run
    
    # Fix with single-pass mode (faster, less memory)
    python scripts/fix_partition_mismatch.py data/processed/ccxt/ticker/ --single-pass
    
    # Fix specific local dataset
    python scripts/fix_partition_mismatch.py data/processed/ccxt/orderbook/bars/

Usage - S3 Storage:
    # Fix S3 dataset using config.yaml
    python scripts/fix_partition_mismatch.py processed/ccxt/ticker/ --storage=config
    
    # Dry run on S3
    python scripts/fix_partition_mismatch.py processed/ccxt/ticker/ --storage=config --dry-run
    
    # Single-pass mode on S3 (fastest)
    python scripts/fix_partition_mismatch.py processed/ccxt/ticker/ --storage=config --single-pass

Usage - Custom Config:
    # Use specific config file
    python scripts/fix_partition_mismatch.py processed/ccxt/ticker/ --storage=config --config=/path/to/config.yaml
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

import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
from tqdm import tqdm

# Import storage abstractions
from config import load_config
from storage.factory import create_etl_storage_output
from storage.base import StorageBackend, LocalStorage, S3Storage

logger = logging.getLogger(__name__)


def normalize_value(value: str) -> str:
    """Normalize partition value (replace / with -)."""
    return str(value).replace("/", "-")


def find_parquet_files(storage: StorageBackend, dataset_path: str) -> List[Dict[str, Any]]:
    """
    Find all Parquet files in dataset.
    Works with both local and S3 storage.
    """
    return storage.list_files(dataset_path, pattern="**/*.parquet", recursive=True)


def extract_partition_from_path(file_path: str, dataset_path: str) -> Dict[str, str]:
    """
    Extract partition values from Hive-style path.
    Works with both local paths and S3 keys.
    
    Example:
        Path: data/processed/ccxt/ticker/exchange=binanceus/symbol=BTC-USD/date=2025-12-12/part_xxx.parquet
        Returns: {'exchange': 'binanceus', 'symbol': 'BTC-USD', 'date': '2025-12-12'}
    """
    partitions = {}
    
    # Normalize paths (handle both Windows and Unix style)
    file_path = file_path.replace("\\", "/")
    dataset_path = dataset_path.replace("\\", "/").rstrip("/")
    
    # Get relative path
    if file_path.startswith(dataset_path):
        relative_path = file_path[len(dataset_path):].lstrip("/")
    else:
        relative_path = file_path
    
    # Parse partition key=value pairs
    parts = relative_path.split("/")
    for part in parts[:-1]:  # Exclude filename
        if "=" in part:
            key, value = part.split("=", 1)
            partitions[key] = value
    
    return partitions


def check_file_needs_fix(
    storage: StorageBackend,
    file_path: str,
    dataset_path: str
) -> bool:
    """
    Check if a Parquet file has partition mismatch.
    Works with both local and S3 storage.
    
    Returns True if data values don't match directory partition values.
    Uses memory-efficient PyArrow operations - only reads first row group.
    """
    try:
        # Read partition info from path
        path_partitions = extract_partition_from_path(file_path, dataset_path)
        
        if not path_partitions:
            # No partitions in path, skip
            return False
        
        # Read file through storage backend
        full_path = storage.get_full_path(file_path)
        
        # Memory optimization: Read only first row group to check
        # For S3, use storage_options; for local, use direct path
        if storage.backend_type == "s3":
            parquet_file = pq.ParquetFile(full_path, filesystem=storage.get_filesystem())
        else:
            parquet_file = pq.ParquetFile(full_path)
        
        table = parquet_file.read_row_group(0, columns=list(path_partitions.keys()))
        
        # Check if any partition column has mismatched values
        for col, expected_value in path_partitions.items():
            if col in table.column_names:
                column = table.column(col)
                
                # Decode dictionary-encoded columns if needed
                if pa.types.is_dictionary(column.type):
                    column = column.dictionary_decode()
                
                # Get unique values - convert to Python set for efficiency
                unique_values = set(column.to_pylist())
                
                # Check if any value doesn't match expected
                for actual_value in unique_values:
                    if normalize_value(actual_value) != expected_value:
                        logger.debug(
                            f"Mismatch in {Path(file_path).name}: "
                            f"{col} path={expected_value} data={actual_value}"
                        )
                        return True
        
        return False
    
    except Exception as e:
        logger.error(f"Error checking {file_path}: {e}")
        return False


def fix_file(
    storage: StorageBackend,
    file_path: str,
    dataset_path: str,
    dry_run: bool = False
) -> bool:
    """
    Fix partition mismatch in a single Parquet file.
    Works with both local and S3 storage.
    
    Simple approach for small files (KB to 0.5MB):
    - Read to pandas (handles all encoding automatically)
    - Normalize string values
    - Write back
    """
    try:
        # Read partition info from path
        path_partitions = extract_partition_from_path(file_path, dataset_path)
        
        if not path_partitions:
            return True  # No partitions, nothing to fix
        
        # Get full path for reading
        full_path = storage.get_full_path(file_path)
        
        # Simple approach: Read via pandas (handles all PyArrow quirks)
        import pandas as pd
        if storage.backend_type == "s3":
            df = pd.read_parquet(full_path, storage_options=storage.get_storage_options())
        else:
            df = pd.read_parquet(full_path)
        
        # Check if normalization needed
        modified = False
        
        for col, expected_value in path_partitions.items():
            if col in df.columns:
                # Get unique values before normalization
                unique_values = df[col].unique().tolist()
                
                # Check if normalization needed
                needs_fix = any(
                    normalize_value(str(val)) != str(val) 
                    for val in unique_values
                )
                
                if needs_fix:
                    # Apply normalization - simple string replacement
                    df[col] = df[col].astype(str).str.replace('/', '-', regex=False)
                    modified = True
                    
                    new_unique = df[col].unique().tolist()
                    logger.info(f"  Normalized {col}: {unique_values} -> {new_unique}")
        
        if not modified:
            logger.debug(f"No changes needed for {Path(file_path).name}")
            return True
        
        if dry_run:
            logger.info(f"[DRY RUN] Would rewrite {file_path}")
            return True
        
        # Write through storage backend
        if storage.backend_type == "local":
            # Local: Use temp file for atomic operation
            local_path = Path(full_path)
            temp_file = local_path.with_suffix('.parquet.tmp')
            
            try:
                df.to_parquet(
                    temp_file,
                    engine='pyarrow',
                    compression='zstd',
                    index=False
                )
                shutil.move(str(temp_file), str(local_path))
                logger.info(f"✓ Fixed {file_path}")
                return True
            except Exception as e:
                if temp_file.exists():
                    temp_file.unlink()
                raise e
        else:
            # S3: Write to temp file then upload
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                temp_path = tmp.name
            
            try:
                df.to_parquet(
                    temp_path,
                    engine='pyarrow',
                    compression='zstd',
                    index=False
                )
                
                # Upload to S3 (overwrites existing)
                storage.write_file(temp_path, file_path)
                logger.info(f"✓ Fixed {file_path}")
                return True
            finally:
                # Cleanup temp file
                if Path(temp_path).exists():
                    Path(temp_path).unlink()
    
    except Exception as e:
        logger.error(f"✗ Failed to fix {file_path}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Fix partition schema mismatch in Parquet datasets (local or S3)"
    )
    
    parser.add_argument(
        "dataset_path",
        type=str,
        help="Path to dataset directory (e.g., data/processed/ccxt/ticker/ or processed/ccxt/ticker/ for S3)"
    )
    
    parser.add_argument(
        "--storage",
        type=str,
        choices=["local", "s3", "config"],
        default="local",
        help="Storage backend: 'local' for filesystem, 's3' for S3, 'config' to use config.yaml"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Path to config file (used when --storage=config)"
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
    
    parser.add_argument(
        "--single-pass",
        action="store_true",
        help="Fix files in single pass without pre-checking (faster, uses less memory)"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Create storage backend
    if args.storage == "config":
        # Load from config file
        config = load_config(args.config)
        storage = create_etl_storage_output(config)
        logger.info(f"Using storage from config: {storage.backend_type}")
    elif args.storage == "s3":
        # Use S3 storage (requires config for credentials)
        config = load_config(args.config)
        storage = create_etl_storage_output(config)
        if storage.backend_type != "s3":
            logger.error("Config does not specify S3 storage for etl_storage_output")
            logger.error("Update config.yaml or use --storage=local")
            sys.exit(1)
    else:
        # Local storage
        # For local, use the dataset_path as base_dir if it's absolute, otherwise use current directory
        dataset_path_obj = Path(args.dataset_path)
        if dataset_path_obj.is_absolute():
            base_dir = str(dataset_path_obj.parent)
            dataset_relative = dataset_path_obj.name
        else:
            base_dir = "."
            dataset_relative = args.dataset_path
        storage = LocalStorage(base_path=base_dir)
    
    dataset_path = args.dataset_path
    
    # Validate dataset exists
    if storage.backend_type == "local":
        full_path = Path(storage.get_full_path(dataset_path))
        if not full_path.exists():
            logger.error(f"Dataset directory not found: {full_path}")
            sys.exit(1)
    
    logger.info("=" * 80)
    logger.info("Partition Mismatch Fix Tool (S3 + Local Support)")
    logger.info("=" * 80)
    logger.info(f"Storage: {storage.backend_type}")
    logger.info(f"Dataset: {dataset_path}")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE FIX'}")
    logger.info(f"Strategy: {'Single-pass (faster)' if args.single_pass else 'Two-pass (safer)'}")
    logger.info("=" * 80)
    
    # Find all Parquet files
    logger.info("\nScanning for Parquet files...")
    parquet_files = find_parquet_files(storage, dataset_path)
    logger.info(f"Found {len(parquet_files)} Parquet files")
    
    if not parquet_files:
        logger.warning("No Parquet files found")
        return
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    if args.single_pass:
        # OPTIMIZED: Single-pass mode - check and fix in one operation
        # Uses less memory and is faster (no double file reads)
        logger.info("\n[Single-pass mode] Processing all files...")
        
        for file_info in tqdm(parquet_files, desc="Processing files", unit="file"):
            file_path = file_info['path']
            result = fix_file(storage, file_path, dataset_path, dry_run=args.dry_run)
            if result:
                # Note: fix_file returns True even if no changes needed
                success_count += 1
            else:
                failed_count += 1
        
        # In single-pass mode, we don't know which files were actually modified
        logger.info(f"\nProcessed: {success_count} files")
        if failed_count > 0:
            logger.warning(f"Failed: {failed_count} files")
    
    else:
        # STANDARD: Two-pass mode - check first, then fix
        # Safer for dry-run and provides better reporting
        logger.info("\nChecking for partition mismatches...")
        files_to_fix = []
        
        for file_info in tqdm(parquet_files, desc="Checking files", unit="file"):
            file_path = file_info['path']
            if check_file_needs_fix(storage, file_path, dataset_path):
                files_to_fix.append(file_path)
        
        logger.info(f"\nFiles needing fix: {len(files_to_fix)} / {len(parquet_files)}")
        
        if not files_to_fix:
            logger.info("✓ All files are consistent! No fixes needed.")
            return
        
        if args.dry_run:
            logger.info("\n[DRY RUN] Files that would be fixed:")
            for file_path in files_to_fix:
                partitions = extract_partition_from_path(file_path, dataset_path)
                logger.info(f"  - {file_path} {partitions}")
            logger.info("\nRun without --dry-run to apply fixes")
            return
        
        # Fix files
        logger.info("\nApplying fixes...")
        
        for file_path in tqdm(files_to_fix, desc="Fixing files", unit="file"):
            if fix_file(storage, file_path, dataset_path, dry_run=False):
                success_count += 1
            else:
                failed_count += 1
        
        skipped_count = len(parquet_files) - len(files_to_fix)
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total files: {len(parquet_files)}")
    logger.info(f"Files processed: {success_count}")
    logger.info(f"Files failed: {failed_count}")
    if not args.single_pass:
        logger.info(f"Files unchanged: {skipped_count}")
    
    if failed_count > 0:
        logger.error(f"\n⚠️  {failed_count} files failed to fix. Check logs above.")
        sys.exit(1)
    else:
        logger.info("\n✓ All files processed successfully!")


if __name__ == "__main__":
    main()
