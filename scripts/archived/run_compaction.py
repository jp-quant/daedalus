#!/usr/bin/env python
"""
Script to run Parquet compaction on a dataset.

Usage:
    python scripts/run_compaction.py F:/processed/coinbase/level2
    python scripts/run_compaction.py F:/processed/coinbase/level2 --dry-run
    python scripts/run_compaction.py F:/processed/coinbase/level2 --target-file-count 4
"""
import argparse
import logging
import sys
from pathlib import Path

# Add project root to path so we can import from etl
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from etl.repartitioner import ParquetCompactor

def main():
    parser = argparse.ArgumentParser(
        description="Run Parquet compaction on a dataset to consolidate small files."
    )
    
    # Required argument
    parser.add_argument(
        "dataset_dir", 
        type=str, 
        help="Path to the dataset directory containing partitioned Parquet files"
    )
    
    # Optional arguments
    parser.add_argument(
        "--min-file-count", 
        type=int, 
        default=1,
        help="Minimum number of files in a partition to trigger compaction (default: 1)"
    )
    
    parser.add_argument(
        "--target-file-count", 
        type=int, 
        default=None,
        help="Target number of output files per partition. If not set, calculated from target-size-mb."
    )
    
    parser.add_argument(
        "--keep-source-files", 
        action="store_true",
        help="Keep original files after compaction (default: delete source files)"
    )
    
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Print plan without executing changes"
    )

    parser.add_argument(
        "--target-size-mb",
        type=int,
        default=100,
        help="Target file size in MB (used for reference) (default: 100)"
    )
    
    parser.add_argument(
        "--max-file-size-mb",
        type=int,
        default=None,
        help="Only compact files smaller than this size in MB (default: None = all files)"
    )

    parser.add_argument(
        "--sort-by",
        nargs="+",
        help="Columns to sort by (e.g. --sort-by timestamp product_id)"
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger("run_compaction")

    logger.info(f"Starting compaction for: {args.dataset_dir}")
    
    try:
        compactor = ParquetCompactor(
            dataset_dir=args.dataset_dir,
            target_file_size_mb=args.target_size_mb
        )
        
        # Determine delete_source_files based on flag
        # Default behavior is delete_source_files=True
        # If --keep-source-files is passed, args.keep_source_files is True, so delete is False
        delete_source = not args.keep_source_files

        stats = compactor.compact(
            min_file_count=args.min_file_count,
            target_file_count=args.target_file_count,
            max_file_size_mb=args.max_file_size_mb,
            sort_by=args.sort_by,
            delete_source_files=delete_source,
            dry_run=args.dry_run
        )
        
        if stats['partitions_compacted'] > 0:
            logger.info("Compaction completed successfully.")
        elif stats['partitions_scanned'] > 0:
            logger.info("Scanned partitions but none required compaction.")
        else:
            logger.warning("No partitions found or dataset directory is empty.")
            
    except Exception as e:
        logger.error(f"Compaction failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
