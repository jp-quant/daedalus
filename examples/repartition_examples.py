"""
Example usage of Repartitioner and ParquetCompactor.

Demonstrates:
1. Three repartitioning methods: streaming, file_by_file, batched
2. Changing partition schema (repartitioning)
3. File consolidation (compaction)
4. Best practices and common patterns
"""
import logging
from pathlib import Path
from etl.repartitioner import Repartitioner, ParquetCompactor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# =============================================================================
# Example 1: STREAMING Repartitioning (Most Sophisticated)
# =============================================================================

def example_repartition_streaming():
    """
    STREAMING method: Uses sink_parquet for zero-copy streaming.
    
    Characteristics:
    - Memory usage: Minimal (~100MB streaming buffer)
    - Best for: Very large datasets (TB+)
    - Speed: Fast (Polars-optimized streaming)
    - Default method (recommended for production)
    
    Use case: Change from coarse to fine-grained partitioning on large dataset.
    """
    logger.info("=" * 80)
    logger.info("Example 1: STREAMING Repartitioning (Most Sophisticated)")
    logger.info("=" * 80)
    
    repartitioner = Repartitioner(
        source_dir="F:/processed/coinbase/level2",
        target_dir="F:/processed/coinbase/level2_streaming",
        new_partition_cols=['product_id', 'year', 'month', 'day', 'hour'],
    )
    
    # Estimate before executing
    estimates = repartitioner.estimate_size()
    logger.info(f"Estimated partitions: {estimates['num_partitions']}")
    logger.info(f"Estimated size: {estimates['estimated_size_mb']:.1f} MB")
    
    # Execute with streaming method (default)
    stats = repartitioner.repartition(
        method="streaming",   # Zero-copy streaming with sink_parquet
        delete_source=False,  # Keep source for safety
        validate=True,        # Verify row counts match
        dry_run=False,        # Set to True to preview only
    )
    
    logger.info(f"\n✓ Streaming repartition complete!")
    logger.info(f"  New location: F:/processed/coinbase/level2_streaming")
    logger.info(f"  Records: {stats['records_written']:,}")
    logger.info(f"  Files: {stats['files_written']}")
    logger.info(f"  Partitions: {stats['partitions_created']}")
    logger.info(f"  Memory usage: Minimal (streaming buffer only)")


# =============================================================================
# Example 2: FILE-BY-FILE Repartitioning
# =============================================================================

def example_repartition_file_by_file():
    """
    FILE-BY-FILE method: Processes source files individually.
    
    Characteristics:
    - Memory usage: Bounded by largest single source file
    - Best for: Moderate datasets (GB-TB range)
    - Speed: Moderate (file-by-file processing)
    - Good compromise between memory and speed
    
    Use case: Add product_id partitioning when memory is limited.
    """
    logger.info("=" * 80)
    logger.info("Example 2: FILE-BY-FILE Repartitioning")
    logger.info("=" * 80)
    
    repartitioner = Repartitioner(
        source_dir="F:/processed/coinbase/ticker",
        target_dir="F:/processed/coinbase/ticker_fbf",
        new_partition_cols=['product_id', 'year', 'month', 'day'],
    )
    
    # Execute with file-by-file method
    stats = repartitioner.repartition(
        method="file_by_file",  # Process files one at a time
        delete_source=False,
        validate=True,
        dry_run=False,
    )
    
    logger.info(f"\n✓ File-by-file repartition complete!")
    logger.info(f"  Processed {stats['files_read']} source files")
    logger.info(f"  Created {stats['files_written']} output files")
    logger.info(f"  Partitions: {stats['partitions_created']}")
    logger.info(f"  Memory usage: Bounded by largest file")


# =============================================================================
# Example 3: BATCHED Repartitioning
# =============================================================================

def example_repartition_batched():
    """
    BATCHED method: Loads full dataset into memory.
    
    Characteristics:
    - Memory usage: Full dataset size (⚠️ HIGH)
    - Best for: Small datasets (MB-GB range)
    - Speed: Fastest (in-memory operations)
    - ⚠️ Only use if dataset fits comfortably in RAM
    
    Use case: Quick repartitioning of small market_trades dataset.
    """
    logger.info("=" * 80)
    logger.info("Example 3: BATCHED Repartitioning (Memory-Intensive)")
    logger.info("=" * 80)
    logger.warning("⚠️  This method loads entire dataset into memory!")
    
    repartitioner = Repartitioner(
        source_dir="F:/processed/coinbase/market_trades",
        target_dir="F:/processed/coinbase/market_trades_batch",
        new_partition_cols=['product_id', 'date'],  # Coarser partitioning
        batch_size=50_000,  # Batch size for processing
    )
    
    # Check size first
    estimates = repartitioner.estimate_size()
    size_gb = estimates['estimated_size_mb'] / 1024
    
    if size_gb > 10:  # More than 10GB
        logger.warning(f"Dataset is {size_gb:.1f} GB - consider using streaming or file_by_file method")
        return
    
    # Execute with batched method
    stats = repartitioner.repartition(
        method="batched",  # Load all into memory
        delete_source=False,
        validate=True,
        dry_run=False,
    )
    
    logger.info(f"\n✓ Batched repartition complete!")
    logger.info(f"  Records: {stats['records_written']:,}")
    logger.info(f"  Files: {stats['files_written']}")
    logger.info(f"  Speed: Fastest (in-memory)")
    logger.info(f"  ⚠️ Memory usage: Full dataset loaded")


# =============================================================================
# Example 4: Method Comparison
# =============================================================================

def example_method_comparison():
    """
    Demonstrate when to use each method.
    
    Decision tree:
    1. Default: Use streaming (safest, handles any size)
    2. If streaming too slow: Use file_by_file (good compromise)
    3. If data is small: Use batched (fastest)
    """
    logger.info("=" * 80)
    logger.info("Example 4: Repartitioning Method Comparison")
    logger.info("=" * 80)
    
    comparison = """
    ┌─────────────────┬──────────────────────┬─────────────────────┬─────────────────────┐
    │ Method          │ Memory Usage         │ Best For            │ Speed               │
    ├─────────────────┼──────────────────────┼─────────────────────┼─────────────────────┤
    │ STREAMING       │ Minimal (buffer)     │ TB+ datasets        │ Fast (optimized)    │
    │ (sink_parquet)  │ ~100MB               │ Production use      │ Polars streaming    │
    ├─────────────────┼──────────────────────┼─────────────────────┼─────────────────────┤
    │ FILE-BY-FILE    │ Largest single file  │ GB-TB datasets      │ Moderate            │
    │                 │ Bounded              │ Moderate memory     │ File-by-file reads  │
    ├─────────────────┼──────────────────────┼─────────────────────┼─────────────────────┤
    │ BATCHED         │ Full dataset         │ MB-GB datasets      │ Fastest             │
    │ (iter_slices)   │ ⚠️ HIGH              │ Small data in RAM   │ In-memory ops       │
    └─────────────────┴──────────────────────┴─────────────────────┴─────────────────────┘
    
    RECOMMENDATION:
    - Default: method="streaming" (most sophisticated, memory-safe)
    - Moderate data: method="file_by_file" (good compromise)
    - Small data: method="batched" (fastest if it fits in RAM)
    
    EXAMPLE USAGE:
        repartitioner.repartition(
            method="streaming",     # Choose: "streaming", "file_by_file", "batched"
            delete_source=False,    # Safety first
            validate=True,          # Always validate
            dry_run=False,          # Set False to execute
        )
    """
    
    logger.info(comparison)


# =============================================================================
# Example 5: Repartition with Transformation
# =============================================================================

def example_repartition_with_transform():
    """
    Repartition AND transform data (add/fix columns).
    
    Works with all three methods - transformation function is applied
    during repartitioning regardless of method used.
    
    Use case: Fix data quality issues during schema migration.
    """
    import polars as pl
    
    logger.info("=" * 80)
    logger.info("Example 5: Repartition with Transformation (Any Method)")
    logger.info("=" * 80)
    
    def add_derived_columns(df: pl.DataFrame) -> pl.DataFrame:
        """Add derived columns during repartition."""
        return df.with_columns([
            # Normalize side column
            pl.when(pl.col("side") == "offer")
            .then(pl.lit("ask"))
            .otherwise(pl.col("side"))
            .alias("side_normalized"),
            
            # Add time-based features
            pl.col("timestamp").dt.weekday().alias("day_of_week"),
            pl.col("timestamp").dt.hour().alias("hour_of_day"),
            
            # Market hours flag
            (pl.col("timestamp").dt.hour() >= 9) & 
            (pl.col("timestamp").dt.hour() < 16)
            .alias("market_hours")
        ])
    
    repartitioner = Repartitioner(
        source_dir="F:/processed/coinbase/level2",
        target_dir="F:/processed/coinbase/level2_enhanced",
        new_partition_cols=['product_id', 'year', 'month', 'day'],
    )
    
    # Works with any method - transformation applied during processing
    stats = repartitioner.repartition(
        method="streaming",           # Most efficient for large data
        delete_source=False,
        validate=True,
        transform_fn=add_derived_columns,  # Apply transformation
        dry_run=False,
    )
    
    logger.info(f"\n✓ Repartitioned and transformed {stats['records_written']:,} rows")
    logger.info(f"  Method: streaming (with transformation)")
    logger.info(f"  New columns: side_normalized, day_of_week, hour_of_day, market_hours")


# =============================================================================
# Example 6: File Compaction
# =============================================================================

def example_compact_fragmented_files():
    """
    Consolidate many small files into fewer large files.
    
    Use case: After many incremental writes, partitions have 100+ small files.
              Consolidate for better query performance.
    """
    logger.info("=" * 80)
    logger.info("Example 4: File Compaction")
    logger.info("=" * 80)
    
    compactor = ParquetCompactor(
        dataset_dir="F:/processed/coinbase/market_trades",
        target_file_size_mb=100,  # Target 100MB files
    )
    
    stats = compactor.compact(
        min_file_count=5,          # Only compact partitions with 5+ files
        max_file_size_mb=20,       # Only consolidate files < 20MB
        delete_source_files=True,  # Delete original files after compaction
        dry_run=False,
    )
    
    logger.info(f"\n✓ Compaction complete!")
    logger.info(f"  Files before: {stats['files_before']}")
    logger.info(f"  Files after: {stats['files_after']}")
    logger.info(f"  Space saved: {(stats['bytes_before'] - stats['bytes_after']) / (1024**2):.1f} MB")


# =============================================================================
# Example 7: Complete Migration Workflow
# =============================================================================

def example_complete_migration():
    """
    Complete workflow: Repartition → Validate → Swap → Cleanup.
    
    This is the production-safe way to change partition schemas.
    Uses streaming method by default for safety.
    """
    import time
    
    logger.info("=" * 80)
    logger.info("Example 7: Complete Migration Workflow (Production-Safe)")
    logger.info("=" * 80)
    
    source_dir = Path("F:/processed/coinbase/level2")
    temp_dir = Path("F:/processed/coinbase/level2_temp")
    backup_dir = Path("F:/processed/coinbase/level2_backup")
    
    # Step 1: Repartition to temp directory
    logger.info("\n[Step 1/5] Repartition to temp directory")
    repartitioner = Repartitioner(
        source_dir=str(source_dir),
        target_dir=str(temp_dir),
        new_partition_cols=['product_id', 'year', 'month', 'day', 'hour'],
    )
    
    stats = repartitioner.repartition(
        method="streaming",   # Use most sophisticated method for production
        delete_source=False,
        validate=True,
    )
    
    if stats['errors'] > 0:
        logger.error("Repartitioning failed! Aborting.")
        return
    
    # Step 2: Backup original
    logger.info("\n[Step 2/5] Backup original directory")
    if backup_dir.exists():
        import shutil
        shutil.rmtree(backup_dir)
    source_dir.rename(backup_dir)
    logger.info(f"  ✓ Backup created: {backup_dir}")
    
    # Step 3: Move temp to production
    logger.info("\n[Step 3/5] Move temp to production location")
    temp_dir.rename(source_dir)
    logger.info(f"  ✓ New partition schema active: {source_dir}")
    
    # Step 4: Verify queries work
    logger.info("\n[Step 4/5] Verify queries")
    try:
        import polars as pl
        df = pl.scan_parquet(str(source_dir / "**/*.parquet"))
        count = df.select(pl.count()).collect().item()
        logger.info(f"  ✓ Query successful: {count:,} rows")
    except Exception as e:
        logger.error(f"  ✗ Query failed: {e}")
        logger.error("  Rolling back...")
        source_dir.rename(temp_dir)
        backup_dir.rename(source_dir)
        return
    
    # Step 5: Delete backup (after confirming everything works)
    logger.info("\n[Step 5/5] Cleanup")
    logger.info(f"  Keeping backup for safety: {backup_dir}")
    logger.info(f"  Manual cleanup: rm -rf {backup_dir}")
    
    logger.info("\n" + "=" * 80)
    logger.info("MIGRATION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"  Old schema backed up: {backup_dir}")
    logger.info(f"  New schema active: {source_dir}")
    logger.info(f"  Rows: {stats['records_written']:,}")
    logger.info(f"  Files: {stats['files_written']}")
    logger.info("=" * 80)


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    # Choose which example to run
    
    # Uncomment one at a time:
    
    # Example 1: Streaming repartitioning (most sophisticated, default)
    # example_repartition_streaming()
    
    # Example 2: File-by-file repartitioning (good for moderate datasets)
    # example_repartition_file_by_file()
    
    # Example 3: Batched repartitioning (fastest but memory-intensive)
    # example_repartition_batched()
    
    # Example 4: Method comparison guide
    # example_method_comparison()
    
    # Example 5: Repartition with transformation (works with any method)
    # example_repartition_with_transform()
    
    # Example 6: File compaction (consolidate small files)
    # example_compact_fragmented_files()
    
    # Example 7: Complete production-safe migration workflow
    # example_complete_migration()
    
    logger.info("\nRepartitioning examples ready!")
    logger.info("Uncomment one example to run.")
    logger.info("\nRECOMMENDATION: Start with example_method_comparison() to understand the differences.")
