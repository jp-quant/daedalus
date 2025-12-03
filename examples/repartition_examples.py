"""
Example usage of Repartitioner and ParquetCompactor.

Demonstrates:
1. Changing partition schema (repartitioning)
2. File consolidation (compaction)
3. Best practices and common patterns
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
# Example 1: Change Partition Schema
# =============================================================================

def example_repartition_simple():
    """
    Change from ['product_id', 'date'] to ['product_id', 'year', 'month', 'day'].
    
    Use case: Finer-grained partitioning for faster queries on specific days.
    """
    logger.info("=" * 80)
    logger.info("Example 1: Simple Repartitioning")
    logger.info("=" * 80)
    
    repartitioner = Repartitioner(
        source_dir="F:/processed/coinbase/level2",
        target_dir="F:/processed/coinbase/level2_new",
        new_partition_cols=['product_id', 'year', 'month', 'day'],
    )
    
    # Estimate before executing
    estimates = repartitioner.estimate_size()
    logger.info(f"Estimated partitions: {estimates['num_partitions']}")
    logger.info(f"Estimated size: {estimates['estimated_size_mb']:.1f} MB")
    
    # Execute repartition
    stats = repartitioner.repartition(
        delete_source=False,  # Keep source for safety (delete manually later)
        validate=True,        # Verify row counts match
        dry_run=False,        # Set to True to preview only
    )
    
    logger.info(f"\n✓ Repartitioning complete!")
    logger.info(f"  New location: F:/processed/coinbase/level2_new")
    logger.info(f"  Records: {stats['records_written']:,}")
    logger.info(f"  Files: {stats['files_written']}")


def example_repartition_with_hour():
    """
    Change from ['date', 'hour'] to ['product_id', 'year', 'month', 'day', 'hour'].
    
    Use case: Add product_id partitioning to ticker data for per-product queries.
    """
    logger.info("=" * 80)
    logger.info("Example 2: Add Product ID Partition")
    logger.info("=" * 80)
    
    repartitioner = Repartitioner(
        source_dir="F:/processed/coinbase/ticker",
        target_dir="F:/processed/coinbase/ticker_new",
        new_partition_cols=['product_id', 'year', 'month', 'day', 'hour'],
    )
    
    stats = repartitioner.repartition(
        delete_source=False,
        validate=True,
    )
    
    logger.info(f"\n✓ Complete! Created {stats['partitions_created']} partitions")


def example_repartition_with_transform():
    """
    Repartition AND transform data (add/fix columns).
    
    Use case: Fix data quality issues during schema migration.
    """
    import polars as pl
    
    logger.info("=" * 80)
    logger.info("Example 3: Repartition with Transformation")
    logger.info("=" * 80)
    
    def fix_side_column(df: pl.DataFrame) -> pl.DataFrame:
        """Fix side normalization during repartition."""
        return df.with_columns(
            pl.when(pl.col("side") == "offer")
            .then(pl.lit("ask"))
            .otherwise(pl.col("side"))
            .alias("side_normalized")
        )
    
    repartitioner = Repartitioner(
        source_dir="F:/processed/coinbase/level2",
        target_dir="F:/processed/coinbase/level2_fixed",
        new_partition_cols=['product_id', 'year', 'month', 'day'],
    )
    
    stats = repartitioner.repartition(
        delete_source=False,
        validate=True,
        transform_fn=fix_side_column,  # Apply transformation
    )
    
    logger.info(f"\n✓ Repartitioned and transformed {stats['records_written']:,} rows")


# =============================================================================
# Example 4: File Compaction
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
# Example 5: Complete Migration Workflow
# =============================================================================

def example_complete_migration():
    """
    Complete workflow: Repartition → Validate → Swap → Cleanup.
    
    This is the production-safe way to change partition schemas.
    """
    import time
    
    logger.info("=" * 80)
    logger.info("Example 5: Complete Migration Workflow")
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
    
    # example_repartition_simple()
    # example_repartition_with_hour()
    # example_repartition_with_transform()
    # example_compact_fragmented_files()
    # example_complete_migration()
    
    logger.info("\nExamples ready! Uncomment one to run.")
