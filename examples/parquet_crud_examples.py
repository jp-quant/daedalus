"""
Example usage of ParquetCRUD operations.

Demonstrates:
1. DELETE: Remove rows matching conditions
2. UPDATE: Modify existing rows
3. UPSERT: Insert or update (merge) data
"""
import logging
from pathlib import Path
import polars as pl
from etl.parquet_crud import ParquetCRUD

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# =============================================================================
# Example 1: DELETE Operations
# =============================================================================

def example_delete_specific_rows():
    """
    Delete rows matching filter condition.
    
    Use case: Remove bad data or outliers.
    """
    logger.info("=" * 80)
    logger.info("Example 1: Delete Specific Rows")
    logger.info("=" * 80)
    
    crud = ParquetCRUD(
        dataset_dir="F:/processed/coinbase/level2",
        backup_dir="F:/backups/level2",
    )
    
    # Delete all BTC-USD data from Nov 26, 2025
    stats = crud.delete(
        filter_expr=(
            (pl.col("product_id") == "BTC-USD") & 
            (pl.col("date") == "2025-11-26")
        ),
        partition_filter={"product_id": "BTC-USD", "date": "2025-11-26"},
        dry_run=False,        # Set to True to preview
        create_backup=True,   # Backup affected files
    )
    
    logger.info(f"\n✓ Deleted {stats['rows_deleted']:,} rows")


def example_delete_outliers():
    """
    Delete outliers based on spread threshold.
    
    Use case: Clean data quality issues.
    """
    logger.info("=" * 80)
    logger.info("Example 2: Delete Outliers")
    logger.info("=" * 80)
    
    crud = ParquetCRUD(dataset_dir="F:/processed/coinbase/ticker")
    
    # Delete tickers with abnormally wide spreads (> 200 bps)
    stats = crud.delete(
        filter_expr=pl.col("spread_bps") > 200,
        partition_filter={"product_id": "BTC-USD"},  # Only check BTC
        dry_run=False,
        create_backup=True,
    )
    
    logger.info(f"\n✓ Cleaned {stats['rows_deleted']:,} outlier rows")


def example_delete_duplicates():
    """
    Delete duplicate trade IDs.
    
    Use case: Remove duplicate records.
    """
    logger.info("=" * 80)
    logger.info("Example 3: Delete Duplicates")
    logger.info("=" * 80)
    
    crud = ParquetCRUD(dataset_dir="F:/processed/coinbase/market_trades")
    
    # This requires custom logic - use update instead
    # For now, we'd read, deduplicate with Polars, and write back
    # See example_fix_duplicates() below for proper approach


# =============================================================================
# Example 4: UPDATE Operations
# =============================================================================

def example_update_derived_column():
    """
    Recalculate derived column (e.g., spread).
    
    Use case: Fix computation bug in processor.
    """
    logger.info("=" * 80)
    logger.info("Example 4: Update Derived Column")
    logger.info("=" * 80)
    
    crud = ParquetCRUD(
        dataset_dir="F:/processed/coinbase/ticker",
        backup_dir="F:/backups/ticker",
    )
    
    # Recalculate spread_bps for all rows
    stats = crud.update(
        filter_expr=pl.col("spread_bps").is_not_null(),  # All rows with existing value
        update_expr={
            "spread_bps": ((pl.col("best_ask") - pl.col("best_bid")) / pl.col("mid_price")) * 10000
        },
        partition_filter={"product_id": "BTC-USD", "date": "2025-11-26"},
        dry_run=False,
        create_backup=True,
    )
    
    logger.info(f"\n✓ Updated {stats['rows_updated']:,} rows")


def example_update_categorical():
    """
    Fix categorical values.
    
    Use case: Standardize side field ("offer" → "ask").
    """
    logger.info("=" * 80)
    logger.info("Example 5: Fix Categorical Values")
    logger.info("=" * 80)
    
    crud = ParquetCRUD(dataset_dir="F:/processed/coinbase/level2")
    
    # Fix side normalization
    stats = crud.update(
        filter_expr=pl.col("side") == "offer",
        update_expr={"side_normalized": "ask"},
        partition_filter={"product_id": "BTC-USD"},
        dry_run=False,
        create_backup=True,
    )
    
    logger.info(f"\n✓ Fixed {stats['rows_updated']:,} side values")


def example_update_with_expression():
    """
    Update using complex expression.
    
    Use case: Add missing timestamp components.
    """
    logger.info("=" * 80)
    logger.info("Example 6: Update with Expression")
    logger.info("=" * 80)
    
    crud = ParquetCRUD(dataset_dir="F:/processed/coinbase/market_trades")
    
    # Add minute field if missing
    stats = crud.update(
        filter_expr=pl.col("minute").is_null(),
        update_expr={
            "minute": pl.col("time").str.to_datetime().dt.minute()
        },
        partition_filter={"date": "2025-11-26"},
        dry_run=False,
    )
    
    logger.info(f"\n✓ Updated {stats['rows_updated']:,} rows with minute field")


# =============================================================================
# Example 7: UPSERT Operations
# =============================================================================

def example_upsert_corrected_data():
    """
    Insert or update corrected trade data.
    
    Use case: Fix data quality issues by upserting corrected records.
    """
    logger.info("=" * 80)
    logger.info("Example 7: Upsert Corrected Data")
    logger.info("=" * 80)
    
    crud = ParquetCRUD(
        dataset_dir="F:/processed/coinbase/market_trades",
        backup_dir="F:/backups/market_trades",
    )
    
    # Corrected trade data
    corrected_trades = pl.DataFrame({
        "product_id": ["BTC-USD", "BTC-USD", "ETH-USD"],
        "trade_id": ["12345", "12346", "67890"],
        "price": [50000.0, 50100.0, 2500.0],
        "size": [1.5, 2.0, 10.0],
        "side": ["BUY", "SELL", "BUY"],
        "time": ["2025-11-26T14:30:00Z", "2025-11-26T14:31:00Z", "2025-11-26T14:32:00Z"],
        "date": ["2025-11-26", "2025-11-26", "2025-11-26"],
        "year": [2025, 2025, 2025],
        "month": [11, 11, 11],
        "day": [26, 26, 26],
    })
    
    stats = crud.upsert(
        new_data=corrected_trades,
        key_cols=["product_id", "trade_id"],  # Unique identifier
        partition_cols=["product_id", "date"],
        update_cols=["price", "size", "side"],  # Only update these columns
        dry_run=False,
        create_backup=True,
    )
    
    logger.info(f"\n✓ Upsert complete:")
    logger.info(f"  Inserted: {stats['rows_inserted']}")
    logger.info(f"  Updated: {stats['rows_updated']}")


def example_upsert_append_new_data():
    """
    Append new data, updating if already exists.
    
    Use case: Incremental data loading with deduplication.
    """
    logger.info("=" * 80)
    logger.info("Example 8: Upsert Append Mode")
    logger.info("=" * 80)
    
    crud = ParquetCRUD(dataset_dir="F:/processed/coinbase/ticker")
    
    # New ticker snapshots (may overlap with existing)
    new_tickers = pl.DataFrame({
        "product_id": ["BTC-USD"] * 100,
        "price": [50000.0 + i for i in range(100)],
        "sequence_num": list(range(10000, 10100)),
        # ... other columns
    })
    
    stats = crud.upsert(
        new_data=new_tickers,
        key_cols=["product_id", "sequence_num"],  # Dedupe by sequence
        partition_cols=["product_id", "date"],
        dry_run=False,
    )
    
    logger.info(f"\n✓ Appended {stats['rows_inserted']} new rows")


# =============================================================================
# Example 9: Complex Workflow
# =============================================================================

def example_fix_duplicates():
    """
    Find and remove duplicate records.
    
    Use case: Clean up duplicate trade IDs.
    """
    logger.info("=" * 80)
    logger.info("Example 9: Remove Duplicates")
    logger.info("=" * 80)
    
    # Step 1: Read data and find duplicates
    dataset_dir = Path("F:/processed/coinbase/market_trades")
    
    logger.info("[1/3] Scanning for duplicates...")
    df = pl.scan_parquet(
        str(dataset_dir / "**/*.parquet"),
    )
    
    # Find duplicate trade_ids
    duplicates = (
        df
        .groupby(["product_id", "trade_id"])
        .agg(pl.count().alias("count"))
        .filter(pl.col("count") > 1)
        .collect()
    )
    
    logger.info(f"  Found {len(duplicates)} duplicate trade IDs")
    
    if len(duplicates) == 0:
        logger.info("  No duplicates to remove")
        return
    
    # Step 2: Create deduplicated dataset
    logger.info("[2/3] Deduplicating...")
    
    df_deduped = (
        df
        .unique(subset=["product_id", "trade_id"], keep="first")
        .collect()
    )
    
    # Step 3: Upsert deduplicated data (will replace duplicates)
    logger.info("[3/3] Writing deduplicated data...")
    
    crud = ParquetCRUD(
        dataset_dir=str(dataset_dir),
        backup_dir="F:/backups/market_trades_dedup",
    )
    
    stats = crud.upsert(
        new_data=df_deduped,
        key_cols=["product_id", "trade_id"],
        partition_cols=["product_id", "date"],
        create_backup=True,
    )
    
    logger.info(f"\n✓ Deduplication complete")
    logger.info(f"  Duplicates removed: {len(duplicates)}")


def example_batch_operations():
    """
    Perform multiple operations in sequence.
    
    Use case: Data cleanup pipeline.
    """
    logger.info("=" * 80)
    logger.info("Example 10: Batch Operations")
    logger.info("=" * 80)
    
    crud = ParquetCRUD(
        dataset_dir="F:/processed/coinbase/level2",
        backup_dir="F:/backups/level2_cleanup",
    )
    
    # Step 1: Delete outliers
    logger.info("[1/3] Removing outliers...")
    stats1 = crud.delete(
        filter_expr=pl.col("spread_bps") > 500,
        partition_filter={"product_id": "BTC-USD"},
        create_backup=True,
    )
    
    # Step 2: Fix side normalization
    logger.info("[2/3] Fixing side values...")
    stats2 = crud.update(
        filter_expr=pl.col("side") == "offer",
        update_expr={"side_normalized": "ask"},
        partition_filter={"product_id": "BTC-USD"},
        create_backup=False,  # Already backed up in step 1
    )
    
    # Step 3: Recalculate spread
    logger.info("[3/3] Recalculating spread...")
    stats3 = crud.update(
        filter_expr=pl.col("spread").is_not_null(),
        update_expr={
            "spread": pl.col("best_ask_price") - pl.col("best_bid_price")
        },
        partition_filter={"product_id": "BTC-USD"},
        create_backup=False,
    )
    
    logger.info("\n" + "=" * 80)
    logger.info("CLEANUP SUMMARY")
    logger.info("=" * 80)
    logger.info(f"  Rows deleted: {stats1['rows_deleted']:,}")
    logger.info(f"  Rows fixed (side): {stats2['rows_updated']:,}")
    logger.info(f"  Rows updated (spread): {stats3['rows_updated']:,}")
    logger.info("=" * 80)


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    # Choose which example to run
    
    # DELETE examples
    # example_delete_specific_rows()
    # example_delete_outliers()
    
    # UPDATE examples
    # example_update_derived_column()
    # example_update_categorical()
    # example_update_with_expression()
    
    # UPSERT examples
    # example_upsert_corrected_data()
    # example_upsert_append_new_data()
    
    # Complex workflows
    # example_fix_duplicates()
    # example_batch_operations()
    
    logger.info("\nExamples ready! Uncomment one to run.")
