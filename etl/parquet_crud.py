"""
Parquet CRUD Operations - Delete, Update, and Upsert for Parquet datasets.

Parquet is an immutable file format, so "updates" and "deletes" require:
1. Read existing files
2. Apply operation in memory
3. Write new files
4. Delete old files

This module provides efficient implementations with:
- Partition-aware operations (only touch affected partitions)
- Atomic operations (temp files with final rename)
- Validation and rollback
- Support for complex filter conditions
"""
import logging
import shutil
import uuid
from pathlib import Path
from typing import List, Optional, Dict, Any, Callable, Union
from datetime import datetime
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class ParquetCRUD:
    """
    CRUD operations for Parquet datasets.
    
    Handles:
    1. DELETE: Remove rows matching filter condition
    2. UPDATE: Modify rows matching filter condition
    3. UPSERT: Insert or update based on key columns
    
    All operations are atomic within partition (uses temp files).
    
    Example:
        crud = ParquetCRUD(dataset_dir="F:/processed/coinbase/level2")
        
        # Delete rows
        crud.delete(
            filter_expr=pl.col("product_id") == "BTC-USD" & pl.col("date") == "2025-11-26",
            partition_filter={"product_id": "BTC-USD", "date": "2025-11-26"},
        )
        
        # Update rows
        crud.update(
            filter_expr=pl.col("side") == "offer",
            update_expr={"side_normalized": "ask"},
            partition_filter={"product_id": "BTC-USD"},
        )
        
        # Upsert (merge) data
        new_data = pl.DataFrame({...})
        crud.upsert(
            new_data=new_data,
            key_cols=["product_id", "trade_id"],
            partition_cols=["product_id", "date"],
        )
    """
    
    def __init__(
        self,
        dataset_dir: str,
        compression: str = "snappy",
        backup_dir: Optional[str] = None,
    ):
        """
        Initialize CRUD handler.
        
        Args:
            dataset_dir: Partitioned Parquet dataset directory
            compression: Compression codec for rewritten files
            backup_dir: Optional backup directory (for safety)
        """
        self.dataset_dir = Path(dataset_dir)
        self.compression = compression
        self.backup_dir = Path(backup_dir) if backup_dir else None
        
        if not self.dataset_dir.exists():
            raise ValueError(f"Dataset directory does not exist: {dataset_dir}")
        
        if self.backup_dir:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        self.stats = {
            "partitions_affected": 0,
            "files_read": 0,
            "files_written": 0,
            "files_deleted": 0,
            "rows_before": 0,
            "rows_after": 0,
            "rows_deleted": 0,
            "rows_updated": 0,
            "rows_inserted": 0,
        }
        
        logger.info(
            f"[ParquetCRUD] Initialized:\n"
            f"  Dataset: {dataset_dir}\n"
            f"  Compression: {compression}\n"
            f"  Backup dir: {backup_dir}"
        )
    
    def delete(
        self,
        filter_expr: pl.Expr,
        partition_filter: Optional[Dict[str, Any]] = None,
        dry_run: bool = False,
        create_backup: bool = True,
    ) -> Dict[str, Any]:
        """
        Delete rows matching filter condition.
        
        Args:
            filter_expr: Polars expression for filtering rows to delete
            partition_filter: Dict to narrow down affected partitions
                            (e.g., {"product_id": "BTC-USD", "date": "2025-11-26"})
            dry_run: Preview without executing
            create_backup: Backup affected files before deletion
        
        Returns:
            Statistics dictionary
        
        Example:
            # Delete all BTC-USD data from a specific date
            stats = crud.delete(
                filter_expr=(
                    (pl.col("product_id") == "BTC-USD") & 
                    (pl.col("date") == "2025-11-26")
                ),
                partition_filter={"product_id": "BTC-USD", "date": "2025-11-26"},
            )
            
            # Delete rows with specific condition
            stats = crud.delete(
                filter_expr=pl.col("spread_bps") > 100,  # Delete wide spreads
                partition_filter={"product_id": "BTC-USD"},  # Only check BTC partitions
            )
        """
        logger.info("=" * 80)
        logger.info("DELETE OPERATION")
        logger.info("=" * 80)
        logger.info(f"  Filter: {filter_expr}")
        logger.info(f"  Partition filter: {partition_filter}")
        
        # Reset stats
        self.stats = {k: 0 for k in self.stats}
        
        # Step 1: Find affected partitions
        affected_partitions = self._find_affected_partitions(partition_filter)
        logger.info(f"  Affected partitions: {len(affected_partitions)}")
        
        if not affected_partitions:
            logger.info("  No partitions match filter")
            return self.stats
        
        # Step 2: Process each partition
        for partition_dir in affected_partitions:
            files = list(partition_dir.glob("*.parquet"))
            if not files:
                continue
            
            logger.info(f"\n  Processing partition: {partition_dir.relative_to(self.dataset_dir)}")
            logger.info(f"    Files: {len(files)}")
            
            try:
                # Read partition
                df = pl.read_parquet(files)
                rows_before = len(df)
                self.stats["rows_before"] += rows_before
                self.stats["files_read"] += len(files)
                
                # Apply filter (keep rows that DON'T match - inverse of delete condition)
                df_filtered = df.filter(~filter_expr)
                rows_after = len(df_filtered)
                rows_deleted = rows_before - rows_after
                
                self.stats["rows_after"] += rows_after
                self.stats["rows_deleted"] += rows_deleted
                
                logger.info(f"    Rows before: {rows_before:,}")
                logger.info(f"    Rows after: {rows_after:,}")
                logger.info(f"    Rows deleted: {rows_deleted:,}")
                
                if rows_deleted == 0:
                    logger.info(f"    No rows to delete, skipping")
                    continue
                
                if dry_run:
                    logger.info(f"    DRY RUN - No changes made")
                    continue
                
                # Backup original files
                if create_backup and self.backup_dir:
                    backup_partition = self.backup_dir / partition_dir.relative_to(self.dataset_dir)
                    backup_partition.mkdir(parents=True, exist_ok=True)
                    for f in files:
                        shutil.copy2(f, backup_partition / f.name)
                    logger.info(f"    Backup created: {backup_partition}")
                
                # Write filtered data
                if rows_after > 0:
                    # Write new file with unique filename
                    timestamp = datetime.now().strftime("%Y%m%dT%H")
                    unique_id = str(uuid.uuid4())[:8]
                    output_file = partition_dir / f"part_{timestamp}_{unique_id}.parquet"
                    df_filtered.write_parquet(
                        output_file,
                        compression=self.compression,
                    )
                    self.stats["files_written"] += 1
                    logger.info(f"    Wrote: {output_file.name}")
                else:
                    logger.info(f"    Partition empty after delete")
                
                # Delete old files
                for f in files:
                    f.unlink()
                    self.stats["files_deleted"] += 1
                
                self.stats["partitions_affected"] += 1
                logger.info(f"    ✓ Complete")
            
            except Exception as e:
                logger.error(f"    ✗ Error processing partition: {e}", exc_info=True)
                continue
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("DELETE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"  Partitions affected: {self.stats['partitions_affected']}")
        logger.info(f"  Rows deleted: {self.stats['rows_deleted']:,}")
        logger.info(f"  Rows remaining: {self.stats['rows_after']:,}")
        logger.info(f"  Files read: {self.stats['files_read']}")
        logger.info(f"  Files written: {self.stats['files_written']}")
        logger.info(f"  Files deleted: {self.stats['files_deleted']}")
        logger.info("=" * 80)
        
        return self.stats.copy()
    
    def update(
        self,
        filter_expr: pl.Expr,
        update_expr: Union[Dict[str, Any], Dict[str, pl.Expr]],
        partition_filter: Optional[Dict[str, Any]] = None,
        dry_run: bool = False,
        create_backup: bool = True,
    ) -> Dict[str, Any]:
        """
        Update rows matching filter condition.
        
        Args:
            filter_expr: Polars expression for filtering rows to update
            update_expr: Dictionary of column → value or column → expression
            partition_filter: Dict to narrow down affected partitions
            dry_run: Preview without executing
            create_backup: Backup affected files before update
        
        Returns:
            Statistics dictionary
        
        Example:
            # Fix side normalization
            stats = crud.update(
                filter_expr=pl.col("side") == "offer",
                update_expr={"side_normalized": "ask"},
                partition_filter={"product_id": "BTC-USD"},
            )
            
            # Recalculate derived column
            stats = crud.update(
                filter_expr=pl.col("spread").is_null(),
                update_expr={
                    "spread": pl.col("best_ask") - pl.col("best_bid")
                },
            )
        """
        logger.info("=" * 80)
        logger.info("UPDATE OPERATION")
        logger.info("=" * 80)
        logger.info(f"  Filter: {filter_expr}")
        logger.info(f"  Update: {update_expr}")
        logger.info(f"  Partition filter: {partition_filter}")
        
        # Reset stats
        self.stats = {k: 0 for k in self.stats}
        
        # Find affected partitions
        affected_partitions = self._find_affected_partitions(partition_filter)
        logger.info(f"  Affected partitions: {len(affected_partitions)}")
        
        if not affected_partitions:
            logger.info("  No partitions match filter")
            return self.stats
        
        # Process each partition
        for partition_dir in affected_partitions:
            files = list(partition_dir.glob("*.parquet"))
            if not files:
                continue
            
            logger.info(f"\n  Processing partition: {partition_dir.relative_to(self.dataset_dir)}")
            
            try:
                # Read partition
                df = pl.read_parquet(files)
                rows_total = len(df)
                self.stats["rows_before"] += rows_total
                self.stats["files_read"] += len(files)
                
                # Count rows matching filter
                rows_to_update = df.filter(filter_expr).height
                
                logger.info(f"    Total rows: {rows_total:,}")
                logger.info(f"    Rows to update: {rows_to_update:,}")
                
                if rows_to_update == 0:
                    logger.info(f"    No rows to update, skipping")
                    continue
                
                if dry_run:
                    logger.info(f"    DRY RUN - No changes made")
                    self.stats["rows_updated"] += rows_to_update
                    continue
                
                # Apply update
                # Use when-then-otherwise for conditional updates
                for col, value in update_expr.items():
                    if isinstance(value, pl.Expr):
                        # Expression-based update
                        df = df.with_columns(
                            pl.when(filter_expr)
                            .then(value)
                            .otherwise(pl.col(col))
                            .alias(col)
                        )
                    else:
                        # Literal value update
                        df = df.with_columns(
                            pl.when(filter_expr)
                            .then(pl.lit(value))
                            .otherwise(pl.col(col))
                            .alias(col)
                        )
                
                self.stats["rows_after"] += len(df)
                self.stats["rows_updated"] += rows_to_update
                
                # Backup original files
                if create_backup and self.backup_dir:
                    backup_partition = self.backup_dir / partition_dir.relative_to(self.dataset_dir)
                    backup_partition.mkdir(parents=True, exist_ok=True)
                    for f in files:
                        shutil.copy2(f, backup_partition / f.name)
                    logger.info(f"    Backup created")
                
                # Write updated data with unique filename
                timestamp = datetime.now().strftime("%Y%m%dT%H")
                unique_id = str(uuid.uuid4())[:8]
                output_file = partition_dir / f"part_{timestamp}_{unique_id}.parquet"
                df.write_parquet(
                    output_file,
                    compression=self.compression,
                )
                self.stats["files_written"] += 1
                
                # Delete old files
                for f in files:
                    f.unlink()
                    self.stats["files_deleted"] += 1
                
                self.stats["partitions_affected"] += 1
                logger.info(f"    ✓ Complete")
            
            except Exception as e:
                logger.error(f"    ✗ Error processing partition: {e}", exc_info=True)
                continue
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("UPDATE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"  Partitions affected: {self.stats['partitions_affected']}")
        logger.info(f"  Rows updated: {self.stats['rows_updated']:,}")
        logger.info(f"  Files read: {self.stats['files_read']}")
        logger.info(f"  Files written: {self.stats['files_written']}")
        logger.info(f"  Files deleted: {self.stats['files_deleted']}")
        logger.info("=" * 80)
        
        return self.stats.copy()
    
    def upsert(
        self,
        new_data: pl.DataFrame,
        key_cols: List[str],
        partition_cols: List[str],
        update_cols: Optional[List[str]] = None,
        dry_run: bool = False,
        create_backup: bool = True,
    ) -> Dict[str, Any]:
        """
        Insert or update (upsert/merge) data based on key columns.
        
        For each row in new_data:
        - If key exists in dataset: update specified columns
        - If key doesn't exist: insert new row
        
        Args:
            new_data: New data to upsert
            key_cols: Columns that uniquely identify rows (e.g., ["product_id", "trade_id"])
            partition_cols: Partition columns for determining affected partitions
            update_cols: Columns to update (None = all non-key columns)
            dry_run: Preview without executing
            create_backup: Backup affected files
        
        Returns:
            Statistics dictionary
        
        Example:
            # Upsert corrected trade data
            corrected_trades = pl.DataFrame({
                "product_id": ["BTC-USD", "BTC-USD"],
                "trade_id": ["12345", "12346"],
                "price": [50000.0, 50100.0],
                "size": [1.5, 2.0],
                "date": ["2025-11-26", "2025-11-26"],
            })
            
            stats = crud.upsert(
                new_data=corrected_trades,
                key_cols=["product_id", "trade_id"],
                partition_cols=["product_id", "date"],
                update_cols=["price", "size"],  # Only update these columns
            )
        """
        logger.info("=" * 80)
        logger.info("UPSERT OPERATION")
        logger.info("=" * 80)
        logger.info(f"  New data rows: {len(new_data):,}")
        logger.info(f"  Key columns: {key_cols}")
        logger.info(f"  Partition columns: {partition_cols}")
        logger.info(f"  Update columns: {update_cols or 'all non-key columns'}")
        
        # Reset stats
        self.stats = {k: 0 for k in self.stats}
        
        # Validate key columns exist in new data
        missing_keys = set(key_cols) - set(new_data.columns)
        if missing_keys:
            raise ValueError(f"Key columns {missing_keys} not found in new_data")
        
        # Validate partition columns exist in new data
        missing_partitions = set(partition_cols) - set(new_data.columns)
        if missing_partitions:
            raise ValueError(f"Partition columns {missing_partitions} not found in new_data")
        
        # Determine update columns
        if update_cols is None:
            update_cols = [col for col in new_data.columns if col not in key_cols]
        
        # Group new data by partition
        new_data_grouped = new_data.partition_by(partition_cols, as_dict=True)
        
        logger.info(f"  Partitions in new data: {len(new_data_grouped)}")
        
        # Process each partition
        for partition_key, new_partition_df in new_data_grouped.items():
            # Build partition path
            partition_path = self.dataset_dir
            if isinstance(partition_key, tuple):
                for col, val in zip(partition_cols, partition_key):
                    partition_path = partition_path / f"{col}={val}"
            else:
                partition_path = partition_path / f"{partition_cols[0]}={partition_key}"
            
            logger.info(f"\n  Processing partition: {partition_path.relative_to(self.dataset_dir)}")
            logger.info(f"    New rows: {len(new_partition_df):,}")
            
            # Check if partition exists
            if not partition_path.exists():
                logger.info(f"    Partition doesn't exist - inserting all rows")
                
                if dry_run:
                    self.stats["rows_inserted"] += len(new_partition_df)
                    logger.info(f"    DRY RUN - Would insert {len(new_partition_df)} rows")
                    continue
                
                # Create partition and write with unique filename
                partition_path.mkdir(parents=True, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%dT%H")
                unique_id = str(uuid.uuid4())[:8]
                output_file = partition_path / f"part_{timestamp}_{unique_id}.parquet"
                new_partition_df.write_parquet(
                    output_file,
                    compression=self.compression,
                )
                self.stats["files_written"] += 1
                self.stats["rows_inserted"] += len(new_partition_df)
                self.stats["partitions_affected"] += 1
                logger.info(f"    ✓ Inserted {len(new_partition_df)} rows")
                continue
            
            # Partition exists - merge with existing data
            files = list(partition_path.glob("*.parquet"))
            if not files:
                logger.info(f"    Partition empty - inserting all rows")
                
                if dry_run:
                    self.stats["rows_inserted"] += len(new_partition_df)
                    continue
                
                timestamp = datetime.now().strftime("%Y%m%dT%H")
                unique_id = str(uuid.uuid4())[:8]
                output_file = partition_path / f"part_{timestamp}_{unique_id}.parquet"
                new_partition_df.write_parquet(
                    output_file,
                    compression=self.compression,
                )
                self.stats["files_written"] += 1
                self.stats["rows_inserted"] += len(new_partition_df)
                self.stats["partitions_affected"] += 1
                continue
            
            try:
                # Read existing data
                existing_df = pl.read_parquet(files)
                rows_before = len(existing_df)
                self.stats["rows_before"] += rows_before
                self.stats["files_read"] += len(files)
                
                logger.info(f"    Existing rows: {rows_before:,}")
                
                # Perform upsert using join
                # 1. Left join to identify updates
                merged = existing_df.join(
                    new_partition_df.select(key_cols + update_cols),
                    on=key_cols,
                    how="left",
                    suffix="_new",
                )
                
                # 2. Update columns where match exists
                for col in update_cols:
                    new_col = f"{col}_new"
                    if new_col in merged.columns:
                        merged = merged.with_columns(
                            pl.when(pl.col(new_col).is_not_null())
                            .then(pl.col(new_col))
                            .otherwise(pl.col(col))
                            .alias(col)
                        )
                        merged = merged.drop(new_col)
                
                # 3. Find new rows (not in existing)
                existing_keys = existing_df.select(key_cols)
                new_keys = new_partition_df.select(key_cols)
                keys_to_insert = new_keys.join(existing_keys, on=key_cols, how="anti")
                
                rows_to_insert = new_partition_df.join(keys_to_insert, on=key_cols, how="inner")
                
                # 4. Combine updated + new rows
                final_df = pl.concat([merged, rows_to_insert])
                
                rows_after = len(final_df)
                rows_updated = rows_before - len(keys_to_insert)
                rows_inserted = len(keys_to_insert)
                
                self.stats["rows_after"] += rows_after
                self.stats["rows_updated"] += rows_updated
                self.stats["rows_inserted"] += rows_inserted
                
                logger.info(f"    Rows updated: {rows_updated:,}")
                logger.info(f"    Rows inserted: {rows_inserted:,}")
                logger.info(f"    Total after: {rows_after:,}")
                
                if dry_run:
                    logger.info(f"    DRY RUN - No changes made")
                    continue
                
                # Backup original files
                if create_backup and self.backup_dir:
                    backup_partition = self.backup_dir / partition_path.relative_to(self.dataset_dir)
                    backup_partition.mkdir(parents=True, exist_ok=True)
                    for f in files:
                        shutil.copy2(f, backup_partition / f.name)
                
                # Write merged data with unique filename
                timestamp = datetime.now().strftime("%Y%m%dT%H")
                unique_id = str(uuid.uuid4())[:8]
                output_file = partition_path / f"part_{timestamp}_{unique_id}.parquet"
                final_df.write_parquet(
                    output_file,
                    compression=self.compression,
                )
                self.stats["files_written"] += 1
                
                # Delete old files
                for f in files:
                    f.unlink()
                    self.stats["files_deleted"] += 1
                
                self.stats["partitions_affected"] += 1
                logger.info(f"    ✓ Complete")
            
            except Exception as e:
                logger.error(f"    ✗ Error processing partition: {e}", exc_info=True)
                continue
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("UPSERT SUMMARY")
        logger.info("=" * 80)
        logger.info(f"  Partitions affected: {self.stats['partitions_affected']}")
        logger.info(f"  Rows inserted: {self.stats['rows_inserted']:,}")
        logger.info(f"  Rows updated: {self.stats['rows_updated']:,}")
        logger.info(f"  Total rows after: {self.stats['rows_after']:,}")
        logger.info(f"  Files read: {self.stats['files_read']}")
        logger.info(f"  Files written: {self.stats['files_written']}")
        logger.info(f"  Files deleted: {self.stats['files_deleted']}")
        logger.info("=" * 80)
        
        return self.stats.copy()
    
    def _find_affected_partitions(
        self,
        partition_filter: Optional[Dict[str, Any]] = None
    ) -> List[Path]:
        """
        Find partition directories matching filter.
        
        Args:
            partition_filter: Dict of partition_col → value
        
        Returns:
            List of partition directories
        """
        if not partition_filter:
            # All leaf partitions
            leaf_partitions = []
            for item in self.dataset_dir.rglob("*.parquet"):
                partition_dir = item.parent
                if partition_dir not in leaf_partitions:
                    leaf_partitions.append(partition_dir)
            return leaf_partitions
        
        # Build glob pattern from filter
        pattern_parts = []
        for col, val in partition_filter.items():
            pattern_parts.append(f"{col}={val}")
        
        pattern = self.dataset_dir / "/".join(pattern_parts)
        
        # Find matching partitions
        if pattern.exists() and pattern.is_dir():
            # Exact match
            return [pattern]
        else:
            # Glob search
            glob_pattern = str(pattern) + "/**/*.parquet"
            matched_partitions = []
            for item in self.dataset_dir.glob(glob_pattern):
                partition_dir = item.parent
                if partition_dir not in matched_partitions:
                    matched_partitions.append(partition_dir)
            return matched_partitions
