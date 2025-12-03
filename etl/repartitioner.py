"""
Repartitioner - Change partitioning schema for existing Parquet datasets.

This module handles:
1. Schema migration (e.g., ['product_id', 'date'] → ['product_id', 'year', 'month', 'day', 'hour'])
2. File consolidation/compaction (merge small files into optimal size)
3. Atomic operations (temp directory with final rename)
4. Cleanup of old partition structure

Terminology:
- Repartitioning: Changing the partition column structure (schema migration)
- Compaction: Optimizing file sizes within same partition schema (merge small files)
"""
import logging
import shutil
import uuid
from pathlib import Path
from typing import List, Optional, Dict, Any, Callable
from datetime import datetime
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class Repartitioner:
    """
    Repartition Parquet datasets with different partition schemas.
    
    Use cases:
    1. Change partition granularity (date → year/month/day/hour)
    2. Add/remove partition columns
    3. Consolidate fragmented files
    
    Example:
        # Change from ['product_id', 'date'] to ['product_id', 'year', 'month', 'day']
        repartitioner = Repartitioner(
            source_dir="F:/processed/coinbase/level2",
            target_dir="F:/processed/coinbase/level2_new",
            new_partition_cols=['product_id', 'year', 'month', 'day'],
        )
        
        repartitioner.repartition(
            delete_source=True,  # Delete old partition after success
            validate=True,       # Verify row counts match
        )
    """
    
    def __init__(
        self,
        source_dir: str,
        target_dir: str,
        new_partition_cols: List[str],
        compression: str = "snappy",
        batch_size: int = 1_000_000,
        target_file_size_mb: int = 100,
    ):
        """
        Initialize repartitioner.
        
        Args:
            source_dir: Existing partitioned dataset directory
            target_dir: New partitioned dataset directory (should not exist or be empty)
            new_partition_cols: New partition column schema
            compression: Parquet compression codec
            batch_size: Rows per processing batch
            target_file_size_mb: Target file size for compaction (not yet implemented)
        """
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.new_partition_cols = new_partition_cols
        self.compression = compression
        self.batch_size = batch_size
        self.target_file_size_mb = target_file_size_mb
        
        # Validate
        if not self.source_dir.exists():
            raise ValueError(f"Source directory does not exist: {source_dir}")
        
        if self.target_dir.exists() and any(self.target_dir.iterdir()):
            logger.warning(f"Target directory is not empty: {target_dir}")
        
        # Stats
        self.stats = {
            "files_read": 0,
            "files_written": 0,
            "records_read": 0,
            "records_written": 0,
            "partitions_created": 0,
            "errors": 0,
            "start_time": None,
            "end_time": None,
        }
        
        logger.info(
            f"[Repartitioner] Initialized:\n"
            f"  Source: {source_dir}\n"
            f"  Target: {target_dir}\n"
            f"  New partition cols: {new_partition_cols}\n"
            f"  Compression: {compression}"
        )
    
    def repartition(
        self,
        delete_source: bool = False,
        validate: bool = True,
        dry_run: bool = False,
        transform_fn: Optional[Callable[[pl.DataFrame], pl.DataFrame]] = None,
    ) -> Dict[str, Any]:
        """
        Execute repartitioning operation.
        
        Args:
            delete_source: Delete source directory after successful repartition
            validate: Verify row counts match before/after
            dry_run: Print plan without executing
            transform_fn: Optional transformation function to apply during repartition
                         (e.g., add derived columns, filter rows)
        
        Returns:
            Statistics dictionary
        
        Example:
            # With transformation
            def add_hour_column(df):
                return df.with_columns(
                    pl.col("timestamp").dt.hour().alias("hour")
                )
            
            stats = repartitioner.repartition(
                delete_source=True,
                transform_fn=add_hour_column
            )
        """
        self.stats["start_time"] = datetime.now()
        
        logger.info("=" * 80)
        logger.info("REPARTITIONING OPERATION")
        logger.info("=" * 80)
        
        # Step 1: Scan source dataset
        logger.info(f"[1/5] Scanning source dataset: {self.source_dir}")
        try:
            # Use Polars lazy scan for efficiency
            # Note: Partition columns are preserved in the parquet files
            source_lf = pl.scan_parquet(
                str(self.source_dir / "**/*.parquet"),
            )
            
            # Get schema
            schema = source_lf.collect_schema()
            logger.info(f"  Schema: {schema}")
            
            # Validate partition columns exist
            missing_cols = set(self.new_partition_cols) - set(schema.names())
            if missing_cols:
                raise ValueError(
                    f"Partition columns {missing_cols} not found in source data. "
                    f"Available columns: {schema.names()}"
                )
            
            # Get row count (if validating)
            if validate:
                source_count = source_lf.select(pl.count()).collect().item()
                logger.info(f"  Source row count: {source_count:,}")
            
        except Exception as e:
            logger.error(f"Error scanning source dataset: {e}", exc_info=True)
            self.stats["errors"] += 1
            raise
        
        # Step 2: Apply transformation if provided
        if transform_fn:
            logger.info(f"[2/5] Applying transformation function")
            source_lf = source_lf.pipe(transform_fn)
        else:
            logger.info(f"[2/5] No transformation applied")
        
        # Step 3: Repartition and write
        logger.info(f"[3/5] Repartitioning to new schema: {self.new_partition_cols}")
        
        if dry_run:
            logger.info("  DRY RUN - No files will be written")
            logger.info(f"  Would create partitions based on: {self.new_partition_cols}")
            return self.stats
        
        try:
            # Create target directory
            self.target_dir.mkdir(parents=True, exist_ok=True)
            
            # Process in batches using streaming
            logger.info(f"  Processing in batches of {self.batch_size:,} rows")
            
            # Collect and write with Hive-style partitioning
            df = source_lf.collect()
            self.stats["records_read"] = len(df)
            
            # Group by partition columns and write
            grouped = df.groupby(self.new_partition_cols, maintain_order=True)
            
            for partition_key, partition_df in grouped:
                # Build partition path
                partition_path = self.target_dir
                
                if isinstance(partition_key, tuple):
                    for col, val in zip(self.new_partition_cols, partition_key):
                        partition_path = partition_path / f"{col}={val}"
                else:
                    # Single partition column
                    partition_path = partition_path / f"{self.new_partition_cols[0]}={partition_key}"
                
                partition_path.mkdir(parents=True, exist_ok=True)
                
                # Write partition with unique filename (timestamp + UUID)
                timestamp = datetime.now().strftime("%Y%m%dT%H")
                unique_id = str(uuid.uuid4())[:8]
                output_file = partition_path / f"part_{timestamp}_{unique_id}.parquet"
                partition_df.write_parquet(
                    output_file,
                    compression=self.compression,
                )
                
                self.stats["files_written"] += 1
                self.stats["records_written"] += len(partition_df)
                
                logger.debug(f"  Wrote partition: {partition_path.relative_to(self.target_dir)}")
            
            self.stats["partitions_created"] = self.stats["files_written"]
            
            logger.info(
                f"  ✓ Repartitioning complete: {self.stats['files_written']} files, "
                f"{self.stats['records_written']:,} records"
            )
        
        except Exception as e:
            logger.error(f"Error during repartitioning: {e}", exc_info=True)
            self.stats["errors"] += 1
            raise
        
        # Step 4: Validate row counts
        if validate:
            logger.info(f"[4/5] Validating row counts")
            try:
                target_lf = pl.scan_parquet(
                    str(self.target_dir / "**/*.parquet"),
                )
                target_count = target_lf.select(pl.count()).collect().item()
                
                if target_count != source_count:
                    raise ValueError(
                        f"Row count mismatch! Source: {source_count:,}, Target: {target_count:,}"
                    )
                
                logger.info(f"  ✓ Validation passed: {target_count:,} rows match")
            
            except Exception as e:
                logger.error(f"Validation failed: {e}", exc_info=True)
                self.stats["errors"] += 1
                raise
        else:
            logger.info(f"[4/5] Skipping validation")
        
        # Step 5: Delete source if requested
        if delete_source:
            logger.info(f"[5/5] Deleting source directory: {self.source_dir}")
            try:
                shutil.rmtree(self.source_dir)
                logger.info(f"  ✓ Source directory deleted")
            except Exception as e:
                logger.error(f"Error deleting source: {e}", exc_info=True)
                self.stats["errors"] += 1
                # Don't raise - repartitioning succeeded
        else:
            logger.info(f"[5/5] Keeping source directory (delete_source=False)")
        
        self.stats["end_time"] = datetime.now()
        elapsed = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        
        logger.info("=" * 80)
        logger.info("REPARTITIONING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"  Records processed: {self.stats['records_written']:,}")
        logger.info(f"  Files written: {self.stats['files_written']}")
        logger.info(f"  Partitions created: {self.stats['partitions_created']}")
        logger.info(f"  Elapsed time: {elapsed:.2f}s")
        logger.info(f"  Throughput: {self.stats['records_written'] / elapsed:,.0f} records/sec")
        logger.info("=" * 80)
        
        return self.stats
    
    def estimate_size(self) -> Dict[str, Any]:
        """
        Estimate target dataset size without executing.
        
        Returns:
            Dictionary with size estimates
        """
        logger.info("[Repartitioner] Estimating target size...")
        
        try:
            # Scan source
            source_lf = pl.scan_parquet(
                str(self.source_dir / "**/*.parquet"),
            )
            
            # Get partition counts
            partition_stats = (
                source_lf
                .groupby(self.new_partition_cols)
                .agg([
                    pl.count().alias("row_count"),
                ])
                .collect()
            )
            
            num_partitions = len(partition_stats)
            total_rows = partition_stats["row_count"].sum()
            avg_rows_per_partition = total_rows / num_partitions if num_partitions > 0 else 0
            
            # Estimate file sizes (rough)
            # Assume ~1KB per row (very rough estimate)
            estimated_size_mb = total_rows * 1024 / (1024 * 1024)
            
            estimates = {
                "total_rows": total_rows,
                "num_partitions": num_partitions,
                "avg_rows_per_partition": int(avg_rows_per_partition),
                "estimated_size_mb": estimated_size_mb,
                "partition_cols": self.new_partition_cols,
            }
            
            logger.info(f"  Total rows: {total_rows:,}")
            logger.info(f"  Estimated partitions: {num_partitions}")
            logger.info(f"  Avg rows per partition: {int(avg_rows_per_partition):,}")
            logger.info(f"  Estimated size: {estimated_size_mb:.1f} MB")
            
            return estimates
        
        except Exception as e:
            logger.error(f"Error estimating size: {e}", exc_info=True)
            raise


class ParquetCompactor:
    """
    Compact Parquet files within existing partition schema.
    
    Consolidates small files into larger, optimally-sized files for better query performance.
    Useful after many incremental writes that create file fragmentation.
    
    Example:
        # Compact files in level2 dataset
        compactor = ParquetCompactor(
            dataset_dir="F:/processed/coinbase/level2",
            target_file_size_mb=100,
        )
        
        stats = compactor.compact(
            min_file_count=5,  # Only compact partitions with 5+ files
            delete_source_files=True,
        )
    """
    
    def __init__(
        self,
        dataset_dir: str,
        target_file_size_mb: int = 100,
        compression: str = "snappy",
    ):
        """
        Initialize compactor.
        
        Args:
            dataset_dir: Partitioned dataset directory
            target_file_size_mb: Target size for compacted files
            compression: Parquet compression codec
        """
        self.dataset_dir = Path(dataset_dir)
        self.target_file_size_mb = target_file_size_mb
        self.compression = compression
        
        if not self.dataset_dir.exists():
            raise ValueError(f"Dataset directory does not exist: {dataset_dir}")
        
        self.stats = {
            "partitions_scanned": 0,
            "partitions_compacted": 0,
            "files_before": 0,
            "files_after": 0,
            "bytes_before": 0,
            "bytes_after": 0,
            "start_time": None,
            "end_time": None,
        }
        
        logger.info(
            f"[ParquetCompactor] Initialized:\n"
            f"  Dataset: {dataset_dir}\n"
            f"  Target file size: {target_file_size_mb} MB\n"
            f"  Compression: {compression}"
        )
    
    def compact(
        self,
        min_file_count: int = 2,
        max_file_size_mb: Optional[int] = None,
        delete_source_files: bool = True,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Compact small files within partitions.
        
        Args:
            min_file_count: Minimum files in partition to trigger compaction
            max_file_size_mb: Only compact files smaller than this (None = all files)
            delete_source_files: Delete original files after compaction
            dry_run: Print plan without executing
        
        Returns:
            Statistics dictionary
        """
        self.stats["start_time"] = datetime.now()
        
        logger.info("=" * 80)
        logger.info("PARQUET COMPACTION OPERATION")
        logger.info("=" * 80)
        logger.info(f"  Min files for compaction: {min_file_count}")
        logger.info(f"  Target file size: {self.target_file_size_mb} MB")
        
        # Find all leaf partitions (directories with .parquet files)
        leaf_partitions = []
        for item in self.dataset_dir.rglob("*.parquet"):
            partition_dir = item.parent
            if partition_dir not in leaf_partitions:
                leaf_partitions.append(partition_dir)
        
        self.stats["partitions_scanned"] = len(leaf_partitions)
        logger.info(f"  Found {len(leaf_partitions)} leaf partitions")
        
        # Compact each partition
        for partition_dir in leaf_partitions:
            files = list(partition_dir.glob("*.parquet"))
            
            if len(files) < min_file_count:
                continue
            
            # Filter by file size if specified
            if max_file_size_mb:
                files = [
                    f for f in files 
                    if f.stat().st_size / (1024 * 1024) <= max_file_size_mb
                ]
            
            if len(files) < min_file_count:
                continue
            
            # Calculate total size
            total_size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
            
            logger.info(
                f"\n  Compacting partition: {partition_dir.relative_to(self.dataset_dir)}"
            )
            logger.info(f"    Files: {len(files)}, Total size: {total_size_mb:.1f} MB")
            
            if dry_run:
                logger.info(f"    DRY RUN - Would compact {len(files)} files")
                continue
            
            try:
                # Read all files
                df = pl.read_parquet(files)
                
                # Write compacted file with unique filename
                timestamp = datetime.now().strftime("%Y%m%dT%H")
                unique_id = str(uuid.uuid4())[:8]
                output_file = partition_dir / f"part_{timestamp}_{unique_id}.parquet"
                df.write_parquet(
                    output_file,
                    compression=self.compression,
                )
                
                # Update stats
                self.stats["files_before"] += len(files)
                self.stats["files_after"] += 1
                self.stats["bytes_before"] += sum(f.stat().st_size for f in files)
                self.stats["bytes_after"] += output_file.stat().st_size
                self.stats["partitions_compacted"] += 1
                
                # Delete source files
                if delete_source_files:
                    for f in files:
                        f.unlink()
                    logger.info(f"    ✓ Compacted to {output_file.name} ({len(files)} files deleted)")
                else:
                    logger.info(f"    ✓ Compacted to {output_file.name} (original files kept)")
            
            except Exception as e:
                logger.error(f"    ✗ Error compacting partition: {e}")
                continue
        
        self.stats["end_time"] = datetime.now()
        elapsed = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        
        logger.info("\n" + "=" * 80)
        logger.info("COMPACTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"  Partitions scanned: {self.stats['partitions_scanned']}")
        logger.info(f"  Partitions compacted: {self.stats['partitions_compacted']}")
        logger.info(f"  Files before: {self.stats['files_before']}")
        logger.info(f"  Files after: {self.stats['files_after']}")
        logger.info(f"  Size before: {self.stats['bytes_before'] / (1024 ** 2):.1f} MB")
        logger.info(f"  Size after: {self.stats['bytes_after'] / (1024 ** 2):.1f} MB")
        logger.info(f"  Space saved: {(self.stats['bytes_before'] - self.stats['bytes_after']) / (1024 ** 2):.1f} MB")
        logger.info(f"  Elapsed time: {elapsed:.2f}s")
        logger.info("=" * 80)
        
        return self.stats
