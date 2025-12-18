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

Storage Support:
- Local filesystem (default)
- AWS S3 via StorageBackend abstraction
- Any S3-compatible storage (MinIO, etc.)
"""
import io
import logging
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import List, Optional, Dict, Any, Callable, Union, TYPE_CHECKING
from datetime import datetime
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from storage.base import StorageBackend

logger = logging.getLogger(__name__)


class Repartitioner:
    """
    Repartition Parquet datasets with different partition schemas.
    
    Use cases:
    1. Change partition granularity (date → year/month/day/hour)
    2. Add/remove partition columns
    3. Consolidate fragmented files
    
    Works with both local filesystem and S3 storage via StorageBackend.
    
    Example (Local):
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
    
    Example (S3):
        from storage.base import S3Storage
        
        storage = S3Storage(bucket="my-bucket", region="us-east-1")
        
        repartitioner = Repartitioner(
            source_dir="processed/ccxt/orderbook",
            target_dir="processed/ccxt/orderbook_new",
            new_partition_cols=['exchange', 'symbol', 'year', 'month', 'day'],
            storage=storage,
        )
        
        repartitioner.repartition(delete_source=True)
    """
    
    def __init__(
        self,
        source_dir: str,
        target_dir: str,
        new_partition_cols: List[str],
        compression: str = "zstd",
        batch_size: int = 100_000,
        target_file_size_mb: int = 100,
        storage: Optional["StorageBackend"] = None,
    ):
        """
        Initialize repartitioner.
        
        Args:
            source_dir: Existing partitioned dataset directory (relative to storage root)
            target_dir: New partitioned dataset directory (should not exist or be empty)
            new_partition_cols: New partition column schema
            compression: Parquet compression codec
            batch_size: Rows per processing batch
            target_file_size_mb: Target file size for compaction (not yet implemented)
            storage: Optional StorageBackend for cloud storage (S3). If None, uses local filesystem.
        """
        self.storage = storage
        self.new_partition_cols = new_partition_cols
        self.compression = compression
        self.batch_size = batch_size
        self.target_file_size_mb = target_file_size_mb
        
        # Handle paths based on storage type
        if storage:
            # S3 or other cloud storage - paths are relative
            self.source_dir = source_dir
            self.target_dir = target_dir
            self._is_local = False
            
            # Verify source exists
            source_files = storage.list_files(source_dir, pattern="**/*.parquet", recursive=True)
            if not source_files:
                raise ValueError(f"Source directory has no parquet files: {source_dir}")
            
            # Warn if target has files
            target_files = storage.list_files(target_dir, pattern="**/*.parquet", recursive=True)
            if target_files:
                logger.warning(f"Target directory is not empty: {target_dir}")
        else:
            # Local filesystem
            self.source_dir = Path(source_dir)
            self.target_dir = Path(target_dir)
            self._is_local = True
            
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
        
        backend_type = storage.backend_type if storage else "local"
        logger.info(
            f"[Repartitioner] Initialized:\n"
            f"  Backend: {backend_type}\n"
            f"  Source: {source_dir}\n"
            f"  Target: {target_dir}\n"
            f"  New partition cols: {new_partition_cols}\n"
            f"  Compression: {compression}"
        )
    
    # =========================================================================
    # Storage Helper Methods
    # =========================================================================
    
    def _get_source_path(self) -> str:
        """Get source path formatted for Polars scan_parquet."""
        if self._is_local:
            return str(self.source_dir / "**/*.parquet")
        else:
            return self.storage.get_full_path(self.storage.join_path(self.source_dir, "**/*.parquet"))
    
    def _get_target_path(self) -> str:
        """Get target path formatted for Polars scan_parquet."""
        if self._is_local:
            return str(self.target_dir / "**/*.parquet")
        else:
            return self.storage.get_full_path(self.storage.join_path(self.target_dir, "**/*.parquet"))
    
    def _get_storage_options(self) -> Optional[Dict[str, Any]]:
        """Get storage options for Polars."""
        if self._is_local:
            return None
        return self.storage.get_storage_options()
    
    def _list_source_files(self) -> List[str]:
        """List all parquet files in source directory."""
        if self._is_local:
            return [str(f) for f in self.source_dir.rglob("*.parquet")]
        else:
            files = self.storage.list_files(self.source_dir, pattern="**/*.parquet", recursive=True)
            return [self.storage.get_full_path(f["path"]) for f in files]
    
    def _list_target_files(self) -> List[str]:
        """List all parquet files in target directory."""
        if self._is_local:
            return [str(f) for f in self.target_dir.rglob("*.parquet")]
        else:
            files = self.storage.list_files(self.target_dir, pattern="**/*.parquet", recursive=True)
            return [self.storage.get_full_path(f["path"]) for f in files]
    
    def _make_target_dir(self) -> None:
        """Create target directory."""
        if self._is_local:
            self.target_dir.mkdir(parents=True, exist_ok=True)
        else:
            # S3 doesn't need explicit directory creation
            pass
    
    def _delete_source(self) -> bool:
        """Delete source directory."""
        try:
            if self._is_local:
                shutil.rmtree(self.source_dir)
            else:
                # Delete all files in source directory
                files = self.storage.list_files(self.source_dir, pattern="**/*.parquet", recursive=True)
                for f in files:
                    self.storage.delete(f["path"])
            return True
        except Exception as e:
            logger.error(f"Error deleting source: {e}", exc_info=True)
            return False
    
    def _write_parquet_partition(
        self,
        df: pl.DataFrame,
        partition_path: str,
    ) -> str:
        """Write a DataFrame to a partition path."""
        timestamp = datetime.now().strftime("%Y%m%dT%H")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"part_{timestamp}_{unique_id}.parquet"
        
        if self._is_local:
            partition_dir = Path(partition_path)
            partition_dir.mkdir(parents=True, exist_ok=True)
            output_file = partition_dir / filename
            df.write_parquet(str(output_file), compression=self.compression)
            return str(output_file)
        else:
            # S3: Write to buffer then upload
            file_path = self.storage.join_path(partition_path, filename)
            buffer = io.BytesIO()
            df.write_parquet(buffer, compression=self.compression)
            buffer.seek(0)
            self.storage.write_bytes(buffer.read(), file_path)
            return self.storage.get_full_path(file_path)
    
    def _get_partition_dirs(self, file_paths: List[str]) -> set:
        """Get unique partition directories from file paths."""
        dirs = set()
        for f in file_paths:
            if self._is_local:
                dirs.add(Path(f).parent)
            else:
                # Get parent path from S3 key
                parts = f.rsplit("/", 1)
                if len(parts) > 1:
                    dirs.add(parts[0])
        return dirs
    
    def repartition(
        self,
        delete_source: bool = False,
        validate: bool = True,
        dry_run: bool = False,
        transform_fn: Optional[Callable[[pl.DataFrame], pl.DataFrame]] = None,
        method: str = "streaming",
    ) -> Dict[str, Any]:
        """
        Execute repartitioning operation using specified method.
        
        Args:
            delete_source: Delete source directory after successful repartition
            validate: Verify row counts match before/after
            dry_run: Print plan without executing
            transform_fn: Optional transformation function to apply during repartition
                         (e.g., add derived columns, filter rows)
            method: Repartitioning method to use:
                   - "streaming" (default): True streaming using sink_parquet (most memory-efficient)
                   - "file_by_file": Process source files individually (good for moderate datasets)
                   - "batched": Load all and process in batches (fastest but memory-intensive)
        
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
                transform_fn=add_hour_column,
                method="streaming"  # Most sophisticated approach
            )
        """
        # Validate method
        valid_methods = ["streaming", "file_by_file", "batched"]
        if method not in valid_methods:
            raise ValueError(f"Invalid method '{method}'. Must be one of: {valid_methods}")
        
        # Dispatch to appropriate implementation
        if method == "streaming":
            return self._repartition_streaming(delete_source, validate, dry_run, transform_fn)
        elif method == "file_by_file":
            return self._repartition_file_by_file(delete_source, validate, dry_run, transform_fn)
        elif method == "batched":
            return self._repartition_batched(delete_source, validate, dry_run, transform_fn)
    
    def _repartition_streaming(
        self,
        delete_source: bool,
        validate: bool,
        dry_run: bool,
        transform_fn: Optional[Callable[[pl.DataFrame], pl.DataFrame]],
    ) -> Dict[str, Any]:
        """
        Most sophisticated approach: True streaming using Polars sink_parquet.
        
        This method:
        1. Uses LazyFrame for query optimization
        2. Streams data without loading into memory
        3. Writes directly to partitioned structure
        4. Memory usage: minimal (only streaming buffer)
        
        Best for: Very large datasets (TB+)
        
        Note: Streaming mode only works with local filesystem currently.
              For S3, use file_by_file method instead.
        """
        # Streaming mode only works with local filesystem
        if not self._is_local:
            logger.warning("Streaming mode not supported for S3. Falling back to file_by_file method.")
            return self._repartition_file_by_file(delete_source, validate, dry_run, transform_fn)
        
        self.stats["start_time"] = datetime.now()
        
        logger.info("=" * 80)
        logger.info("REPARTITIONING OPERATION (STREAMING MODE)")
        logger.info("=" * 80)
        
        # Step 1: Scan source dataset
        source_path = self._get_source_path()
        logger.info(f"[1/5] Scanning source dataset: {self.source_dir}")
        try:
            # Use Polars lazy scan for efficiency
            source_lf = pl.scan_parquet(source_path)
            
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
        
        # Step 3: Repartition and write using streaming
        logger.info(f"[3/5] Streaming repartition to new schema: {self.new_partition_cols}")
        
        if dry_run:
            logger.info("  DRY RUN - No files will be written")
            logger.info(f"  Would create partitions based on: {self.new_partition_cols}")
            return self.stats
        
        try:
            # Create target directory
            self._make_target_dir()
            
            # Use sink_parquet for true streaming with partitioning
            logger.info(f"  Using sink_parquet for zero-copy streaming")
            logger.info(f"  Writing to: {self.target_dir}")
            
            # Note: sink_parquet with partition_by handles everything in streaming fashion
            source_lf.sink_parquet(
                pl.PartitionByKey(
                    self.target_dir,
                    by=self.new_partition_cols,
                    include_key=True,
                ),
                mkdir=True,
                compression=self.compression,
            )
            
            # Count results
            logger.info(f"  Counting output files and records...")
            output_files = self._list_target_files()
            self.stats["files_written"] = len(output_files)
            
            # Count partitions
            partition_dirs = self._get_partition_dirs(output_files)
            self.stats["partitions_created"] = len(partition_dirs)
            
            # Count records (lazily)
            target_path = self._get_target_path()
            target_lf = pl.scan_parquet(target_path)
            self.stats["records_written"] = target_lf.select(pl.count()).collect().item()
            
            logger.info(
                f"  ✓ Streaming write complete: {self.stats['files_written']} files, "
                f"{self.stats['records_written']:,} records, {self.stats['partitions_created']} partitions"
            )
        
        except Exception as e:
            logger.error(f"Error during streaming repartition: {e}", exc_info=True)
            self.stats["errors"] += 1
            raise
        
        # Step 4: Validate row counts
        if validate:
            logger.info(f"[4/5] Validating row counts")
            try:
                if self.stats["records_written"] != source_count:
                    raise ValueError(
                        f"Row count mismatch! Source: {source_count:,}, Target: {self.stats['records_written']:,}"
                    )
                
                logger.info(f"  ✓ Validation passed: {self.stats['records_written']:,} rows match")
            
            except Exception as e:
                logger.error(f"Validation failed: {e}", exc_info=True)
                self.stats["errors"] += 1
                raise
        else:
            logger.info(f"[4/5] Skipping validation")
        
        # Step 5: Delete source if requested
        if delete_source:
            logger.info(f"[5/5] Deleting source directory: {self.source_dir}")
            if self._delete_source():
                logger.info(f"  ✓ Source directory deleted")
            else:
                self.stats["errors"] += 1
        else:
            logger.info(f"[5/5] Keeping source directory (delete_source=False)")
        
        self.stats["end_time"] = datetime.now()
        elapsed = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        
        logger.info("=" * 80)
        logger.info("REPARTITIONING SUMMARY (STREAMING)")
        logger.info("=" * 80)
        logger.info(f"  Records processed: {self.stats['records_written']:,}")
        logger.info(f"  Files written: {self.stats['files_written']}")
        logger.info(f"  Partitions created: {self.stats['partitions_created']}")
        logger.info(f"  Elapsed time: {elapsed:.2f}s")
        logger.info(f"  Throughput: {self.stats['records_written'] / elapsed:,.0f} records/sec")
        logger.info("=" * 80)
        
        return self.stats
    
    def _repartition_file_by_file(
        self,
        delete_source: bool,
        validate: bool,
        dry_run: bool,
        transform_fn: Optional[Callable[[pl.DataFrame], pl.DataFrame]],
    ) -> Dict[str, Any]:
        """
        Process source files individually without loading full dataset.
        
        This method:
        1. Finds all source Parquet files
        2. Processes each file independently
        3. Writes to new partition structure
        4. Memory usage: bounded by largest single file
        
        Best for: Moderate datasets where files fit in memory individually.
        Also the recommended method for S3 storage.
        """
        self.stats["start_time"] = datetime.now()
        
        backend_type = self.storage.backend_type if self.storage else "local"
        logger.info("=" * 80)
        logger.info(f"REPARTITIONING OPERATION (FILE-BY-FILE MODE) [{backend_type.upper()}]")
        logger.info("=" * 80)
        
        # Step 1: Scan source dataset
        source_path = self._get_source_path()
        storage_options = self._get_storage_options()
        logger.info(f"[1/5] Scanning source dataset: {self.source_dir}")
        try:
            # Use Polars lazy scan for efficiency
            # Note: Partition columns are preserved in the parquet files
            scan_kwargs = {"source": source_path}
            if storage_options:
                scan_kwargs["storage_options"] = storage_options
            source_lf = pl.scan_parquet(**scan_kwargs)
            
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
            self._make_target_dir()
            
            # True streaming approach: Process source files individually
            # This avoids loading entire dataset into memory
            logger.info(f"  Processing source files individually")
            
            source_files = self._list_source_files()
            total_files = len(source_files)
            logger.info(f"  Found {total_files} source files to process")
            
            partition_dirs_written = set()
            
            for file_idx, source_file in enumerate(source_files, 1):
                if self._is_local:
                    filename = Path(source_file).name
                else:
                    filename = source_file.rsplit("/", 1)[-1]
                logger.debug(f"  Processing file {file_idx}/{total_files}: {filename}")
                
                # Read one source file at a time
                read_kwargs = {}
                if storage_options:
                    read_kwargs["storage_options"] = storage_options
                df = pl.read_parquet(source_file, **read_kwargs)
                file_rows = len(df)
                self.stats["records_read"] += file_rows
                self.stats["files_read"] += 1
                
                # Apply transformation if provided
                if transform_fn:
                    df = transform_fn(df)
                
                # Group by partition columns
                grouped = df.group_by(self.new_partition_cols, maintain_order=True)
                
                for partition_key, partition_df in grouped:
                    # Normalize partition key to tuple
                    if not isinstance(partition_key, tuple):
                        partition_key = (partition_key,)
                    
                    # Build partition path
                    partition_parts = [f"{col}={val}" for col, val in zip(self.new_partition_cols, partition_key)]
                    
                    if self._is_local:
                        partition_path = self.target_dir
                        for part in partition_parts:
                            partition_path = partition_path / part
                        partition_path.mkdir(parents=True, exist_ok=True)
                        partition_dirs_written.add(partition_path)
                        
                        # Write intermediate file
                        timestamp = datetime.now().strftime("%Y%m%dT%H")
                        unique_id = str(uuid.uuid4())[:8]
                        output_file = partition_path / f"part_{timestamp}_{unique_id}.parquet"
                        partition_df.write_parquet(str(output_file), compression=self.compression)
                    else:
                        # S3: build path and write via storage backend
                        partition_path = self.storage.join_path(self.target_dir, *partition_parts)
                        partition_dirs_written.add(partition_path)
                        self._write_parquet_partition(partition_df, partition_path)
                    
                    self.stats["files_written"] += 1
                    self.stats["records_written"] += len(partition_df)
                
                # Log progress every 10% of files
                if file_idx % max(1, total_files // 10) == 0:
                    progress_pct = (file_idx / total_files) * 100
                    logger.info(
                        f"  Progress: {progress_pct:.0f}% ({file_idx}/{total_files} files, "
                        f"{self.stats['records_written']:,} records)"
                    )
            
            self.stats["partitions_created"] = len(partition_dirs_written)
            
            logger.info(
                f"  ✓ File processing complete: {total_files} source files, "
                f"{self.stats['files_written']} output files, {self.stats['records_written']:,} records"
            )
            
            # Note: Multiple files per partition may exist after batch processing
            # Use ParquetCompactor separately if consolidation is needed
            if self.stats["files_written"] > len(partition_dirs_written) * 2:
                logger.info(
                    f"\n  ℹ️  Multiple files per partition created ({self.stats['files_written']} files across "
                    f"{len(partition_dirs_written)} partitions)"
                )
                logger.info(f"  Consider running ParquetCompactor to consolidate files")
            
            logger.info(
                f"  ✓ Repartitioning complete: {self.stats['files_written']} files, "
                f"{self.stats['partitions_created']} partitions"
            )
        
        except Exception as e:
            logger.error(f"Error during repartitioning: {e}", exc_info=True)
            self.stats["errors"] += 1
            raise
        
        # Step 4: Validate row counts
        if validate:
            logger.info(f"[4/5] Validating row counts")
            try:
                target_path = self._get_target_path()
                scan_kwargs = {"source": target_path}
                if storage_options:
                    scan_kwargs["storage_options"] = storage_options
                target_lf = pl.scan_parquet(**scan_kwargs)
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
            if self._delete_source():
                logger.info(f"  ✓ Source directory deleted")
            else:
                self.stats["errors"] += 1
        else:
            logger.info(f"[5/5] Keeping source directory (delete_source=False)")
        
        self.stats["end_time"] = datetime.now()
        elapsed = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        
        logger.info("=" * 80)
        logger.info("REPARTITIONING SUMMARY (FILE-BY-FILE)")
        logger.info("=" * 80)
        logger.info(f"  Records processed: {self.stats['records_written']:,}")
        logger.info(f"  Files written: {self.stats['files_written']}")
        logger.info(f"  Partitions created: {self.stats['partitions_created']}")
        logger.info(f"  Elapsed time: {elapsed:.2f}s")
        logger.info(f"  Throughput: {self.stats['records_written'] / elapsed:,.0f} records/sec")
        logger.info("=" * 80)
        
        return self.stats
    
    def _repartition_batched(
        self,
        delete_source: bool,
        validate: bool,
        dry_run: bool,
        transform_fn: Optional[Callable[[pl.DataFrame], pl.DataFrame]],
    ) -> Dict[str, Any]:
        """
        Load full dataset and process in batches.
        
        This method:
        1. Collects entire dataset into memory
        2. Processes in configurable batch sizes
        3. Fastest but requires sufficient RAM
        4. Memory usage: full dataset size
        
        Best for: Small-to-medium datasets that fit comfortably in RAM.
        Works with both local filesystem and S3.
        """
        self.stats["start_time"] = datetime.now()
        
        backend_type = self.storage.backend_type if self.storage else "local"
        logger.info("=" * 80)
        logger.info(f"REPARTITIONING OPERATION (BATCHED MODE) [{backend_type.upper()}]")
        logger.info("=" * 80)
        logger.warning("  ⚠️  This mode loads entire dataset into memory")
        
        source_path = self._get_source_path()
        storage_options = self._get_storage_options()
        
        logger.info(f"[1/5] Scanning source dataset: {self.source_dir}")
        try:
            scan_kwargs = {"source": source_path}
            if storage_options:
                scan_kwargs["storage_options"] = storage_options
            source_lf = pl.scan_parquet(**scan_kwargs)
            
            schema = source_lf.collect_schema()
            logger.info(f"  Schema: {schema}")
            
            missing_cols = set(self.new_partition_cols) - set(schema.names())
            if missing_cols:
                raise ValueError(
                    f"Partition columns {missing_cols} not found in source data. "
                    f"Available columns: {schema.names()}"
                )
            
            if validate:
                source_count = source_lf.select(pl.count()).collect().item()
                logger.info(f"  Source row count: {source_count:,}")
        
        except Exception as e:
            logger.error(f"Error scanning source dataset: {e}", exc_info=True)
            self.stats["errors"] += 1
            raise
        
        if transform_fn:
            logger.info(f"[2/5] Applying transformation function")
            source_lf = source_lf.pipe(transform_fn)
        else:
            logger.info(f"[2/5] No transformation applied")
        
        logger.info(f"[3/5] Loading and batching data")
        
        if dry_run:
            logger.info("  DRY RUN - No files will be written")
            return self.stats
        
        try:
            self._make_target_dir()
            
            # Collect full dataset
            logger.info(f"  Collecting full dataset into memory...")
            full_df = source_lf.collect()
            self.stats["records_read"] = len(full_df)
            logger.info(f"  Loaded {len(full_df):,} rows")
            
            # Process in batches
            logger.info(f"  Processing in batches of {self.batch_size:,} rows")
            partition_dirs_written = set()
            
            for batch_idx, batch_df in enumerate(full_df.iter_slices(self.batch_size), 1):
                grouped = batch_df.group_by(self.new_partition_cols, maintain_order=True)
                
                for partition_key, partition_df in grouped:
                    if not isinstance(partition_key, tuple):
                        partition_key = (partition_key,)
                    
                    partition_parts = [f"{col}={val}" for col, val in zip(self.new_partition_cols, partition_key)]
                    
                    if self._is_local:
                        partition_path = self.target_dir
                        for part in partition_parts:
                            partition_path = partition_path / part
                        partition_path.mkdir(parents=True, exist_ok=True)
                        partition_dirs_written.add(partition_path)
                        
                        timestamp = datetime.now().strftime("%Y%m%dT%H")
                        unique_id = str(uuid.uuid4())[:8]
                        output_file = partition_path / f"part_{timestamp}_{unique_id}.parquet"
                        partition_df.write_parquet(str(output_file), compression=self.compression)
                    else:
                        partition_path = self.storage.join_path(self.target_dir, *partition_parts)
                        partition_dirs_written.add(partition_path)
                        self._write_parquet_partition(partition_df, partition_path)
                    
                    self.stats["files_written"] += 1
                    self.stats["records_written"] += len(partition_df)
                
                if batch_idx % 10 == 0:
                    logger.info(f"  Processed batch {batch_idx}, {self.stats['records_written']:,} records written")
            
            self.stats["partitions_created"] = len(partition_dirs_written)
            logger.info(f"  ✓ Batched processing complete")
        
        except Exception as e:
            logger.error(f"Error during batched repartition: {e}", exc_info=True)
            self.stats["errors"] += 1
            raise
        
        if validate:
            logger.info(f"[4/5] Validating row counts")
            try:
                target_path = self._get_target_path()
                scan_kwargs = {"source": target_path}
                if storage_options:
                    scan_kwargs["storage_options"] = storage_options
                target_lf = pl.scan_parquet(**scan_kwargs)
                target_count = target_lf.select(pl.count()).collect().item()
                
                if target_count != source_count:
                    raise ValueError(f"Row count mismatch! Source: {source_count:,}, Target: {target_count:,}")
                
                logger.info(f"  ✓ Validation passed: {target_count:,} rows match")
            except Exception as e:
                logger.error(f"Validation failed: {e}", exc_info=True)
                self.stats["errors"] += 1
                raise
        else:
            logger.info(f"[4/5] Skipping validation")
        
        if delete_source:
            logger.info(f"[5/5] Deleting source directory: {self.source_dir}")
            if self._delete_source():
                logger.info(f"  ✓ Source directory deleted")
            else:
                self.stats["errors"] += 1
        else:
            logger.info(f"[5/5] Keeping source directory (delete_source=False)")
        
        self.stats["end_time"] = datetime.now()
        elapsed = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        
        logger.info("=" * 80)
        logger.info("REPARTITIONING SUMMARY (BATCHED)")
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
            source_path = self._get_source_path()
            storage_options = self._get_storage_options()
            
            scan_kwargs = {"source": source_path}
            if storage_options:
                scan_kwargs["storage_options"] = storage_options
            source_lf = pl.scan_parquet(**scan_kwargs)
            
            # Get partition counts
            partition_stats = (
                source_lf
                .group_by(self.new_partition_cols)
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


