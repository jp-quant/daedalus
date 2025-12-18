"""
Parquet Compaction - Consolidate small files into optimally-sized files.

Compacts Parquet files within existing partition schema for better query performance.
Useful after many incremental writes that create file fragmentation.

Storage Support:
- Local filesystem (default)
- AWS S3 via StorageBackend abstraction
- Any S3-compatible storage (MinIO, etc.)
"""
import io
import logging
import math
import uuid
from pathlib import Path
from typing import List, Optional, Dict, Any, TYPE_CHECKING
from datetime import datetime
import polars as pl

if TYPE_CHECKING:
    from storage.base import StorageBackend

logger = logging.getLogger(__name__)


class ParquetCompactor:
    """
    Compact Parquet files within existing partition schema.
    
    Consolidates small files into larger, optimally-sized files for better query performance.
    Useful after many incremental writes that create file fragmentation.
    
    Works with both local filesystem and S3 storage via StorageBackend.
    
    Example (Local):
        compactor = ParquetCompactor(
            dataset_dir="F:/processed/coinbase/level2",
            target_file_size_mb=100,
        )
        
        stats = compactor.compact(
            min_file_count=5,
            delete_source_files=True,
        )
    
    Example (S3):
        from storage.base import S3Storage
        
        storage = S3Storage(bucket="my-bucket", region="us-east-1")
        
        compactor = ParquetCompactor(
            dataset_dir="processed/ccxt/orderbook",
            target_file_size_mb=100,
            storage=storage,
        )
        
        stats = compactor.compact(min_file_count=5)
    """
    
    def __init__(
        self,
        dataset_dir: str,
        target_file_size_mb: int = 100,
        compression: str = "zstd",
        storage: Optional["StorageBackend"] = None,
    ):
        """
        Initialize compactor.
        
        Args:
            dataset_dir: Partitioned dataset directory (relative to storage root for S3)
            target_file_size_mb: Target size for compacted files
            compression: Parquet compression codec
            storage: Optional StorageBackend for cloud storage (S3). If None, uses local filesystem.
        """
        self.storage = storage
        self.target_file_size_mb = target_file_size_mb
        self.compression = compression
        
        if storage:
            # S3 or other cloud storage
            self.dataset_dir = dataset_dir
            self._is_local = False
            
            # Verify files exist
            files = storage.list_files(dataset_dir, pattern="**/*.parquet", recursive=True)
            if not files:
                raise ValueError(f"Dataset directory has no parquet files: {dataset_dir}")
        else:
            # Local filesystem
            self.dataset_dir = Path(dataset_dir)
            self._is_local = True
            
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
        
        backend_type = storage.backend_type if storage else "local"
        logger.info(
            f"[ParquetCompactor] Initialized:\n"
            f"  Backend: {backend_type}\n"
            f"  Dataset: {dataset_dir}\n"
            f"  Target file size: {target_file_size_mb} MB\n"
            f"  Compression: {compression}"
        )
    
    def _get_storage_options(self) -> Optional[Dict[str, Any]]:
        """Get storage options for Polars."""
        if self._is_local:
            return None
        return self.storage.get_storage_options()
    
    def _write_parquet(self, df: pl.DataFrame, path: str) -> int:
        """Write DataFrame to parquet and return file size."""
        if self._is_local:
            df.write_parquet(path, compression=self.compression)
            return Path(path).stat().st_size
        else:
            # S3: Write to buffer then upload
            buffer = io.BytesIO()
            df.write_parquet(buffer, compression=self.compression)
            size = buffer.tell()
            buffer.seek(0)
            self.storage.write_bytes(buffer.read(), path)
            return size
    
    def _delete_files(self, files: List[str]) -> None:
        """Delete files."""
        if self._is_local:
            for f in files:
                Path(f).unlink()
        else:
            for f in files:
                self.storage.delete(f)
    
    def compact(
        self,
        min_file_count: int = 2,
        max_file_size_mb: Optional[int] = None,
        target_file_count: Optional[int] = None,
        sort_by: Optional[List[str]] = None,
        delete_source_files: bool = True,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Compact small files within partitions.
        
        Args:
            min_file_count: Minimum files in partition to trigger compaction
            max_file_size_mb: Only compact files smaller than this (None = all files)
            target_file_count: Force specific number of output files (overrides target_size calculation)
            sort_by: Optional list of columns to sort data by before writing
            delete_source_files: Delete original files after compaction
            dry_run: Print plan without executing
        
        Returns:
            Statistics dictionary
        """
        self.stats["start_time"] = datetime.now()
        
        backend_type = self.storage.backend_type if self.storage else "local"
        storage_options = self._get_storage_options()
        
        logger.info("=" * 80)
        logger.info(f"PARQUET COMPACTION OPERATION [{backend_type.upper()}]")
        logger.info("=" * 80)
        logger.info(f"  Min files for compaction: {min_file_count}")
        if target_file_count:
            logger.info(f"  Target file count: {target_file_count} (Fixed)")
        else:
            logger.info(f"  Target file size: {self.target_file_size_mb} MB (Dynamic count)")
        
        if sort_by:
            if isinstance(sort_by, str):
                sort_by = [sort_by]
            logger.info(f"  Sorting by: {sort_by}")
        
        # Find all leaf partitions (directories with .parquet files)
        if self._is_local:
            leaf_partitions = []
            for item in self.dataset_dir.rglob("*.parquet"):
                partition_dir = item.parent
                if partition_dir not in leaf_partitions:
                    leaf_partitions.append(partition_dir)
            
            # Build partition info: { partition_dir: [(filepath, size), ...] }
            partition_files = {}
            for partition_dir in leaf_partitions:
                files_info = []
                for f in partition_dir.glob("*.parquet"):
                    files_info.append((str(f), f.stat().st_size))
                partition_files[str(partition_dir)] = files_info
        else:
            # S3: list all files and group by partition
            all_files = self.storage.list_files(self.dataset_dir, pattern="**/*.parquet", recursive=True)
            partition_files = {}
            for f in all_files:
                # Get partition directory (parent of file)
                parts = f["path"].rsplit("/", 1)
                partition_dir = parts[0] if len(parts) > 1 else self.dataset_dir
                if partition_dir not in partition_files:
                    partition_files[partition_dir] = []
                partition_files[partition_dir].append((f["path"], f["size"]))
            leaf_partitions = list(partition_files.keys())
        
        self.stats["partitions_scanned"] = len(leaf_partitions)
        logger.info(f"  Found {len(leaf_partitions)} leaf partitions")
        
        # Compact each partition
        for partition_dir in leaf_partitions:
            files_info = partition_files.get(str(partition_dir), [])
            
            if len(files_info) < min_file_count:
                continue
            
            # Filter by file size if specified
            if max_file_size_mb:
                files_info = [
                    (f, size) for f, size in files_info 
                    if size / (1024 * 1024) <= max_file_size_mb
                ]
            
            if len(files_info) < min_file_count:
                continue
            
            file_paths = [f for f, _ in files_info]
            file_sizes = [size for _, size in files_info]
            
            # Calculate total size
            total_size_mb = sum(file_sizes) / (1024 * 1024)
            
            # Determine output file count
            if target_file_count is not None:
                num_output_files = target_file_count
            else:
                num_output_files = math.ceil(total_size_mb / self.target_file_size_mb)
                num_output_files = max(1, num_output_files)
            
            if self._is_local:
                rel_partition = Path(partition_dir).relative_to(self.dataset_dir)
            else:
                rel_partition = partition_dir.replace(self.dataset_dir, "").lstrip("/")

            logger.info(f"\n  Compacting partition: {rel_partition}")
            logger.info(f"    Files: {len(file_paths)}, Total size: {total_size_mb:.1f} MB")
            logger.info(f"    Target: {num_output_files} file(s)")
            
            if dry_run:
                logger.info(f"    DRY RUN - Would compact {len(file_paths)} files into {num_output_files}")
                continue
            
            try:
                # Read all files
                read_kwargs = {}
                if storage_options:
                    read_kwargs["storage_options"] = storage_options
                    # For S3, get full paths
                    read_paths = [self.storage.get_full_path(f) for f in file_paths]
                else:
                    read_paths = file_paths
                
                df = pl.read_parquet(read_paths, **read_kwargs)
                total_rows = len(df)
                
                # Sort if requested
                if sort_by:
                    missing_cols = set(sort_by) - set(df.columns)
                    if missing_cols:
                        logger.warning(f"    ⚠️ Cannot sort by {sort_by}: columns {missing_cols} missing. Skipping sort.")
                    else:
                        logger.info(f"    Sorting data by {sort_by}...")
                        df = df.sort(sort_by)
                
                # Determine how to write output
                if num_output_files <= 1:
                    # Single file output
                    timestamp = datetime.now().strftime("%Y%m%dT%H")
                    unique_id = str(uuid.uuid4())[:8]
                    
                    if self._is_local:
                        output_file = str(Path(partition_dir) / f"part_{timestamp}_{unique_id}.parquet")
                    else:
                        output_file = self.storage.join_path(partition_dir, f"part_{timestamp}_{unique_id}.parquet")
                    
                    size = self._write_parquet(df, output_file)
                    
                    self.stats["files_after"] += 1
                    self.stats["bytes_after"] += size
                    logger.info(f"    ✓ Compacted to {Path(output_file).name if self._is_local else output_file.rsplit('/', 1)[-1]}")
                    
                else:
                    # Multiple file output
                    rows_per_file = math.ceil(total_rows / num_output_files)
                    logger.info(f"    Splitting {total_rows:,} rows into {num_output_files} files (~{rows_per_file:,} rows/file)")
                    
                    for i, chunk_df in enumerate(df.iter_slices(rows_per_file)):
                        timestamp = datetime.now().strftime("%Y%m%dT%H")
                        unique_id = str(uuid.uuid4())[:8]
                        
                        if self._is_local:
                            output_file = str(Path(partition_dir) / f"part_{timestamp}_{unique_id}_{i+1}.parquet")
                        else:
                            output_file = self.storage.join_path(partition_dir, f"part_{timestamp}_{unique_id}_{i+1}.parquet")
                        
                        size = self._write_parquet(chunk_df, output_file)
                        
                        self.stats["files_after"] += 1
                        self.stats["bytes_after"] += size
                    
                    logger.info(f"    ✓ Compacted to {num_output_files} files")

                # Update stats
                self.stats["files_before"] += len(file_paths)
                self.stats["bytes_before"] += sum(file_sizes)
                self.stats["partitions_compacted"] += 1
                
                # Delete source files
                if delete_source_files:
                    self._delete_files(file_paths)
                    logger.info(f"    ✓ Deleted {len(file_paths)} source files")
                else:
                    logger.info(f"    ✓ Kept original files")
            
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
