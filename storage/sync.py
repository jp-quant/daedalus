"""
Storage synchronization module for Daedalus.

Provides robust file synchronization between storage backends (local <-> S3).
Supports bidirectional sync with optional compaction before upload.
"""
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable, Literal, TYPE_CHECKING
from dataclasses import dataclass, field
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from storage.base import StorageBackend

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    # Fallback no-op tqdm
    class tqdm:
        def __init__(self, iterable=None, **kwargs):
            self.iterable = iterable
            self.n = 0
            self.total = kwargs.get('total', 0)
        
        def __iter__(self):
            return iter(self.iterable) if self.iterable else iter([])
        
        def __enter__(self):
            return self
        
        def __exit__(self, *args):
            pass
        
        def update(self, n=1):
            self.n += n
        
        def set_description(self, desc):
            pass
        
        def set_postfix(self, ordered_dict=None, refresh=True, **kwargs):
            pass
        
        def close(self):
            pass

# Type checking imports for better IDE support
if TYPE_CHECKING:
    from storage.base import S3Storage

logger = logging.getLogger(__name__)


@dataclass
class SyncStats:
    """Statistics from a sync operation."""
    files_found: int = 0
    files_transferred: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    bytes_transferred: int = 0
    duration_seconds: float = 0.0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def __str__(self) -> str:
        mb = self.bytes_transferred / (1024 * 1024)
        return (
            f"SyncStats(transferred={self.files_transferred}, "
            f"skipped={self.files_skipped}, failed={self.files_failed}, "
            f"bytes={mb:.2f}MB, duration={self.duration_seconds:.1f}s)"
        )


class StorageSync:
    """
    Synchronize files between storage backends.
    
    Supports:
    - Local → S3 (upload)
    - S3 → Local (download)
    - S3 → S3 (copy between buckets/regions)
    - Optional file deletion after successful transfer
    - Parallel transfers for performance
    - Pattern filtering
    - Pre-transfer hooks (e.g., for compaction)
    
    Example:
        from storage.factory import create_etl_storage_input, create_etl_storage_output
        
        sync = StorageSync(
            source=create_etl_storage_input(config),  # local
            destination=create_etl_storage_output(config),  # S3
        )
        
        # Sync processed parquet files
        stats = sync.sync(
            source_path="processed/ccxt/ticker/",
            dest_path="processed/ccxt/ticker/",
            pattern="**/*.parquet",
            delete_after_transfer=True,
            max_workers=10,
        )
    """
    
    def __init__(
        self,
        source: StorageBackend,
        destination: StorageBackend,
    ):
        """
        Initialize storage sync.
        
        Args:
            source: Source storage backend
            destination: Destination storage backend
        """
        self.source = source
        self.destination = destination
        
        logger.info(
            f"[StorageSync] Initialized: "
            f"{source.backend_type}:{source.base_path} → "
            f"{destination.backend_type}:{destination.base_path}"
        )
    
    def sync(
        self,
        source_path: str,
        dest_path: Optional[str] = None,
        pattern: str = "**/*",
        recursive_list_files: bool = True,
        delete_after_transfer: bool = False,
        max_workers: int = 5,
        skip_existing: bool = False,
        dry_run: bool = False,
        on_file_transferred: Optional[Callable[[str, str], None]] = None,
        on_file_failed: Optional[Callable[[str, Exception], None]] = None,
    ) -> SyncStats:
        """
        Synchronize files from source to destination.
        
        Args:
            source_path: Path in source storage to sync from
            dest_path: Path in destination storage. If None, uses source_path.
            pattern: Glob pattern for files to sync (default: all files)
            recursive_list_files: Recursively list files in subdirectories (default: True for partitioned data)
            delete_after_transfer: Delete source files after successful transfer
            max_workers: Number of parallel transfer threads
            skip_existing: Skip files that already exist at destination
            dry_run: If True, only simulate without actual transfers
            on_file_transferred: Callback after each successful transfer
            on_file_failed: Callback after each failed transfer
        
        Returns:
            SyncStats with transfer details
        """
        start_time = time.time()
        dest_path = dest_path or source_path
        
        stats = SyncStats()
        
        # List source files
        logger.info(f"[StorageSync] Listing files in {source_path} with pattern {pattern}")
        source_files = self.source.list_files(source_path, pattern=pattern, recursive=recursive_list_files)
        stats.files_found = len(source_files)
        
        if not source_files:
            logger.info(f"[StorageSync] No files found matching pattern")
            stats.duration_seconds = time.time() - start_time
            return stats
        
        logger.info(f"[StorageSync] Found {len(source_files)} files to sync")
        
        if dry_run:
            logger.info("[StorageSync] DRY RUN - no files will be transferred")
            for f in source_files:
                logger.info(f"  Would transfer: {f['path']} ({f['size']} bytes)")
            stats.files_skipped = len(source_files)
            stats.duration_seconds = time.time() - start_time
            return stats
        
        # Build transfer tasks
        tasks = []
        for file_info in source_files:
            src_file_path = file_info['path']
            
            # Calculate destination path
            # Remove source_path prefix and add dest_path prefix
            if src_file_path.startswith(source_path):
                relative_path = src_file_path[len(source_path):].lstrip('/')
            else:
                relative_path = src_file_path.split('/')[-1]
            
            dst_file_path = self.destination.join_path(dest_path, relative_path)
            
            tasks.append({
                'src_path': src_file_path,
                'dst_path': dst_file_path,
                'size': file_info.get('size', 0),
            })
        
        # Filter out existing files if skip_existing
        if skip_existing:
            logger.info("[StorageSync] Checking for existing files at destination...")
            
            # Efficient: List all destination files once instead of checking each file individually
            # This reduces N API calls to 1 API call for S3
            try:
                existing_files = self.destination.list_files(
                    dest_path, 
                    pattern=pattern, 
                    recursive=recursive_list_files
                )
                existing_paths = {f['path'] for f in existing_files}
                logger.info(f"[StorageSync] Found {len(existing_paths)} existing files at destination")
            except Exception as e:
                logger.warning(f"[StorageSync] Failed to list destination files, falling back to individual checks: {e}")
                existing_paths = None
            
            tasks_to_transfer = []
            if existing_paths is not None:
                # Fast path: O(1) set lookup per file
                for task in tasks:
                    if task['dst_path'] not in existing_paths:
                        tasks_to_transfer.append(task)
                    else:
                        stats.files_skipped += 1
            else:
                # Fallback: Individual existence checks (slow but reliable)
                for task in tqdm(tasks, desc="Checking existing", unit="file", disable=not TQDM_AVAILABLE):
                    if not self.destination.exists(task['dst_path']):
                        tasks_to_transfer.append(task)
                    else:
                        stats.files_skipped += 1
            
            tasks = tasks_to_transfer
            logger.info(f"[StorageSync] {stats.files_skipped} files already exist, {len(tasks)} to transfer")
        
        if not tasks:
            logger.info("[StorageSync] No files to transfer")
            stats.duration_seconds = time.time() - start_time
            return stats
        
        # Calculate total bytes to transfer
        total_bytes = sum(task['size'] for task in tasks)
        total_mb = total_bytes / (1024 * 1024)
        
        logger.info(f"[StorageSync] Transferring {len(tasks)} files ({total_mb:.2f} MB)")
        
        # Execute transfers in parallel with progress bar
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            
            for task in tasks:
                future = executor.submit(
                    self._transfer_file,
                    task['src_path'],
                    task['dst_path'],
                    delete_after_transfer,
                )
                futures[future] = task
            
            # Process results with progress bar
            with tqdm(
                total=len(tasks),
                desc="Transferring files",
                unit="file",
                unit_scale=False,
                disable=not TQDM_AVAILABLE,
            ) as pbar:
                transferred_bytes = 0
                
                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        result = future.result()
                        if result['success']:
                            stats.files_transferred += 1
                            stats.bytes_transferred += task['size']
                            transferred_bytes += task['size']
                            
                            # Update progress bar with transfer stats
                            pbar.set_postfix({
                                'MB': f"{transferred_bytes / (1024 * 1024):.1f}/{total_mb:.1f}",
                                'failed': stats.files_failed
                            })
                            pbar.update(1)
                            
                            if on_file_transferred:
                                on_file_transferred(task['src_path'], task['dst_path'])
                        else:
                            stats.files_failed += 1
                            stats.errors.append({
                                'path': task['src_path'],
                                'error': result.get('error', 'Unknown error'),
                            })
                            pbar.update(1)
                            
                            if on_file_failed:
                                on_file_failed(task['src_path'], Exception(result.get('error')))
                    
                    except Exception as e:
                        stats.files_failed += 1
                        stats.errors.append({
                            'path': task['src_path'],
                            'error': str(e),
                        })
                        logger.error(f"[StorageSync] Transfer failed for {task['src_path']}: {e}")
                        pbar.update(1)
                        
                        if on_file_failed:
                            on_file_failed(task['src_path'], e)
        
        stats.duration_seconds = time.time() - start_time
        
        logger.info(f"[StorageSync] Sync complete: {stats}")
        
        return stats
    
    def _transfer_file(
        self,
        src_path: str,
        dst_path: str,
        delete_after: bool,
    ) -> Dict[str, Any]:
        """
        Transfer a single file from source to destination.
        
        Handles all combinations of storage backends:
        - Local → S3: Upload via write_file
        - S3 → Local: Download bytes and write
        - Local → Local: Copy
        - S3 → S3: Copy via S3 copy_object
        """
        try:
            src_type = self.source.backend_type
            dst_type = self.destination.backend_type
            
            if src_type == "local" and dst_type == "s3":
                # Local → S3: Direct upload
                local_path = self.source.get_full_path(src_path)
                self.destination.write_file(local_path, dst_path)
                
            elif src_type == "s3" and dst_type == "local":
                # S3 → Local: Download and write
                data = self.source.read_bytes(src_path)
                self.destination.write_bytes(data, dst_path)
                
            elif src_type == "local" and dst_type == "local":
                # Local → Local: Copy
                src_full = self.source.get_full_path(src_path)
                self.destination.write_file(src_full, dst_path)
                
            elif src_type == "s3" and dst_type == "s3":
                # S3 → S3: Use S3 copy (more efficient than download/upload)
                self._s3_to_s3_copy(src_path, dst_path)
                
            else:
                # Generic fallback: read bytes and write
                data = self.source.read_bytes(src_path)
                self.destination.write_bytes(data, dst_path)
            
            # Delete source after successful transfer
            if delete_after:
                self.source.delete(src_path)
            
            return {'success': True, 'path': src_path}
        
        except Exception as e:
            return {'success': False, 'path': src_path, 'error': str(e)}
    
    def _s3_to_s3_copy(self, src_path: str, dst_path: str):
        """
        Copy file between S3 locations using S3 copy_object.
        
        More efficient than download/upload for S3-to-S3 transfers.
        """
        # Import S3Storage for type checking at runtime
        from storage.base import S3Storage
        
        if not isinstance(self.source, S3Storage) or not isinstance(self.destination, S3Storage):
            raise TypeError("S3-to-S3 copy requires both source and destination to be S3Storage")
        
        src_bucket = self.source.bucket
        src_key = self.source._get_s3_key(src_path)
        dst_bucket = self.destination.bucket
        dst_key = self.destination._get_s3_key(dst_path)
        
        copy_source = {'Bucket': src_bucket, 'Key': src_key}
        
        # Use destination client for the copy
        self.destination.s3_client.copy_object(
            CopySource=copy_source,
            Bucket=dst_bucket,
            Key=dst_key,
        )
    
    def sync_with_compaction(
        self,
        source_path: str,
        dest_path: Optional[str] = None,
        pattern: str = "**/*.parquet",
        compact_before_upload: bool = True,
        target_file_size_mb: int = 100,
        min_file_count: int = 2,
        delete_after_transfer: bool = True,
        max_workers: int = 5,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Sync with optional compaction of Parquet files before upload.
        
        This is optimized for the workflow:
        1. ETL processes data locally (fast)
        2. This job compacts partitions to larger files
        3. Uploads compacted files to S3
        4. Deletes local files after successful upload
        
        Args:
            source_path: Path to parquet dataset (e.g., "processed/ccxt/ticker/")
            dest_path: Destination path (default: same as source)
            pattern: File pattern (default: parquet files)
            compact_before_upload: Run compaction before uploading
            target_file_size_mb: Target file size for compaction
            min_file_count: Minimum files in partition to trigger compaction
            delete_after_transfer: Delete source files after upload
            max_workers: Parallel upload threads
            dry_run: Simulate without actual changes
        
        Returns:
            Dict with compaction_stats and sync_stats
        """
        dest_path = dest_path or source_path
        result: Dict[str, Any] = {
            'compaction_stats': None,
            'sync_stats': None,
        }
        
        # Step 1: Compaction (optional)
        if compact_before_upload:
            logger.info(f"[StorageSync] Running compaction on {source_path}")
            
            # Only compact if source is local (compaction reads/writes to same storage)
            if self.source.backend_type == "local":
                try:
                    from etl.repartitioner import ParquetCompactor
                    
                    full_source_path = self.source.get_full_path(source_path)
                    
                    compactor = ParquetCompactor(
                        dataset_dir=full_source_path,
                        target_file_size_mb=target_file_size_mb,
                    )
                    
                    compaction_stats = compactor.compact(
                        min_file_count=min_file_count,
                        target_file_count=1,
                        delete_source_files=True,
                        dry_run=dry_run,
                    )
                    
                    result['compaction_stats'] = compaction_stats
                    logger.info(f"[StorageSync] Compaction complete: {compaction_stats}")
                    
                except ImportError:
                    logger.warning("[StorageSync] ParquetCompactor not available, skipping compaction")
                except Exception as e:
                    logger.error(f"[StorageSync] Compaction failed: {e}")
            else:
                logger.warning("[StorageSync] Compaction only supported for local source storage")
        
        # Step 2: Sync to destination
        logger.info(f"[StorageSync] Starting sync to destination")
        
        sync_stats = self.sync(
            source_path=source_path,
            dest_path=dest_path,
            pattern=pattern,
            delete_after_transfer=delete_after_transfer,
            max_workers=max_workers,
            dry_run=dry_run,
        )
        
        result['sync_stats'] = sync_stats
        
        return result


class StorageSyncJob:
    """
    Configurable sync job that can be run as a scheduled task.
    
    Designed for production use with multiple source paths,
    configurable schedules, and comprehensive logging.
    
    Example:
        job = StorageSyncJob(
            source=local_storage,
            destination=s3_storage,
            paths=[
                {"source": "processed/ccxt/ticker/", "compact": True},
                {"source": "processed/ccxt/trades/", "compact": True},
                {"source": "processed/ccxt/orderbook/", "compact": True},
            ],
        )
        
        # Run once
        results = job.run()
        
        # Or run continuously
        await job.run_scheduled(interval_seconds=300)
    """
    
    def __init__(
        self,
        source: StorageBackend,
        destination: StorageBackend,
        paths: List[Dict[str, Any]],
        default_pattern: str = "**/*.parquet",
        default_compact: bool = True,
        default_delete_after: bool = True,
        target_file_size_mb: int = 100,
        max_workers: int = 5,
    ):
        """
        Initialize sync job.
        
        Args:
            source: Source storage backend
            destination: Destination storage backend
            paths: List of path configs, each with:
                   - source: Source path (required)
                   - dest: Destination path (default: same as source)
                   - pattern: File pattern (default: default_pattern)
                   - compact: Whether to compact (default: default_compact)
                   - delete_after: Delete after transfer (default: default_delete_after)
            default_pattern: Default file pattern
            default_compact: Default compaction setting
            default_delete_after: Default delete setting
            target_file_size_mb: Target file size for compaction
            max_workers: Parallel transfer threads
        """
        self.sync = StorageSync(source, destination)
        self.paths = paths
        self.default_pattern = default_pattern
        self.default_compact = default_compact
        self.default_delete_after = default_delete_after
        self.target_file_size_mb = target_file_size_mb
        self.max_workers = max_workers
        
        # Statistics
        self.last_run_time: Optional[datetime] = None
        self.total_runs: int = 0
        self.total_files_transferred: int = 0
    
    def run(self, dry_run: bool = False) -> List[Dict[str, Any]]:
        """
        Run sync for all configured paths.
        
        Args:
            dry_run: Simulate without actual changes
        
        Returns:
            List of results for each path
        """
        logger.info("=" * 80)
        logger.info("[StorageSyncJob] Starting sync job")
        logger.info("=" * 80)
        
        start_time = time.time()
        results = []
        
        # Use progress bar for paths
        path_iterator = tqdm(
            self.paths,
            desc="Processing paths",
            unit="path",
            disable=not TQDM_AVAILABLE,
        ) if TQDM_AVAILABLE else self.paths
        
        for path_config in path_iterator:
            source_path = path_config['source']
            dest_path = path_config.get('dest', source_path)
            pattern = path_config.get('pattern', self.default_pattern)
            compact = path_config.get('compact', self.default_compact)
            delete_after = path_config.get('delete_after', self.default_delete_after)
            
            if TQDM_AVAILABLE and hasattr(path_iterator, 'set_description'):
                path_iterator.set_description(f"Processing {source_path[:40]}")
            
            logger.info(f"\n[StorageSyncJob] Processing: {source_path}")
            logger.info(f"  Pattern: {pattern}, Compact: {compact}, Delete: {delete_after}")
            
            try:
                result = self.sync.sync_with_compaction(
                    source_path=source_path,
                    dest_path=dest_path,
                    pattern=pattern,
                    compact_before_upload=compact,
                    target_file_size_mb=self.target_file_size_mb,
                    delete_after_transfer=delete_after,
                    max_workers=self.max_workers,
                    dry_run=dry_run,
                )
                
                result['source_path'] = source_path
                result['success'] = True
                results.append(result)
                
                if result['sync_stats']:
                    self.total_files_transferred += result['sync_stats'].files_transferred
                
            except Exception as e:
                logger.error(f"[StorageSyncJob] Failed for {source_path}: {e}")
                results.append({
                    'source_path': source_path,
                    'success': False,
                    'error': str(e),
                })
        
        duration = time.time() - start_time
        self.last_run_time = datetime.now()
        self.total_runs += 1
        
        logger.info("=" * 80)
        logger.info(f"[StorageSyncJob] Complete in {duration:.1f}s")
        logger.info(f"  Paths processed: {len(results)}")
        logger.info(f"  Total files transferred this run: {sum(r.get('sync_stats', SyncStats()).files_transferred for r in results if r.get('success'))}")
        logger.info("=" * 80)
        
        return results
    
    async def run_scheduled(
        self,
        interval_seconds: int = 300,
        max_iterations: Optional[int] = None,
    ):
        """
        Run sync job on a schedule.
        
        Args:
            interval_seconds: Seconds between runs
            max_iterations: Maximum number of runs (None = infinite)
        """
        import asyncio
        
        iteration = 0
        
        logger.info(f"[StorageSyncJob] Starting scheduled sync (interval={interval_seconds}s)")
        
        while max_iterations is None or iteration < max_iterations:
            try:
                # Run sync in executor to not block
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.run)
                
                iteration += 1
                
                if max_iterations and iteration >= max_iterations:
                    break
                
                logger.info(f"[StorageSyncJob] Sleeping for {interval_seconds}s...")
                await asyncio.sleep(interval_seconds)
                
            except asyncio.CancelledError:
                logger.info("[StorageSyncJob] Scheduled sync cancelled")
                break
            except Exception as e:
                logger.error(f"[StorageSyncJob] Error in scheduled run: {e}")
                await asyncio.sleep(interval_seconds)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get job statistics."""
        return {
            'total_runs': self.total_runs,
            'total_files_transferred': self.total_files_transferred,
            'last_run_time': self.last_run_time.isoformat() if self.last_run_time else None,
            'paths_configured': len(self.paths),
        }
    
    def update_paths(self, new_paths: List[Dict[str, Any]]):
        """
        Update the paths to sync (useful for dynamic discovery).
        
        This allows continuous sync to pick up new partitions
        that are created after the job was started.
        
        Args:
            new_paths: New list of path configurations
        """
        # Merge new paths, avoiding duplicates by source
        existing_sources = {p['source'] for p in self.paths}
        
        for new_path in new_paths:
            if new_path['source'] not in existing_sources:
                self.paths.append(new_path)
                logger.info(f"[StorageSyncJob] Added new path: {new_path['source']}")
                existing_sources.add(new_path['source'])
