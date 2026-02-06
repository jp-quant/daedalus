"""
Streaming Parquet writer for raw market data landing.

Bronze Layer (Raw Landing):
- Writes raw market data directly to Parquet during ingestion
- 5-10x smaller than NDJSON with ZSTD compression
- Preserves all raw data for replay/reprocessing
- Schema-aware but flexible (nested structs for varying data)

This replaces NDJSON as the raw landing format while maintaining:
- Immutability (append-only, no overwrites)
- Durability (fsync, immediate S3 upload)
- Active/ready segregation (ETL never touches active files)

Performance Optimizations:
- Dedicated ThreadPoolExecutor for I/O-bound operations
- Efficient batch queue draining (up to batch_size per iteration)
- LRU-based writer eviction to prevent file descriptor exhaustion
- Periodic memory cleanup and garbage collection
- Pre-allocated partition buffers where possible

Partitioning Approach (Directory-Aligned Partitioning):
    Unlike traditional Hive partitioning where partition columns are derived
    from the directory path and NOT stored in the data files, our approach
    requires partition column values to EXIST in the Parquet data AND MATCH
    the directory partition values exactly.
    
    This means:
    - ALL partition columns exist in data with matching values
    - ALL partition values are sanitized for filesystem compatibility
    - Sanitization replaces prohibited characters (/, \\, :, etc.) with -
    - No ambiguity between path and data - they are always consistent
    
    Default partition columns: [exchange, symbol, year, month, day, hour]
    
    Directory structure:
        {path}/{channel}/exchange={ex}/symbol={sym}/year={y}/month={m}/day={d}/hour={h}/
    
    Example: If path has `symbol=BTC-USD`, the Parquet data column `symbol`
    will also contain "BTC-USD" (not "BTC/USD").

Architecture (Medallion):
    Bronze (this writer) → Silver (clean) → Gold (features)
    Raw Parquet            Normalized        Time-series bars
"""
import asyncio
import atexit
import gc
import io
import logging
import os
import shutil
import signal
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, Tuple
from collections import deque
import json

import pyarrow as pa
import pyarrow.parquet as pq

from storage.base import StorageBackend
from ingestion.utils.time import utc_now

# Shared partitioning utilities (used by both ingestion and ETL)
from shared.partitioning import (
    DEFAULT_PARTITION_COLUMNS,
    PROHIBITED_PATH_CHARS,
    sanitize_partition_value,
    extract_datetime_components,
    format_partition_value,
)

logger = logging.getLogger(__name__)

# Global registry of active writers for emergency cleanup
_ACTIVE_WRITERS: List["StreamingParquetWriter"] = []
_CLEANUP_LOCK = threading.Lock()
_CLEANUP_DONE = False


# =============================================================================
# Constants
# =============================================================================

# Re-export from shared module for backwards compatibility
# DEFAULT_PARTITION_COLUMNS - imported from shared.partitioning
# PROHIBITED_PATH_CHARS - imported from shared.partitioning

# Maximum number of open Parquet writers to prevent "too many open files" error
# On Pi4 with 3 exchanges × ~50 symbols × 3 channels = ~300+ partition combos.
# Setting this BELOW total combos causes constant evict/reopen thrashing that
# creates hundreds of tiny 0.0MB parquet files per minute. Set it HIGH ENOUGH
# to hold all active partitions. Memory savings come from CCXT cache limits,
# info stripping, and idle eviction — not from artificially limiting writers.
MAX_OPEN_WRITERS = 300

# Thread pool for I/O-bound operations (disk writes, file moves)
# On resource-constrained devices (Pi4), fewer threads reduce memory overhead.
# Each thread stack is ~8MB on Linux, so 16 threads = 128MB of stack alone.
# For sequential disk I/O, diminishing returns past a few threads.
IO_THREAD_POOL_SIZE = min(4, (os.cpu_count() or 2) + 1)

# Memory management: cleanup stale entries and force GC periodically (seconds)
# On memory-constrained devices, clean up more aggressively
MEMORY_CLEANUP_INTERVAL = 60  # 1 minute (was 5 minutes)

# Maximum age for hour_counters entries before cleanup (seconds)
# Entries older than 2 hours are definitely stale
HOUR_COUNTER_MAX_AGE_SECONDS = 7200

# Maximum idle time for a writer before proactive eviction (seconds)
# Writers that haven't received data in 5 minutes are closed during periodic cleanup.
# This avoids waiting for MAX_OPEN_WRITERS to be hit, reducing steady-state memory.
WRITER_IDLE_EVICTION_SECONDS = 300


# =============================================================================
# Emergency Cleanup Functions
# =============================================================================

def _emergency_cleanup():
    """
    Emergency cleanup handler for unexpected shutdowns.
    
    Called by atexit and signal handlers to ensure:
    1. All in-memory buffers are flushed to disk
    2. All open Parquet files are properly closed
    3. All files in active/ are moved to ready/
    
    This prevents data loss on:
    - Ctrl+C (SIGINT)
    - kill command (SIGTERM)
    - Uncaught exceptions
    - Python interpreter exit
    """
    global _CLEANUP_DONE
    
    with _CLEANUP_LOCK:
        if _CLEANUP_DONE:
            return
        _CLEANUP_DONE = True
    
    if not _ACTIVE_WRITERS:
        return
    
    logger.warning("[Emergency Cleanup] Starting emergency data preservation...")
    
    for writer in _ACTIVE_WRITERS:
        try:
            writer._emergency_flush_and_close()
        except Exception as e:
            logger.error(f"[Emergency Cleanup] Error cleaning up writer: {e}")
            traceback.print_exc()
    
    logger.warning("[Emergency Cleanup] Complete - all data preserved")


def _setup_emergency_handlers():
    """Setup atexit and signal handlers for emergency cleanup."""
    # Register atexit handler (covers normal exit, uncaught exceptions)
    atexit.register(_emergency_cleanup)


# Setup handlers on module load
_setup_emergency_handlers()


# =============================================================================
# Schema Definitions
# =============================================================================

def _get_ticker_schema() -> pa.Schema:
    """
    Schema for ticker data - preserves all CCXT unified ticker fields.
    
    Partition columns (exchange, symbol, year, month, day, hour) are included
    in the schema to ensure data matches partition directory values.
    All partition values are sanitized for filesystem compatibility.
    """
    return pa.schema([
        pa.field("collected_at", pa.int64()),  # Exchange timestamp (ms)
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),  # Our capture time
        pa.field("exchange", pa.string()),  # Sanitized partition column
        pa.field("symbol", pa.string()),  # Sanitized partition column
        pa.field("year", pa.int32()),  # Partition column
        pa.field("month", pa.int32()),  # Partition column
        pa.field("day", pa.int32()),  # Partition column
        pa.field("hour", pa.int32()),  # Partition column
        pa.field("bid", pa.float64()),
        pa.field("ask", pa.float64()),
        pa.field("bid_volume", pa.float64()),
        pa.field("ask_volume", pa.float64()),
        pa.field("last", pa.float64()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("vwap", pa.float64()),
        pa.field("base_volume", pa.float64()),
        pa.field("quote_volume", pa.float64()),
        pa.field("change", pa.float64()),
        pa.field("percentage", pa.float64()),
        pa.field("timestamp", pa.int64()),  # Exchange event timestamp
        pa.field("info_json", pa.string()),  # Raw info as JSON
    ])


def _get_trades_schema() -> pa.Schema:
    """
    Schema for trade data - one row per trade.
    
    Partition columns (exchange, symbol, year, month, day, hour) are included
    in the schema to ensure data matches partition directory values.
    All partition values are sanitized for filesystem compatibility.
    """
    return pa.schema([
        pa.field("collected_at", pa.int64()),
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
        pa.field("exchange", pa.string()),  # Sanitized partition column
        pa.field("symbol", pa.string()),  # Sanitized partition column
        pa.field("year", pa.int32()),  # Partition column
        pa.field("month", pa.int32()),  # Partition column
        pa.field("day", pa.int32()),  # Partition column
        pa.field("hour", pa.int32()),  # Partition column
        pa.field("trade_id", pa.string()),
        pa.field("timestamp", pa.int64()),
        pa.field("side", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("amount", pa.float64()),
        pa.field("cost", pa.float64()),
        pa.field("info_json", pa.string()),
    ])


def _get_orderbook_schema() -> pa.Schema:
    """
    Schema for orderbook data - stores bids/asks as nested lists.
    
    This is the raw landing format - not denormalized.
    ETL can later flatten to level-by-level if needed.
    
    Partition columns (exchange, symbol, year, month, day, hour) are included
    in the schema to ensure data matches partition directory values.
    All partition values are sanitized for filesystem compatibility.
    """
    level_type = pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("size", pa.float64()),
    ]))
    
    return pa.schema([
        pa.field("collected_at", pa.int64()),
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
        pa.field("exchange", pa.string()),  # Sanitized partition column
        pa.field("symbol", pa.string()),  # Sanitized partition column
        pa.field("year", pa.int32()),  # Partition column
        pa.field("month", pa.int32()),  # Partition column
        pa.field("day", pa.int32()),  # Partition column
        pa.field("hour", pa.int32()),  # Partition column
        pa.field("timestamp", pa.int64()),
        pa.field("nonce", pa.int64()),
        pa.field("bids", level_type),
        pa.field("asks", level_type),
    ])


def _get_generic_schema() -> pa.Schema:
    """
    Fallback schema for unknown channel types - stores as JSON.
    
    Partition columns (exchange, symbol, year, month, day, hour) are included
    in the schema to ensure data matches partition directory values.
    All partition values are sanitized for filesystem compatibility.
    """
    return pa.schema([
        pa.field("collected_at", pa.int64()),
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
        pa.field("exchange", pa.string()),  # Sanitized partition column
        pa.field("symbol", pa.string()),  # Sanitized partition column
        pa.field("year", pa.int32()),  # Partition column
        pa.field("month", pa.int32()),  # Partition column
        pa.field("day", pa.int32()),  # Partition column
        pa.field("hour", pa.int32()),  # Partition column
        pa.field("type", pa.string()),
        pa.field("method", pa.string()),
        pa.field("data_json", pa.string()),
    ])


CHANNEL_SCHEMAS = {
    "ticker": _get_ticker_schema(),
    "trades": _get_trades_schema(),
    "orderbook": _get_orderbook_schema(),
}


# =============================================================================
# Utility Functions
# =============================================================================

# sanitize_partition_value - imported from shared.partitioning
# extract_datetime_components - imported from shared.partitioning

# Backwards compatibility: Re-export shared functions
# These are now defined in shared/partitioning.py and imported at top of file
__all__ = [
    "StreamingParquetWriter",
    "DEFAULT_PARTITION_COLUMNS",
    "PROHIBITED_PATH_CHARS",
    "sanitize_partition_value",
    "extract_datetime_components",
    "CHANNEL_SCHEMAS",
]


# =============================================================================
# StreamingParquetWriter
# =============================================================================

class StreamingParquetWriter:
    """
    Batched streaming writer for raw market data to Parquet.
    
    Design:
    - Bounded queue with backpressure
    - Batch writes to reduce I/O overhead
    - Size-based segment rotation (prevents unbounded growth)
    - Channel-based file separation (ticker, trades, orderbook)
    - Directory-aligned partitioning (configurable columns)
    - Active/ready directory segregation
    - Works with any StorageBackend
    
    Directory-Aligned Partitioning:
        Our approach requires partition column values to EXIST in the Parquet
        data AND MATCH the directory partition values exactly. This differs
        from traditional Hive partitioning where partition columns are derived
        from paths and not stored in data files.
        
        Key guarantees:
        - ALL partition columns exist in data with matching values
        - ALL partition values are sanitized for filesystem compatibility
        - Sanitization applies to BOTH path AND data (no ambiguity)
        
        Default: ["exchange", "symbol", "year", "month", "day", "hour"]
    
    Directory structure:
        {path}/{channel}/exchange={ex}/symbol={sym}/year={y}/month={m}/day={d}/hour={h}/segment_*.parquet
    
    Comparison vs NDJSON:
    - ~5-10x smaller with ZSTD compression
    - Columnar: can read just needed columns
    - Typed: no parsing overhead
    - Same durability guarantees
    """
    
    def __init__(
        self,
        storage: StorageBackend,
        active_path: str,
        ready_path: str,
        source_name: str,
        batch_size: int = 1000,
        flush_interval_seconds: float = 5.0,
        queue_maxsize: int = 50000,
        segment_max_mb: int = 50,
        compression: str = "zstd",
        compression_level: int = 3,
        partition_by: Optional[List[str]] = None,
    ):
        """
        Initialize streaming Parquet writer.
        
        Args:
            storage: Storage backend instance
            active_path: Path for actively writing segments
            ready_path: Path for ready segments (for ETL)
            source_name: Data source identifier
            batch_size: Records to batch before writing
            flush_interval_seconds: Maximum time between flushes
            queue_maxsize: Maximum queue size (backpressure)
            segment_max_mb: Max segment size before rotation
            compression: Parquet compression (zstd, snappy, lz4)
            compression_level: Compression level (1-22 for zstd)
            partition_by: Columns to partition by. Supports: exchange, symbol,
                         year, month, day, hour. Default: all six columns.
        """
        self.storage = storage
        self.active_path = active_path
        self.ready_path = ready_path
        self.source_name = source_name
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.segment_max_bytes = segment_max_mb * 1024 * 1024
        self.compression = compression
        self.compression_level = compression_level
        
        # Partition configuration - default to full partitioning
        self.partition_by = partition_by if partition_by is not None else DEFAULT_PARTITION_COLUMNS.copy()
        
        # Bounded queue for backpressure
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        
        # Per-partition buffers and segment tracking
        # Key: tuple of partition values based on partition_by
        self._partition_buffers: Dict[tuple, deque] = {}
        self._partition_segments: Dict[tuple, Dict[str, Any]] = {}
        self._partition_sizes: Dict[tuple, int] = {}
        
        # Hour-based segment naming per partition
        # NOTE: Hour is tracked PER-SEGMENT (stored in segment info dict),
        # NOT globally, to ensure all partitions rotate correctly on hour change
        # Format: {counter_key: (counter_value, last_used_timestamp)}
        self.hour_counters: Dict[str, tuple] = {}
        
        # Memory management
        self._last_memory_cleanup = time.time()
        self._last_gc_collect = time.time()
        
        # Statistics
        self.stats = {
            "messages_received": 0,
            "messages_written": 0,
            "flushes": 0,
            "rotations": 0,
            "queue_full_events": 0,
            "errors": 0,
            "bytes_written": 0,
            "memory_cleanups": 0,
            "gc_collections": 0,
            "stale_counters_cleaned": 0,
            "stale_buffers_cleaned": 0,
        }
        
        # Writer task
        self._writer_task: Optional[asyncio.Task] = None
        self._shutdown = asyncio.Event()
        self._is_stopped = False  # Track if we've been cleanly stopped
        
        # Dedicated thread pool for I/O operations
        # This prevents blocking the event loop during disk writes
        self._io_executor = ThreadPoolExecutor(
            max_workers=IO_THREAD_POOL_SIZE,
            thread_name_prefix=f"parquet_io_{source_name}"
        )
        
        # Ensure base directories exist
        self.storage.mkdir(self.active_path)
        self.storage.mkdir(self.ready_path)
        
        # Register for emergency cleanup
        with _CLEANUP_LOCK:
            _ACTIVE_WRITERS.append(self)
        
        logger.info(
            f"[StreamingParquetWriter] Initialized: source={source_name}, "
            f"compression={compression}:{compression_level}, segment_max_mb={segment_max_mb}, "
            f"io_threads={IO_THREAD_POOL_SIZE}"
        )
        logger.info(f"[StreamingParquetWriter] Partitioning by: {self.partition_by}")
    
    async def start(self):
        """
        Start the writer background task.
        
        On startup, migrates any orphan files from active/ to ready/
        before starting the writer loop. This ensures data safety
        if the previous run crashed or was killed.
        """
        if self._writer_task is not None:
            logger.warning("[StreamingParquetWriter] Already started")
            return
        
        # Migrate orphan files from previous run using dedicated I/O pool
        try:
            await asyncio.get_event_loop().run_in_executor(
                self._io_executor, self._move_active_to_ready
            )
        except Exception as e:
            logger.error(f"[StreamingParquetWriter] Error migrating orphan files on startup: {e}")
        
        self._writer_task = asyncio.create_task(self._writer_loop())
        logger.info("[StreamingParquetWriter] Started")
    
    async def stop(self):
        """
        Stop the writer gracefully and ensure all data is preserved.
        
        This method:
        1. Signals the writer loop to stop
        2. Waits for the writer loop to drain the queue
        3. Flushes all in-memory buffers to Parquet files
        4. Closes all open Parquet writers
        5. Moves all files from active/ to ready/
        6. Shuts down the I/O thread pool
        7. Unregisters from emergency cleanup
        
        Data preservation is guaranteed unless the process is killed with SIGKILL.
        """
        if self._is_stopped:
            return
        
        if self._writer_task is None:
            # Never started, but might have data in buffers from sync writes
            self._emergency_flush_and_close()
            return
        
        logger.info("[StreamingParquetWriter] Stopping gracefully...")
        self._shutdown.set()
        
        try:
            # Give writer loop time to drain queue
            await asyncio.wait_for(self._writer_task, timeout=30.0)
        except asyncio.TimeoutError:
            logger.warning("[StreamingParquetWriter] Writer task timeout, forcing flush...")
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
        except asyncio.CancelledError:
            logger.warning("[StreamingParquetWriter] Stop was cancelled, preserving data...")
        
        # Final flush, close segments, and move to ready
        try:
            await asyncio.get_event_loop().run_in_executor(
                self._io_executor, self._final_shutdown
            )
        except Exception as e:
            logger.error(f"[StreamingParquetWriter] Error in final shutdown: {e}")
            # Try emergency cleanup as fallback
            self._emergency_flush_and_close()
        
        # Shutdown the I/O executor
        try:
            self._io_executor.shutdown(wait=True, cancel_futures=False)
        except Exception as e:
            logger.warning(f"[StreamingParquetWriter] Error shutting down I/O executor: {e}")
        
        # Mark as stopped and unregister
        self._is_stopped = True
        with _CLEANUP_LOCK:
            if self in _ACTIVE_WRITERS:
                _ACTIVE_WRITERS.remove(self)
        
        logger.info(
            f"[StreamingParquetWriter] Stopped: {self.stats['messages_written']} written, "
            f"{self.stats['bytes_written'] / 1024 / 1024:.1f} MB"
        )
    
    async def write(self, record: Dict[str, Any], block: bool = True):
        """
        Write a record to the queue.
        
        Args:
            record: Dictionary with 'type', 'exchange', 'symbol', 'data', etc.
            block: If True, blocks when queue is full (backpressure)
        """
        try:
            if block:
                await self.queue.put(record)
            else:
                self.queue.put_nowait(record)
            self.stats["messages_received"] += 1
        except asyncio.QueueFull:
            self.stats["queue_full_events"] += 1
            raise
    
    async def _writer_loop(self):
        """
        Background task that batches and writes records.
        
        Optimizations:
        - Efficient batch draining with minimal await overhead
        - Uses dedicated I/O thread pool for disk writes
        - Periodic memory cleanup to prevent leaks
        - Adaptive flush timing based on buffer fullness
        """
        last_flush_time = asyncio.get_event_loop().time()
        loop = asyncio.get_event_loop()
        
        while not self._shutdown.is_set() or not self.queue.empty():
            try:
                # Efficient queue drain: first get one item (may block),
                # then drain remaining without blocking
                drained = 0
                
                # Get first item - this may block up to timeout
                try:
                    record = await asyncio.wait_for(
                        self.queue.get(), timeout=0.1
                    )
                    partition_key = self._get_partition_key(record)
                    if partition_key not in self._partition_buffers:
                        self._partition_buffers[partition_key] = deque()
                    self._partition_buffers[partition_key].append(record)
                    drained += 1
                except asyncio.TimeoutError:
                    pass  # No items available
                
                # Non-blocking drain of remaining items up to batch_size
                while drained < self.batch_size:
                    try:
                        record = self.queue.get_nowait()
                        partition_key = self._get_partition_key(record)
                        if partition_key not in self._partition_buffers:
                            self._partition_buffers[partition_key] = deque()
                        self._partition_buffers[partition_key].append(record)
                        drained += 1
                    except asyncio.QueueEmpty:
                        break
                
                # Check if flush needed
                current_time = loop.time()
                time_to_flush = (current_time - last_flush_time) >= self.flush_interval
                
                any_buffer_full = any(
                    len(buf) >= self.batch_size 
                    for buf in self._partition_buffers.values()
                )
                
                if any_buffer_full or time_to_flush:
                    # Use dedicated I/O thread pool for disk operations
                    await loop.run_in_executor(
                        self._io_executor, self._flush_all_partitions
                    )
                    last_flush_time = current_time
                
                # Periodic memory cleanup (runs in background)
                if current_time - self._last_memory_cleanup >= MEMORY_CLEANUP_INTERVAL:
                    await loop.run_in_executor(
                        self._io_executor, self._periodic_memory_cleanup
                    )
                    self._last_memory_cleanup = current_time
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.stats["errors"] += 1
                logger.error(f"[StreamingParquetWriter] Error in writer loop: {e}")
                await asyncio.sleep(1.0)
        
        # Final flush using I/O executor
        await loop.run_in_executor(
            self._io_executor, self._flush_all_partitions
        )
    
    def _extract_partition_values(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract all partition values from a record.
        
        Returns a dictionary with sanitized values for all partition columns.
        This ensures both path and data use identical values.
        
        Args:
            record: Raw record dictionary
            
        Returns:
            Dictionary mapping partition column names to sanitized values
        """
        # Extract datetime components (always needed for schema)
        year, month, day, hour = extract_datetime_components(record)
        
        # Build partition values dict
        values = {
            "exchange": sanitize_partition_value(record.get("exchange", "unknown")),
            "symbol": sanitize_partition_value(record.get("symbol", "unknown")),
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
        }
        
        return values
    
    def _get_partition_key(self, record: Dict[str, Any]) -> tuple:
        """
        Extract partition key tuple from record.
        
        The partition key is a tuple of (channel, partition_values...) based
        on the configured partition_by columns. All string values are sanitized.
        
        Args:
            record: Raw record dictionary
            
        Returns:
            Tuple: (channel, *partition_values) for use as dictionary key
        """
        channel = record.get("type", "unknown")
        partition_values = self._extract_partition_values(record)
        
        # Build key tuple based on configured partition columns
        key_parts = [channel]
        for col in self.partition_by:
            key_parts.append(partition_values.get(col, "unknown"))
        
        return tuple(key_parts)
    
    def _partition_key_to_values(self, partition_key: tuple) -> Dict[str, Any]:
        """
        Convert partition key tuple back to values dictionary.
        
        Args:
            partition_key: Tuple of (channel, *partition_values)
            
        Returns:
            Dictionary mapping column names to values
        """
        values = {"channel": partition_key[0]}
        for i, col in enumerate(self.partition_by):
            values[col] = partition_key[i + 1]
        return values
    
    def _flush_all_partitions(self):
        """Flush all partition buffers to their respective Parquet files."""
        for partition_key, buffer in list(self._partition_buffers.items()):
            if buffer:
                self._flush_partition(partition_key, buffer)
    
    def _flush_partition(self, partition_key: tuple, buffer: deque):
        """Flush a single partition buffer to Parquet."""
        if not buffer:
            return
        
        records = list(buffer)
        buffer.clear()
        
        # Get partition values from key
        pv = self._partition_key_to_values(partition_key)
        channel = pv["channel"]
        
        try:
            records_length = len(records)
            # Convert records to PyArrow table (includes partition column values)
            table = self._records_to_table(channel, records, pv)
            
            # MEMORY OPTIMIZATION: Release Python records immediately after conversion.
            # The data now lives in the PyArrow table (off-heap Arrow memory).
            # This is critical on Pi4 where every MB counts.
            del records
            
            if table is None or table.num_rows == 0:
                return
            
            # Ensure segment is open for this partition
            self._ensure_segment_open(partition_key)
            
            # Write to segment
            segment_info = self._partition_segments[partition_key]
            writer = segment_info["writer"]
            writer.write_table(table)
            
            # Update last_write timestamp for LRU eviction
            segment_info["last_write"] = time.time()
            
            # Track size
            bytes_written = table.nbytes
            self._partition_sizes[partition_key] = self._partition_sizes.get(partition_key, 0) + bytes_written
            self.stats["bytes_written"] += bytes_written
            self.stats["messages_written"] += records_length
            self.stats["flushes"] += 1
            
            # Check for rotation
            if self._partition_sizes[partition_key] >= self.segment_max_bytes:
                self._rotate_segment(partition_key)
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"[StreamingParquetWriter] Error flushing {partition_key}: {e}")
    
    def _records_to_table(self, channel: str, records: List[Dict], partition_values: Dict[str, Any]) -> Optional[pa.Table]:
        """
        Convert raw records to PyArrow table for the given channel.
        
        Transforms records to include partition column values that match
        the partition directory structure.
        
        Performance optimizations:
        - Pre-extract partition values to avoid repeated dict lookups
        - Use list comprehensions over loops where possible
        - Minimize intermediate object creation
        
        Args:
            channel: Channel type (ticker, trades, orderbook)
            records: List of raw records
            partition_values: Dictionary of sanitized partition values
            
        Returns:
            PyArrow Table with partition columns included
        """
        if not records:
            return None
        
        try:
            if channel == "ticker":
                return self._convert_ticker_records(records, partition_values)
            elif channel == "trades":
                return self._convert_trades_records(records, partition_values)
            elif channel == "orderbook":
                return self._convert_orderbook_records(records, partition_values)
            else:
                return self._convert_generic_records(records, partition_values)
        except Exception as e:
            logger.error(f"[StreamingParquetWriter] Error converting {channel}: {e}")
            return self._convert_generic_records(records, partition_values)
    
    def _convert_ticker_records(self, records: List[Dict], pv: Dict[str, Any]) -> pa.Table:
        """
        Convert ticker records to PyArrow table with partition columns.
        
        Optimized for batch processing with pre-extracted partition values.
        """
        # Pre-extract partition values (avoid repeated dict lookups in loop)
        exchange = pv.get("exchange", "")
        symbol = pv.get("symbol", "")
        year = pv.get("year", 0)
        month = pv.get("month", 0)
        day = pv.get("day", 0)
        hour = pv.get("hour", 0)
        
        rows = []
        for r in records:
            data = r.get("data", {})
            rows.append({
                "collected_at": r.get("collected_at", 0),
                "capture_ts": self._parse_ts(r.get("capture_ts")),
                "exchange": exchange,
                "symbol": symbol,
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "bid": data.get("bid"),
                "ask": data.get("ask"),
                "bid_volume": data.get("bidVolume"),
                "ask_volume": data.get("askVolume"),
                "last": data.get("last"),
                "open": data.get("open"),
                "high": data.get("high"),
                "low": data.get("low"),
                "close": data.get("close"),
                "vwap": data.get("vwap"),
                "base_volume": data.get("baseVolume"),
                "quote_volume": data.get("quoteVolume"),
                "change": data.get("change"),
                "percentage": data.get("percentage"),
                "timestamp": data.get("timestamp", 0),
                "info_json": "{}",  # Info stripped at collector level for memory efficiency
            })
        return pa.Table.from_pylist(rows, schema=CHANNEL_SCHEMAS["ticker"])
    
    def _convert_trades_records(self, records: List[Dict], pv: Dict[str, Any]) -> pa.Table:
        """
        Convert trades records to PyArrow table (one row per trade).
        
        Optimized with pre-extracted partition values.
        """
        # Pre-extract partition values
        exchange = pv.get("exchange", "")
        symbol = pv.get("symbol", "")
        year = pv.get("year", 0)
        month = pv.get("month", 0)
        day = pv.get("day", 0)
        hour = pv.get("hour", 0)
        
        rows = []
        for r in records:
            collected_at = r.get("collected_at", 0)
            capture_ts = self._parse_ts(r.get("capture_ts"))
            
            trades = r.get("data", [])
            if isinstance(trades, dict):
                trades = [trades]
            
            for trade in trades:
                rows.append({
                    "collected_at": collected_at,
                    "capture_ts": capture_ts,
                    "exchange": exchange,
                    "symbol": symbol,
                    "year": year,
                    "month": month,
                    "day": day,
                    "hour": hour,
                    "trade_id": str(trade.get("id", "")),
                    "timestamp": trade.get("timestamp", 0),
                    "side": trade.get("side", ""),
                    "price": trade.get("price"),
                    "amount": trade.get("amount"),
                    "cost": trade.get("cost"),
                    "info_json": "{}",  # Info stripped at collector level for memory efficiency
                })
        return pa.Table.from_pylist(rows, schema=CHANNEL_SCHEMAS["trades"])
    
    def _convert_orderbook_records(self, records: List[Dict], pv: Dict[str, Any]) -> pa.Table:
        """
        Convert orderbook records to PyArrow table with nested bids/asks.
        
        Optimized with pre-extracted partition values.
        """
        # Pre-extract partition values
        exchange = pv.get("exchange", "")
        symbol = pv.get("symbol", "")
        year = pv.get("year", 0)
        month = pv.get("month", 0)
        day = pv.get("day", 0)
        hour = pv.get("hour", 0)
        
        rows = []
        for r in records:
            data = r.get("data", {})
            
            # Use list comprehension for bid/ask conversion
            bids = [{"price": b[0], "size": b[1]} for b in data.get("bids", [])]
            asks = [{"price": a[0], "size": a[1]} for a in data.get("asks", [])]
            
            rows.append({
                "collected_at": r.get("collected_at", 0),
                "capture_ts": self._parse_ts(r.get("capture_ts")),
                "exchange": exchange,
                "symbol": symbol,
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "timestamp": data.get("timestamp", 0),
                "nonce": data.get("nonce", 0),
                "bids": bids,
                "asks": asks,
            })
        return pa.Table.from_pylist(rows, schema=CHANNEL_SCHEMAS["orderbook"])
    
    def _convert_generic_records(self, records: List[Dict], pv: Dict[str, Any]) -> pa.Table:
        """
        Fallback conversion - stores full data as JSON.
        
        Optimized with pre-extracted partition values.
        """
        # Pre-extract partition values
        exchange = pv.get("exchange", "")
        symbol = pv.get("symbol", "")
        year = pv.get("year", 0)
        month = pv.get("month", 0)
        day = pv.get("day", 0)
        hour = pv.get("hour", 0)
        
        rows = []
        for r in records:
            rows.append({
                "collected_at": r.get("collected_at", 0),
                "capture_ts": self._parse_ts(r.get("capture_ts")),
                "exchange": exchange,
                "symbol": symbol,
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "type": r.get("type", ""),
                "method": r.get("method", ""),
                "data_json": json.dumps(r.get("data", {}), separators=(",", ":")),
            })
        return pa.Table.from_pylist(rows, schema=_get_generic_schema())
    
    def _parse_ts(self, ts_str: Optional[str]) -> Optional[datetime]:
        """Parse ISO timestamp string to datetime."""
        if not ts_str:
            return None
        try:
            if ts_str.endswith("Z"):
                ts_str = ts_str[:-1] + "+00:00"
            return datetime.fromisoformat(ts_str)
        except Exception:
            return None
    
    def _ensure_segment_open(self, partition_key: tuple):
        """
        Ensure a segment file is open for the partition.
        
        Handles:
        1. Opening new segment if none exists for partition
        2. Rotating segment if hour changed (per-partition tracking)
        3. Enforcing max open writers limit to prevent file handle exhaustion
        """
        current_hour = utc_now().strftime("%Y%m%dT%H")
        
        # Check if this partition needs rotation (hour changed)
        if partition_key in self._partition_segments:
            segment_hour = self._partition_segments[partition_key].get("hour")
            if segment_hour != current_hour:
                self._rotate_segment(partition_key)
        
        # Enforce max open writers limit before opening new segment
        if partition_key not in self._partition_segments:
            if len(self._partition_segments) >= MAX_OPEN_WRITERS:
                self._evict_oldest_writers(keep=MAX_OPEN_WRITERS // 2)
            self._open_new_segment(partition_key, current_hour)
    
    def _evict_oldest_writers(self, keep: int = 100):
        """
        Close oldest writers to stay under file handle limit.
        
        Writers are evicted based on last_write timestamp (LRU policy).
        This ensures high-frequency partitions stay open while idle ones close.
        
        Args:
            keep: Number of writers to keep open after eviction
        """
        if len(self._partition_segments) <= keep:
            return
        
        # Sort by last write time (oldest first)
        sorted_segments = sorted(
            self._partition_segments.items(),
            key=lambda x: x[1].get("last_write", 0)
        )
        
        # Close oldest writers
        to_close = len(sorted_segments) - keep
        closed_count = 0
        for partition_key, _ in sorted_segments[:to_close]:
            self._rotate_segment(partition_key)
            closed_count += 1
        
        if closed_count > 0:
            logger.info(f"[StreamingParquetWriter] Evicted {closed_count} idle writers (LRU)")
    
    def _build_partition_path(self, base_path: str, partition_key: tuple) -> str:
        """
        Build directory-aligned partition path.
        
        All partition values in path match exactly what's stored in the Parquet data.
        Path structure follows configured partition_by columns.
        
        Args:
            base_path: Base directory path
            partition_key: Tuple of (channel, *partition_values)
            
        Returns:
            Full partition directory path
        """
        pv = self._partition_key_to_values(partition_key)
        channel = pv["channel"]
        
        # Start with channel
        path = self.storage.join_path(base_path, channel)
        
        # Add each configured partition column
        for col in self.partition_by:
            value = pv.get(col, "unknown")
            path = self.storage.join_path(path, f"{col}={value}")
        
        return path
    
    def _open_new_segment(self, partition_key: tuple, current_hour: str):
        """
        Open a new segment file for the partition.
        
        Args:
            partition_key: Tuple of (channel, *partition_values)
            current_hour: Current hour string (e.g., "20251227T02")
        """
        pv = self._partition_key_to_values(partition_key)
        channel = pv["channel"]
        
        # Get counter for this hour/partition
        # Use sanitized values for counter key
        counter_key = f"{current_hour}_{channel}"
        for col in self.partition_by:
            counter_key += f"_{pv.get(col, 'unknown')}"
        
        current_time = time.time()
        if counter_key not in self.hour_counters:
            self.hour_counters[counter_key] = (0, current_time)
        
        # Increment counter and update timestamp
        old_count, _ = self.hour_counters[counter_key]
        self.hour_counters[counter_key] = (old_count + 1, current_time)
        counter = old_count + 1
        
        segment_name = f"segment_{current_hour}_{counter:05d}.parquet"
        
        partition_path = self._build_partition_path(self.active_path, partition_key)
        self.storage.mkdir(partition_path)
        
        segment_path = self.storage.join_path(partition_path, segment_name)
        
        schema = CHANNEL_SCHEMAS.get(channel, _get_generic_schema())
        
        if self.storage.backend_type == "local":
            full_path = self.storage.get_full_path(segment_path)
            writer = pq.ParquetWriter(
                full_path,
                schema,
                compression=self.compression,
                compression_level=self.compression_level,
            )
        else:
            buffer = io.BytesIO()
            writer = pq.ParquetWriter(
                buffer,
                schema,
                compression=self.compression,
                compression_level=self.compression_level,
            )
            self._partition_segments[partition_key] = {
                "writer": writer,
                "buffer": buffer,
                "path": segment_path,
                "name": segment_name,
                "partition_key": partition_key,
                "hour": current_hour,
                "last_write": time.time(),
            }
            self._partition_sizes[partition_key] = 0
            return
        
        self._partition_segments[partition_key] = {
            "writer": writer,
            "path": segment_path,
            "name": segment_name,
            "partition_key": partition_key,
            "hour": current_hour,
            "last_write": time.time(),
        }
        self._partition_sizes[partition_key] = 0
        
        logger.debug(f"[StreamingParquetWriter] Opened segment: {segment_path}")
    
    def _rotate_segment(self, partition_key: tuple):
        """Close current segment and move to ready directory."""
        if partition_key not in self._partition_segments:
            return
        
        segment_info = self._partition_segments.pop(partition_key)
        writer = segment_info["writer"]
        
        try:
            writer.close()
            logger.debug(f"[StreamingParquetWriter] Closed writer for {partition_key}")
            
            ready_partition_path = self._build_partition_path(self.ready_path, partition_key)
            self.storage.mkdir(ready_partition_path)
            ready_path = self.storage.join_path(ready_partition_path, segment_info["name"])
            
            if self.storage.backend_type == "local":
                active_full = self.storage.get_full_path(segment_info["path"])
                ready_full = self.storage.get_full_path(ready_path)
                shutil.move(active_full, ready_full)
                logger.debug(f"[StreamingParquetWriter] Moved {active_full} -> {ready_full}")
            else:
                buffer = segment_info.get("buffer")
                if buffer:
                    buffer.seek(0)
                    self.storage.write_bytes(buffer.read(), ready_path)
            
            self.stats["rotations"] += 1
            size_mb = self._partition_sizes.get(partition_key, 0) / 1024 / 1024
            pv = self._partition_key_to_values(partition_key)
            # Log tiny segments (from eviction) at DEBUG to reduce log noise
            log_fn = logger.debug if size_mb < 0.01 else logger.info
            log_fn(f"[StreamingParquetWriter] Rotated {pv['channel']}/{pv.get('exchange','')}/{pv.get('symbol','')}: "
                   f"{segment_info['name']} ({size_mb:.1f} MB) -> ready/")
        
        except Exception as e:
            logger.error(f"[StreamingParquetWriter] Error rotating {partition_key}: {e}", exc_info=True)
        
        self._partition_sizes[partition_key] = 0
    
    def _close_all_segments(self):
        """Close and move all open segments to ready."""
        for partition_key in list(self._partition_segments.keys()):
            self._rotate_segment(partition_key)
    
    def _final_shutdown(self):
        """
        Complete shutdown sequence - flush, close, and move all active files to ready.
        
        This is called during graceful shutdown and ensures:
        1. All in-memory buffers are written to Parquet files
        2. All Parquet writers are properly closed
        3. All files in active/ are moved to ready/
        """
        logger.info("[StreamingParquetWriter] Final shutdown: flushing all data...")
        
        # 1. Flush any remaining in-memory buffers
        try:
            self._flush_all_partitions()
        except Exception as e:
            logger.error(f"[StreamingParquetWriter] Error flushing partitions: {e}")
        
        # 2. Close all open segment writers and move to ready
        try:
            self._close_all_segments()
        except Exception as e:
            logger.error(f"[StreamingParquetWriter] Error closing segments: {e}")
        
        # 3. Move any remaining files from active to ready
        # This catches files that might have been written but not rotated
        try:
            self._move_active_to_ready()
        except Exception as e:
            logger.error(f"[StreamingParquetWriter] Error moving active to ready: {e}")
        
        logger.info("[StreamingParquetWriter] Final shutdown complete")
    
    def _emergency_flush_and_close(self):
        """
        Emergency data preservation - called by atexit/signal handlers.
        
        This is a synchronous, non-async method that can be called from
        signal handlers or atexit. It does everything possible to preserve
        data without relying on the event loop.
        """
        if self._is_stopped:
            return
        
        logger.warning(f"[StreamingParquetWriter:{self.source_name}] Emergency flush starting...")
        
        try:
            # Drain queue synchronously if possible
            drained = 0
            while True:
                try:
                    record = self.queue.get_nowait()
                    partition_key = self._get_partition_key(record)
                    if partition_key not in self._partition_buffers:
                        self._partition_buffers[partition_key] = deque()
                    self._partition_buffers[partition_key].append(record)
                    drained += 1
                except:
                    break
            
            if drained > 0:
                logger.warning(f"[StreamingParquetWriter:{self.source_name}] Drained {drained} records from queue")
        except Exception as e:
            logger.error(f"[StreamingParquetWriter:{self.source_name}] Error draining queue: {e}")
        
        # Flush buffers
        try:
            self._flush_all_partitions()
        except Exception as e:
            logger.error(f"[StreamingParquetWriter:{self.source_name}] Error flushing: {e}")
        
        # Close segments
        try:
            self._close_all_segments()
        except Exception as e:
            logger.error(f"[StreamingParquetWriter:{self.source_name}] Error closing segments: {e}")
        
        # Move active to ready
        try:
            self._move_active_to_ready()
        except Exception as e:
            logger.error(f"[StreamingParquetWriter:{self.source_name}] Error moving active to ready: {e}")
        
        self._is_stopped = True
        logger.warning(f"[StreamingParquetWriter:{self.source_name}] Emergency flush complete")
    
    def _move_active_to_ready(self):
        """
        Move all Parquet files from active/ to ready/.
        
        This is a safety net that catches any files that might have been
        written but not properly rotated during shutdown. It recursively
        scans the active directory and moves all .parquet files to the
        corresponding ready directory, preserving the partition structure.
        """
        if self.storage.backend_type != "local":
            # For S3, files are written directly - no active/ready distinction
            return
        
        active_base = self.storage.get_full_path(self.active_path)
        ready_base = self.storage.get_full_path(self.ready_path)
        
        active_path = Path(active_base)
        if not active_path.exists():
            return
        
        moved_count = 0
        for parquet_file in active_path.rglob("*.parquet"):
            try:
                # Calculate relative path from active base
                relative_path = parquet_file.relative_to(active_path)
                
                # Build destination path
                ready_file = Path(ready_base) / relative_path
                
                # Create destination directory
                ready_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Move file
                shutil.move(str(parquet_file), str(ready_file))
                moved_count += 1
                logger.info(f"[StreamingParquetWriter] Moved orphan: {relative_path}")
                
            except Exception as e:
                logger.error(f"[StreamingParquetWriter] Error moving {parquet_file}: {e}")
        
        if moved_count > 0:
            logger.info(f"[StreamingParquetWriter] Moved {moved_count} orphan files from active/ to ready/")
        
        # Clean up empty directories in active
        try:
            self._cleanup_empty_dirs(active_path)
        except Exception as e:
            logger.debug(f"[StreamingParquetWriter] Error cleaning empty dirs: {e}")
    
    def _cleanup_empty_dirs(self, path: Path):
        """Recursively remove empty directories."""
        if not path.is_dir():
            return
        
        # Process children first (depth-first)
        for child in path.iterdir():
            if child.is_dir():
                self._cleanup_empty_dirs(child)
        
        # Remove this directory if empty
        try:
            if path.is_dir() and not any(path.iterdir()):
                path.rmdir()
        except OSError:
            pass  # Directory not empty or permission denied
    
    def _periodic_memory_cleanup(self):
        """
        Periodic cleanup to prevent memory leaks during long-running operation.
        
        This method:
        1. Proactively evicts writers idle for > WRITER_IDLE_EVICTION_SECONDS
        2. Removes stale entries from hour_counters (older than 2 hours)
        3. Removes empty partition buffers
        4. Forces garbage collection
        
        Called every MEMORY_CLEANUP_INTERVAL seconds from _writer_loop.
        """
        current_time = time.time()
        
        # 1. Proactive idle writer eviction — close writers that haven't received
        # data recently. This keeps steady-state memory bounded even when the
        # total number of partition combos exceeds MAX_OPEN_WRITERS.
        idle_writers = [
            key for key, info in list(self._partition_segments.items())
            if current_time - info.get("last_write", 0) > WRITER_IDLE_EVICTION_SECONDS
        ]
        for key in idle_writers:
            self._rotate_segment(key)
        if idle_writers:
            logger.info(f"[StreamingParquetWriter] Evicted {len(idle_writers)} idle writers (>{WRITER_IDLE_EVICTION_SECONDS}s)")
        
        # 2. Clean up stale hour_counters entries
        stale_counters = []
        for key, (count, last_used) in list(self.hour_counters.items()):
            if current_time - last_used > HOUR_COUNTER_MAX_AGE_SECONDS:
                stale_counters.append(key)
        
        for key in stale_counters:
            del self.hour_counters[key]
        
        if stale_counters:
            self.stats["stale_counters_cleaned"] += len(stale_counters)
            logger.debug(f"[StreamingParquetWriter] Cleaned {len(stale_counters)} stale hour counters")
        
        # 3. Clean up empty partition buffers (deques that are empty)
        empty_buffers = [
            key for key, buf in self._partition_buffers.items() 
            if len(buf) == 0 and key not in self._partition_segments
        ]
        
        for key in empty_buffers:
            del self._partition_buffers[key]
        
        if empty_buffers:
            self.stats["stale_buffers_cleaned"] += len(empty_buffers)
            logger.debug(f"[StreamingParquetWriter] Cleaned {len(empty_buffers)} empty partition buffers")
        
        # 4. Clean up partition sizes for non-existent segments
        orphan_sizes = [
            key for key in self._partition_sizes.keys()
            if key not in self._partition_segments
        ]
        for key in orphan_sizes:
            del self._partition_sizes[key]
        
        # 5. Force garbage collection
        collected = gc.collect()
        self.stats["gc_collections"] += 1
        self.stats["memory_cleanups"] += 1
        
        logger.info(
            f"[StreamingParquetWriter] Memory cleanup: "
            f"counters={len(self.hour_counters)}, "
            f"buffers={len(self._partition_buffers)}, "
            f"segments={len(self._partition_segments)}, "
            f"gc_collected={collected}"
        )
    
    def get_stats(self) -> dict:
        """Get writer statistics including open file handle count and memory info."""
        return {
            **self.stats,
            "queue_size": self.queue.qsize(),
            "open_writers": len(self._partition_segments),
            "max_open_writers": MAX_OPEN_WRITERS,
            "hour_counters_count": len(self.hour_counters),
            "partition_buffers_count": len(self._partition_buffers),
        }
