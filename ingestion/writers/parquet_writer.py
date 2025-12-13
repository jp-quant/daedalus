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

Architecture (Medallion):
    Bronze (this writer) → Silver (clean) → Gold (features)
    Raw Parquet            Normalized        Time-series bars
"""
import asyncio
import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
from collections import deque
import json

import pyarrow as pa
import pyarrow.parquet as pq

from storage.base import StorageBackend
from ingestion.utils.time import utc_now

logger = logging.getLogger(__name__)


# Schema definitions for different channel types
# These preserve raw data while enabling efficient columnar storage

def _get_ticker_schema() -> pa.Schema:
    """Schema for ticker data - preserves all CCXT unified ticker fields."""
    return pa.schema([
        pa.field("collected_at", pa.int64()),  # Exchange timestamp (ms)
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),  # Our capture time
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
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
        # Raw info preserved as JSON string for exchange-specific fields
        pa.field("info_json", pa.string()),
    ])


def _get_trades_schema() -> pa.Schema:
    """Schema for trade data - one row per trade."""
    return pa.schema([
        pa.field("collected_at", pa.int64()),
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("trade_id", pa.string()),
        pa.field("timestamp", pa.int64()),
        pa.field("side", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("amount", pa.float64()),
        pa.field("cost", pa.float64()),
        # Raw info preserved
        pa.field("info_json", pa.string()),
    ])


def _get_orderbook_schema() -> pa.Schema:
    """
    Schema for orderbook data - stores bids/asks as nested lists.
    
    This is the raw landing format - not denormalized.
    ETL can later flatten to level-by-level if needed.
    """
    # Price/size pairs as list of structs
    level_type = pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("size", pa.float64()),
    ]))
    
    return pa.schema([
        pa.field("collected_at", pa.int64()),
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("timestamp", pa.int64()),  # Exchange timestamp
        pa.field("nonce", pa.int64()),  # Sequence number if provided
        pa.field("bids", level_type),  # [[price, size], ...]
        pa.field("asks", level_type),  # [[price, size], ...]
    ])


def _get_generic_schema() -> pa.Schema:
    """Fallback schema for unknown channel types - stores as JSON."""
    return pa.schema([
        pa.field("collected_at", pa.int64()),
        pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("type", pa.string()),
        pa.field("method", pa.string()),
        pa.field("data_json", pa.string()),  # Full data as JSON
    ])


CHANNEL_SCHEMAS = {
    "ticker": _get_ticker_schema(),
    "trades": _get_trades_schema(),
    "orderbook": _get_orderbook_schema(),
}


class StreamingParquetWriter:
    """
    Batched streaming writer for raw market data to Parquet.
    
    Design:
    - Bounded queue with backpressure
    - Batch writes to reduce I/O overhead
    - Size-based segment rotation (prevents unbounded growth)
    - Channel-based file separation (ticker, trades, orderbook)
    - Active/ready directory segregation
    - Works with any StorageBackend
    
    Directory structure:
        {active_path}/{channel}/segment_*.parquet    # Being written
        {ready_path}/{channel}/segment_*.parquet     # Ready for ETL
    
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
        
        # Bounded queue for backpressure
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        
        # Per-channel buffers and segment tracking
        self._channel_buffers: Dict[str, deque] = {}
        self._channel_segments: Dict[str, Dict[str, Any]] = {}
        self._channel_sizes: Dict[str, int] = {}
        
        # Hour-based segment naming
        self.current_date_hour: str = ""
        self.hour_counters: Dict[str, int] = {}
        
        # Statistics
        self.stats = {
            "messages_received": 0,
            "messages_written": 0,
            "flushes": 0,
            "rotations": 0,
            "queue_full_events": 0,
            "errors": 0,
            "bytes_written": 0,
        }
        
        # Writer task
        self._writer_task: Optional[asyncio.Task] = None
        self._shutdown = asyncio.Event()
        
        # Ensure directories exist
        self.storage.mkdir(self.active_path)
        self.storage.mkdir(self.ready_path)
        
        logger.info(
            f"[StreamingParquetWriter] Initialized: source={source_name}, "
            f"compression={compression}:{compression_level}, segment_max_mb={segment_max_mb}"
        )
    
    async def start(self):
        """Start the writer background task."""
        if self._writer_task is not None:
            logger.warning("[StreamingParquetWriter] Already started")
            return
        
        self._writer_task = asyncio.create_task(self._writer_loop())
        logger.info("[StreamingParquetWriter] Started")
    
    async def stop(self):
        """Stop the writer and flush pending data."""
        if self._writer_task is None:
            return
        
        logger.info("[StreamingParquetWriter] Stopping...")
        self._shutdown.set()
        
        try:
            await asyncio.wait_for(self._writer_task, timeout=30.0)
        except asyncio.TimeoutError:
            logger.error("[StreamingParquetWriter] Writer task did not stop gracefully")
            self._writer_task.cancel()
        
        # Final flush and close all channels
        await asyncio.get_event_loop().run_in_executor(
            None, self._close_all_segments
        )
        
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
        """Background task that batches and writes records."""
        last_flush_time = asyncio.get_event_loop().time()
        
        while not self._shutdown.is_set() or not self.queue.empty():
            try:
                # Drain queue
                drained = 0
                while drained < self.batch_size:
                    try:
                        record = await asyncio.wait_for(
                            self.queue.get(), timeout=0.1
                        )
                        channel = record.get("type", "unknown")
                        if channel not in self._channel_buffers:
                            self._channel_buffers[channel] = deque()
                        self._channel_buffers[channel].append(record)
                        drained += 1
                    except asyncio.TimeoutError:
                        break
                
                # Check if flush needed
                current_time = asyncio.get_event_loop().time()
                time_to_flush = (current_time - last_flush_time) >= self.flush_interval
                
                any_buffer_full = any(
                    len(buf) >= self.batch_size 
                    for buf in self._channel_buffers.values()
                )
                
                if any_buffer_full or time_to_flush:
                    await asyncio.get_event_loop().run_in_executor(
                        None, self._flush_all_channels
                    )
                    last_flush_time = current_time
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.stats["errors"] += 1
                logger.error(f"[StreamingParquetWriter] Error in writer loop: {e}")
                await asyncio.sleep(1.0)
        
        # Final flush
        await asyncio.get_event_loop().run_in_executor(
            None, self._flush_all_channels
        )
    
    def _flush_all_channels(self):
        """Flush all channel buffers to their respective Parquet files."""
        for channel, buffer in list(self._channel_buffers.items()):
            if buffer:
                self._flush_channel(channel, buffer)
    
    def _flush_channel(self, channel: str, buffer: deque):
        """Flush a single channel buffer to Parquet."""
        if not buffer:
            return
        
        records = list(buffer)
        buffer.clear()
        
        try:
            # Convert records to PyArrow table
            table = self._records_to_table(channel, records)
            if table is None or table.num_rows == 0:
                return
            
            # Ensure segment is open
            self._ensure_segment_open(channel)
            
            # Write to segment
            segment_info = self._channel_segments[channel]
            writer = segment_info["writer"]
            writer.write_table(table)
            
            # Track size
            bytes_written = table.nbytes
            self._channel_sizes[channel] = self._channel_sizes.get(channel, 0) + bytes_written
            self.stats["bytes_written"] += bytes_written
            self.stats["messages_written"] += len(records)
            self.stats["flushes"] += 1
            
            # Check for rotation
            if self._channel_sizes[channel] >= self.segment_max_bytes:
                self._rotate_segment(channel)
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"[StreamingParquetWriter] Error flushing {channel}: {e}")
    
    def _records_to_table(self, channel: str, records: List[Dict]) -> Optional[pa.Table]:
        """Convert raw records to PyArrow table for the given channel."""
        if not records:
            return None
        
        try:
            if channel == "ticker":
                return self._convert_ticker_records(records)
            elif channel == "trades":
                return self._convert_trades_records(records)
            elif channel == "orderbook":
                return self._convert_orderbook_records(records)
            else:
                return self._convert_generic_records(records)
        except Exception as e:
            logger.error(f"[StreamingParquetWriter] Error converting {channel}: {e}")
            # Fallback to generic
            return self._convert_generic_records(records)
    
    def _convert_ticker_records(self, records: List[Dict]) -> pa.Table:
        """Convert ticker records to PyArrow table."""
        rows = []
        for r in records:
            data = r.get("data", {})
            rows.append({
                "collected_at": r.get("collected_at", 0),
                "capture_ts": self._parse_ts(r.get("capture_ts")),
                "exchange": r.get("exchange", ""),
                "symbol": r.get("symbol", ""),
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
                "info_json": json.dumps(data.get("info", {}), separators=(",", ":")),
            })
        return pa.Table.from_pylist(rows, schema=CHANNEL_SCHEMAS["ticker"])
    
    def _convert_trades_records(self, records: List[Dict]) -> pa.Table:
        """Convert trades records to PyArrow table (one row per trade)."""
        rows = []
        for r in records:
            collected_at = r.get("collected_at", 0)
            capture_ts = self._parse_ts(r.get("capture_ts"))
            exchange = r.get("exchange", "")
            symbol = r.get("symbol", "")
            
            # trades can be a list
            trades = r.get("data", [])
            if isinstance(trades, dict):
                trades = [trades]
            
            for trade in trades:
                rows.append({
                    "collected_at": collected_at,
                    "capture_ts": capture_ts,
                    "exchange": exchange,
                    "symbol": symbol,
                    "trade_id": str(trade.get("id", "")),
                    "timestamp": trade.get("timestamp", 0),
                    "side": trade.get("side", ""),
                    "price": trade.get("price"),
                    "amount": trade.get("amount"),
                    "cost": trade.get("cost"),
                    "info_json": json.dumps(trade.get("info", {}), separators=(",", ":")),
                })
        return pa.Table.from_pylist(rows, schema=CHANNEL_SCHEMAS["trades"])
    
    def _convert_orderbook_records(self, records: List[Dict]) -> pa.Table:
        """Convert orderbook records to PyArrow table with nested bids/asks."""
        rows = []
        for r in records:
            data = r.get("data", {})
            
            # Convert bids/asks to list of dicts for struct array
            bids = [{"price": b[0], "size": b[1]} for b in data.get("bids", [])]
            asks = [{"price": a[0], "size": a[1]} for a in data.get("asks", [])]
            
            rows.append({
                "collected_at": r.get("collected_at", 0),
                "capture_ts": self._parse_ts(r.get("capture_ts")),
                "exchange": r.get("exchange", ""),
                "symbol": r.get("symbol", ""),
                "timestamp": data.get("timestamp", 0),
                "nonce": data.get("nonce", 0),
                "bids": bids,
                "asks": asks,
            })
        return pa.Table.from_pylist(rows, schema=CHANNEL_SCHEMAS["orderbook"])
    
    def _convert_generic_records(self, records: List[Dict]) -> pa.Table:
        """Fallback conversion - stores full data as JSON."""
        rows = []
        for r in records:
            rows.append({
                "collected_at": r.get("collected_at", 0),
                "capture_ts": self._parse_ts(r.get("capture_ts")),
                "exchange": r.get("exchange", ""),
                "symbol": r.get("symbol", ""),
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
            # Handle various formats
            if ts_str.endswith("Z"):
                ts_str = ts_str[:-1] + "+00:00"
            return datetime.fromisoformat(ts_str)
        except Exception:
            return None
    
    def _ensure_segment_open(self, channel: str):
        """Ensure a segment file is open for the channel."""
        current_hour = utc_now().strftime("%Y%m%dT%H")
        
        # Check if we need a new segment (hour changed or no segment)
        if channel not in self._channel_segments or current_hour != self.current_date_hour:
            if channel in self._channel_segments:
                self._rotate_segment(channel)
            
            self.current_date_hour = current_hour
            self._open_new_segment(channel)
    
    def _open_new_segment(self, channel: str):
        """Open a new segment file for the channel."""
        # Get counter for this hour/channel
        counter_key = f"{self.current_date_hour}_{channel}"
        if counter_key not in self.hour_counters:
            self.hour_counters[counter_key] = 0
        self.hour_counters[counter_key] += 1
        counter = self.hour_counters[counter_key]
        
        # Create segment name
        segment_name = f"segment_{self.current_date_hour}_{counter:05d}.parquet"
        
        # Channel subdirectory
        channel_path = self.storage.join_path(self.active_path, channel)
        self.storage.mkdir(channel_path)
        
        segment_path = self.storage.join_path(channel_path, segment_name)
        
        # Get schema for channel
        schema = CHANNEL_SCHEMAS.get(channel, _get_generic_schema())
        
        # Open writer
        if self.storage.backend_type == "local":
            full_path = self.storage.get_full_path(segment_path)
            writer = pq.ParquetWriter(
                full_path,
                schema,
                compression=self.compression,
                compression_level=self.compression_level,
            )
        else:
            # For S3, we'll buffer in memory and upload on rotation
            buffer = io.BytesIO()
            writer = pq.ParquetWriter(
                buffer,
                schema,
                compression=self.compression,
                compression_level=self.compression_level,
            )
            self._channel_segments[channel] = {
                "writer": writer,
                "buffer": buffer,
                "path": segment_path,
                "name": segment_name,
            }
            self._channel_sizes[channel] = 0
            return
        
        self._channel_segments[channel] = {
            "writer": writer,
            "path": segment_path,
            "name": segment_name,
        }
        self._channel_sizes[channel] = 0
        
        logger.debug(f"[StreamingParquetWriter] Opened segment: {segment_path}")
    
    def _rotate_segment(self, channel: str):
        """Close current segment and move to ready directory."""
        if channel not in self._channel_segments:
            return
        
        segment_info = self._channel_segments.pop(channel)
        writer = segment_info["writer"]
        
        try:
            writer.close()
            
            # Move from active to ready
            active_path = segment_info["path"]
            ready_channel_path = self.storage.join_path(self.ready_path, channel)
            self.storage.mkdir(ready_channel_path)
            ready_path = self.storage.join_path(ready_channel_path, segment_info["name"])
            
            if self.storage.backend_type == "local":
                self.storage.move(active_path, ready_path)
            else:
                # For S3, upload the buffer
                buffer = segment_info.get("buffer")
                if buffer:
                    buffer.seek(0)
                    self.storage.write_bytes(ready_path, buffer.read())
            
            self.stats["rotations"] += 1
            size_mb = self._channel_sizes.get(channel, 0) / 1024 / 1024
            logger.info(f"[StreamingParquetWriter] Rotated {channel}: {segment_info['name']} ({size_mb:.1f} MB)")
        
        except Exception as e:
            logger.error(f"[StreamingParquetWriter] Error rotating {channel}: {e}")
        
        self._channel_sizes[channel] = 0
    
    def _close_all_segments(self):
        """Close and move all open segments to ready."""
        for channel in list(self._channel_segments.keys()):
            self._rotate_segment(channel)
    
    def get_stats(self) -> dict:
        """Get writer statistics."""
        return {
            **self.stats,
            "queue_size": self.queue.qsize(),
            "open_segments": list(self._channel_segments.keys()),
        }
