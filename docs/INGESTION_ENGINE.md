# Daedalus Ingestion Engine

## Overview

The Daedalus Ingestion Engine is a high-performance, real-time market data collection system optimized for 24/7 operation on resource-constrained hardware (e.g., Raspberry Pi 4). It streams cryptocurrency market data from multiple exchanges via WebSocket connections and persists it to Parquet files for downstream processing.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Daedalus Supervisor (systemd)                        │
│  - Process management, health monitoring, auto-restart, memory limits       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
        ┌───────────────────┐           ┌───────────────────┐
        │  Ingestion Proc   │           │    Sync Process   │
        │  (run_ingestion)  │           │    (run_sync)     │
        └───────────────────┘           └───────────────────┘
                    │
                    ▼
        ┌───────────────────────────────────────────────────┐
        │              IngestionPipeline                     │
        │  - Orchestrates collectors and writers             │
        │  - Manages lifecycle and shutdown coordination     │
        └───────────────────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
┌───────────────┐       ┌───────────────┐
│ CcxtCollector │ ...   │ CoinbaseWS    │
│ (per exchange)│       │  Collector    │
└───────────────┘       └───────────────┘
        │                       │
        └───────────┬───────────┘
                    ▼
        ┌───────────────────────────────────────────────────┐
        │         StreamingParquetWriter                     │
        │  - Bounded async queue (backpressure)              │
        │  - Dedicated I/O thread pool                       │
        │  - Per-partition buffering & file rotation         │
        │  - Directory-aligned partitioning                  │
        └───────────────────────────────────────────────────┘
                    │
                    ▼
        ┌───────────────────────────────────────────────────┐
        │              Storage Backend                       │
        │  - Local filesystem or S3                          │
        │  - Active/Ready directory segregation              │
        └───────────────────────────────────────────────────┘
```

## Components

### 1. Daedalus Supervisor (`scripts/daedalus_supervisor.py`)

The supervisor is a Python-based process manager that:

- **Starts/stops** ingestion and sync processes
- **Monitors health** via memory usage and process responsiveness
- **Auto-restarts** crashed processes with exponential backoff
- **Prevents crash loops** by limiting restarts per time window
- **Applies CPU affinity** and process priority (Unix systems)

**Configuration:**

```python
@dataclass
class SupervisorConfig:
    # Memory limits
    memory_warning_mb: int = 3000     # Log warning at 3GB
    memory_critical_mb: int = 3500    # Force restart at 3.5GB
    
    # Shutdown timeouts
    ingestion_shutdown_timeout: int = 120  # 2 min for graceful flush
    sync_shutdown_timeout: int = 60
    
    # Process tuning (Linux only)
    process_nice: int = 0             # -20 (high) to 19 (low)
    cpu_affinity: List[int] = []      # Pin to specific cores
```

### 2. Ingestion Pipeline (`ingestion/orchestrators/ingestion_pipeline.py`)

The orchestrator manages the lifecycle of all collectors and writers:

- **Initializes storage** backend (local or S3)
- **Creates writers** per data source (Parquet or NDJSON)
- **Spawns collectors** for each configured exchange
- **Coordinates shutdown** ensuring data preservation

### 3. CCXT Collector (`ingestion/collectors/ccxt_collector.py`)

Streams market data from any CCXT-supported exchange:

**Features:**
- Concurrent subscription initialization with staggered starts
- Bulk subscription methods for supported exchanges
- Non-blocking writes with backpressure handling
- Automatic reconnection with exponential backoff

**Supported Channels:**
- `watchTicker` / `watchTickers` - Real-time price quotes
- `watchTrades` / `watchTradesForSymbols` - Trade executions
- `watchOrderBook` / `watchOrderBookForSymbols` - L2 order book snapshots

**Performance Constants:**
```python
SUBSCRIPTION_STAGGER_MS = 100     # Delay between subscriptions
MAX_CONCURRENT_SUBSCRIPTIONS = 5  # Parallel subscription limit
```

### 4. Streaming Parquet Writer (`ingestion/writers/parquet_writer.py`)

High-performance writer optimized for continuous operation:

**Features:**
- **Bounded async queue** with configurable backpressure (default: 50,000 msgs)
- **Dedicated ThreadPoolExecutor** for I/O operations
- **Efficient batch draining** with non-blocking queue access
- **LRU-based writer eviction** to prevent file descriptor exhaustion
- **Directory-aligned partitioning** (exchange/symbol/year/month/day/hour)
- **Automatic segment rotation** on size limit or hour change
- **Emergency cleanup handlers** for unexpected shutdowns

**Performance Tuning:**
```python
MAX_OPEN_WRITERS = 350            # Concurrent Parquet file handles
IO_THREAD_POOL_SIZE = min(32, CPU_COUNT * 4)  # I/O threads
MEMORY_CLEANUP_INTERVAL = 300     # GC every 5 minutes
HOUR_COUNTER_MAX_AGE_SECONDS = 7200  # Cleanup stale counters
```

## Data Flow

### Hot Path (Minimal Latency)

```
WebSocket recv()
    │
    ▼ Capture timestamp immediately
CcxtCollector._bulk_loop()
    │
    ▼ Non-blocking put
Writer.queue.put_nowait(record)
    │
    ▼ Background drain loop
_partition_buffers[key].append(record)
```

### Cold Path (I/O Thread Pool)

```
_writer_loop (async)
    │
    ▼ When batch full or interval elapsed
run_in_executor(_io_executor, _flush_all_partitions)
    │
    ▼ In thread pool
_records_to_table() → PyArrow Table
    │
    ▼
ParquetWriter.write_table()
    │
    ▼ On rotation/shutdown
shutil.move(active → ready)
```

## Performance Optimizations

### 1. Queue Drain Efficiency

```python
# First get (may block up to timeout)
record = await asyncio.wait_for(self.queue.get(), timeout=0.1)

# Non-blocking drain of remaining items
while drained < self.batch_size:
    try:
        record = self.queue.get_nowait()  # No await overhead
        ...
    except asyncio.QueueEmpty:
        break
```

### 2. Dedicated I/O Thread Pool

```python
self._io_executor = ThreadPoolExecutor(
    max_workers=IO_THREAD_POOL_SIZE,
    thread_name_prefix=f"parquet_io_{source_name}"
)

# All disk operations use the dedicated pool
await loop.run_in_executor(self._io_executor, self._flush_all_partitions)
```

### 3. Pre-extracted Partition Values

```python
def _convert_ticker_records(self, records, pv):
    # Pre-extract once (avoid repeated dict lookups)
    exchange = pv.get("exchange", "")
    symbol = pv.get("symbol", "")
    year = pv.get("year", 0)
    ...
    
    for r in records:
        # Use pre-extracted values in loop
        rows.append({"exchange": exchange, "symbol": symbol, ...})
```

### 4. Memory Leak Prevention

```python
def _periodic_memory_cleanup(self):
    # Clean stale hour_counters (older than 2 hours)
    for key, (count, last_used) in list(self.hour_counters.items()):
        if current_time - last_used > HOUR_COUNTER_MAX_AGE_SECONDS:
            del self.hour_counters[key]
    
    # Clean empty partition buffers
    for key, buf in self._partition_buffers.items():
        if len(buf) == 0 and key not in self._partition_segments:
            del self._partition_buffers[key]
    
    # Force garbage collection
    gc.collect()
```

## Configuration

### config.yaml

```yaml
ingestion:
  raw_format: "parquet"           # or "ndjson" (legacy)
  batch_size: 1000                # Records per batch
  flush_interval_seconds: 5.0     # Max time between flushes
  queue_maxsize: 50000            # Queue backpressure limit
  segment_max_mb: 100             # Rotate files at 100MB
  parquet_compression: "zstd"     # zstd, snappy, lz4
  parquet_compression_level: 3    # 1-22 for zstd
  partition_by:                   # Directory partitioning
    - exchange
    - symbol
    - year
    - month
    - day
    - hour
  auto_reconnect: true
  max_reconnect_attempts: -1      # -1 = infinite for 24/7
  reconnect_delay: 5.0

ccxt:
  exchanges:
    coinbaseadvanced:
      api_key: ""
      api_secret: ""
      channels:
        watchOrderBook:
          - BTC/USD
          - ETH/USD
        watchTrades:
          - BTC/USD
        watchTicker:
          - BTC/USD
      options:
        orderbook_watch_batch_size: 20
      max_orderbook_depth: 20
```

## Deployment

### systemd Service

```ini
# /etc/systemd/system/daedalus.service
[Unit]
Description=Daedalus Market Data Pipeline
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=admin
Group=admin
WorkingDirectory=/home/admin/Desktop/daedalus
ExecStart=/home/admin/Desktop/daedalus/venv/bin/python \
    scripts/daedalus_supervisor.py --sources ccxt
Restart=always
RestartSec=30

# Resource limits
MemoryMax=4G
MemoryHigh=3.5G

[Install]
WantedBy=multi-user.target
```

### Commands

```bash
# Enable and start
sudo systemctl enable daedalus
sudo systemctl start daedalus

# View logs
journalctl -u daedalus -f

# Check status
python scripts/daedalus_supervisor.py --status
```

## Monitoring

### Health Metrics

The ingestion engine exposes health metrics via `pipeline.print_stats()`:

```
================================================================================
Pipeline Statistics
================================================================================
✓ 🟢 ccxt_coinbaseadvanced: msgs=1,234,567, errors=0, reconnects=2, uptime=86400s
Writer ccxt: written=1,234,567, queue=1234 (2.5%), flushes=12345, open_writers=42
  └─ Memory: cleanups=288, gc_runs=288, counters=24, buffers=42
================================================================================
Health: 1/1 collectors healthy
================================================================================
```

### Key Indicators

| Metric | Healthy Range | Action if Unhealthy |
|--------|---------------|---------------------|
| Queue % | < 80% | Increase batch_size or add resources |
| Queue overflow events | 0 | Messages being dropped - reduce symbols or add capacity |
| Memory (MB) | < 3000 | Check for leaks, reduce partition count |
| Open writers | < 350 | Reduce symbol count or increase MAX_OPEN_WRITERS |
| Reconnections | Low | Network issues if frequent |

## Troubleshooting

### High Memory Usage

1. Check `hour_counters_count` and `partition_buffers_count` in stats
2. Reduce number of symbols/channels
3. Decrease `segment_max_mb` for faster rotation
4. Lower `memory_warning_mb` threshold

### Queue Overflow

1. Increase `queue_maxsize` (requires more RAM)
2. Increase `batch_size` for more efficient writes
3. Reduce number of symbols
4. Upgrade storage (SSD vs HDD)

### Slow Writes

1. Ensure `parquet_compression_level` ≤ 3 (higher = slower)
2. Use local SSD instead of network storage
3. Increase `IO_THREAD_POOL_SIZE` if CPU-bound

### Graceful Shutdown Timeout

1. Increase `ingestion_shutdown_timeout` in supervisor config
2. Check logs for flush progress
3. Reduce in-flight data by lowering `queue_maxsize`

## File Structure

```
data/
├── raw/
│   ├── active/                   # Currently writing
│   │   └── ccxt/
│   │       └── orderbook/
│   │           └── exchange=coinbaseadvanced/
│   │               └── symbol=BTC-USD/
│   │                   └── year=2026/
│   │                       └── month=2/
│   │                           └── day=3/
│   │                               └── hour=14/
│   │                                   └── segment_20260203T14_00001.parquet
│   └── ready/                    # Ready for ETL
│       └── ccxt/
│           └── ...
└── processed/                    # After ETL
    └── silver/
        └── ...
```

## See Also

- [PI4_DEPLOYMENT.md](PI4_DEPLOYMENT.md) - Raspberry Pi deployment guide
- [CCXT_ETL_ARCHITECTURE.md](CCXT_ETL_ARCHITECTURE.md) - ETL pipeline documentation
- [PARQUET_ETL_ARCHITECTURE.md](PARQUET_ETL_ARCHITECTURE.md) - Parquet processing details
- [UNIFIED_STORAGE_ARCHITECTURE.md](UNIFIED_STORAGE_ARCHITECTURE.md) - Storage backend configuration
