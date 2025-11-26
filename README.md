# FluxForge

**A modular, high-throughput market data ingestion + ETL pipeline with size-based segment rotation**

FluxForge is a production-grade data ingestion engine that "forges" raw market flux into structured, queryable datasets for research and live trading. Built for reliability, it runs seamlessly from Raspberry Pi to high-end servers.

---

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Architecture](#-architecture)
- [Features](#-features)
- [Installation](#-installation)
- [Configuration](#%EF%B8%8F-configuration)
- [Usage](#-usage)
- [Directory Structure](#-directory-structure)
- [Segment Rotation](#-segment-rotation-system)
- [Monitoring](#-monitoring)
- [Production Setup](#-production-setup)
- [Troubleshooting](#-troubleshooting)
- [Development](#-development)

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure (copy example and edit with your credentials)
cp config/secrets.example.yaml config/config.yaml
# Edit config/config.yaml with your API credentials

# 3. Start ingestion (collects real-time data)
python scripts/run_ingestion.py --config config/config.yaml

# 4. Start continuous ETL (processes data as segments close)
python scripts/run_etl_watcher.py --poll-interval 30

# 5. Query processed data
python scripts/query_parquet.py data/processed/coinbase/ticker

# 6. Check system health
python scripts/check_health.py
```

**Data flow**: WebSocket → NDJSON segments → Partitioned Parquet files

**Output structure**:
```
data/processed/coinbase/
  ├── ticker/product_id=BTC-USD/date=2025-11-26/part_*.parquet
  ├── level2/product_id=BTC-USD/date=2025-11-26/part_*.parquet
  └── market_trades/product_id=BTC-USD/date=2025-11-26/part_*.parquet
```

---

## 🏗️ Architecture

FluxForge follows a **strict three-layer architecture** that separates concerns and ensures reliability:

### Layer 1: WebSocket Collectors (I/O Only)

**Purpose**: Capture raw messages from market data sources

**Design**:
- Pure async I/O - zero CPU-intensive processing
- Connect to exchange WebSocket APIs
- Add capture timestamp
- Push to bounded asyncio queue
- Automatic reconnection with exponential backoff

**Key files**: `ingestion/collectors/`

```python
# Collectors are I/O-only
async def _collect(self):
    async with websockets.connect(url) as ws:
        message = await ws.recv()
        capture_ts = utc_now()  # Timestamp immediately
        
        # Push to queue (no parsing!)
        await log_writer.write({
            "capture_ts": capture_ts,
            "data": json.loads(message),
            "metadata": {"source": "coinbase"}
        })
```

### Layer 2: Batched Log Writer with Segment Rotation

**Purpose**: Durable, append-only storage with size-based file rotation

**Design**:
- Pulls from asyncio queue
- Batches records (configurable size)
- Writes NDJSON to active segment
- **Rotates when segment reaches size limit** (default: 100 MB)
- Atomic move from `active/` → `ready/` directory
- fsync for durability guarantees

**Key files**: `ingestion/writers/log_writer.py`

**Directory structure**:
```
data/raw/
  ├── active/coinbase/    # ONE open segment (currently writing)
  │   └── segment_20251120T14_00042.ndjson
  │
  └── ready/coinbase/     # Closed segments (ready for ETL)
      ├── segment_20251120T14_00038.ndjson
      ├── segment_20251120T14_00039.ndjson
      ├── segment_20251120T14_00040.ndjson
      └── segment_20251120T14_00041.ndjson
```

**Segment naming**: `segment_<YYYYMMDDTHH>_<COUNTER>.ndjson`
- Counter resets each hour (prevents overflow)
- Example: `segment_20251120T14_00042.ndjson` = 42nd segment at 2PM
- Example: `segment_20251120T15_00001.ndjson` = 1st segment at 3PM (counter reset)

**Rotation logic**:
```python
# After each batch flush:
if current_segment_size >= segment_max_bytes:
    close_file()
    os.rename("active/segment.ndjson", "ready/segment.ndjson")  # Atomic!
    if hour_changed:
        counter = 1  # Reset
    else:
        counter += 1  # Increment
    create_new_segment()
```

### Layer 3: Offline ETL Workers (CPU-Intensive)

**Purpose**: Transform raw NDJSON segments into structured, partitioned Parquet files

**Design**:
- Reads from `ready/` directory (never touches `active/`)
- Atomically moves segment to `processing/` (prevents double-processing)
- Uses composable pipeline architecture:
  - **Readers**: NDJSONReader, ParquetReader (with Polars)
  - **Processors**: Source-specific transforms (coinbase/, databento/, ibkr/)
  - **Writers**: Flexible ParquetWriter with Hive-style partitioning
- Routes channels to specialized processors (level2 → LOB reconstruction, trades → VWAP)
- **Partitions by product_id AND date** (Hive-style for distributed queries)
- Deletes processed segment (configurable)

**Key files**: 
- `etl/orchestrators/coinbase_segment_pipeline.py` - Multi-channel routing
- `etl/processors/coinbase/` - Coinbase-specific transforms
- `etl/parsers/coinbase_parser.py` - Message extraction
- `etl/writers/parquet_writer.py` - Partitioned output

**ETL workflow**:
```
ready/segment_X.ndjson
  → processing/segment_X.ndjson  (atomic move)
  → CoinbaseSegmentPipeline
      ├─ NDJSONReader
      ├─ RawParser (extract by channel)
      ├─ Level2Processor / TradesProcessor / TickerProcessor
      └─ ParquetWriter (with partitioning)
  → write to processed/coinbase/{channel}/product_id=X/date=Y/part_*.parquet
  → delete segment_X.ndjson
```

**Output structure** (Hive-style partitioning for distributed queries):
```
data/processed/
  └── coinbase/
      ├── ticker/
      │   ├── product_id=BTC-USD/
      │   │   ├── date=2025-11-26/
      │   │   │   ├── part_segment_001_abc123.parquet
      │   │   │   └── part_segment_002_def456.parquet
      │   │   └── date=2025-11-27/
      │   │       └── part_segment_003_ghi789.parquet
      │   └── product_id=ETH-USD/
      │       └── date=2025-11-26/
      │           └── part_segment_001_jkl012.parquet
      ├── level2/
      │   └── product_id=BTC-USD/
      │       └── date=2025-11-26/
      │           └── part_*.parquet
      └── market_trades/
          └── product_id=BTC-USD/
              └── date=2025-11-26/
                  └── part_*.parquet
```

**Scalable Parquet Strategy**:
- ✅ **Composable ETL**: Reader → Processor → Writer architecture
- ✅ **Source-specific processors**: `etl/processors/coinbase/`, `databento/`, `ibkr/`
- ✅ **Channel routing**: Automatic routing to specialized processors
- ✅ **Flexible partitioning**: Configure partition columns per channel
- ✅ **No in-memory merging**: Writes append-only, never loads existing files
- ✅ **Hive-style partitioning**: Product + date enables efficient filtering
- ✅ **Distributed queries**: DuckDB, Spark, Polars can query in parallel
- ✅ **Schema evolution**: Each file stores its schema independently

**Query Examples**:

```python
# DuckDB (recommended for local analytics)
import duckdb
df = duckdb.query("""
    SELECT * FROM 'data/processed/coinbase/ticker/**/*.parquet'
    WHERE product_id = 'BTC-USD' AND date = '2025-11-26'
""").to_df()

# Polars (blazing fast, lazy evaluation)
import polars as pl
df = pl.scan_parquet("data/processed/coinbase/ticker/**/*.parquet") \
    .filter(pl.col("product_id") == "BTC-USD") \
    .filter(pl.col("date") == "2025-11-26") \
    .collect()

# Pandas (simple and familiar)
import pandas as pd
df = pd.read_parquet("data/processed/coinbase/ticker/product_id=BTC-USD/date=2025-11-26")
```

See `scripts/query_parquet.py` for full query examples.

### Critical Separation

**Why three layers?**

1. **Ingestion never blocks**: I/O-only, no CPU work
2. **No data loss**: Bounded queues + backpressure + fsync
3. **ETL independence**: Reprocess data without re-ingesting
4. **Segment isolation**: Ingestion and ETL never touch same file
5. **Scalability**: Runs on Raspberry Pi or high-end servers

**Data flow**:
```
WebSocket → Collector → Queue → LogWriter → NDJSON segment
                                                  ↓
                                            (size limit hit)
                                                  ↓
                                         active/ → ready/
                                                  ↓
                                            ETL processes
                                                  ↓
                                            Parquet files
```

---

## ✨ Features

- ✅ **Multi-source support**: Coinbase (crypto), Databento (equities), IBKR (futures) - extensible
- ✅ **Zero message loss**: Bounded queues with backpressure handling
- ✅ **Size-based segment rotation**: Prevents unbounded file growth (configurable MB limit)
- ✅ **Active/ready segregation**: ETL never interferes with ingestion
- ✅ **Atomic operations**: `os.rename()` ensures no partial files
- ✅ **Low-power friendly**: Proven on Raspberry Pi
- ✅ **Durability**: fsync guarantees, append-only logs
- ✅ **Near real-time ETL**: Process segments as they close (30-second polling)
- ✅ **Config-driven**: YAML configuration, no code changes needed
- ✅ **Production-ready**: Logging, stats, graceful shutdown, health checks

---

## 📦 Installation

### Requirements
- Python 3.10 or higher
- pip (or poetry)
- 10+ GB disk space recommended

### Install

```bash
# Clone repository
git clone https://github.com/yourusername/fluxforge.git
cd fluxforge

# Install dependencies
pip install -r requirements.txt

# OR install as editable package (optional)
pip install -e .
```

---

## ⚙️ Configuration

### 1. Create config file

```bash
cp config/secrets.example.yaml config/config.yaml
```

### 2. Edit `config/config.yaml`

```yaml
# Coinbase Advanced Trade API
coinbase:
  api_key: "organizations/xxx/apiKeys/xxx"
  api_secret: "-----BEGIN EC PRIVATE KEY-----\n...\n-----END EC PRIVATE KEY-----\n"
  product_ids:
    - "BTC-USD"
    - "ETH-USD"
  channels:
    - "ticker"
    - "level2"
    - "market_trades"

# Ingestion settings
ingestion:
  output_dir: "./data/raw"
  batch_size: 100
  flush_interval_seconds: 5.0
  queue_maxsize: 10000
  enable_fsync: true
  segment_max_mb: 100  # Rotate at 100 MB

# ETL settings
etl:
  input_dir: "./data/raw/ready/coinbase"  # Read from ready/ only!
  output_dir: "./data/processed"
  compression: "snappy"
  delete_after_processing: true  # Auto-cleanup
  processing_dir: "./data/raw/processing"
  
  # Channel-specific ETL configuration
  channels:
    level2:
      partition_cols:
        - "product_id"
        - "date"
      processor_options:
        compute_mid_price: true
        compute_spread: true
    
    market_trades:
      partition_cols:
        - "product_id"
        - "date"
    
    ticker:
      partition_cols:
        - "product_id"
        - "date"
```

### 3. Important: Add to .gitignore

```bash
echo "config/config.yaml" >> .gitignore
```

---

## 🎮 Usage

### Start Ingestion

Collect real-time market data:

```bash
python scripts/run_ingestion.py --config config/config.yaml
```

**Output**:
```
[LogWriter] Initialized segment 20251120T14_00001: segment_20251120T14_00001.ndjson
[CoinbaseCollector] Connected to wss://advanced-trade-ws.coinbase.com
[CoinbaseCollector] Subscribed to channel=ticker, products=['BTC-USD', 'ETH-USD']
Ingestion pipeline running with 1 collector(s)
Press Ctrl+C to stop
```

**Stats** (printed every 60 seconds):
```python
{
  "messages_written": 15234,
  "flushes": 152,
  "rotations": 3,  # Segments rotated
  "current_date_hour": "20251120T14",
  "current_hour_counter": 4,
  "current_segment_size_mb": 45.23,
  "current_segment_file": "segment_20251120T14_00004.ndjson"
}
```

### Run ETL

#### Option 1: Continuous ETL (Recommended)

Process segments automatically as they close:

```bash
python scripts/run_etl_watcher.py --poll-interval 30
```

Polls `ready/` every 30 seconds and processes new segments.

#### Option 2: Manual ETL

Process on-demand:

```bash
# Process all available segments
python scripts/run_etl.py --mode all

# Process specific date
python scripts/run_etl.py --mode date --date 2025-11-26

# Process date range
python scripts/run_etl.py --mode range \
  --start-date 2025-11-15 --end-date 2025-11-26
```

### Query Processed Data

```python
# Option 1: Query with DuckDB (recommended)
python scripts/query_parquet.py data/processed/coinbase/ticker

# Option 2: Use pandas directly
import pandas as pd
df = pd.read_parquet("data/processed/coinbase/ticker/product_id=BTC-USD/date=2025-11-26")
print(df.head())

# Option 3: Use Polars for larger datasets
import polars as pl
df = pl.scan_parquet("data/processed/coinbase/ticker/**/*.parquet") \
    .filter(pl.col("product_id") == "BTC-USD") \
    .collect()
```

**Output structure**:
```
data/processed/coinbase/
  ├── ticker/
  │   └── product_id=BTC-USD/
  │       └── date=2025-11-26/
  │           └── part_*.parquet
  ├── level2/
  │   └── product_id=BTC-USD/
  │       └── date=2025-11-26/
  │           └── part_*.parquet
  └── market_trades/
      └── product_id=BTC-USD/
          └── date=2025-11-26/
              └── part_*.parquet
```

## 📁 Directory Structure

### Project Structure

```
FluxForge/
├── ingestion/              # Layer 1 & 2: Data collection
│   ├── collectors/         # WebSocket collectors (I/O only)
│   │   ├── base_collector.py
│   │   ├── coinbase_ws.py
│   │   ├── databento_ws.py
│   │   └── ibkr_ws.py
│   ├── writers/            # Log writers with rotation
│   │   └── log_writer.py
│   └── utils/              # Utilities
│       ├── time.py
│       └── serialization.py
│
├── etl/                    # Layer 3: Transformation
│   ├── readers/            # Abstract data loading
│   │   ├── ndjson_reader.py
│   │   └── parquet_reader.py
│   ├── processors/         # Transform & aggregate
│   │   ├── coinbase/       # Coinbase-specific
│   │   │   ├── level2_processor.py
│   │   │   ├── trades_processor.py
│   │   │   └── ticker_processor.py
│   │   └── raw_parser.py   # Bridge to parsers
│   ├── parsers/            # Parse NDJSON segments
│   │   └── coinbase_parser.py
│   ├── writers/            # Parquet writers
│   │   └── parquet_writer.py
│   ├── orchestrators/      # Pipeline composition
│   │   ├── pipeline.py
│   │   └── coinbase_segment_pipeline.py
│   └── job.py              # ETL orchestration
│
├── config/                 # Configuration
│   ├── config.py           # Pydantic models
│   ├── config.yaml         # Your config (gitignored)
│   └── secrets.example.yaml
│
├── scripts/                # Entry points
│   ├── run_ingestion.py    # Start ingestion
│   ├── run_etl.py          # Run ETL (manual)
│   ├── run_etl_watcher.py  # Run ETL (continuous)
│   └── check_health.py     # Health check
│
├── tests/                  # Test suite
│   ├── conftest.py
│   ├── test_log_writer.py
│   ├── test_log_rotation.py
│   └── test_parsers.py
│
├── docs/archive/           # Archived documentation
│
├── requirements.txt        # Python dependencies
├── pyproject.toml         # Package metadata
└── README.md              # This file
```

### Data Directory Structure

```
data/
├── raw/                    # Raw NDJSON segments
│   ├── active/             # Currently being written
│   │   └── coinbase/
│   │       └── segment_20251120T14_00042.ndjson  (open)
│   │
│   ├── ready/              # Closed segments (ready for ETL)
│   │   └── coinbase/
│   │       ├── segment_20251120T14_00038.ndjson
│   │       ├── segment_20251120T14_00039.ndjson
│   │       └── segment_20251120T14_00040.ndjson
│   │
│   └── processing/         # Temp during ETL
│       └── coinbase/
│           └── segment_20251120T14_00038.ndjson  (being processed)
│
└── processed/              # Parquet files (Hive-style partitioning)
    └── coinbase/
        ├── ticker/
        │   └── product_id=BTC-USD/
        │       └── date=2025-11-26/
        │           └── part_*.parquet
        ├── level2/
        │   └── product_id=BTC-USD/
        │       └── date=2025-11-26/
        │           └── part_*.parquet
        └── market_trades/
            └── product_id=BTC-USD/
                └── date=2025-11-26/
                    └── part_*.parquet
```

---

## 🔄 Segment Rotation System

### Why Segment Rotation?

**Problem**: With 100+ symbols sending ticker updates, a single NDJSON file grows to 100+ MB in 15-30 minutes. This causes:
- Unbounded file growth
- ETL can't process while ingestion is writing
- Memory issues on low-power devices

**Solution**: Size-based segment rotation with active/ready directories

### How It Works

1. **Ingestion writes to active/**
   ```
   active/coinbase/segment_20251120T14_00001.ndjson  (writing...)
   ```

2. **When size > 100 MB: Rotate**
   ```python
   # LogWriter checks after each flush:
   if current_segment_size >= segment_max_bytes:
       close_file()
       os.rename(active_path, ready_path)  # Atomic move
       create_new_segment()
   ```

3. **Closed segments move to ready/**
   ```
   ready/coinbase/segment_20251120T14_00001.ndjson  (closed, 98.45 MB)
   active/coinbase/segment_20251120T14_00002.ndjson  (new, writing...)
   ```

4. **ETL processes from ready/**
   ```python
   # ETL never touches active/
   for segment in glob("ready/*.ndjson"):
       process(segment)
       delete(segment)
   ```

### Segment Naming Convention

Format: `segment_<YYYYMMDDTHH>_<COUNTER>.ndjson`

**Counter resets every hour** to prevent overflow:
- `segment_20251120T14_00001.ndjson` - 1st segment at 2PM
- `segment_20251120T14_00042.ndjson` - 42nd segment at 2PM
- `segment_20251120T15_00001.ndjson` - 1st segment at 3PM (reset!)

**Benefits**:
- No overflow risk (99,999 segments per hour max)
- Intuitive naming (counter = position in hour)
- Deterministic ordering (sort by filename)
- Easy date filtering for ETL

### Configuration

```yaml
ingestion:
  segment_max_mb: 100  # Rotate at 100 MB (adjustable)
```

**Tuning**:
- **High volume**: `segment_max_mb: 50` (smaller, more frequent rotation)
- **Low volume**: `segment_max_mb: 500` (larger, less frequent rotation)

---

## 📊 Monitoring

### Health Check

```bash
python scripts/check_health.py --config config/config.yaml
```

**Output**:
```
📊 Source: coinbase
--------------------------------------------------------------------------------
✓ Active directory: F:/raw/active/coinbase
  Segments: 1
  - segment_20251120T14_00042.ndjson: 45.23 MB (age: 5 min)

✓ Ready directory: F:/raw/ready/coinbase
  Segments: 8
  Total size: 784.12 MB
  Oldest: segment_20251120T14_00034.ndjson (35 min ago)
  ⚡ Moderate backlog - consider faster ETL

💡 Recommendations
--------------------------------------------------------------------------------
⚡ Consider running ETL watcher more frequently
  python scripts/run_etl_watcher.py --poll-interval 15
```

### Check Segment Sizes

```bash
# Active segments (currently being written)
ls -lh data/raw/active/coinbase/

# Ready segments (waiting for ETL)
ls -lh data/raw/ready/coinbase/

# Count backlog
ls data/raw/ready/coinbase/ | wc -l
```

### Ingestion Logs

Watch for rotation events:

```
[LogWriter] Flushed 100 records (8532 bytes) to segment_20251120T14_00042.ndjson, total size: 98.45 MB
[LogWriter] Rotated segment 20251120T14_00042 (98.45 MB): segment_20251120T14_00042.ndjson → ready/
[LogWriter] New segment 20251120T14_00043: segment_20251120T14_00043.ndjson
```

Hour changes:

```
[LogWriter] Hour changed: 20251120T14 → 20251120T15, counter reset to 1
[LogWriter] New segment 20251120T15_00001: segment_20251120T15_00001.ndjson
```

---

## 🚀 Production Setup

### Option 1: Separate Terminal Windows

```bash
# Terminal 1: Ingestion (always running)
python scripts/run_ingestion.py --config config/config.yaml

# Terminal 2: Continuous ETL (always running)
python scripts/run_etl_watcher.py --poll-interval 30

# Terminal 3: Monitor health (periodic)
watch -n 300 python scripts/check_health.py
```

### Option 2: systemd Services (Linux)

**Ingestion service**: `/etc/systemd/system/fluxforge-ingestion.service`

```ini
[Unit]
Description=FluxForge Ingestion Pipeline
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/FluxForge
ExecStart=/usr/bin/python3 scripts/run_ingestion.py --config config/config.yaml
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**ETL service**: `/etc/systemd/system/fluxforge-etl-watcher.service`

```ini
[Unit]
Description=FluxForge ETL Watcher
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/FluxForge
ExecStart=/usr/bin/python3 scripts/run_etl_watcher.py --poll-interval 30
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Enable and start**:

```bash
sudo systemctl enable fluxforge-ingestion fluxforge-etl-watcher
sudo systemctl start fluxforge-ingestion fluxforge-etl-watcher
sudo systemctl status fluxforge-ingestion fluxforge-etl-watcher
```

### Option 3: Cron (Batch ETL)

For periodic batch processing instead of continuous:

```bash
# Run ETL every 5 minutes
*/5 * * * * cd /path/to/FluxForge && python scripts/run_etl.py --source coinbase --mode all >> /var/log/fluxforge-etl.log 2>&1

# Clean old logs (keep 7 days)
0 2 * * * find /path/to/FluxForge/data/raw/ready -name "*.ndjson" -mtime +7 -delete
```

---

## 🛠️ Troubleshooting

### Segments Not Rotating

**Symptom**: Active segment growing beyond limit

**Check**: Current segment size
```bash
ls -lh data/raw/active/coinbase/
```

**Solutions**:
- Lower `segment_max_mb` in config
- Check if ingestion is receiving data (verify stats)
- Ensure `enable_fsync: true` in config

### ETL Not Finding Segments

**Symptom**: ETL reports "No segments found"

**Check**: Ready directory
```bash
ls data/raw/ready/coinbase/
```

**Solutions**:
- Verify `etl.input_dir` points to `ready/coinbase` (not just `ready/`)
- Check if segments are still in `active/` (rotation not triggered)
- Wait for rotation or stop ingestion to flush final segment

### Large Backlog in ready/

**Symptom**: Many segments accumulating in `ready/`

**Check**: Count segments
```bash
ls data/raw/ready/coinbase/ | wc -l
```

**Solutions**:
- Increase ETL poll frequency: `--poll-interval 10`
- Run manual ETL: `python scripts/run_etl.py --source coinbase --mode all`
- Check for ETL errors in logs
- Verify `delete_after_processing: true` in config

### Queue Full Warnings

**Symptom**: `[LogWriter] Queue full - backpressure active`

**Solutions**:
- Increase `queue_maxsize` (e.g., from 10000 to 50000)
- Decrease `flush_interval_seconds` (e.g., from 5.0 to 2.0)
- Increase `batch_size` (e.g., from 100 to 200)
- Check disk I/O performance

### Connection Drops

**Symptom**: `[CoinbaseCollector] WebSocket connection closed`

**Solutions**:
- Verify API credentials
- Check network connectivity
- Review `max_reconnect_attempts` setting
- Increase `reconnect_delay` if hitting rate limits

### High Memory Usage

**Solutions**:
- Reduce `batch_size` in config
- Decrease `queue_maxsize`
- Run ETL more frequently to reduce backlog

---

## 🔧 Development

### Adding a New Data Source

1. **Create collector** in `ingestion/collectors/`:

```python
from .base_collector import BaseCollector

class MySourceCollector(BaseCollector):
    async def _collect(self):
        # Connect to WebSocket
        # Capture messages
        # Push to log_writer
```

2. **Create parser** in `etl/parsers/`:

```python
class MySourceParser:
    def parse_record(self, record):
        # Parse NDJSON record
        # Return normalized dict/list
```

3. **Update config** in `config/config.py`:

```python
class MySourceConfig(BaseModel):
    api_key: str
    symbols: list[str]
```

4. **Register in runners**:
   - Add to `scripts/run_ingestion.py`
   - Add to `scripts/run_etl.py`
   - Add to `scripts/run_etl_watcher.py`

### Running Tests

```bash
# Install dev dependencies
pip install pytest pytest-asyncio pytest-cov

# Run all tests
pytest

# Run with coverage
pytest --cov=ingestion --cov=etl

# Run specific test file
pytest tests/test_log_rotation.py -v
```

### Code Style

```bash
# Install formatters
pip install black ruff

# Format code
black .

# Lint
ruff check .
```

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

Refactored from the original **cryptoTrading** repository, extracting and modernizing the data ingestion layer into a standalone, production-grade pipeline.

---

## 💡 Why FluxForge?

The name represents the core mission: forging raw market "flux" (streaming data) into structured datasets. Like a forge transforms raw metal into refined tools, FluxForge transforms raw market data streams into clean, queryable datasets ready for research and trading.

---

## 📚 Additional Documentation

- **Archived docs**: `docs/archive/` (migration guides, project history)
- **Segment rotation details**: See [Architecture](#-architecture) section above
- **Configuration reference**: See [Configuration](#%EF%B8%8F-configuration) section above

---

**Questions?** Open an issue on GitHub or contact the maintainers.

**Ready to forge some market data?** 🔨

```bash
python scripts/run_ingestion.py --config config/config.yaml
```
