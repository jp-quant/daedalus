# Daedalus Market Data Pipeline

**Production-grade cryptocurrency market data infrastructure for quantitative trading**

Daedalus is a comprehensive market data ingestion, processing, and feature engineering pipeline designed for **mid-frequency quantitative crypto trading** (5-second to 4-hour horizons). Built for reliability and efficiency, it runs seamlessly from Raspberry Pi to high-end servers, with hybrid local/cloud storage support.

**Key Differentiators:**
- 🎯 **Mid-frequency focus**: Optimized for seconds-to-hours trading, not HFT
- 🧠 **60+ microstructure features**: Order Flow Imbalance, realized volatility, regime detection
- 🔄 **Real-time feature engineering**: Streaming statistics, bar aggregates, multi-timeframe analysis
- 🌐 **Multi-exchange**: Unified CCXT Pro interface to 100+ exchanges
- 💾 **Hybrid storage**: Local + S3 with seamless syncing and compaction
- ⚙️ **Config-driven**: Zero-code feature customization via YAML

---

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Architecture](#-architecture)
- [Features](#-features)
- [Installation](#-installation)
- [Configuration](#%EF%B8%8F-configuration)
- [Usage](#-usage)
- [Storage Architecture](#-storage-architecture)
- [Directory Structure](#-directory-structure)
- [Feature Engineering](#-feature-engineering)
- [Monitoring](#-monitoring)
- [Production Setup](#-production-setup)
- [Troubleshooting](#-troubleshooting)
- [Development](#-development)

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure (copy example and edit)
cp config/config.examples.yaml config/config.yaml
# Edit config/config.yaml with your exchange credentials and processor_options

# 3. Start live ingestion (Terminal 1)
python scripts/run_ingestion.py --sources ccxt

# 4. Start continuous ETL + feature engineering (Terminal 2)
python scripts/run_etl_watcher.py --poll-interval 30

# 5. Start local → S3 sync (Terminal 3, optional)
python storage/sync.py upload processed/ccxt/ s3://my-bucket/processed/ccxt/

# 6. Run compaction for older data (Terminal 4, periodic)
python scripts/run_compaction.py --source ccxt --partition exchange=binanceus/symbol=BTC-USDT

# 7. Query processed features
python scripts/query_parquet.py data/processed/ccxt/orderbook/hf

# 8. Check system health
python scripts/check_health.py
```

**Typical Production Setup:**
- **Terminal 1**: `run_ingestion.py` (continuous WebSocket collection → Bronze Parquet)
- **Terminal 2**: `run_etl_watcher.py` (near-real-time feature engineering)
- **Terminal 3**: Periodic `storage/sync.py` (local → S3 backup)
- **Terminal 4**: Daily `run_compaction.py` (merge small files)

**Data flow**: WebSocket → Raw Parquet (Bronze) → Feature-Engineered Parquet (Silver/Gold)

**Output structure**:
```
data/
  raw/ready/ccxt/                    # Bronze Layer (raw data preserved)
    ├── ticker/segment_*.parquet
    ├── trades/segment_*.parquet
    └── orderbook/segment_*.parquet
  processed/ccxt/                    # Silver/Gold Layer (features)
    ├── ticker/exchange=binanceus/symbol=BTC-USDT/date=2025-12-09/part_*.parquet
    ├── trades/exchange=binanceus/symbol=BTC-USDT/date=2025-12-09/part_*.parquet
    └── orderbook/
        ├── hf/exchange=binanceus/symbol=BTC-USDT/date=2025-12-09/part_*.parquet
        └── bars/exchange=binanceus/symbol=BTC-USDT/date=2025-12-09/part_*.parquet
```

---

## 🏗️ Architecture

Daedalus follows a **Medallion Architecture** (Bronze → Silver → Gold) that preserves raw data for iterative feature development:

### Layer 1: WebSocket Collectors (I/O Only)

**Purpose**: Capture raw messages from market data sources

**Design**:
- Pure async I/O - zero CPU-intensive processing
- Connect to exchange WebSocket APIs via CCXT Pro
- Add capture timestamp
- Push to bounded asyncio queue
- Automatic reconnection with exponential backoff

**Supported Sources**:
- **CCXT** (Primary): Unified interface to 100+ exchanges (Binance, Coinbase, OKX, etc.)
- Coinbase Advanced Trade (Native WebSocket)

**Key files**: `ingestion/collectors/`

### Layer 2: StreamingParquetWriter (BRONZE LAYER)

**Purpose**: Durable raw data landing with compression and size-based rotation

**Design**:
- Pulls from asyncio queue
- Batches records (configurable size)
- Writes directly to **Parquet format** (5-10x smaller than NDJSON)
- Channel-separated files (ticker/, trades/, orderbook/)
- **Rotates when segment reaches size limit** (default: 50 MB)
- Atomic move from `active/` → `ready/` directory
- ZSTD compression (configurable level)
- **Preserves ALL raw data** for replay/reprocessing
- **Unified storage backend** (works with local filesystem or S3)

**Why Parquet for raw data?**
- Preserve raw data to iterate on features without re-collecting
- 5-10x smaller than NDJSON with ZSTD compression
- Columnar format - can read just needed columns
- Typed schema - no JSON parsing overhead

**Key files**: `ingestion/writers/parquet_writer.py`

**Segment naming**: `segment_<YYYYMMDDTHH>_<COUNTER>.parquet`
- Counter resets each hour (prevents overflow)
- Example: `segment_20251209T14_00042.parquet` = 42nd segment at 2PM

### Layer 3: ETL Workers (SILVER/GOLD LAYERS)

**Purpose**: Transform raw Parquet segments into feature-engineered, partitioned files

**Design**:
- Reads from `ready/` directory (never touches `active/`)
- Atomically moves segment to `processing/` (prevents double-processing)
- Uses composable pipeline architecture:
  - **Readers**: NDJSONReader, ParquetReader (with Polars)
  - **Processors**: Source-specific transforms + feature engineering
  - **Writers**: Flexible ParquetWriter with Hive-style partitioning
- Routes channels to specialized processors
- **60+ microstructure features** from orderbook data
- **Multi-output**: High-frequency snapshots + bar aggregates
- **Partitions by exchange, symbol, AND date** (Hive-style for distributed queries)
- Optionally deletes processed segment (configurable)

**Key files**: 
- `etl/orchestrators/ccxt_segment_pipeline.py` - Multi-channel routing
- `etl/processors/ccxt/advanced_orderbook_processor.py` - Feature engineering
- `etl/features/` - Feature extraction modules
- `etl/writers/parquet_writer.py` - Partitioned output

---

## ✨ Features

- ✅ **Multi-exchange support**: CCXT Pro unified interface to 100+ exchanges
- ✅ **Zero message loss**: Bounded queues with backpressure handling
- ✅ **Size-based segment rotation**: Prevents unbounded file growth (configurable MB limit)
- ✅ **Active/ready segregation**: ETL never interferes with ingestion
- ✅ **Atomic operations**: Ensures no partial files
- ✅ **Low-power friendly**: Proven on Raspberry Pi
- ✅ **Durability**: fsync guarantees, append-only logs
- ✅ **Near real-time ETL**: Process segments as they close (30-second polling)
- ✅ **Unified storage**: Local filesystem or S3 with same codebase
- ✅ **Advanced feature engineering**: 60+ microstructure features from orderbooks
- ✅ **Multi-output pipelines**: High-frequency features + bar aggregates
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
git clone https://github.com/yourusername/daedalus.git
cd daedalus

# Install dependencies
pip install -r requirements.txt

# OR install as editable package (optional)
pip install -e .
```

---

## ⚙️ Configuration

### 1. Create config file

```bash
cp config/config.examples.yaml config/config.yaml
```

### 2. Edit `config/config.yaml`

```yaml
# Storage configuration (local or S3)
storage:
  ingestion_storage:
    backend: "local"
    base_dir: "F:/"  # Or "./data" for relative path
  
  etl_storage_input:
    backend: "local"
    base_dir: "F:/"
  
  etl_storage_output:
    backend: "local"  # Or "s3" for cloud storage
    base_dir: "F:/"

  paths:
    raw_dir: "raw"
    active_subdir: "active"
    ready_subdir: "ready"
    processing_subdir: "processing"
    processed_dir: "processed"

# CCXT Multi-Exchange Configuration (Primary)
ccxt:
  exchanges:
    binanceus:
      max_orderbook_depth: 50
      api_key: ""
      api_secret: ""
      channels:
        watchOrderBook: ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        watchTicker: ["BTC/USDT", "ETH/USDT"]
        watchTrades: ["BTC/USDT"]
    
    coinbaseadvanced:
      max_orderbook_depth: 50
      api_key: ""
      api_secret: ""
      channels:
        watchOrderBook: ["BTC/USD", "ETH/USD"]

# Ingestion settings
ingestion:
  batch_size: 100
  flush_interval_seconds: 5.0
  queue_maxsize: 10000
  enable_fsync: true
  segment_max_mb: 100

# ETL settings
etl:
  compression: "zstd"
  delete_after_processing: true
  
  channels:
    ticker:
      enabled: true
      partition_cols: ["exchange", "symbol", "date"]
    
    trades:
      enabled: true
      partition_cols: ["exchange", "symbol", "date"]
    
    orderbook:
      enabled: true
      partition_cols: ["exchange", "symbol", "date"]
      processor_options:
        hf_emit_interval: 1.0
        bar_durations: [1, 5, 30, 60]
        horizons: [1, 5, 30, 60]

log_level: "INFO"
```

### 3. Important: Add to .gitignore

```bash
echo "config/config.yaml" >> .gitignore
```

---

## 🎮 Usage

### Start Ingestion

Collect real-time market data from multiple exchanges:

```bash
# Start CCXT ingestion (recommended)
python scripts/run_ingestion.py --source ccxt

# Or run all configured sources
python scripts/run_ingestion.py
```

**Output**:
```
[IngestionPipeline] Ingestion pipeline running with 2 collector(s)
[CcxtCollector] Starting binanceus watchOrderBook ['BTC/USDT', 'ETH/USDT']
[CcxtCollector] Starting coinbaseadvanced watchOrderBook ['BTC/USD', 'ETH/USD']
Press Ctrl+C to stop
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
python scripts/run_etl.py --source ccxt --mode all

# Process specific date
python scripts/run_etl.py --source ccxt --mode date --date 2025-12-09
```

### Query Processed Data

```python
# DuckDB (recommended for analytics)
import duckdb
df = duckdb.query("""
    SELECT * FROM 'data/processed/ccxt/orderbook/bars/**/*.parquet'
    WHERE exchange = 'binanceus' AND symbol = 'BTC/USDT' AND duration = 5
""").to_df()

# Polars (blazing fast with lazy evaluation)
import polars as pl
df = pl.scan_parquet("data/processed/ccxt/orderbook/bars/**/*.parquet") \
    .filter(pl.col("exchange") == "binanceus") \
    .filter(pl.col("duration") == 5) \
    .collect()

# Pandas
import pandas as pd
df = pd.read_parquet("data/processed/ccxt/ticker/exchange=binanceus/symbol=BTC-USDT/date=2025-12-09")
```

---

## 🗄️ Storage Architecture

Daedalus uses a **unified storage abstraction** that works seamlessly with both local filesystem and AWS S3.

### Storage Backends

| Backend | Use Case | Configuration |
|---------|----------|---------------|
| **Local** | Development, single-server | `backend: "local"` |
| **S3** | Production, cloud-native | `backend: "s3"` |
| **Hybrid** | Ingest local, ETL to S3 | Mixed backends |

### Configuration Examples

**All Local** (Development):
```yaml
storage:
  ingestion_storage:
    backend: "local"
    base_dir: "./data"
  etl_storage_input:
    backend: "local"
    base_dir: "./data"
  etl_storage_output:
    backend: "local"
    base_dir: "./data"
```

**Hybrid** (Ingest Local, Process to S3):
```yaml
storage:
  ingestion_storage:
    backend: "local"
    base_dir: "F:/"
  etl_storage_input:
    backend: "local"
    base_dir: "F:/"
  etl_storage_output:
    backend: "s3"
    base_dir: "my-datalake"
    s3:
      bucket: "my-datalake"
      region: "us-east-1"
```

---

## 📁 Directory Structure

### Project Structure

```
Daedalus/
├── ingestion/              # Layer 1 & 2: Data collection
│   ├── collectors/         # WebSocket collectors (I/O only)
│   │   ├── base_collector.py
│   │   ├── ccxt_collector.py    # CCXT Pro multi-exchange
│   │   └── coinbase_ws.py       # Native Coinbase WebSocket
│   ├── writers/            # Log writers with rotation
│   │   └── log_writer.py        # Unified local/S3 writer
│   ├── orchestrators/      # Pipeline coordination
│   │   └── ingestion_pipeline.py
│   └── utils/              # Utilities
│       ├── time.py
│       └── serialization.py
│
├── etl/                    # Layer 3: Transformation
│   ├── readers/            # Data loading
│   │   ├── ndjson_reader.py
│   │   └── parquet_reader.py
│   ├── processors/         # Transform & aggregate
│   │   ├── ccxt/           # CCXT-specific processors
│   │   │   ├── advanced_orderbook_processor.py  # HF features + bars
│   │   │   ├── ticker_processor.py
│   │   │   └── trades_processor.py
│   │   ├── coinbase/       # Coinbase-specific (legacy)
│   │   └── raw_processor.py
│   ├── parsers/            # Parse NDJSON segments
│   │   ├── ccxt_parser.py
│   │   └── coinbase_parser.py
│   ├── writers/            # Parquet writers
│   │   └── parquet_writer.py    # Unified local/S3 writer
│   ├── orchestrators/      # Pipeline composition
│   │   ├── pipeline.py
│   │   ├── multi_output_pipeline.py
│   │   └── ccxt_segment_pipeline.py
│   ├── features/           # Feature engineering
│   │   ├── snapshot.py     # Structural features
│   │   ├── streaming.py    # Rolling statistics
│   │   └── state.py        # Symbol state management
│   ├── repartitioner.py    # Compaction & repartitioning
│   ├── parquet_crud.py     # CRUD operations
│   └── job.py              # ETL orchestration
│
├── storage/                # Storage abstraction
│   ├── base.py             # StorageBackend, LocalStorage, S3Storage
│   └── factory.py          # Backend factory & path utilities
│
├── config/                 # Configuration
│   ├── config.py           # Pydantic models
│   └── config.examples.yaml
│
├── scripts/                # Entry points
│   ├── run_ingestion.py    # Start ingestion
│   ├── run_etl.py          # Run ETL (manual)
│   ├── run_etl_watcher.py  # Run ETL (continuous)
│   ├── run_compaction.py   # Compact Parquet files
│   ├── check_health.py     # Health check
│   └── query_parquet.py    # Query examples
│
└── tests/                  # Test suite
```

### Data Directory Structure

```
data/
├── raw/                    # Raw NDJSON segments
│   ├── active/ccxt/        # Currently being written
│   │   └── segment_20251209T14_00001.ndjson
│   ├── ready/ccxt/         # Closed segments (ready for ETL)
│   │   ├── segment_20251209T14_00001.ndjson
│   │   └── segment_20251209T14_00002.ndjson
│   └── processing/ccxt/    # Temp during ETL
│
└── processed/ccxt/         # Parquet files (Hive-style partitioning)
    ├── ticker/
    │   └── exchange=binanceus/
    │       └── symbol=BTC-USDT/
    │           └── date=2025-12-09/
    │               └── part_*.parquet
    ├── trades/
    │   └── exchange=binanceus/
    │       └── symbol=BTC-USDT/
    │           └── date=2025-12-09/
    │               └── part_*.parquet
    └── orderbook/
        ├── hf/             # High-frequency features (10Hz)
        │   └── exchange=binanceus/
        │       └── symbol=BTC-USDT/
        │           └── date=2025-12-09/
        │               └── part_*.parquet
        └── bars/           # Time bars (1s, 5s, 30s, 60s)
            └── exchange=binanceus/
                └── symbol=BTC-USDT/
                    └── date=2025-12-09/
                        └── part_*.parquet
```

---

## 🧮 Feature Engineering

Daedalus includes sophisticated orderbook feature engineering for quantitative research.

### Structural Features (Per Snapshot)

| Category | Features |
|----------|----------|
| **Price/Spread** | mid_price, spread, relative_spread, microprice |
| **Depth** | bid_size_L0-L9, ask_size_L0-L9, imbalance_L1 |
| **Volume Bands** | depth_0_5bps, depth_5_10bps, depth_10_25bps |
| **Shape** | bid_50pct_depth, ask_50pct_depth, concentration |
| **Impact** | vwap_bid_5, vwap_ask_5, smart_depth, kyle_lambda |

### Streaming Features (Rolling Windows)

| Horizon | Features |
|---------|----------|
| **1s, 5s, 30s, 60s** | log_return, realized_volatility, ofi_sum |
| **Trade Flow** | buy_volume, sell_volume, trade_flow_imbalance |
| **Regime** | spread_regime (tight/wide tracking) |

### Bar Aggregates

For each duration (1s, 5s, 30s, 60s):
- OHLC (Open, High, Low, Close of mid-price)
- mean_spread, mean_relative_spread
- mean_l1_imbalance, sum_ofi
- realized_variance

---

## 📊 Monitoring

### Health Check

```bash
python scripts/check_health.py
```

### Check Segment Status

```bash
# Active segments (currently being written)
ls -lh data/raw/active/ccxt/

# Ready segments (waiting for ETL)
ls -lh data/raw/ready/ccxt/

# Count backlog
ls data/raw/ready/ccxt/ | wc -l
```

### Compaction

Consolidate small Parquet files for better query performance:

```bash
python scripts/run_compaction.py data/processed/ccxt/orderbook/bars \
    --target-file-count 1 \
    --min-file-count 2
```

---

## 🚀 Production Setup

### Option 1: Separate Terminal Windows

```bash
# Terminal 1: Ingestion (always running)
python scripts/run_ingestion.py --source ccxt

# Terminal 2: Continuous ETL (always running)
python scripts/run_etl_watcher.py --poll-interval 30

# Terminal 3: Monitor health (periodic)
watch -n 300 python scripts/check_health.py
```

### Option 2: systemd Services (Linux)

**Ingestion service**: `/etc/systemd/system/daedalus-ingestion.service`

```ini
[Unit]
Description=Daedalus Ingestion Pipeline
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/Daedalus
ExecStart=/usr/bin/python3 scripts/run_ingestion.py --source ccxt
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**ETL service**: `/etc/systemd/system/daedalus-etl-watcher.service`

```ini
[Unit]
Description=Daedalus ETL Watcher
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/Daedalus
ExecStart=/usr/bin/python3 scripts/run_etl_watcher.py --poll-interval 30
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

---

## 🛠️ Troubleshooting

### Segments Not Rotating

**Symptom**: Active segment growing beyond limit

**Solutions**:
- Lower `segment_max_mb` in config
- Check if ingestion is receiving data
- Ensure `enable_fsync: true` in config

### ETL Not Finding Segments

**Symptom**: ETL reports "No segments found"

**Solutions**:
- Verify paths in config point to correct `ready/` directory
- Check if segments are still in `active/` (rotation not triggered)
- Wait for rotation or stop ingestion to flush final segment

### Queue Full Warnings

**Symptom**: `[LogWriter] Queue full - backpressure active`

**Solutions**:
- Increase `queue_maxsize` (e.g., 50000)
- Decrease `flush_interval_seconds` (e.g., 2.0)
- Check disk I/O performance

### Connection Drops

**Symptom**: `[CcxtCollector] Error in watchOrderBook`

**Solutions**:
- Verify API credentials
- Check network connectivity
- Increase `reconnect_delay` if hitting rate limits

---

## � Documentation

### For New Developers / Agent Sessions

**Start Here**: [`docs/NEW_AGENT_PROMPT.md`](docs/NEW_AGENT_PROMPT.md)
- Quick onboarding for new Copilot agent sessions
- Copy-paste into fresh session to get up to speed
- 10-minute read, points to all critical files

**Complete Reference**: [`docs/SYSTEM_ONBOARDING.md`](docs/SYSTEM_ONBOARDING.md)
- Comprehensive system documentation (30-page deep dive)
- Architecture, feature engineering, configuration flow
- Recent enhancements, debugging guide, success criteria

### Feature Engineering

**Configuration Guide**: [`docs/PROCESSOR_OPTIONS.md`](docs/PROCESSOR_OPTIONS.md)
- How to customize StateConfig parameters
- Complete parameter reference with examples
- Performance recommendations for different use cases

**Research Prompt**: [`docs/RESEARCH_PROMPT_ORDERBOOK_QUANT.md`](docs/RESEARCH_PROMPT_ORDERBOOK_QUANT.md)
- Deep research prompt for optimal configurations
- Questions about horizons, bar durations, new features
- Awaiting research results for implementation

### Storage & Operations

**Storage Architecture**: [`docs/UNIFIED_STORAGE_ARCHITECTURE.md`](docs/UNIFIED_STORAGE_ARCHITECTURE.md)
- Storage backend abstraction design
- LocalStorage vs S3Storage

**Hybrid Patterns**: [`docs/HYBRID_STORAGE_GUIDE.md`](docs/HYBRID_STORAGE_GUIDE.md)
- Local ingestion + S3 output patterns
- Sync strategies, compaction workflows

**CCXT ETL**: [`docs/CCXT_ETL_ARCHITECTURE.md`](docs/CCXT_ETL_ARCHITECTURE.md)
- CCXT-specific pipeline design
- Multi-channel routing

**Parquet Operations**: [`docs/PARQUET_OPERATIONS.md`](docs/PARQUET_OPERATIONS.md)
- Best practices for Parquet files
- Compression, partitioning, schema evolution

**Filename Patterns**: [`docs/FILENAME_PATTERNS.md`](docs/FILENAME_PATTERNS.md)
- Segment naming conventions
- Timestamp format reference

### Trading Strategy

**US Crypto Strategy**: [`docs/US_CRYPTO_STRATEGY.md`](docs/US_CRYPTO_STRATEGY.md)
- Regulatory considerations for US-based trading
- Exchange selection, liquidity analysis

---

## 🔧 Development

### Running Tests

```bash
# Install dev dependencies
pip install pytest pytest-asyncio pytest-cov

# Run all tests
pytest

# Run with coverage
pytest --cov=ingestion --cov=etl

# Run specific test file
pytest tests/test_features.py -v

# Test configuration flow
python scripts/test_config_flow.py
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

### Adding New Features

1. **Static features** (from single snapshot):
   - Edit `etl/features/snapshot.py` → `extract_orderbook_features()`
   - Add feature calculation
   - Return in feature dict

2. **Dynamic features** (requires state):
   - Edit `etl/features/state.py` → `SymbolState.process_snapshot()`
   - Access previous state via `self.prev_*` variables
   - Compute delta-based features

3. **Rolling statistics**:
   - Add tracker in `SymbolState.__init__()`:
     ```python
     self.my_new_stat = RollingSum(horizon) for horizon in config.horizons
     ```
   - Update in `process_snapshot()`:
     ```python
     self.my_new_stat.update(value, timestamp)
     features[f'my_stat_{h}s'] = self.my_new_stat.sum
     ```

4. **Configuration parameters**:
   - Add to `StateConfig` dataclass in `etl/features/state.py`
   - Document in `docs/PROCESSOR_OPTIONS.md`
   - Update `config/config.yaml` example

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 💡 Why Daedalus?

The name represents the core mission: like Daedalus, the master craftsman of Greek mythology who built the labyrinth, this system architects a sophisticated data labyrinth—transforming chaotic market streams into structured, navigable datasets. Just as Daedalus crafted wings from feathers and wax, we forge raw market data into refined tools for quantitative research and trading.

---

**Ready to navigate the data labyrinth?** 🏛️

```bash
python scripts/run_ingestion.py --source ccxt
```
