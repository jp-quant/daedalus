# Daedalus Market Data Pipeline

**Production-grade cryptocurrency market data infrastructure for quantitative trading**

> *Last Updated: February 2026*

Daedalus is a comprehensive market data ingestion, processing, and feature engineering pipeline designed for **mid-frequency quantitative crypto trading** (5-second to 4-hour horizons). Built for reliability and efficiency, it runs seamlessly from Raspberry Pi 4 to high-end workstations (ROG Flow Z13), with hybrid local/cloud storage support.

**Key Differentiators:**
- 🎯 **Mid-frequency focus**: Optimized for seconds-to-hours trading, not HFT
- 🧠 **60+ microstructure features**: OFI, MLOFI, realized volatility, Kyle's Lambda, VPIN
- 🔄 **Research-optimized defaults**: Parameters tuned per academic literature (Cont, Kyle, López de Prado)
- 🌐 **Multi-exchange**: Unified CCXT Pro interface to 100+ exchanges
- 💾 **Hybrid storage**: Local + S3 with seamless syncing and compaction
- ⚙️ **Config-driven**: Zero-code feature customization via YAML
- 🔬 **Research-ready**: ASOF resampling, multi-asset alignment, Polars lazy evaluation
- 🍓 **Pi4-optimized**: 24/7 operation on Raspberry Pi with memory management

---

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Architecture](#-architecture)
- [ETL Framework](#-etl-framework)
- [Research & Analysis](#-research--analysis)
- [Features](#-features)
- [Installation](#-installation)
- [Configuration](#%EF%B8%8F-configuration)
- [Usage](#-usage)
- [Storage Architecture](#-storage-architecture)
- [Directory Structure](#-directory-structure)
- [Feature Engineering](#-feature-engineering)
- [Monitoring](#-monitoring)
- [Pi4 Production Deployment](#-pi4-production-deployment)
- [Troubleshooting & Diagnostics](#-troubleshooting--diagnostics)
- [Development](#-development)

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure (copy example and edit)
cp config/config.examples.yaml config/config.yaml
# Edit config/config.yaml with your exchange credentials and processor_options

# 3. Start live ingestion via Daedalus Supervisor (recommended)
python scripts/daedalus_supervisor.py --sources ccxt

# OR start ingestion directly (for development)
python scripts/run_ingestion.py --sources ccxt

# 4. Start continuous ETL + feature engineering (separate terminal)
python scripts/etl/run_watcher.py --poll-interval 30

# 5. (Alternative) Run batch feature engineering
python scripts/etl/run_orderbook_features.py --input data/raw/ready/ccxt/orderbook --output data/processed

# 6. Query processed features
python scripts/query_parquet.py data/processed/ccxt/orderbook/hf
```

**Typical Production Setup (Pi4):**
- **systemd service**: `daedalus.service` runs supervisor (ingestion + sync)
- **ETL watcher**: `run_watcher.py` for continuous feature engineering
- **Periodic compaction**: `run_compaction.py` to merge small files

**Data flow**: WebSocket → Raw Parquet (Bronze) → Feature-Engineered Parquet (Silver/Gold)

**Output structure**:
```
data/
  raw/                               # Bronze Layer (raw data preserved)
    ├── active/ccxt/orderbook/...    # Currently writing
    └── ready/ccxt/orderbook/...     # Closed, ready for ETL
  processed/                         # Silver/Gold Layer (features)
    └── silver/orderbook/
        └── exchange=coinbaseadvanced/
            └── symbol=BTC-USD/
                └── year=2026/month=2/day=3/
                    └── part_*.parquet
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
- Pulls from asyncio queue via **dedicated I/O thread pool**
- Batches records (configurable size) with efficient non-blocking queue drain
- Writes directly to **Parquet format** (5-10x smaller than NDJSON)
- **Directory-aligned partitioning**: `exchange/symbol/year/month/day/hour`
- **Rotates when segment reaches size limit** (default: 100 MB) or hour changes
- Atomic move from `active/` → `ready/` directory
- ZSTD compression (configurable level)
- **LRU-based writer eviction** to prevent file descriptor exhaustion
- **Automatic memory cleanup** with periodic GC

**Performance tuning (configurable)**:
```python
MAX_OPEN_WRITERS = 350            # Concurrent Parquet file handles
IO_THREAD_POOL_SIZE = min(32, CPU_COUNT * 4)  # I/O threads
MEMORY_CLEANUP_INTERVAL = 300     # GC every 5 minutes
```

**Key files**: `ingestion/writers/parquet_writer.py`

**Segment naming**: `segment_<YYYYMMDDTHH>_<COUNTER>.parquet`
- Counter resets each hour (prevents overflow)
- Example: `segment_20260203T14_00042.parquet` = 42nd segment at 2PM

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

## 🔧 ETL Framework

Daedalus provides a **declarative, config-driven ETL framework** built on Polars for efficient market data transformation.

### Core Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     TransformExecutor (Orchestrator)                    │
│  etl/core/executor.py                                                   │
│  - Resolves inputs with FilterSpec partition pruning                    │
│  - Executes transform logic                                             │
│  - Writes Hive-partitioned Parquet outputs                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    transform.transform(inputs, context)
                                    │
┌───────────────────────────────────▼─────────────────────────────────────┐
│                     Transform Implementations                           │
│  etl/transforms/                                                        │
│  ├── trades.py      → TradesFeatureTransform (direction, returns)       │
│  ├── ticker.py      → TickerFeatureTransform (mid, spread)              │
│  ├── orderbook.py   → OrderbookFeatureTransform (60+ features)          │
│  └── bars.py        → BarAggregationTransform (OHLCV aggregates)        │
└─────────────────────────────────────────────────────────────────────────┘
```

### ETL Scripts

| Script | Input Tier | Output Tier | Description |
|--------|------------|-------------|-------------|
| `run_trades_features.py` | Bronze | Silver | Trade direction, dollar volume, log returns |
| `run_ticker_features.py` | Bronze | Silver | Mid price, spread, derived metrics |
| `run_orderbook_features.py` | Bronze | Silver | 60+ microstructure features |
| `run_bars.py` | Silver | Gold | Time bar aggregations (1m, 5m, 15m, 1h) |
| `run_watcher.py` | Bronze | Silver | Continuous processing of `data/raw/ready/*` |

### Quick Usage

```bash
# Feature engineering (Bronze → Silver)
python -m scripts.etl.run_trades_features --limit 10000
python -m scripts.etl.run_orderbook_features --exchange binanceus --symbol BTC/USDT
python -m scripts.etl.run_ticker_features --dry-run

# Bar aggregation (Silver → Gold)
python -m scripts.etl.run_bars --channel trades --output data/processed

# Continuous processing
python -m scripts.etl.run_watcher --poll-interval 30
```

### Key Components

| Component | Location | Purpose |
|-----------|----------|---------|
| `TransformExecutor` | `etl/core/executor.py` | Central orchestrator for I/O and execution |
| `BaseTransform` | `etl/core/base.py` | Abstract base class for transforms |
| `FilterSpec` | `etl/core/config.py` | Declarative partition filtering (exact match or date range) |
| `FeatureConfig` | `etl/core/config.py` | Feature computation parameters |
| `TransformRegistry` | `etl/core/registry.py` | Transform discovery via `@register_transform` |

### Configuration Integration

ETL parameters are configurable via `config/config.yaml`:

```yaml
features:
  categories: ["structural", "dynamic", "rolling"]
  depth_levels: 20
  rolling_windows: [5, 15, 60, 300, 900]
  bar_durations: [60, 300, 900, 3600]
  ofi_decay_alpha: 0.5
```

Load and use:

```python
from config.config import load_config
from etl.core.executor import TransformExecutor

config = load_config()
executor = TransformExecutor.from_config(config)
```

### State Management

- **Checkpointing**: `etl/utils/state_manager.py` provides automatic checkpoint on SIGINT/SIGTERM
- **Periodic saves**: Background checkpointing at configurable intervals
- **State files**: Located in `storage.paths.state_dir` (default: `data/temp/state/`)

**Full documentation**: See [`docs/ETL_CORE_FRAMEWORK.md`](docs/ETL_CORE_FRAMEWORK.md)

### Research-Optimized Parameters

| Parameter | Default | Research Basis |
|-----------|---------|----------------|
| `depth_levels` | 20 | Zhang et al. (2019) - DeepLOB captures "walls" |
| `rolling_windows` | [5, 15, 60, 300, 900] | Multi-scale for mid-frequency |
| `bar_durations` | [60, 300, 900, 3600] | Skip sub-minute (redundant at 1Hz) |
| `ofi_decay_alpha` | 0.5 | Xu et al. (2019) - MLOFI exponential decay |
| `vpin_bucket_size` | 1.0 | Easley et al. (2012) - Volume-sync buckets |

**References**:
- Cont et al. (2014): OFI price impact
- Kyle & Obizhaeva (2016): Microstructure invariance  
- López de Prado (2018): Volume clocks
- Zhang et al. (2019): DeepLOB deep book features
- Xu et al. (2019): Multi-level OFI
- Easley et al. (2012): VPIN for flow toxicity

---

## 🔬 Research & Analysis

### ASOF Resampling (Point-in-Time Safe)

The `etl/utils/resampling.py` module provides ML/backtesting-safe resampling:

```python
from etl.utils.resampling import resample_orderbook, resample_timeseries

# Resample orderbook to 1-second bars (ASOF semantics)
resampled = resample_orderbook(orderbook_lf, frequency="1s")

# Generic timeseries resampling
resampled = resample_timeseries(df, frequency="5m", value_cols=["mid_price", "spread"])
```

**Key properties:**
- **No lookahead bias**: Each timestamp T contains only data available BEFORE T
- **Staleness detection**: Optional `max_staleness` flag for data gaps
- **Lazy evaluation**: Works with Polars LazyFrames for massive datasets

### Multi-Asset Alignment (Cross-Asset Modeling)

Build horizontally concatenated DataFrames for multi-asset analysis:

```python
import polars as pl
from etl.utils.resampling import resample_timeseries

# Scan all symbols lazily (hive partitioning)
ROOT_PL_DF = pl.scan_parquet("data/processed/silver/orderbook/**/*.parquet", hive_partitioning=True)
ALL_SYMBOLS = ROOT_PL_DF.select("symbol").unique().collect().to_series().to_list()

def build_multi_asset_lf(root_lf, symbols, frequency="1s"):
    """Build wide-format DataFrame: timestamp + {symbol}_{feature} columns."""
    
    def process_symbol(symbol):
        symbol_lf = root_lf.filter(pl.col("symbol") == symbol)
        resampled = resample_timeseries(symbol_lf, frequency=frequency, group_cols=[])
        # Rename columns with symbol prefix
        prefix = symbol.replace("/", "_").replace("-", "_")
        return resampled.rename({col: f"{prefix}_{col}" for col in feature_cols})
    
    # Fold-join all symbols on timestamp (inner join = complete rows only)
    result = process_symbol(symbols[0])
    for symbol in symbols[1:]:
        result = result.join(process_symbol(symbol), on="timestamp", how="inner")
    
    return result.sort("timestamp")

# Build and collect (single materialization point)
multi_asset_lf = build_multi_asset_lf(ROOT_PL_DF, ALL_SYMBOLS, "1s")
multi_asset_df = multi_asset_lf.collect()  # Only collect at the end
```

**For memory-constrained scenarios:**
- Use `lf.sink_parquet()` for streaming to disk
- Process in time-based chunks (1 day at a time)

See `research/01_orderbook_feature_analysis.ipynb` for complete examples.

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
- ✅ **Production-ready**: Logging, stats, graceful shutdown with state persistence, health checks
- ✅ **Research-optimized**: Parameters tuned per academic literature

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
python scripts/etl/run_watcher.py --poll-interval 30
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
python scripts/etl/run_watcher.py --poll-interval 30
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
│   │   ├── ccxt_collector.py    # CCXT Pro multi-exchange (optimized)
│   │   └── coinbase_ws.py       # Native Coinbase WebSocket
│   ├── writers/            # Streaming writers with rotation
│   │   └── parquet_writer.py    # StreamingParquetWriter (ThreadPoolExecutor)
│   ├── orchestrators/      # Pipeline coordination
│   │   └── ingestion_pipeline.py
│   └── utils/              # Utilities
│       ├── time.py
│       └── serialization.py
│
├── etl/                    # Layer 3: Transformation
│   ├── core/               # Core framework (NEW)
│   │   ├── base.py         # BaseTransform abstract class
│   │   ├── config.py       # FilterSpec, FeatureConfig
│   │   ├── executor.py     # TransformExecutor orchestrator
│   │   └── registry.py     # @register_transform decorator
│   ├── readers/            # Data loading
│   │   ├── ndjson_reader.py
│   │   └── parquet_reader.py
│   ├── processors/         # Transform & aggregate
│   │   ├── ccxt/           # CCXT-specific processors
│   │   │   ├── advanced_orderbook_processor.py  # HF features + bars
│   │   │   ├── ticker_processor.py
│   │   │   └── trades_processor.py
│   │   └── raw_processor.py
│   ├── writers/            # Parquet writers
│   │   └── parquet_writer.py    # Unified local/S3 writer
│   ├── orchestrators/      # Pipeline composition
│   │   ├── pipeline.py
│   │   ├── multi_output_pipeline.py
│   │   └── ccxt_segment_pipeline.py
│   ├── features/           # Feature engineering
│   │   ├── orderbook.py    # Vectorized orderbook features
│   │   ├── stateful.py     # Stateful features: OFI/MLOFI/TFI/Kyle/VPIN
│   │   └── streaming.py    # Rolling statistics primitives
│   ├── transforms/         # Channel transforms
│   │   ├── ticker.py       # Ticker feature transform
│   │   ├── trades.py       # Trades feature transform
│   │   ├── orderbook.py    # Orderbook feature transform
│   │   └── bars.py         # Bar aggregation
│   ├── utils/              # ETL utilities
│   │   ├── state_manager.py # Checkpoint/resume + graceful shutdown
│   │   ├── compaction.py   # File compaction
│   │   ├── resampling.py   # ASOF resampling (NEW)
│   │   └── crud.py         # CRUD operations
│   └── job.py              # ETL orchestration
│
├── storage/                # Storage abstraction
│   ├── base.py             # StorageBackend, LocalStorage, S3Storage
│   ├── factory.py          # Backend factory & path utilities
│   └── sync.py             # S3 sync utilities
│
├── shared/                 # Shared utilities
│   └── partitioning.py     # Partition alignment helpers
│
├── config/                 # Configuration
│   ├── config.py           # Pydantic models
│   └── config.examples.yaml
│
├── scripts/                # Entry points
│   ├── daedalus_supervisor.py  # Process supervisor (Pi4)
│   ├── daedalus.service    # systemd service file
│   ├── run_ingestion.py    # Start ingestion
│   ├── run_sync.py         # S3 sync
│   ├── run_compaction.py   # Compact Parquet files
│   └── etl/                # ETL scripts
│       ├── run_orderbook_features.py  # Batch orderbook features
│       ├── run_trades_features.py     # Batch trades features
│       ├── run_ticker_features.py     # Batch ticker features
│       └── run_watcher.py             # Continuous ETL watcher
│
├── research/               # Research notebooks
│   └── 01_orderbook_feature_analysis.ipynb  # Multi-asset analysis
│
└── tests/                  # Test suite
```

### Data Directory Structure

```
data/
├── raw/                    # Bronze Layer (Parquet segments)
│   ├── active/ccxt/        # Currently being written
│   │   └── orderbook/
│   │       └── exchange=coinbaseadvanced/
│   │           └── symbol=BTC-USD/
│   │               └── year=2026/month=2/day=3/hour=14/
│   │                   └── segment_20260203T14_00001.parquet
│   ├── ready/ccxt/         # Closed segments (ready for ETL)
│   └── processing/ccxt/    # Temp during ETL
│
└── processed/              # Silver/Gold Layer (Feature-engineered)
    └── silver/
        └── orderbook/
            └── exchange=coinbaseadvanced/
                └── symbol=BTC-USD/
                    └── year=2026/month=2/day=3/
            └── exchange=binanceus/
                └── symbol=BTC-USDT/
                    └── date=2025-12-09/
                        └── part_*.parquet
```

---

## 🧮 Feature Engineering

Daedalus includes sophisticated orderbook feature engineering for quantitative research, based on academic literature.

### Structural Features (Per Snapshot - Vectorized)

| Category | Features | Reference |
|----------|----------|-----------|
| **Price/Spread** | mid_price, spread, relative_spread, microprice | Core metrics |
| **Depth (20 levels)** | bid_size_L0-L19, ask_size_L0-L19, bid_price_L0-L19, ask_price_L0-L19 | Zhang et al. (2019) |
| **Imbalance** | imbalance_L1, imbalance_L3, imbalance_L5, imbalance_L10, total_imbalance | Cont et al. (2014) |
| **Volume Bands** | bid/ask_vol_band_0_5bps, 5_10bps, 10_25bps, 25_50bps, 50_100bps | Crypto volatility |
| **VWAP** | vwap_bid_5, vwap_ask_5, vwap_spread | Volume-weighted pricing |
| **Smart Depth** | smart_bid_depth, smart_ask_depth, smart_depth_imbalance | Exponential decay weighted |
| **Book Shape** | bid_slope, ask_slope, center_of_gravity, cog_vs_mid | Ghysels & Nguyen |
| **Liquidity** | lambda_like, amihud_like, bid/ask_concentration | Kyle (1985) |

### Stateful Features (Path-Dependent - Sequential)

| Feature | Description | Reference |
|---------|-------------|-----------|
| **OFI (Order Flow Imbalance)** | Net aggressive order flow at L1 | Cont et al. (2014) |
| **MLOFI (Multi-Level OFI)** | Decay-weighted OFI across 10 levels | Xu et al. (2019) |
| **TFI (Trade Flow Imbalance)** | Signed trade volume between snapshots | Trade classification |
| **Kyle's Lambda** | Rolling price impact coefficient | Kyle & Obizhaeva (2016) |
| **VPIN** | Volume-synchronized informed trading probability | Easley et al. (2012) |
| **Spread Regime** | Dynamic tight/normal/wide classification | Percentile-based |
| **Mid Velocity/Acceleration** | Price momentum derivatives | Trend detection |

### Rolling Statistics (Multi-Horizon)

For each horizon (5s, 15s, 60s, 300s, 900s):
- `rv_{h}s`: Realized volatility (rolling std of log returns)
- `mean_return_{h}s`: Rolling mean log return
- `mean_spread_{h}s`: Rolling mean relative spread
- `ofi_sum_{h}s`: Cumulative OFI

### Bar Aggregates (Gold Layer)

For each duration (60s, 300s, 900s, 3600s):
- **OHLC**: Open, High, Low, Close of mid-price
- **Spread**: mean_spread, mean_relative_spread
- **Imbalance**: mean_l1_imbalance, mean_total_imbalance
- **Flow**: sum_ofi, sum_mlofi
- **Volatility**: realized_variance, daily_vol_ann
- **Metadata**: snapshot_count, bar_duration_sec

---

## 📊 Monitoring

### Health Check

```bash
python scripts/check_health.py
```

## 📊 Monitoring

### Health Check

```bash
# Quick pipeline status
python scripts/daedalus_supervisor.py --status

# Detailed health check
python scripts/check_health.py
```

### Check Segment Status

```bash
# Active segments (currently being written)
ls -lh data/raw/active/ccxt/

# Ready segments (waiting for ETL)
ls -lh data/raw/ready/ccxt/

# Count backlog
find data/raw/ready -name "*.parquet" | wc -l
```

### Writer Statistics

The ingestion engine exposes health metrics via logs:

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
| Open writers | < 350 | Reduce symbol count or increase MAX_OPEN_WRITERS |
| Memory (MB) | < 3000 | Check for leaks, reduce partition count |
| Reconnections | Low | Network issues if frequent |

### Compaction

Consolidate small Parquet files for better query performance:

```bash
python scripts/run_compaction.py data/processed/silver/orderbook \
    --target-file-count 1 \
    --min-file-count 2
```

---

## 🚀 Pi4 Production Deployment

### Quick Deploy (Fresh Install)

```bash
# 1. Update code
cd ~/market-data-pipeline
git pull  # or scp/rsync updated files

# 2. Install dependencies
source venv/bin/activate
pip install psutil  # Required for supervisor memory monitoring

# 3. Install/update systemd service
sudo cp scripts/daedalus.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable daedalus
sudo systemctl restart daedalus

# 4. Verify it's running
sudo systemctl status daedalus
journalctl -u daedalus -f  # Follow logs
```

### Monitoring Logs

```bash
# Watch both ingestion and sync logs simultaneously
tail -f ~/market-data-pipeline/logs/ingestion.log ~/market-data-pipeline/logs/sync.log

# Or in separate terminals:
# Terminal 1: Ingestion
tail -f ~/market-data-pipeline/logs/ingestion.log

# Terminal 2: Sync
tail -f ~/market-data-pipeline/logs/sync.log

# Check supervisor status
python scripts/daedalus_supervisor.py --status
```

### Service Management

```bash
# Start/stop/restart
sudo systemctl start daedalus
sudo systemctl stop daedalus
sudo systemctl restart daedalus

# View service status
sudo systemctl status daedalus

# View recent logs
journalctl -u daedalus -n 100

# Follow logs in real-time
journalctl -u daedalus -f

# View logs since last boot
journalctl -u daedalus -b
```

### Supervisor Configuration

The `daedalus_supervisor.py` manages both ingestion and sync processes:

```python
# Key settings in SupervisorConfig (scripts/daedalus_supervisor.py)
memory_warning_mb: int = 3000     # Log warning at 3GB
memory_critical_mb: int = 3500    # Force restart at 3.5GB
ingestion_shutdown_timeout: int = 120  # 2 min for graceful flush
gc_interval: int = 300            # Force GC every 5 minutes

# Optional CPU tuning (Linux only)
process_nice: int = 0             # -20 (high priority) to 19 (low)
cpu_affinity: List[int] = []      # Pin to specific cores
```

### Pi4 Optimization Guide

#### 1. Remove Bloatware (Maximize Resources for Backend)

```bash
# Remove GUI and desktop environment (saves ~500MB RAM)
sudo apt purge --auto-remove xserver* lightdm* raspberrypi-ui-mods \
    pi-greeter rpd-plym-splash chromium-browser vlc thonny geany \
    realvnc-vnc-server scratch* sonic-pi minecraft-pi libreoffice* -y

# Remove unused services
sudo systemctl disable cups cups-browsed avahi-daemon bluetooth \
    triggerhappy ModemManager wpa_supplicant  # if using ethernet only

# Remove orphaned packages
sudo apt autoremove -y
sudo apt clean

# Disable swap file GUI
sudo systemctl disable dphys-swapfile
```

#### 2. Configure Swap (Essential for Long-Running)

```bash
# Check current swap
free -h

# Configure 2GB swap (recommended)
sudo dphys-swapfile swapoff
sudo nano /etc/dphys-swapfile
# Set: CONF_SWAPSIZE=2048
sudo dphys-swapfile setup
sudo dphys-swapfile swapon

# Verify
free -h
```

#### 3. Optimize Memory Settings

```bash
# Reduce GPU memory (headless server doesn't need it)
sudo nano /boot/config.txt
# Add: gpu_mem=16

# Reduce kernel memory pressure
sudo nano /etc/sysctl.conf
# Add:
#   vm.swappiness=10
#   vm.vfs_cache_pressure=50

sudo sysctl -p
```

#### 4. Disable Unnecessary Services

```bash
# List enabled services
systemctl list-unit-files --state=enabled

# Disable unused ones
sudo systemctl disable bluetooth.service
sudo systemctl disable hciuart.service
sudo systemctl disable cups.service
sudo systemctl disable avahi-daemon.service
sudo systemctl disable ModemManager.service
sudo systemctl disable triggerhappy.service

# If not using WiFi
sudo systemctl disable wpa_supplicant.service
```

#### 5. Network Optimization

```bash
# Use ethernet over WiFi for stability
# If using WiFi, disable power management
sudo iwconfig wlan0 power off

# Persist across reboots
echo 'wireless-power off' | sudo tee -a /etc/network/interfaces
```

---

## 🛠️ Troubleshooting & Diagnostics

### Diagnosing Unexpected Shutdowns

When Pi4 experiences unexpected shutdown or process death:

```bash
# 1. Check system logs for OOM killer
sudo dmesg | grep -i "killed process"
sudo journalctl -k | grep -i "oom"

# 2. Check service status and exit codes
sudo systemctl status daedalus
journalctl -u daedalus --since "1 hour ago"

# 3. Check power issues (undervoltage)
sudo dmesg | grep -i "voltage"
vcgencmd get_throttled  # 0x0 = no issues

# 4. Check temperature (thermal throttling)
vcgencmd measure_temp

# 5. Check disk space
df -h

# 6. Check memory usage history (if sysstat installed)
sar -r 1 10  # Memory stats every 1 sec for 10 samples

# 7. Check last boot reason
last reboot
journalctl --list-boots
```

### Common Issues and Fixes

#### Process Killed by OOM

**Symptom**: Process dies after days/weeks, `dmesg` shows "Killed process"

**Fix**:
```bash
# Increase swap
sudo dphys-swapfile swapoff
sudo sed -i 's/CONF_SWAPSIZE=.*/CONF_SWAPSIZE=2048/' /etc/dphys-swapfile
sudo dphys-swapfile setup
sudo dphys-swapfile swapon

# Lower memory thresholds in supervisor
# Edit scripts/daedalus_supervisor.py:
# memory_warning_mb = 2500
# memory_critical_mb = 3000
```

#### Undervoltage Detected

**Symptom**: `vcgencmd get_throttled` shows non-zero, random crashes

**Fix**:
- Use official Raspberry Pi power supply (5V 3A)
- Use short, thick USB-C cable
- Avoid USB hubs drawing power

#### SD Card Corruption

**Symptom**: Read-only filesystem errors, service won't start

**Fix**:
```bash
# Check filesystem
sudo fsck -y /dev/mmcblk0p2

# Consider using USB SSD instead of SD card
# Much more reliable for 24/7 write operations
```

#### Network Reconnection Issues

**Symptom**: WebSocket connections don't recover after network blip

**Fix**:
```bash
# Check if supervisor is restarting collectors
journalctl -u daedalus | grep -i "reconnect"

# Ensure auto_reconnect is enabled in config.yaml:
# ingestion:
#   auto_reconnect: true
#   max_reconnect_attempts: -1  # -1 = infinite
#   reconnect_delay: 5.0
```

### Health Check Commands

```bash
# Quick health overview
python scripts/daedalus_supervisor.py --status

# Memory usage by process
ps aux --sort=-%mem | head -20

# Check open files (should be < 1024)
ls /proc/$(pgrep -f run_ingestion)/fd | wc -l

# Check network connections
ss -tuln | grep ESTABLISHED

# Disk I/O
iostat -x 1 5

# CPU usage
top -bn1 | head -20
```

### Segment Not Rotating

**Symptom**: Active segment growing beyond limit

**Solutions**:
- Lower `segment_max_mb` in config
- Check if ingestion is receiving data: `tail -f logs/ingestion.log`
- Ensure `enable_fsync: true` in config

### Queue Full Warnings

**Symptom**: `[LogWriter] Queue full - backpressure active`

**Solutions**:
- Increase `queue_maxsize` (e.g., 50000)
- Decrease `flush_interval_seconds` (e.g., 2.0)
- Check disk I/O performance (use SSD over SD card)

---

## 📚 Documentation

### For New Developers / Agent Sessions

**Start Here**: [`docs/NEW_AGENT_PROMPT.md`](docs/NEW_AGENT_PROMPT.md)
- Quick onboarding for new Copilot agent sessions
- Copy-paste into fresh session to get up to speed
- 10-minute read, points to all critical files

**Complete Reference**: [`docs/SYSTEM_ONBOARDING.md`](docs/SYSTEM_ONBOARDING.md)
- Comprehensive system documentation (30-page deep dive)
- Architecture, feature engineering, configuration flow
- Recent enhancements, debugging guide, success criteria

### Ingestion Pipeline (NEW)

**Ingestion Engine**: [`docs/INGESTION_ENGINE.md`](docs/INGESTION_ENGINE.md)
- Complete architecture of the ingestion pipeline
- Performance optimizations (ThreadPoolExecutor, queue drain, etc.)
- Memory management and configuration reference
- Monitoring and troubleshooting guide

**Pi4 Deployment**: [`docs/PI4_DEPLOYMENT.md`](docs/PI4_DEPLOYMENT.md)
- Complete Raspberry Pi 4 deployment guide
- Memory optimization, swap configuration
- systemd service setup

### Feature Engineering

**Configuration Guide**: [`docs/PROCESSOR_OPTIONS.md`](docs/PROCESSOR_OPTIONS.md)
- How to customize StateConfig parameters
- Complete parameter reference with examples
- Performance recommendations for different use cases

**Research Prompt**: [`docs/RESEARCH_PROMPT_ORDERBOOK_QUANT.md`](docs/RESEARCH_PROMPT_ORDERBOOK_QUANT.md)
- Deep research prompt for optimal configurations
- Questions about horizons, bar durations, new features

### ETL Framework

**Core Framework**: [`docs/ETL_CORE_FRAMEWORK.md`](docs/ETL_CORE_FRAMEWORK.md)
- Transform architecture and base classes
- FilterSpec for partition pruning
- State management and checkpointing

**CCXT ETL**: [`docs/CCXT_ETL_ARCHITECTURE.md`](docs/CCXT_ETL_ARCHITECTURE.md)
- CCXT-specific pipeline design
- Multi-channel routing

### Storage & Operations

**Storage Architecture**: [`docs/UNIFIED_STORAGE_ARCHITECTURE.md`](docs/UNIFIED_STORAGE_ARCHITECTURE.md)
- Storage backend abstraction design
- LocalStorage vs S3Storage

**Hybrid Patterns**: [`docs/HYBRID_STORAGE_GUIDE.md`](docs/HYBRID_STORAGE_GUIDE.md)
- Local ingestion + S3 output patterns
- Sync strategies, compaction workflows

**Parquet Operations**: [`docs/PARQUET_OPERATIONS.md`](docs/PARQUET_OPERATIONS.md)
- Best practices for Parquet files
- Compression, partitioning, schema evolution

**Filename Patterns**: [`docs/FILENAME_PATTERNS.md`](docs/FILENAME_PATTERNS.md)
- Segment naming conventions
- Timestamp format reference

### Quick Reference

**Documentation Index**: [`docs/INDEX.md`](docs/INDEX.md)
- Complete index of all documentation
- Topic-based navigation
- Critical code locations

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
   - Edit `etl/features/orderbook.py`
   - Add vectorized feature calculation
   - Return in feature dict

2. **Dynamic features** (requires state):
   - Edit `etl/features/stateful.py`
   - Access previous state via class attributes
   - Compute delta-based features (OFI, MLOFI, etc.)

3. **Rolling statistics**:
   - Add tracker in feature class `__init__()`:
     ```python
     self.my_new_stat = RollingSum(horizon) for horizon in config.horizons
     ```
   - Update in processing method:
     ```python
     self.my_new_stat.update(value, timestamp)
     features[f'my_stat_{h}s'] = self.my_new_stat.sum
     ```

4. **Configuration parameters**:
   - Add to config dataclass in `etl/core/config.py`
   - Document in `docs/PROCESSOR_OPTIONS.md`
   - Update `config/config.examples.yaml`

5. **New transforms**:
   - Create new file in `etl/transforms/`
   - Inherit from `BaseTransform`
   - Register with `@register_transform` decorator

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 💡 Why Daedalus?

The name represents the core mission: like Daedalus, the master craftsman of Greek mythology who built the labyrinth, this system architects a sophisticated data labyrinth—transforming chaotic market streams into structured, navigable datasets. Just as Daedalus crafted wings from feathers and wax, we forge raw market data into refined tools for quantitative research and trading.

---

**Ready to navigate the data labyrinth?** 🏛️

```bash
# Production (Pi4)
python scripts/daedalus_supervisor.py --sources ccxt

# Development (direct)
python scripts/run_ingestion.py --sources ccxt
```
