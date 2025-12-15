# FluxForge System Onboarding - Complete Agent Briefing

**Purpose**: This document provides a comprehensive briefing for a new Copilot agent session to understand the FluxForge market data pipeline system inside-out.

---

## 🎯 Mission & Objectives

### Primary Goal
Build a **production-grade market data infrastructure** for **mid-frequency quantitative cryptocurrency trading** (5-second to 4-hour holding periods).

### Key Insight
**We cannot compete with institutional HFT infrastructure.** Our edge comes from:
1. **Information processing**, not speed
2. **Advanced microstructure features** (60+ features from orderbook data)
3. **Multi-timeframe analysis** (1s to 1h bars + rolling statistics)
4. **Statistical patterns** that persist over longer horizons

### Trading Context
- **Asset Class**: Cryptocurrency (BTC, ETH, SOL, etc.)
- **Exchanges**: Multi-exchange (Binance US, Coinbase Advanced, OKX, etc.)
- **Infrastructure**: Retail/prosumer (Raspberry Pi to cloud servers)
- **Latency**: 50-500ms to exchange (not co-located)
- **Capital**: $10K-$1M range
- **Target Sharpe**: > 2, Max Drawdown < 20%

---

## 🏗️ System Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 1: WebSocket Collectors (ingestion/)                          │
│ ├─ Pure async I/O, zero CPU-intensive processing                   │
│ ├─ CCXT Pro (primary), Coinbase Advanced Trade (native)            │
│ ├─ Push to bounded asyncio queue (backpressure handling)           │
│ └─ Automatic reconnection with exponential backoff                  │
└─────────────────────────────────────────────────────────────────────┘
              ↓ (asyncio queue)
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 2: StreamingParquetWriter (ingestion/writers/)                │
│ ├─ BRONZE LAYER: Raw data landing in Parquet format               │
│ ├─ 5-10x smaller than NDJSON with ZSTD compression                │
│ ├─ Channel-separated files (ticker/, trades/, orderbook/)          │
│ ├─ Size-based segment rotation (default: 50 MB)                   │
│ ├─ active/ → ready/ atomic moves                                   │
│ ├─ Schema-aware: typed columns, nested structs for orderbook      │
│ └─ Preserves ALL raw data for replay/reprocessing                 │
└─────────────────────────────────────────────────────────────────────┘
              ↓ (file system / S3)
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 3: ETL Workers (etl/) - SILVER/GOLD LAYERS                   │
│ ├─ Reads ready/ segments (Parquet or NDJSON based on config)      │
│ ├─ Atomic moves to processing/ (prevents double-processing)        │
│ ├─ Feature engineering: 60+ microstructure features                │
│ ├─ Multi-output: High-frequency features + bar aggregates          │
│ └─ Hive-partitioned Parquet: exchange=X/symbol=Y/date=Z/           │
└─────────────────────────────────────────────────────────────────────┘
```

**Why preserve raw data?** We keep Bronze layer (raw Parquet) to iterate on feature engineering and model development without re-collecting data from exchanges.

### Data Flow

```
Exchange WebSocket
    ↓ (CCXT Pro / Coinbase native)
Raw Market Data (tick-by-tick)
    ↓ (Collector adds capture_ts)
asyncio.Queue (bounded, backpressure)
    ↓ (StreamingParquetWriter batches)
Bronze: Raw Parquet Segments (50MB, ZSTD compressed)
    ├─ raw/active/ccxt/ticker/segment_*.parquet     (writing)
    ├─ raw/active/ccxt/trades/segment_*.parquet     (writing)
    ├─ raw/active/ccxt/orderbook/segment_*.parquet  (writing)
    └─ raw/ready/ccxt/{channel}/segment_*.parquet   (ready for ETL)
    ↓ (ETL Job picks up)
Silver/Gold: Feature-Engineered Parquet
    ├─ processed/ccxt/orderbook/hf/exchange=binanceus/symbol=BTC-USDT/date=2025-12-12/*.parquet
    └─ processed/ccxt/orderbook/bars/exchange=binanceus/symbol=BTC-USDT/date=2025-12-12/*.parquet
```

---

## 📂 Critical Code Locations

### Ingestion Layer (`ingestion/`) - BRONZE

**collectors/**
- `base_collector.py` - Abstract base for all collectors
- `ccxt_collector.py` - **PRIMARY**: CCXT Pro unified interface (100+ exchanges)
- `coinbase_ws.py` - Native Coinbase Advanced Trade WebSocket

**writers/**
- `parquet_writer.py` - **CRITICAL**: StreamingParquetWriter for raw Parquet landing (Bronze)
- `log_writer.py` - Legacy NDJSON writer (still supported via `raw_format` config)

**orchestrators/**
- `ingestion_pipeline.py` - Coordinates collectors + writer, selects writer based on `raw_format`

**utils/**
- `serialization.py` - JSON encoding for market data types
- `time.py` - Timestamp utilities

### ETL Layer (`etl/`) - SILVER/GOLD

**orchestrators/**
- `pipeline.py` - Base ETL pipeline (Reader → Processor → Writer)
- `ccxt_segment_pipeline.py` - **CRITICAL**: Multi-channel routing for CCXT data
- `coinbase_segment_pipeline.py` - Coinbase-specific routing
- `multi_output_pipeline.py` - Splits processor output to multiple destinations

**processors/**
- `base_processor.py` - Abstract processor interface
- `raw_processor.py` - Pass-through (no transformation)
- `ccxt/ticker_processor.py` - Ticker data enrichment
- `ccxt/trades_processor.py` - Trade data enrichment
- `ccxt/advanced_orderbook_processor.py` - **MOST IMPORTANT**: Feature engineering engine
- `coinbase/` - Coinbase-specific processors

**features/** - **CORE FEATURE ENGINEERING**
- `snapshot.py` - **60+ static/structural features** from single orderbook snapshot
  - Best bid/ask (L0), multi-level prices/sizes (L1-L{max_levels})
  - Mid price, spread, relative spread (bps)
  - Microprice (volume-weighted fair value)
  - Imbalance at each level
  - Cumulative depth at price bands (0-5bps, 5-10bps, 10-25bps, 25-50bps)
  
- `state.py` - **CRITICAL**: Stateful per-symbol processing
  - `StateConfig` dataclass - configurable parameters
  - `SymbolState` class - maintains rolling statistics
  - **Dynamic features**: OFI (Order Flow Imbalance), log returns, velocity, acceleration
  - **Rolling stats**: Realized volatility, OFI sums, Trade Flow Imbalance (TFI)
  - **Bar aggregation**: OHLCV + microstructure stats
  - **Regime detection**: Spread regimes (tight/wide), liquidity regimes
  
- `streaming.py` - Online algorithms
  - `RollingWelford` - Variance/std estimation (Welford's method)
  - `RollingSum` - Time-windowed sum with decay
  - `RegimeStats` - Regime fraction tracking

**parsers/**
- `ccxt_parser.py` - Extract channel + data from CCXT messages
- `coinbase_parser.py` - Parse Coinbase WebSocket messages

**readers/**
- `ndjson_reader.py` - **PRIMARY**: Read NDJSON segments
- `parquet_reader.py` - Read existing Parquet files (for repartitioning)

**writers/**
- `parquet_writer.py` - **CRITICAL**: Hive-partitioned Parquet output with configurable compression

**job.py** - **ENTRY POINT**: ETL job orchestration
- Discovers ready segments
- Routes to appropriate pipeline (CCXT vs Coinbase)
- Atomic moves: ready/ → processing/ → delete
- Configurable deletion after processing

### Storage Layer (`storage/`)

**base.py** - **CRITICAL**: Unified storage abstraction
- `StorageBackend` - Abstract interface
- `LocalStorage` - Local filesystem operations
- `S3Storage` - S3 operations with boto3
  - **Connection pool configuration**: `max_pool_connections=50` (default 10)
  - Exponential backoff retries
  - Efficient list_files with pagination

**factory.py** - Storage backend factory
- Creates storage backends from config
- Supports hybrid patterns (local ingestion, S3 output)

**sync.py** - **CRITICAL**: Bidirectional sync
- Local → S3 (upload/backup)
- S3 → Local (download)
- S3 → S3 (cross-bucket copy)
- Parallel transfers with ThreadPoolExecutor
- Optional file deletion after successful transfer
- Pre-transfer hooks (e.g., compaction)

### Configuration (`config/`)

**config.py** - **CRITICAL**: Pydantic configuration models
- `AppConfig` - Root configuration
- `StorageConfig` - Storage backend configuration
- `CcxtConfig` - CCXT exchange configuration
- `IngestionConfig` - Ingestion settings
- `ETLConfig` - ETL settings
- `ChannelETLConfig` - **Per-channel processor_options**
- `S3Config` - S3-specific settings (includes `max_pool_connections`)

**config.yaml** - **RUNTIME CONFIGURATION**
- **NOT committed to git** (contains API keys)
- Copied from `config.examples.yaml`
- **processor_options** section for feature engineering customization

### Scripts (`scripts/`)

**run_ingestion.py** - **Terminal 1**: Start live data collection
- Usage: `python scripts/run_ingestion.py --sources ccxt`
- Runs continuously until Ctrl+C
- Prints stats every 60 seconds

**run_etl_watcher.py** - **Terminal 2**: Continuous ETL
- Usage: `python scripts/run_etl_watcher.py --poll-interval 30`
- Polls ready/ directory every 30 seconds
- Processes new segments automatically

**run_etl.py** - **Manual ETL** (batch mode)
- Usage: `python scripts/run_etl.py --source ccxt --mode all`
- Process specific date: `--mode date --date 2025-12-12`
- Process date range: `--mode range --start-date 2025-12-01 --end-date 2025-12-12`

**run_compaction.py** - **Periodic maintenance**
- Usage: `python scripts/run_compaction.py --source ccxt --partition exchange=binanceus/symbol=BTC-USDT`
- Merges small Parquet files to reduce overhead
- Recommended: Daily run for active partitions

**Other scripts:**
- `query_parquet.py` - Query processed data
- `check_health.py` - System health checks
- `validate_partitioning.py` - Verify partition integrity
- `migrate_schema.py` - Schema migration tool
- `fix_partition_mismatch.py` - **NEW**: Fix partition value mismatches (BTC/USD → BTC-USD)
- `test_config_flow.py` - **NEW**: Verify processor_options configuration flow

### Documentation (`docs/`)

**CCXT_ETL_ARCHITECTURE.md** - CCXT-specific ETL design
**UNIFIED_STORAGE_ARCHITECTURE.md** - Storage backend abstraction
**HYBRID_STORAGE_GUIDE.md** - Local + S3 hybrid patterns
**PARQUET_OPERATIONS.md** - Parquet best practices
**FILENAME_PATTERNS.md** - Segment naming conventions
**PROCESSOR_OPTIONS.md** - **NEW**: Complete guide to feature engineering configuration
**US_CRYPTO_STRATEGY.md** - US-based crypto trading strategy
**RESEARCH_PROMPT_ORDERBOOK_QUANT.md** - **NEW**: Deep research prompt for optimal configurations

---

## 🧠 Feature Engineering Deep Dive

### Current Features (60+ features per snapshot)

#### 1. Structural/Static Features
From `etl/features/snapshot.py`:
```python
- best_bid, best_ask (L0)
- bid_price_L0 to bid_price_L{max_levels-1}
- bid_size_L0 to bid_size_L{max_levels-1}
- ask_price_L0 to ask_price_L{max_levels-1}
- ask_size_L0 to ask_size_L{max_levels-1}
- mid_price = (best_bid + best_ask) / 2
- spread = best_ask - best_bid
- relative_spread = spread / mid_price (in decimal, e.g. 0.0001 = 1bp)
- microprice = (bid_size * ask_price + ask_size * bid_price) / (bid_size + ask_size)
- imbalance_L0, imbalance_L1, ..., imbalance_L{max_levels-1}
  - imbalance = (bid_size - ask_size) / (bid_size + ask_size)
- weighted_mid = sum(level_price * level_size) / total_size
- bid_vol_band_0_5bps, bid_vol_band_5_10bps, etc.
- ask_vol_band_0_5bps, ask_vol_band_5_10bps, etc.
- total_bid_volume, total_ask_volume (across all bands)
- book_depth = number of valid levels
```

#### 2. Flow/Dynamic Features
From `etl/features/state.py`:
```python
- log_return_step = log(mid_t / mid_t-1)
- ofi_step = Order Flow Imbalance (Cont's method, L1)
- ofi_multi = Multi-level OFI (sum across top ofi_levels)
- mid_velocity = (mid_t - mid_t-1) / dt
- mid_accel = (velocity_t - velocity_t-1) / dt
- micro_minus_mid = microprice - mid_price (fair value deviation)
```

#### 3. Rolling/Streaming Statistics
From `etl/features/state.py`:
```python
# For each horizon in [1s, 5s, 30s, 60s]:
- rv_{horizon}s = Realized volatility (Welford's method)
- ofi_sum_{horizon}s = Cumulative OFI over window
- tfi_{horizon}s = Trade Flow Imbalance = (buy_vol - sell_vol) / total_vol
- trade_vol_{horizon}s = Total trade volume

# Fixed 30s window:
- depth_0_5bps_sigma = Std dev of 0-5bps band depth
- spread_regime_tight_frac = Fraction of time spread was "tight" (< 1bp)
```

#### 4. Time Features
From `etl/processors/time_utils.py`:
```python
- hour_of_day (0-23)
- day_of_week (0-6)
- is_weekend (0 or 1)
- minute_of_hour (0-59)
```

#### 5. Bar Aggregation (OHLCV+)
From `etl/features/state.py` → `BarBuilder`:
```python
# For each bar_duration in [1s, 5s, 30s, 60s]:
- timestamp (bar start time)
- duration
- open, high, low, close (of mid price)
- mean_spread, min_spread, max_spread
- mean_relative_spread
- mean_l1_imbalance
- sum_ofi (cumulative OFI within bar)
- realized_variance (returns variance within bar)
- count (number of snapshots in bar)
```

### Configurable Parameters (StateConfig)

**Location**: `etl/features/state.py` → `StateConfig` dataclass

```python
@dataclass
class StateConfig:
    # Rolling statistic windows (seconds)
    horizons: List[int] = [1, 5, 30, 60]
    
    # Bar aggregation intervals (seconds)
    bar_durations: List[int] = [1, 5, 30, 60]
    
    # High-frequency emission rate (seconds)
    hf_emit_interval: float = 1.0  # Emit features every 1s
    
    # Feature extraction depth
    max_levels: int = 10           # Orderbook levels to process
    ofi_levels: int = 5            # Levels for multi-level OFI
    
    # Liquidity band analysis (basis points)
    bands_bps: List[int] = [5, 10, 25, 50]
    
    # Storage options
    keep_raw_arrays: bool = False  # Include raw bid/ask arrays
    
    # Regime classification
    tight_spread_threshold: float = 0.0001  # 1 bp
```

**Configuration Flow**:
```
config/config.yaml
  └─ etl.channels.orderbook.processor_options
      ↓ (Pydantic model)
AppConfig
  └─ ETLConfig.channels["orderbook"]
      ↓ (dict conversion)
ETLJob
  └─ channel_config parameter
      ↓ (pipeline creation)
CcxtSegmentPipeline
  └─ processor_options extracted
      ↓ (processor initialization)
CcxtAdvancedOrderbookProcessor(**processor_options)
  └─ StateConfig created
      ↓ (feature engineering)
SymbolState uses config for all feature extraction
```

**How to modify**: Edit `config/config.yaml`:
```yaml
etl:
  channels:
    orderbook:
      processor_options:
        max_levels: 20              # More depth
        hf_emit_interval: 0.5       # Faster sampling
        bar_durations: [5, 15, 60]  # Skip 1s bars
        horizons: [1, 5, 30, 60, 300]  # Add 5-minute window
```

---

## 🔧 Recent Enhancements

### 1. Processor Options Configuration Flow (Dec 2025)
**Problem**: User couldn't customize feature engineering parameters
**Solution**: 
- Added `processor_options` to channel config
- Updated `CcxtSegmentPipeline` to pass options to processors
- Added parameter name mapping for backward compatibility (`hf_sample_interval` → `hf_emit_interval`)
- Created test script: `scripts/test_config_flow.py`
- Documented in: `docs/PROCESSOR_OPTIONS.md`

### 2. S3 Connection Pool Configuration (Dec 2025)
**Problem**: Default boto3 pool (10 connections) bottlenecked ThreadPoolExecutor
**Solution**:
- Added `max_pool_connections` to `S3Config` (default: 50)
- Applied via `botocore.config.Config` in `S3Storage`
- Enables efficient parallel S3 operations

### 3. Partition Mismatch Fix Script (Dec 2025)
**Problem**: Symbol format mismatches (BTC/USD vs BTC-USD) in partition paths
**Solution**:
- Created `scripts/fix_partition_mismatch.py`
- Pandas-based (memory-efficient for KB-sized files)
- Supports both local filesystem and S3
- Usage: `python scripts/fix_partition_mismatch.py --storage s3 --partition exchange=X/symbol=Y`

### 4. Research Prompt for Optimal Configurations (Dec 2025)
**Purpose**: Determine optimal StateConfig defaults for mid-frequency crypto trading
**Document**: `docs/RESEARCH_PROMPT_ORDERBOOK_QUANT.md`
**Status**: Awaiting research agent results
**Next Steps**: Implement recommended features and configurations based on research

---

## 📊 Typical Production Workflow

### Setup Phase (One-time)

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure exchanges**
   ```bash
   cp config/config.examples.yaml config/config.yaml
   # Edit config.yaml with API keys and desired symbols
   ```

3. **Configure feature engineering**
   Edit `config.yaml` → `etl.channels.orderbook.processor_options`

### Live Operation (4 concurrent processes)

#### Terminal 1: Ingestion
```bash
python scripts/run_ingestion.py --sources ccxt
```
- Collects live market data via WebSocket
- Writes to NDJSON segments (100MB each)
- Segments rotate from active/ to ready/
- Runs 24/7

#### Terminal 2: ETL + Feature Engineering
```bash
python scripts/run_etl_watcher.py --poll-interval 30
```
- Polls ready/ every 30 seconds
- Processes new segments automatically
- Generates high-frequency features (1s snapshots)
- Generates bar aggregates (1s, 5s, 30s, 60s)
- Writes to Hive-partitioned Parquet
- Runs 24/7

#### Terminal 3: Cloud Sync (Optional)
```bash
# Upload to S3 every hour
while true; do
  python storage/sync.py upload \
    --source-path processed/ccxt/ \
    --dest-path s3://my-bucket/processed/ccxt/ \
    --pattern "*.parquet" \
    --workers 8
  sleep 3600
done
```

#### Terminal 4: Maintenance (Periodic)
```bash
# Daily compaction for active symbols
python scripts/run_compaction.py \
  --source ccxt \
  --partition exchange=binanceus/symbol=BTC-USDT/date=2025-12-12
```

### Monitoring & Health Checks

```bash
# System health
python scripts/check_health.py

# Query features
python scripts/query_parquet.py data/processed/ccxt/orderbook/hf

# Validate partitions
python scripts/validate_partitioning.py
```

---

## 🚨 Critical Design Decisions & Rationale

### 1. Why NDJSON instead of binary formats?
- **Debuggability**: Plain text, grep-able
- **Append-only**: Perfect for write-once semantics
- **Simplicity**: No schema evolution issues
- **Good enough**: Compression makes size acceptable

### 2. Why size-based rotation instead of time-based?
- **Predictable file sizes**: Critical for memory-constrained devices (Raspberry Pi)
- **Load balancing**: ETL jobs process similar-sized chunks
- **Variable market activity**: Time-based would create huge files during volatile periods

### 3. Why active/ready/processing segregation?
- **Zero data loss**: Ingestion never blocks on ETL
- **Idempotency**: Atomic moves prevent double-processing
- **Crash recovery**: Can resume from processing/ directory
- **Clear state machine**: File location = state

### 4. Why separate storage backends for input/output?
- **Hybrid patterns**: Collect locally (fast), store in cloud (durable)
- **Cost optimization**: Local SSD for hot data, S3 for cold data
- **Flexibility**: Can migrate incrementally

### 5. Why stateful feature engineering?
- **Time-series nature**: OFI, rolling stats require previous state
- **Efficiency**: Avoid recomputing windows from scratch
- **Streaming algorithms**: Welford for variance, exponential decay for sums

### 6. Why multi-output pipelines for orderbook?
- **Different use cases**: 
  - HF features (1s snapshots) for ML model input
  - Bars (aggregated) for backtesting and charting
- **Storage efficiency**: Don't duplicate raw data
- **Query optimization**: Separate HF vs bars for different access patterns

---

## 🎓 Key Concepts to Master

### 1. Order Flow Imbalance (OFI)
**Paper**: "Price Impact of Order Book Events" (Cont, Stoikov, Talreja, 2010)

**Formula** (Cont's method for level k):
```
e_k,t = I(p_k,t > p_k,t-1) * q_k,t           # Price moved up → aggressive buying
      + I(p_k,t = p_k,t-1) * (q_k,t - q_k,t-1)  # Price unchanged → size change
      + I(p_k,t < p_k,t-1) * (-q_k,t-1)        # Price moved down → cancellation
```

**Interpretation**:
- OFI > 0: Net buying pressure (bullish)
- OFI < 0: Net selling pressure (bearish)
- Predictive power for short-term price movements

**Implementation**: `etl/features/state.py` → `SymbolState.process_snapshot()`

### 2. Microprice
**Definition**: Volume-weighted fair value

**Formula**:
```
microprice = (bid_size * ask_price + ask_size * bid_price) / (bid_size + ask_size)
```

**Interpretation**:
- More accurate than simple mid price
- Accounts for order book imbalance
- Tends to lead price movements

**Implementation**: `etl/features/snapshot.py` → `extract_orderbook_features()`

### 3. Welford's Online Algorithm
**Purpose**: Compute variance in single pass with numerical stability

**Algorithm**:
```python
count = 0
mean = 0.0
M2 = 0.0

for x in data:
    count += 1
    delta = x - mean
    mean += delta / count
    delta2 = x - mean
    M2 += delta * delta2

variance = M2 / count if count > 1 else 0
```

**Why**: Avoids catastrophic cancellation in naive two-pass algorithm

**Implementation**: `etl/features/streaming.py` → `RollingWelford`

### 4. Hive-style Partitioning
**Format**: `key1=value1/key2=value2/...`

**Example**: `exchange=binanceus/symbol=BTC-USDT/date=2025-12-12/`

**Benefits**:
- **Predicate pushdown**: Query engines skip irrelevant partitions
- **Data locality**: Related data grouped together
- **Parallel processing**: Each partition is independently processable

**Implementation**: `etl/writers/parquet_writer.py`

---

## 🔍 Debugging & Troubleshooting

### Common Issues

#### 1. "Too many open files" error
**Cause**: OS limit on file descriptors
**Solution**:
```bash
# Increase limit (Linux)
ulimit -n 4096

# Permanent fix (add to /etc/security/limits.conf)
* soft nofile 65536
* hard nofile 65536
```

#### 2. S3 slow upload/download
**Cause**: Low connection pool size
**Solution**: Increase `max_pool_connections` in `config.yaml`:
```yaml
s3:
  max_pool_connections: 100  # Increase from default 50
```

#### 3. High memory usage during ETL
**Cause**: Large segments or too many levels
**Solution**:
- Reduce `segment_max_mb` (e.g., 50 MB instead of 100 MB)
- Reduce `max_levels` in processor_options
- Use batch processing with smaller chunks

#### 4. Missing features in output
**Cause**: processor_options not applied
**Solution**: Run test script to verify config flow:
```bash
python scripts/test_config_flow.py
```

#### 5. Partition format mismatch
**Cause**: Symbol contains `/` (e.g., BTC/USD)
**Solution**: Run partition fix script:
```bash
python scripts/fix_partition_mismatch.py \
  --storage local \
  --partition exchange=binanceus/symbol=BTC/USD
```

### Logging

**Levels**: DEBUG, INFO, WARNING, ERROR, CRITICAL

**Configuration**: `config.yaml` → `log_level: "INFO"`

**Key loggers**:
- `ingestion.orchestrators.ingestion_pipeline` - Ingestion stats
- `etl.job` - ETL progress
- `etl.features.state` - Feature engineering
- `storage.sync` - Sync operations

**Enable debug logging**:
```yaml
log_level: "DEBUG"
```

---

## 📚 Next Steps for New Agent

### Immediate Tasks

1. **Read through critical files**:
   - `etl/features/state.py` - Understand StateConfig and SymbolState
   - `etl/features/snapshot.py` - Learn static feature extraction
   - `etl/orchestrators/ccxt_segment_pipeline.py` - Understand routing
   - `storage/base.py` - Master storage abstraction

2. **Review recent changes**:
   - `docs/PROCESSOR_OPTIONS.md` - Configuration guide
   - `scripts/test_config_flow.py` - Configuration test
   - `scripts/fix_partition_mismatch.py` - Partition fix utility

3. **Check documentation**:
   - `docs/RESEARCH_PROMPT_ORDERBOOK_QUANT.md` - Research prompt awaiting results
   - `docs/UNIFIED_STORAGE_ARCHITECTURE.md` - Storage design
   - `docs/HYBRID_STORAGE_GUIDE.md` - Hybrid local/cloud patterns

### Awaiting Research Results

**Topic**: Optimal StateConfig defaults for mid-frequency crypto trading

**Research Questions**:
1. What are the optimal rolling window sizes (horizons)?
2. What bar durations capture meaningful patterns?
3. How many orderbook levels contain useful signal?
4. What additional features should we implement?
5. What model architectures are best for orderbook data?

**Document**: `docs/RESEARCH_PROMPT_ORDERBOOK_QUANT.md`

**Action**: When research results arrive, implement:
1. Updated StateConfig defaults
2. New features (prioritized by effort vs expected alpha)
3. Model architecture recommendations
4. Preprocessing pipeline improvements

---

## 🎯 Success Criteria

The system is working correctly when:

1. ✅ **Ingestion**: 
   - WebSocket connections stable (no disconnects)
   - Segments rotating at predictable intervals
   - No queue overflow warnings

2. ✅ **ETL**:
   - ready/ directory stays near-empty (< 10 segments)
   - Parquet files generated with correct partitions
   - Feature count matches expectations (60+ columns)

3. ✅ **Storage**:
   - Sync transfers complete without errors
   - S3 and local directories in sync
   - Compaction reduces file count

4. ✅ **Features**:
   - OFI values are reasonable (not all zeros)
   - Rolling statistics converge over time
   - Bar counts match snapshot counts

5. ✅ **Performance**:
   - Ingestion: < 1% CPU, < 500 MB RAM
   - ETL: < 50% CPU, < 2 GB RAM
   - Latency: Segments processed within 5 minutes of closing

---

**This document should serve as your complete reference. Read it thoroughly, explore the codebase, and you'll be ready to receive and implement the research results.**
