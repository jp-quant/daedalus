## Project Overview

You are joining **Daedalus** - a production-grade market data infrastructure for **mid-frequency quantitative cryptocurrency trading** (5-second to 4-hour holding periods).

### Mission
Build a competitive edge through **information processing**, not speed. We cannot compete with institutional HFT infrastructure. Our alpha comes from:
- Advanced microstructure features (60+ from orderbook data)
- Multi-timeframe analysis (1s to 60s bars + rolling statistics)
- Statistical patterns that persist over longer horizons
- **Preserving raw data for iterative feature engineering and modeling**

### Trading Context
| Parameter | Value |
|-----------|-------|
| Asset Class | Cryptocurrency (BTC, ETH, SOL, etc.) |
| Exchanges | Coinbase Advanced (primary), Binance US, OKX, 100+ via CCXT Pro |
| Holding Period | 5 seconds to 4 hours |
| Infrastructure | Retail/prosumer (Raspberry Pi 4 for 24/7 collection → cloud for compute) |
| Latency | 50-500ms (not co-located) |
| Target Sharpe | > 2, Max Drawdown < 20% |

---

## System Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 1: WebSocket Collectors (ingestion/)                          │
│ ├─ Pure async I/O, zero CPU-intensive processing                   │
│ ├─ CCXT Pro (100+ exchanges), Coinbase Advanced (native)           │
│ ├─ Push to bounded asyncio queue (backpressure handling)           │
│ └─ Automatic reconnection with exponential backoff                  │
└─────────────────────────────────────────────────────────────────────┘
              ↓ (asyncio queue)
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 2: StreamingParquetWriter (ingestion/writers/)                │
│ ├─ BRONZE LAYER: Raw data landing in Parquet format               │
│ ├─ Directory-Aligned Partitioning (partition cols IN data)         │
│ ├─ 5-10x smaller than NDJSON with ZSTD compression                │
│ ├─ Channel-separated: ticker/, trades/, orderbook/                 │
│ ├─ Size-based segment rotation (default: 50 MB)                   │
│ ├─ active/ → ready/ atomic moves                                   │
│ ├─ Emergency cleanup on SIGINT/SIGTERM                             │
│ └─ Schema-aware: typed columns, nested structs for orderbook      │
└─────────────────────────────────────────────────────────────────────┘
              ↓ (file system / S3 via StorageSync)
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 3: ETL Workers (etl/) - SILVER/GOLD LAYERS                   │
│ ├─ TransformExecutor orchestrates I/O and transform execution     │
│ ├─ FilterSpec enables partition pruning for efficient reads        │
│ ├─ Feature engineering: 60+ microstructure features                │
│ ├─ Stateful features: OFI/MLOFI/TFI with staleness detection      │
│ ├─ State checkpoint/resume via StatefulOrderbookRunner             │
│ └─ Hive-partitioned Parquet output                                 │
└─────────────────────────────────────────────────────────────────────┘
```

### Directory-Aligned Partitioning (Critical Concept!)

Unlike traditional Hive partitioning where partition columns are derived from the directory path and NOT stored in data files, our approach requires partition column values to **EXIST in the Parquet data AND MATCH the directory partition values exactly**.

**Default Partition Columns**: `["exchange", "symbol", "year", "month", "day", "hour"]`

```
raw/ready/ccxt/orderbook/exchange=coinbaseadvanced/symbol=BTC-USD/year=2025/month=06/day=19/hour=14/
                                                          ↑
                                            Values sanitized (/ → -) in BOTH path AND data
```

**Key files**: `shared/partitioning.py` (unified utilities used by ingestion AND ETL)

### Why Raw Parquet (Bronze Layer)?

The ingestion layer now writes **raw data directly to Parquet** instead of NDJSON:

1. **5-10x smaller** with ZSTD compression
2. **Columnar format** - can read just needed columns for feature iteration
3. **Typed schema** - no JSON parsing overhead
4. **Preserves all raw data** - enables iterative feature engineering
5. **Same durability guarantees** - fsync, atomic moves, immediate S3 upload

**Key insight**: We keep raw data (Bronze) to iterate on features and models without re-collecting from exchanges.

### Data Flow

```
Exchange WebSocket
    ↓ (CCXT Pro / Coinbase native)
Raw Market Data (tick-by-tick)
    ↓ (Collector adds capture_ts)
asyncio.Queue (bounded, backpressure)
    ↓ (StreamingParquetWriter batches)
Bronze Layer: Raw Parquet Segments (50MB chunks, Directory-Aligned Partitioned)
    ├─ raw/active/ccxt/{channel}/exchange=X/symbol=Y/.../       (writing)
    └─ raw/ready/ccxt/{channel}/exchange=X/symbol=Y/.../        (ready for ETL)
    ↓ (StorageSync uploads to S3)
S3 Bucket: Durable Cloud Storage
    ↓ (ETL reads from local or S3)
Silver Layer: Feature-Engineered Parquet
    └─ processed/ccxt/orderbook/silver/exchange=X/symbol=Y/.../
    ↓ (Bars Transform)
Gold Layer: Aggregated Bars (future)
```

### Partition Path Example
```
raw/ready/ccxt/orderbook/exchange=coinbaseadvanced/symbol=BTC-USD/year=2025/month=06/day=19/hour=14/segment_001.parquet
```

### Raw Parquet Schemas

**Ticker** (`ingestion/writers/parquet_writer.py`):
```
collected_at (int64), capture_ts (timestamp), exchange, symbol,
bid, ask, bid_volume, ask_volume, last, open, high, low, close,
vwap, base_volume, quote_volume, change, percentage, timestamp,
info_json (string)
```

**Trades**:
```
collected_at, capture_ts, exchange, symbol, trade_id, timestamp,
side, price, amount, cost, info_json
```

**Orderbook**:
```
collected_at, capture_ts, exchange, symbol, timestamp, nonce,
bids (list<struct<price, size>>), asks (list<struct<price, size>>)
```

---

## Critical Code Map

### Shared Utilities

| File | Purpose |
|------|---------|
| `shared/partitioning.py` | **Unified partitioning logic** - sanitize values, build paths, DEFAULT_PARTITION_COLUMNS |

### Ingestion Layer (BRONZE)

| File | Purpose |
|------|---------|
| `ingestion/writers/parquet_writer.py` | **StreamingParquetWriter** - Raw Parquet landing with emergency cleanup |
| `ingestion/writers/log_writer.py` | Legacy NDJSON writer (still supported) |
| `ingestion/collectors/ccxt_collector.py` | CCXT Pro unified WebSocket (100+ exchanges), bulk methods |
| `ingestion/collectors/coinbase_ws.py` | Native Coinbase Advanced Trade WebSocket |
| `ingestion/orchestrators/ingestion_pipeline.py` | Coordinates collectors + writer with graceful shutdown |
| `scripts/run_ingestion.py` | **Primary entry point**: `python scripts/run_ingestion.py --sources ccxt` |

**Config**: `config.ingestion.raw_format = "parquet"` (recommended) or `"ndjson"` (legacy)

### Feature Engineering (SILVER/GOLD)

| File | Purpose |
|------|---------|
| `etl/features/orderbook.py` | Vectorized structural + rolling orderbook features (Polars) |
| `etl/features/stateful.py` | **StatefulFeatureProcessor** - OFI/MLOFI/TFI/Kyle/VPIN with staleness detection |
| `etl/features/streaming.py` | Online stats primitives (RollingWelford, RollingSum, VPINCalculator, KyleLambdaEstimator) |
| `etl/transforms/orderbook.py` | **OrderbookFeatureTransform** - orchestrates structural + stateful features |
| `etl/core/executor.py` | **TransformExecutor** - handles I/O, FilterSpec partition pruning, writes |
| `etl/core/config.py` | FilterSpec, FeatureConfig, InputConfig, OutputConfig |
| `etl/utils/state_manager.py` | State persistence for checkpoint/resume and graceful shutdown |

### ETL Scripts

| File | Purpose |
|------|---------|
| `scripts/etl/run_orderbook_features.py` | **StatefulOrderbookRunner** - batch orderbook ETL with state persistence |
| `scripts/etl/run_trades_features.py` | Batch runner for trades features |
| `scripts/etl/run_ticker_features.py` | Batch runner for ticker features |
| `scripts/etl/run_bars.py` | Bar aggregation transform (silver → gold) |
| `scripts/etl/run_watcher.py` | Continuous watcher for ready/ segments with checkpointing |

**Start here**: Read `scripts/etl/run_orderbook_features.py` for the batch processing pattern.

**Key Feature Components**:
- `extract_structural_features()` in `etl/features/orderbook.py`
- `compute_rolling_features()` in `etl/features/orderbook.py`
- `StatefulFeatureProcessor` in `etl/features/stateful.py` (with `max_state_staleness_seconds`)

### Storage & Sync

| File | Purpose |
|------|---------|
| `storage/base.py` | Unified storage abstraction (LocalStorage, S3Storage) |
| `storage/factory.py` | Storage backend factory |
| `storage/sync.py` | **StorageSync** - bidirectional sync (local ↔ S3) with path normalization |
| `scripts/run_sync.py` | **Sync entry point**: uploads raw data to S3 for durability |

### Configuration

| File | Purpose |
|------|---------|
| `config/config.py` | Pydantic configuration models (CoinbaseConfig, CcxtConfig, S3Config, etc.) |
| `config/config.yaml` | Runtime configuration (NOT in git - contains API keys) |
| `config/config.examples.yaml` | Template for config.yaml |

**Critical Features Enabled by Default**:
- VPIN (Volume-Synchronized Probability of Informed Trading) - flow toxicity, predicts volatility
- Kyle's Lambda - price impact coefficient
- Band-based depth (0-5bps, 5-10bps, etc.) - liquidity at price-distance
- Staleness detection: `max_state_staleness_seconds = 300.0` (auto-resets stale prev_* state)

### Processors

This repo’s current batch path uses the modular framework runners in `scripts/etl/`.

### Orchestration

Orchestration entry points:
- `scripts/run_ingestion.py` (collect raw)
- `scripts/etl/run_orderbook_features.py` (batch orderbook → features)
- `scripts/etl/run_watcher.py` (continuous processing of ready/ segments)

### Configuration

| File | Purpose |
|------|---------|
| `config/config.py` | Pydantic configuration models |
| `config/config.yaml` | Runtime configuration (NOT in git - contains API keys) |
| `config/config.examples.yaml` | Template for config.yaml |

**Tier naming**: output tier folder names are configurable via `storage.paths.tier_*` (e.g. rename `silver` → `platinum`).

**State persistence**: ETL state/checkpoint directory configurable via `storage.paths.state_dir` (default: `temp/state`). Automatic checkpoint on SIGINT/SIGTERM via `StateManager`.

### Storage

| File | Purpose |
|------|---------|
| `storage/base.py` | Unified storage abstraction (LocalStorage, S3Storage) |
| `storage/factory.py` | Storage backend factory |
| `storage/sync.py` | Bidirectional sync (local ↔ S3) |

---

## Configuration

### Ingestion Config (`config.yaml`)

```yaml
ingestion:
  raw_format: "parquet"           # "parquet" (recommended) or "ndjson" (legacy)
  batch_size: 1000                # Records per batch write
  flush_interval_seconds: 5.0     # Force flush every N seconds
  queue_maxsize: 50000            # Max records in memory queue
  segment_max_mb: 50              # Rotate segment after N MB
  parquet_compression: "zstd"     # Compression codec
  parquet_compression_level: 3    # 1-22 for zstd
```

### ETL Channel Config (`config.yaml`)

```yaml
etl:
  channels:
    orderbook:
      enabled: true
      partition_cols: ["exchange", "symbol", "date"]
      processor_options:
                # Structural + rolling features
                compute_features: true
                max_levels: 20
                bands_bps: [5, 10, 25, 50, 100]
                horizons: [5, 15, 60, 300, 900]
                bar_durations: [60, 300, 900, 3600]

                # Stateful features (sequential)
                enable_stateful: true
                ofi_levels: 10
                ofi_decay_alpha: 0.5
                use_dynamic_spread_regime: true
                spread_regime_window: 300
                spread_tight_percentile: 0.2
                spread_wide_percentile: 0.8
                tight_spread_threshold: 0.0001
                kyle_lambda_window: 300
                enable_vpin: true
                vpin_bucket_volume: 1.0
                vpin_window_buckets: 50
```

### Configuration Flow

```
config.yaml
    ↓
IngestionConfig.raw_format → StreamingParquetWriter or LogWriter
    ↓
etl.channels.orderbook.processor_options
    ↓
scripts/etl/run_orderbook_features.py
    ↓
etl/features/orderbook.py (vectorized) + etl/features/stateful.py (sequential)

---

## Deep Research Prompt (Quant Feature Roadmap)

If you need to spin up a research-oriented agent session to propose new feature families and an evaluation plan across orderbook/trades/ticker, use:

- `docs/RESEARCH_PROMPT_ORDERBOOK_QUANT.md` - Focused on orderbook microstructure
- `docs/FRONTIER_QUANT_RESEARCH_PROMPT.md` - Broader frontier approaches for crypto markets

---

## Feature Set Summary

### Static Features (per snapshot)
- Best bid/ask (L0), multi-level depth (L1 to L{max_levels})
- Mid price, spread, relative spread (bps), microprice
- Imbalance at each level
- Cumulative depth at price bands (0-5bps, 5-10bps, 10-25bps, 25-50bps)

### Dynamic Features (delta-based)
- Order Flow Imbalance (OFI) - Cont's method (L1 + multi-level)
- Log returns, velocity, acceleration
- Trade Flow Imbalance (TFI)

### Rolling Statistics (Welford's algorithm)
- Realized volatility over horizons [1s, 5s, 30s, 60s]
- Rolling OFI sums, rolling TFI
- Spread regime fraction (tight/wide)
- Depth variance

### Bar Aggregates
- OHLCV + microstructure stats at [1s, 5s, 30s, 60s]
- Mean/min/max spread, sum OFI, mean imbalance, realized variance

---

## Production Setup (Raspberry Pi 4 Deployment)

```bash
# Terminal 1: WebSocket Collection (24/7) - writes to Bronze layer
python scripts/run_ingestion.py --sources ccxt

# Terminal 2: S3 Sync (periodic uploads to cloud storage)
python scripts/run_sync.py --mode continuous --interval 300

# Manual ETL (typically run in notebook or ad-hoc)
# Process hour-by-hour with state persistence for correct OFI/MLOFI
python scripts/etl/run_orderbook_features.py \
    --exchange coinbaseadvanced --symbol BTC-USD \
    --year 2025 --month 6 --day 19 --hour 14 \
    --state-path state/btc-usd_orderbook.json \
    --trades data/raw/ready/ccxt/trades
```

**Typical Workflow**:
1. Pi runs ingestion 24/7 → raw data lands in `raw/ready/`
2. `run_sync.py` uploads to S3 periodically for durability
3. ETL runs manually (notebook or script) when compute is available
4. State files track processor state for checkpoint/resume

---

## Useful Commands

```bash
# Start ingestion (writes raw Parquet to Bronze layer)
python scripts/run_ingestion.py --sources ccxt

# Run S3 sync (uploads local data to S3)
python scripts/run_sync.py --mode once --dry-run    # Preview
python scripts/run_sync.py --mode once              # Execute

# Batch orderbook ETL with state persistence
python scripts/etl/run_orderbook_features.py \
    --exchange coinbaseadvanced --symbol BTC-USD \
    --year 2025 --month 6 --day 19 --hour 14 \
    --state-path state/btc-usd_orderbook.json

# Verify configuration flow works
python scripts/test_config_flow.py
```

---

## Working with Raw Data (Bronze Layer)

The raw Parquet files in `raw/ready/{source}/{channel}/` preserve all exchange data for:

1. **Feature iteration** - Add new features without re-collecting data
2. **Model retraining** - Replay historical data through updated models
3. **Debugging** - Inspect exact exchange responses
4. **Backtesting** - Build realistic backtests from raw tick data

**Reading raw orderbook data**:
```python
import polars as pl

# Lazy scan for memory efficiency
lf = pl.scan_parquet("raw/ready/ccxt/orderbook/*.parquet")

# Filter and collect
df = lf.filter(
    (pl.col("exchange") == "binanceus") &
    (pl.col("symbol") == "BTC/USDT")
).collect()

# Access nested bids/asks
for row in df.iter_rows(named=True):
    bids = row["bids"]  # List of {price, size} dicts
    asks = row["asks"]
```

---

## Documentation Reference

For deeper understanding, read these docs in `docs/`:

| Document | Purpose | When to Read |
|----------|---------|--------------|
| `PARQUET_ETL_FEATURES.md` | **Feature engineering reference** | Understanding 100+ features |
| `SYSTEM_ONBOARDING.md` | **Complete 30-page reference** | Deep dive into any topic |
| `INDEX.md` | Navigation hub + quick reference | Bookmark for lookups |
| `PROCESSOR_OPTIONS.md` | Feature engineering config | Customizing features |
| `UNIFIED_STORAGE_ARCHITECTURE.md` | Storage design | Understanding storage |
| `HYBRID_STORAGE_GUIDE.md` | Local + S3 patterns | Setting up cloud backup |
| `CCXT_ETL_ARCHITECTURE.md` | CCXT pipeline design | Understanding ETL flow |

---

## Key Concepts to Know

### VPIN (Volume-Synchronized Probability of Informed Trading)
**Paper**: Easley, López de Prado, O'Hara (2012)  
**Purpose**: Measure order flow toxicity - predicts volatility spikes and flash crashes  
**Location**: `etl/features/streaming.py` → `VPINCalculator`  
**Config**: `enable_vpin=True` (default), `vpin_bucket_volume`, `vpin_window_buckets`

### Order Flow Imbalance (OFI)
**Paper**: Cont, Stoikov, Talreja (2010)  
**Purpose**: Measure buying/selling pressure from orderbook changes  
**Location**: `etl/features/stateful.py` → `StatefulFeatureProcessor`

### Kyle's Lambda
**Paper**: Kyle (1985)  
**Purpose**: Price impact coefficient - regression of returns on order flow  
**Location**: `etl/features/streaming.py` → `KyleLambdaEstimator`

### Microprice
**Formula**: `(bid_size × ask + ask_size × bid) / (bid_size + ask_size)`  
**Purpose**: Volume-weighted fair value (more accurate than mid)  
**Location**: `etl/features/orderbook.py` → `extract_structural_features()`

### Welford's Algorithm
**Purpose**: Online variance computation (numerically stable)  
**Location**: `etl/features/streaming.py` → `RollingWelford`

### Directory-Aligned Partitioning
**Purpose**: Unified partitioning across ingestion and ETL  
**Key**: Partition column values exist IN data AND match directory path  
**Location**: `shared/partitioning.py` → `sanitize_partition_value()`, `build_partition_path()`

### Medallion Architecture
**Bronze**: Raw data landing (Parquet, preserves all fields)  
**Silver**: Cleaned, normalized data  
**Gold**: Feature-engineered, ML-ready datasets  
**Location**: `ingestion/writers/parquet_writer.py` (Bronze), `etl/` (Silver/Gold)

---

## Debugging Checklist

| Issue | Solution |
|-------|----------|
| Features missing? | Run `python scripts/test_config_flow.py` |
| S3 sync skipping files? | Check path normalization (`/` vs `\` - fixed in sync.py) |
| High memory? | Reduce `max_levels` or `segment_max_mb` |
| Stale OFI values? | Check `max_state_staleness_seconds` (300s default) |
| Config not applied? | Enable DEBUG logging in config.yaml |
| Raw data format? | Check `config.ingestion.raw_format` |
| Partition mismatch? | Use `shared/partitioning.py` utilities |

---

## Understanding Checklist

You're ready to work on this codebase when you can:

- [ ] Explain the Medallion architecture (Bronze → Silver → Gold)
- [ ] Understand why we preserve raw data in Parquet format
- [ ] Explain Directory-Aligned Partitioning vs Hive partitioning
- [ ] Describe how OFI is calculated (Cont's method)
- [ ] Know how staleness detection prevents bad OFI after gaps
- [ ] Navigate to the right file for any component
- [ ] Explain the difference between structural (vectorized) vs stateful (sequential) features
- [ ] Know where to add a new rolling statistic (`etl/features/streaming.py`)
- [ ] Know where to add a new static feature (`etl/features/orderbook.py`)

---

## Next Steps

After familiarizing yourself with the codebase:

1. **Explore** - Read the files in the Critical Code Map section
2. **Run** - Execute `python scripts/test_config_flow.py` to verify setup
3. **Ask** - Tell me what you need to work on and I'll provide context

**I'm ready to help with whatever task comes next.**
