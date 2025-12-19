# COPILOT AGENT ONBOARDING PROMPT

**Copy and paste this entire prompt into a new Copilot agent session to get it up to speed on the Daedalus codebase.**

---

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
| Exchanges | Binance US, Coinbase Advanced, OKX, 100+ via CCXT |
| Holding Period | 5 seconds to 4 hours |
| Infrastructure | Retail/prosumer (Raspberry Pi to cloud) |
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
│ ├─ 5-10x smaller than NDJSON with ZSTD compression                │
│ ├─ Channel-separated files (ticker/, trades/, orderbook/)          │
│ ├─ Size-based segment rotation (default: 50 MB)                   │
│ ├─ active/ → ready/ atomic moves                                   │
│ └─ Schema-aware: typed columns, nested structs for orderbook      │
└─────────────────────────────────────────────────────────────────────┘
              ↓ (file system / S3)
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 3: ETL Workers (etl/) - SILVER/GOLD LAYERS                   │
│ ├─ Reads ready/ segments (Parquet or NDJSON based on config)      │
│ ├─ Atomic moves to processing/ (prevents double-processing)        │
│ ├─ Feature engineering: 60+ microstructure features                │
│ ├─ Multi-output: High-frequency snapshots + bar aggregates         │
│ └─ Hive-partitioned Parquet output                                 │
└─────────────────────────────────────────────────────────────────────┘
```

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
Bronze Layer: Raw Parquet Segments (50MB chunks)
    ├─ raw/active/ccxt/ticker/segment_*.parquet     (writing)
    ├─ raw/active/ccxt/trades/segment_*.parquet     (writing)
    ├─ raw/active/ccxt/orderbook/segment_*.parquet  (writing)
    └─ raw/ready/ccxt/{channel}/segment_*.parquet   (ready for ETL)
    ↓ (ETL Job picks up)
Silver/Gold Layer: Feature-Engineered Parquet
    ├─ processed/ccxt/orderbook/hf/exchange=X/symbol=Y/date=Z/*.parquet
    └─ processed/ccxt/orderbook/bars/exchange=X/symbol=Y/date=Z/*.parquet
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

### Ingestion Layer (BRONZE)

| File | Purpose |
|------|---------|
| `ingestion/writers/parquet_writer.py` | **StreamingParquetWriter** - Raw Parquet landing |
| `ingestion/writers/log_writer.py` | Legacy NDJSON writer (still supported) |
| `ingestion/collectors/ccxt_collector.py` | CCXT Pro unified WebSocket (100+ exchanges) |
| `ingestion/collectors/coinbase_ws.py` | Native Coinbase Advanced Trade WebSocket |
| `ingestion/orchestrators/ingestion_pipeline.py` | Coordinates collectors + writer |

**Config**: `config.ingestion.raw_format = "parquet"` (recommended) or `"ndjson"` (legacy)

### Feature Engineering (SILVER/GOLD)

| File | Purpose |
|------|---------|
| `etl/features/orderbook.py` | Vectorized structural + rolling orderbook features (Polars) |
| `etl/features/stateful.py` | Stateful (sequential) orderbook features: OFI/MLOFI/TFI/Kyle/VPIN |
| `etl/features/streaming.py` | Online stats primitives (RollingWelford, RollingSum, VPINCalculator, KyleLambdaEstimator) |
| `etl/transforms/` | Channel transforms (ticker/trades/orderbook/bars) used by scripts |
| `scripts/etl/run_orderbook_features.py` | Batch runner for orderbook features (supports `--trades` to drive TFI correctly) |
| `scripts/etl/run_trades_features.py` | Batch runner for trades features (+ optional bars output) |
| `scripts/etl/run_ticker_features.py` | Batch runner for ticker features (+ optional rolling stats) |
| `scripts/etl/run_watcher.py` | Continuous watcher for ready/ segments with checkpointing |
| `etl/utils/state_manager.py` | State persistence for checkpoint/resume and graceful shutdown |
| `etl/features/archived/` | Legacy files (snapshot.py, state.py) - deprecated, kept for reference |

**Start here**: Read `docs/PARQUET_ETL_FEATURES.md` for feature reference and `scripts/etl/run_orderbook_features.py` for the runnable batch path.

**Key Components for orderbook features**:
- `extract_structural_features()` in `etl/features/orderbook.py`
- `compute_rolling_features()` in `etl/features/orderbook.py`
- `StatefulFeatureProcessor` in `etl/features/stateful.py`

**Critical Features Enabled by Default**:
- VPIN (Volume-Synchronized Probability of Informed Trading) - flow toxicity, predicts volatility
- Kyle's Lambda - price impact coefficient
- Band-based depth (0-5bps, 5-10bps, etc.) - liquidity at price-distance

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

## Production Setup (4 Concurrent Terminals)

```bash
# Terminal 1: WebSocket Collection (24/7) - writes to Bronze layer
python scripts/run_ingestion.py --sources ccxt

# Terminal 2: Continuous ETL (real-time feature engineering)
python scripts/etl/run_watcher.py --poll-interval 30

# Terminal 3: Cloud Backup (periodic)
python storage/sync.py upload --source-path processed/ --dest-path s3://bucket/processed/

# Terminal 4: File Compaction (daily maintenance)
python scripts/run_compaction.py --source ccxt --partition exchange=X/symbol=Y
```

---

## Useful Commands

```bash
# Start ingestion (writes raw Parquet to Bronze layer)
python scripts/run_ingestion.py --sources ccxt

# Verify configuration flow works
python scripts/test_config_flow.py

# Check system health
python scripts/check_health.py

# Query processed features
python scripts/query_parquet.py data/processed/ccxt/orderbook/hf

# Manual ETL run (single segment)
python scripts/run_etl.py --source ccxt --channel orderbook
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
**Location**: `etl/features/streaming.py` → `VPINCalculator`, `etl/parquet_etl_pipeline.py`  
**Config**: `enable_vpin=True` (default), `vpin_bucket_volume`, `vpin_window_buckets`

### Order Flow Imbalance (OFI)
**Paper**: Cont, Stoikov, Talreja (2010)  
**Purpose**: Measure buying/selling pressure from orderbook changes  
**Location**: `etl/features/state.py`, `etl/parquet_etl_pipeline.py`

### Kyle's Lambda
**Paper**: Kyle (1985)  
**Purpose**: Price impact coefficient - regression of returns on order flow  
**Location**: `etl/features/streaming.py` → `KyleLambdaEstimator`

### Microprice
**Formula**: `(bid_size × ask + ask_size × bid) / (bid_size + ask_size)`  
**Purpose**: Volume-weighted fair value (more accurate than mid)  
**Location**: `etl/features/snapshot.py`

### Welford's Algorithm
**Purpose**: Online variance computation (numerically stable)  
**Location**: `etl/features/streaming.py` → `RollingWelford`

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
| S3 slow? | Check `max_pool_connections` in config.yaml |
| High memory? | Reduce `max_levels` or `segment_max_mb` |
| Partition issues? | Run `python scripts/fix_partition_mismatch.py` |
| Config not applied? | Enable DEBUG logging in config.yaml |
| Raw data format? | Check `config.ingestion.raw_format` |

---

## Understanding Checklist

You're ready to work on this codebase when you can:

- [ ] Explain the Medallion architecture (Bronze → Silver → Gold)
- [ ] Understand why we preserve raw data in Parquet format
- [ ] Trace a config value from YAML → StateConfig → features
- [ ] Describe how OFI is calculated (Cont's method)
- [ ] Navigate to the right file for any component
- [ ] Explain the difference between HF features vs bars
- [ ] Know where to add a new rolling statistic
- [ ] Know where to add a new static feature

---

## Next Steps

After familiarizing yourself with the codebase:

1. **Explore** - Read the files in the Critical Code Map section
2. **Run** - Execute `python scripts/test_config_flow.py` to verify setup
3. **Ask** - Tell me what you need to work on and I'll provide context

**I'm ready to help with whatever task comes next.**
