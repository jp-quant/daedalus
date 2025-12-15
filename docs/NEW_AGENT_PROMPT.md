# COPILOT AGENT ONBOARDING PROMPT

**Copy and paste this entire prompt into a new Copilot agent session to get it up to speed on the FluxForge codebase.**

---

## Project Overview

You are joining **FluxForge** - a production-grade market data infrastructure for **mid-frequency quantitative cryptocurrency trading** (5-second to 4-hour holding periods).

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
| `etl/features/snapshot.py` | 60+ static features from single orderbook snapshot |
| `etl/features/state.py` | `StateConfig` + `SymbolState` (stateful processing, rolling stats) |
| `etl/features/streaming.py` | Online algorithms (Welford variance, RollingSum, RegimeStats) |

**Start Here**: Read `etl/features/state.py` docstring for comprehensive overview.

### Processors

| File | Purpose |
|------|---------|
| `etl/processors/ccxt/advanced_orderbook_processor.py` | Main orderbook feature engine |
| `etl/processors/ccxt/ticker_processor.py` | Ticker data enrichment |
| `etl/processors/ccxt/trades_processor.py` | Trade data enrichment |

### Orchestration

| File | Purpose |
|------|---------|
| `etl/orchestrators/ccxt_segment_pipeline.py` | **CRITICAL**: Multi-channel routing |
| `etl/orchestrators/pipeline.py` | Base ETL pipeline (Reader → Processor → Writer) |
| `etl/orchestrators/multi_output_pipeline.py` | Splits output (HF + bars) |
| `etl/job.py` | ETL job runner - processes segments |

### Configuration

| File | Purpose |
|------|---------|
| `config/config.py` | Pydantic configuration models |
| `config/config.yaml` | Runtime configuration (NOT in git - contains API keys) |
| `config/config.examples.yaml` | Template for config.yaml |

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
        max_levels: 10           # Orderbook depth levels
        ofi_levels: 5            # Levels for multi-level OFI
        horizons: [1, 5, 30, 60] # Rolling window sizes (seconds)
        bar_durations: [1, 5, 30, 60]  # Bar aggregation intervals
        hf_emit_interval: 1.0    # HF snapshot rate (seconds)
        bands_bps: [5, 10, 25, 50]     # Liquidity bands (basis points)
```

### Configuration Flow

```
config.yaml
    ↓
IngestionConfig.raw_format → StreamingParquetWriter or LogWriter
    ↓
etl.channels.orderbook.processor_options
    ↓
ETLJob → CcxtSegmentPipeline → CcxtAdvancedOrderbookProcessor
    ↓
StateConfig (dataclass) → SymbolState (per-symbol processing)
```

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
python scripts/run_etl_watcher.py --poll-interval 30

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
| `SYSTEM_ONBOARDING.md` | **Complete 30-page reference** | Deep dive into any topic |
| `INDEX.md` | Navigation hub + quick reference | Bookmark for lookups |
| `PROCESSOR_OPTIONS.md` | Feature engineering config | Customizing features |
| `UNIFIED_STORAGE_ARCHITECTURE.md` | Storage design | Understanding storage |
| `HYBRID_STORAGE_GUIDE.md` | Local + S3 patterns | Setting up cloud backup |
| `CCXT_ETL_ARCHITECTURE.md` | CCXT pipeline design | Understanding ETL flow |

---

## Key Concepts to Know

### Order Flow Imbalance (OFI)
**Paper**: Cont, Stoikov, Talreja (2010)  
**Purpose**: Measure buying/selling pressure from orderbook changes  
**Location**: `etl/features/state.py`

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
