# Documentation Index & Quick Reference

**Last Updated**: December 15, 2025

This document provides a map of all documentation and quick access to key information.

---

## 🚀 Getting Started

### For New Developers

**START HERE**: [`NEW_AGENT_PROMPT.md`](NEW_AGENT_PROMPT.md)
- Copy-paste this into a fresh Copilot agent session
- 10-minute read, immediate productivity
- Points to all critical files and concepts

### For Deep Understanding

**COMPLETE REFERENCE**: [`SYSTEM_ONBOARDING.md`](SYSTEM_ONBOARDING.md)
- 30-page comprehensive system documentation
- Medallion architecture, algorithms, workflows, debugging
- Everything you need to master FluxForge

---

## 📚 Documentation by Topic

### Feature Engineering

| Document | Purpose | Read When |
|----------|---------|-----------|
| [`PROCESSOR_OPTIONS.md`](PROCESSOR_OPTIONS.md) | Configure StateConfig parameters | Customizing features |
| [`RESEARCH_PROMPT_ORDERBOOK_QUANT.md`](RESEARCH_PROMPT_ORDERBOOK_QUANT.md) | Deep research on optimal configs | Before major changes |

### Storage & Infrastructure

| Document | Purpose | Read When |
|----------|---------|-----------|
| [`UNIFIED_STORAGE_ARCHITECTURE.md`](UNIFIED_STORAGE_ARCHITECTURE.md) | Storage backend abstraction | Understanding storage layer |
| [`HYBRID_STORAGE_GUIDE.md`](HYBRID_STORAGE_GUIDE.md) | Local + S3 patterns | Setting up cloud backup |
| [`PARQUET_OPERATIONS.md`](PARQUET_OPERATIONS.md) | Parquet best practices | Optimizing storage |

### ETL & Data Processing

| Document | Purpose | Read When |
|----------|---------|-----------|
| [`CCXT_ETL_ARCHITECTURE.md`](CCXT_ETL_ARCHITECTURE.md) | CCXT pipeline design | Understanding ETL flow |
| [`FILENAME_PATTERNS.md`](FILENAME_PATTERNS.md) | Segment naming conventions | Debugging file issues |

### Trading Strategy

| Document | Purpose | Read When |
|----------|---------|-----------|
| [`US_CRYPTO_STRATEGY.md`](US_CRYPTO_STRATEGY.md) | US regulatory considerations | Planning US-based trading |

---

## 🔑 Critical Code Locations

### Feature Engineering (Most Important)

```
etl/features/
├── snapshot.py          # 60+ static features from single snapshot
├── state.py             # StateConfig + SymbolState (dynamic features, rolling stats)
└── streaming.py         # Online algorithms (Welford, RollingSum, RegimeStats)
```

**Start here**: Read `state.py` docstring (comprehensive overview)

### Processors

```
etl/processors/ccxt/
├── advanced_orderbook_processor.py  # Main orderbook feature engine
├── ticker_processor.py              # Ticker data enrichment
└── trades_processor.py              # Trade data enrichment
```

**Key**: `advanced_orderbook_processor.py` orchestrates feature extraction

### Orchestration

```
etl/orchestrators/
├── ccxt_segment_pipeline.py     # Multi-channel routing (CRITICAL)
├── pipeline.py                  # Base ETL pipeline
└── multi_output_pipeline.py     # Splits output (HF + bars)
```

**Key**: `ccxt_segment_pipeline.py` routes data to appropriate processors

### Storage

```
storage/
├── base.py      # StorageBackend, LocalStorage, S3Storage (CRITICAL)
├── factory.py   # Storage backend factory
└── sync.py      # Bidirectional sync (local ↔ S3)
```

**Key**: `base.py` provides unified interface for all storage operations

### Configuration

```
config/
├── config.py            # Pydantic models (CRITICAL)
├── config.yaml          # Runtime config (NOT in git)
└── config.examples.yaml # Template with examples
```

**Key**: `config.py` defines all configuration structures

---

## 📊 Configuration Reference

### StateConfig Parameters

**Location**: `etl/features/state.py` → `StateConfig` dataclass

**Configured in**: `config/config.yaml` → `etl.channels.orderbook.processor_options`

| Parameter | Default | Purpose | Typical Range |
|-----------|---------|---------|---------------|
| `horizons` | `[1, 5, 30, 60]` | Rolling window sizes (seconds) | 1-3600 |
| `bar_durations` | `[1, 5, 30, 60]` | Bar aggregation intervals (seconds) | 1-3600 |
| `hf_emit_interval` | `1.0` | HF snapshot emission rate (seconds) | 0.1-5.0 |
| `max_levels` | `10` | Orderbook depth to process | 5-50 |
| `ofi_levels` | `5` | Levels for multi-level OFI | 1-20 |
| `bands_bps` | `[5, 10, 25, 50]` | Liquidity bands (basis points) | 1-1000 |
| `keep_raw_arrays` | `false` | Include raw bid/ask arrays | true/false |
| `tight_spread_threshold` | `0.0001` | Spread regime threshold (1 bp) | 0.00001-0.001 |

**Documentation**: See [`PROCESSOR_OPTIONS.md`](PROCESSOR_OPTIONS.md) for complete guide

---

## 🎯 Quick Command Reference

### Live Operation

```bash
# Terminal 1: Ingestion (24/7)
python scripts/run_ingestion.py --sources ccxt

# Terminal 2: ETL + Feature Engineering (24/7)
python scripts/run_etl_watcher.py --poll-interval 30

# Terminal 3: Cloud Sync (periodic)
python storage/sync.py upload \
  --source-path processed/ccxt/ \
  --dest-path s3://my-bucket/processed/ccxt/

# Terminal 4: File Compaction (daily)
python scripts/run_compaction.py \
  --source ccxt \
  --partition exchange=binanceus/symbol=BTC-USDT
```

### Verification & Testing

```bash
# Test configuration flow
python scripts/test_config_flow.py

# Check system health
python scripts/check_health.py

# Query processed features
python scripts/query_parquet.py data/processed/ccxt/orderbook/hf

# Validate partitions
python scripts/validate_partitioning.py
```

### Maintenance

```bash
# Fix partition format mismatches
python scripts/fix_partition_mismatch.py \
  --storage local \
  --partition exchange=X/symbol=Y

# Manual ETL (specific date)
python scripts/run_etl.py \
  --source ccxt \
  --mode date \
  --date 2025-12-12

# Manual ETL (date range)
python scripts/run_etl.py \
  --source ccxt \
  --mode range \
  --start-date 2025-12-01 \
  --end-date 2025-12-12
```

---

## 🧠 Key Algorithms & Concepts

### Order Flow Imbalance (OFI)

**Paper**: Cont, Stoikov, Talreja (2010) - "Price Impact of Order Book Events"

**Implementation**: `etl/features/state.py` → `SymbolState.process_snapshot()`

**Formula** (L1):
```
ofi_bid = {
    bid_size           if best_bid > prev_best_bid  (price moved up)
    bid_size - prev    if best_bid = prev_best_bid  (size changed)
    -prev_bid_size     if best_bid < prev_best_bid  (price moved down)
}

ofi_ask = {
    -prev_ask_size     if best_ask > prev_best_ask  (price moved up)
    ask_size - prev    if best_ask = prev_best_ask  (size changed)
    ask_size           if best_ask < prev_best_ask  (price moved down)
}

ofi = ofi_bid - ofi_ask
```

**Interpretation**:
- OFI > 0: Buying pressure (bullish)
- OFI < 0: Selling pressure (bearish)

### Microprice

**Implementation**: `etl/features/snapshot.py` → `extract_orderbook_features()`

**Formula**:
```
microprice = (bid_size * ask_price + ask_size * bid_price) / (bid_size + ask_size)
```

**Purpose**: Volume-weighted fair value, more accurate than mid price

### Welford's Online Variance

**Implementation**: `etl/features/streaming.py` → `RollingWelford`

**Purpose**: Compute variance in single pass with numerical stability

**Algorithm**:
```python
for each new value x:
    delta = x - mean
    mean += delta / count
    M2 += delta * (x - mean)
    variance = M2 / count
```

**Why**: Avoids catastrophic cancellation in naive two-pass methods

### Hive Partitioning

**Implementation**: `etl/writers/parquet_writer.py`

**Format**: `key1=value1/key2=value2/.../filename.parquet`

**Example**: `exchange=binanceus/symbol=BTC-USDT/date=2025-12-12/part_0001.parquet`

**Benefits**:
- Predicate pushdown (query engines skip irrelevant partitions)
- Data locality (related data grouped)
- Parallel processing (each partition independent)

---

## 🚨 Common Issues & Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| "Too many open files" | OS limit on file descriptors | `ulimit -n 4096` |
| S3 slow upload/download | Low connection pool | Increase `max_pool_connections` in config |
| High memory usage | Large segments or too many levels | Reduce `segment_max_mb` or `max_levels` |
| Missing features | Config not applied | Run `python scripts/test_config_flow.py` |
| Partition format mismatch | Symbol contains `/` | Run `python scripts/fix_partition_mismatch.py` |

**Detailed troubleshooting**: See [`SYSTEM_ONBOARDING.md`](SYSTEM_ONBOARDING.md) § Debugging

---

## 📈 Feature Categories

### Static (from single snapshot)
- Best bid/ask, multi-level depth
- Mid price, spread, relative spread
- Microprice
- Imbalance at each level
- Cumulative depth at price bands
- Total volumes, book depth

### Dynamic (requires state)
- Log returns
- OFI (L1 and multi-level)
- Mid price velocity
- Mid price acceleration
- Microprice deviation

### Rolling Statistics
- Realized volatility (per horizon)
- Cumulative OFI (per horizon)
- Trade Flow Imbalance (per horizon)
- Trade volume (per horizon)
- Depth variance (30s)
- Spread regime fraction (30s)

### Time Features
- Hour of day
- Day of week
- Is weekend
- Minute of hour

### Bar Aggregates
- OHLC (mid price)
- Mean/min/max spread
- Mean relative spread
- Mean L1 imbalance
- Sum OFI
- Realized variance
- Count (snapshots)

---

## 🔄 Data Flow Summary

```
Exchange WebSocket (CCXT Pro)
    ↓
Raw Market Data (tick-by-tick)
    ↓ (Collector adds timestamp)
asyncio.Queue (bounded, backpressure)
    ↓ (LogWriter batches)
NDJSON Segments (100MB chunks)
    ├─ active/segment_*.ndjson   (writing)
    └─ ready/segment_*.ndjson    (ready for ETL)
    ↓ (ETL Job atomic move)
processing/segment_*.ndjson
    ↓ (Pipeline: Reader → Processor → Writer)
Partitioned Parquet Files
    ├─ hf/exchange=X/symbol=Y/date=Z/*.parquet      (1s features)
    └─ bars/exchange=X/symbol=Y/date=Z/*.parquet    (aggregates)
    ↓ (Optional: StorageSync)
S3 Bucket (cloud backup)
```

---

## 🎓 Learning Path

### Phase 1: Understand System (1-2 hours)
1. Read [`NEW_AGENT_PROMPT.md`](NEW_AGENT_PROMPT.md)
2. Skim [`SYSTEM_ONBOARDING.md`](SYSTEM_ONBOARDING.md)
3. Run `python scripts/test_config_flow.py`

### Phase 2: Explore Codebase (2-4 hours)
1. Read `etl/features/state.py` (full file, understand StateConfig + SymbolState)
2. Read `etl/features/snapshot.py` (static feature extraction)
3. Read `etl/orchestrators/ccxt_segment_pipeline.py` (routing logic)
4. Read `storage/base.py` (storage abstraction)

### Phase 3: Configure & Test (1-2 hours)
1. Edit `config/config.yaml` → processor_options
2. Run ingestion + ETL locally
3. Query output with `query_parquet.py`
4. Verify features are as expected

### Phase 4: Ready for Research Results
- Understand optimal configurations
- Implement new features
- Optimize performance
- Build ML models

**Total**: 4-8 hours to full mastery

---

## 🔗 External Resources

### Academic Papers
- Cont, Stoikov, Talreja (2010) - "Price Impact of Order Book Events"
- López de Prado (2018) - "Advances in Financial Machine Learning"
- Zhang et al. (2019) - "Deep Learning for Limit Order Books"

### Tools & Libraries
- CCXT Pro: https://ccxt.pro
- PyArrow: https://arrow.apache.org/docs/python/
- Polars: https://pola.rs
- boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

---

**Last Review**: December 12, 2025  
**Maintainer**: FluxForge Team  
**Status**: Production-ready, actively maintained
