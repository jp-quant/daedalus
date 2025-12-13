# COPILOT AGENT ONBOARDING PROMPT

**Copy and paste this entire prompt into a new Copilot agent session to get it up to speed.**

---

## Your Mission

You are joining the **FluxForge** project - a production-grade market data infrastructure for **mid-frequency quantitative cryptocurrency trading** (5-second to 4-hour holding periods). Your role is to help implement advanced orderbook features and optimize the system based on quantitative research.

## Critical Context

### What We're Building
A complete pipeline: WebSocket collection → Feature engineering → ML-ready datasets

**Key Insight**: We cannot compete with HFT. Our edge is **information processing**, not speed.
- Target: Mid-frequency signals (5s to 4h)
- Focus: Advanced microstructure features (60+ from orderbooks)
- Edge: Statistical patterns, regime detection, multi-timeframe analysis

### System Architecture (3 Layers)

```
Layer 1: WebSocket Collectors (ingestion/)
  → Pure async I/O, CCXT Pro (100+ exchanges)
  → Push to bounded queue

Layer 2: Batched Log Writer (ingestion/writers/)
  → Size-based rotation (100MB NDJSON segments)
  → active/ → ready/ atomic moves
  
Layer 3: ETL + Feature Engineering (etl/)
  → Processes ready/ segments
  → 60+ microstructure features
  → Hive-partitioned Parquet output
```

### Live Operation (4 concurrent terminals)

1. **Terminal 1**: `python scripts/run_ingestion.py --sources ccxt` (24/7 WebSocket collection)
2. **Terminal 2**: `python scripts/run_etl_watcher.py --poll-interval 30` (Real-time feature engineering)
3. **Terminal 3**: `python storage/sync.py upload ...` (Periodic cloud backup)
4. **Terminal 4**: `python scripts/run_compaction.py ...` (Daily file merge)

### Current Feature Set

**Static/Structural** (from `etl/features/snapshot.py`):
- Best bid/ask, multi-level depth (L0 to L{max_levels})
- Mid price, spread, relative spread (bps)
- Microprice (volume-weighted fair value)
- Imbalance at each level
- Cumulative depth at price bands (0-5bps, 5-10bps, 10-25bps, 25-50bps)

**Dynamic/Flow** (from `etl/features/state.py`):
- Order Flow Imbalance (OFI) - Cont's method
- Multi-level OFI
- Log returns, velocity, acceleration
- Trade Flow Imbalance (TFI)

**Rolling Statistics**:
- Realized volatility (Welford's algorithm) - [1s, 5s, 30s, 60s]
- Rolling OFI sums
- Rolling TFI
- Spread regime fraction (tight/wide)
- Depth variance

**Bar Aggregates**:
- OHLCV + microstructure stats - [1s, 5s, 30s, 60s]
- Mean/min/max spread
- Sum OFI, mean imbalance
- Realized variance within bar

### Configuration (StateConfig)

**Location**: `etl/features/state.py` → `StateConfig` dataclass

**Configurable in** `config/config.yaml` → `etl.channels.orderbook.processor_options`:

```python
horizons: [1, 5, 30, 60]          # Rolling window sizes (seconds)
bar_durations: [1, 5, 30, 60]     # Bar aggregation intervals
hf_emit_interval: 1.0             # HF snapshot rate
max_levels: 10                    # Orderbook depth
ofi_levels: 5                     # Levels for multi-level OFI
bands_bps: [5, 10, 25, 50]        # Liquidity bands (basis points)
keep_raw_arrays: false
tight_spread_threshold: 0.0001    # 1 bp
```

**Config Flow**:
```
config.yaml 
  → ETLConfig.channels["orderbook"].processor_options
  → ETLJob(channel_config)
  → CcxtSegmentPipeline
  → CcxtAdvancedOrderbookProcessor(**processor_options)
  → StateConfig
  → SymbolState (feature engineering)
```

## Critical Files to Understand

### Feature Engineering (MOST IMPORTANT)
- `etl/features/state.py` - **StateConfig** + **SymbolState** (stateful per-symbol processing)
- `etl/features/snapshot.py` - Static feature extraction (60+ features)
- `etl/features/streaming.py` - Rolling statistics (Welford, RegimeStats)

### Processors
- `etl/processors/ccxt/advanced_orderbook_processor.py` - Main orderbook processor
- `etl/orchestrators/ccxt_segment_pipeline.py` - Multi-channel routing

### Storage & Sync
- `storage/base.py` - Unified storage abstraction (LocalStorage, S3Storage)
- `storage/sync.py` - Bidirectional sync (local ↔ S3)

### Configuration
- `config/config.py` - Pydantic models
- `config/config.yaml` - Runtime configuration (NOT committed, has API keys)

### Scripts
- `scripts/run_ingestion.py` - WebSocket collection
- `scripts/run_etl_watcher.py` - Continuous ETL
- `scripts/run_compaction.py` - File compaction
- `scripts/test_config_flow.py` - Verify processor_options flow

## Recent Enhancements (Dec 2025)

1. **Processor Options Configuration**
   - Added `processor_options` to channel config
   - Updated pipeline to pass options to processors
   - Test script: `scripts/test_config_flow.py`
   - Documented: `docs/PROCESSOR_OPTIONS.md`

2. **S3 Connection Pooling**
   - Added `max_pool_connections` to S3Config (default: 50)
   - Prevents boto3 bottleneck with ThreadPoolExecutor

3. **Partition Fix Utility**
   - Created `scripts/fix_partition_mismatch.py`
   - Fixes symbol format issues (BTC/USD → BTC-USD)
   - Works with local + S3

4. **Research Prompt**
   - Created `docs/RESEARCH_PROMPT_ORDERBOOK_QUANT.md`
   - Awaiting research results on optimal configs and new features

## Documentation to Read

**CRITICAL (Read First)**:
1. `docs/SYSTEM_ONBOARDING.md` - **COMPLETE SYSTEM REFERENCE** (this is your bible)
2. `docs/PROCESSOR_OPTIONS.md` - Feature engineering configuration guide
3. `docs/RESEARCH_PROMPT_ORDERBOOK_QUANT.md` - Research questions awaiting answers

**Secondary**:
4. `docs/UNIFIED_STORAGE_ARCHITECTURE.md` - Storage design
5. `docs/HYBRID_STORAGE_GUIDE.md` - Local + S3 patterns
6. `docs/CCXT_ETL_ARCHITECTURE.md` - CCXT-specific design

## Your Immediate Tasks

### Phase 1: Deep Dive (First 30 minutes)

1. **Read the complete onboarding doc**:
   ```
   Read: docs/SYSTEM_ONBOARDING.md (comprehensive system reference)
   ```

2. **Understand feature engineering**:
   ```
   Read: etl/features/state.py (StateConfig + SymbolState)
   Read: etl/features/snapshot.py (static features)
   Read: etl/features/streaming.py (rolling algorithms)
   ```

3. **Trace configuration flow**:
   ```
   Read: config/config.yaml → etl.channels.orderbook.processor_options
   Read: etl/orchestrators/ccxt_segment_pipeline.py → _create_pipelines()
   Read: etl/processors/ccxt/advanced_orderbook_processor.py → __init__()
   ```

4. **Review recent changes**:
   ```
   Read: docs/PROCESSOR_OPTIONS.md
   Read: scripts/test_config_flow.py
   Read: scripts/fix_partition_mismatch.py
   ```

### Phase 2: Verify Understanding

Run the test script to confirm config flow works:
```bash
python scripts/test_config_flow.py
```

Expected output:
```
✓ max_levels: 20
✓ ofi_levels: 5
✓ hf_emit_interval: 1.0
✓ bar_durations: [5, 15, 60]
...
Configuration flow test PASSED ✓
```

### Phase 3: Ready for Research Results

Once you understand the system, you'll receive research results from a deep research agent on:
1. Optimal StateConfig defaults (horizons, bar_durations, max_levels, etc.)
2. New features to implement (prioritized by effort vs alpha)
3. Model architectures for orderbook data
4. Preprocessing recommendations

Your job will be to:
1. Update StateConfig defaults based on research
2. Implement new features (book slope, Kyle's lambda, VPIN, etc.)
3. Add new rolling statistics
4. Optimize performance

## Key Concepts to Master

### 1. Order Flow Imbalance (OFI)
**Paper**: Cont, Stoikov, Talreja (2010)
**Purpose**: Measure buying/selling pressure from orderbook changes
**Implementation**: `etl/features/state.py` → L1 and multi-level versions

### 2. Microprice
**Formula**: `(bid_size * ask + ask_size * bid) / (bid_size + ask_size)`
**Purpose**: More accurate than mid price, accounts for imbalance
**Implementation**: `etl/features/snapshot.py`

### 3. Welford's Algorithm
**Purpose**: Online variance computation (numerically stable)
**Implementation**: `etl/features/streaming.py` → `RollingWelford`

### 4. Hive Partitioning
**Format**: `exchange=X/symbol=Y/date=Z/`
**Purpose**: Predicate pushdown in distributed queries
**Implementation**: `etl/writers/parquet_writer.py`

## Debugging Checklist

If something doesn't work:

1. **Features missing?** → Run `python scripts/test_config_flow.py`
2. **S3 slow?** → Check `max_pool_connections` in config.yaml
3. **High memory?** → Reduce `max_levels` or `segment_max_mb`
4. **Partition issues?** → Run `python scripts/fix_partition_mismatch.py`
5. **Config not applied?** → Enable DEBUG logging in config.yaml

## Success Criteria

You're ready when you can:

1. ✅ Explain the 3-layer architecture
2. ✅ Trace a config value from YAML → StateConfig → features
3. ✅ Describe how OFI is calculated (Cont's method)
4. ✅ Navigate the codebase confidently
5. ✅ Understand the difference between HF features vs bars
6. ✅ Know where to add a new rolling statistic
7. ✅ Know where to add a new static feature

## Final Note

**This project is about building a competitive edge through better information processing, not faster execution.** Every feature we engineer should:
- Have theoretical justification (academic papers)
- Be computable in real-time (< 10ms per snapshot)
- Have predictive power for mid-frequency signals
- Be configurable (StateConfig parameters)

**Now read `docs/SYSTEM_ONBOARDING.md` thoroughly and explore the codebase. You'll be implementing cutting-edge orderbook research soon!**

---

## Quick Reference Commands

```bash
# Verify config flow
python scripts/test_config_flow.py

# Check system health
python scripts/check_health.py

# Query features
python scripts/query_parquet.py data/processed/ccxt/orderbook/hf

# Live ingestion
python scripts/run_ingestion.py --sources ccxt

# Continuous ETL
python scripts/run_etl_watcher.py --poll-interval 30

# Sync to S3
python storage/sync.py upload --source-path processed/ --dest-path s3://bucket/processed/

# Compact files
python scripts/run_compaction.py --source ccxt --partition exchange=X/symbol=Y
```

**Good luck! Start with `docs/SYSTEM_ONBOARDING.md` and work your way through the codebase.**
