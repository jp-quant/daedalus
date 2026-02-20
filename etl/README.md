# Daedalus ETL: Agent-Ready Engineering Framework

This directory contains the production ETL framework that transforms raw market data into feature-rich datasets:

- **Bronze → Silver**: Feature extraction (`scripts/etl/run_*_features.py`)
- **Silver → Gold**: Bar aggregation (`scripts/etl/run_bars.py`)
- **Batch/Continuous**: High-throughput batch and watcher modes

This README is the operational entrypoint for ETL contributors and Copilot agents.

---

## Start Here (New Agent Session)

1. Read [ETL_AGENT_PROMPT.md](ETL_AGENT_PROMPT.md)
2. Read [docs/ETL_CONTEXT.md](docs/ETL_CONTEXT.md)
3. Read [docs/IMPLEMENTATION_APPENDIX.md](docs/IMPLEMENTATION_APPENDIX.md)
4. Use [docs/INDEX.md](docs/INDEX.md) for component navigation

---

## ETL Mission

Build a robust, reproducible, and extensible ETL pipeline that:

- preserves **data correctness** under stateful microstructure logic,
- maintains **directory-aligned partitioning** contracts,
- supports **fast iterative feature development** for quant research and production,
- scales from Pi4 collection workflows to cloud/offline compute.

---

## Core Execution Paths

### 1) Bronze → Silver (Features)

- `scripts/etl/run_orderbook_features.py` (stateful orderbook features)
- `scripts/etl/run_orderbook_features_batch.py` (parallel, memory-safe batch execution)
- `scripts/etl/run_trades_features.py` (vectorized trades features)
- `scripts/etl/run_ticker_features.py` (vectorized ticker features)

### 2) Silver → Gold (Bars)

- `scripts/etl/run_bars.py`

### 3) Continuous Processing

- `scripts/etl/run_watcher.py` (continuous ready/ scanner + processing loop)

---

## Framework Layers

- `etl/core/`: executor, config, registry, base interfaces
- `etl/transforms/`: transform orchestration per channel
- `etl/features/`: vectorized + stateful feature logic
- `etl/utils/`: resampling, symbol normalization, state helpers
- `shared/partitioning.py`: unified partition contract with ingestion

---

## Non-Negotiable ETL Invariants

- Partition values must exist **in data and path** and must match.
- Stateful features must process snapshots in **strict chronological order**.
- Trades/orderbook alignment for TFI must preserve interval semantics.
- Quote normalization behavior must be explicit (`normalize_quotes` on/off).
- Resampling must remain ASOF-safe (no lookahead).

---

## Typical Commands

```bash
# Orderbook features with trades (recommended)
python scripts/etl/run_orderbook_features.py --trades data/raw/ready/ccxt/trades

# High-throughput batch mode
python scripts/etl/run_orderbook_features_batch.py --workers 4

# Trades and ticker features
python scripts/etl/run_trades_features.py
python scripts/etl/run_ticker_features.py

# Aggregate bars
python scripts/etl/run_bars.py --channel orderbook --durations 60,300,900,3600

# Continuous watcher
python scripts/etl/run_watcher.py --poll-interval 30
```

---

## Documentation Discipline

Every ETL work session should update the ETL docs listed in [docs/INDEX.md](docs/INDEX.md), especially:

- current architecture/context (`docs/ETL_CONTEXT.md`),
- formulas/contracts (`docs/IMPLEMENTATION_APPENDIX.md`),
- and onboarding prompt continuity (`ETL_AGENT_PROMPT.md`).

This keeps handoff quality high across fresh Copilot sessions.