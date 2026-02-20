# ETL Documentation Index

This index is the navigation hub for ETL onboarding, implementation context, and continuity across agent sessions.

---

## Core ETL Docs

| Document | Purpose | Audience |
|----------|---------|----------|
| [../README.md](../README.md) | ETL operational overview and quick start | All contributors |
| [../ETL_AGENT_PROMPT.md](../ETL_AGENT_PROMPT.md) | Copy-paste onboarding prompt for fresh Copilot sessions | Agents |
| [ETL_CONTEXT.md](ETL_CONTEXT.md) | Current architecture snapshot, priorities, and session log | Engineers, agents |
| [IMPLEMENTATION_APPENDIX.md](IMPLEMENTATION_APPENDIX.md) | Feature formulas, invariants, and data contracts | Engineers, quants |

---

## Primary ETL Code Map

### Scripts (`scripts/etl/`)

- `run_orderbook_features.py` — stateful bronze→silver orderbook path
- `run_orderbook_features_batch.py` — parallel batch orchestration with bounded memory
- `run_trades_features.py` — trades bronze→silver
- `run_ticker_features.py` — ticker bronze→silver
- `run_bars.py` — silver→gold aggregation
- `run_watcher.py` — continuous processing loop

### Framework (`etl/core/`)

- `executor.py` — transform orchestration and I/O lifecycle
- `config.py` — declarative configs (`TransformConfig`, `FeatureConfig`, `FilterSpec`)

### Feature & Transform Logic

- `etl/transforms/orderbook.py` — hybrid vectorized + sequential stateful pipeline
- `etl/features/orderbook.py` — structural feature extraction (vectorized)
- `etl/features/stateful.py` — OFI/MLOFI/TFI/VPIN/Kyle + rolling state
- `etl/features/streaming.py` — online rolling estimators

### Shared Contract

- `shared/partitioning.py` — sanitize/build/validate directory-aligned partitions

---

## Adjacent Top-Level Architecture Docs

Use these for deeper design context and historical rationale:

- `docs/ETL_CORE_FRAMEWORK.md`
- `docs/BATCHED_ETL_GUIDE.md`
- `docs/PARQUET_ETL_ARCHITECTURE.md`
- `docs/PARQUET_ETL_FEATURES.md`

---

## Update Protocol

After ETL work sessions, update this index if:

- new ETL docs are added,
- entrypoints or ownership of components changes,
- navigation links become outdated.

Keep this file lightweight and accurate so a new agent can navigate ETL in under 2 minutes.
