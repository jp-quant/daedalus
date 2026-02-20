# ETL Context & Session Trail

## Purpose

This document maintains a living context for ETL work so any new Copilot session can resume accurately without rediscovery.

---

## ETL Objective

Build and maintain a production-grade ETL system that is:

- **Correct** for stateful microstructure features,
- **Efficient** for large partitioned datasets,
- **Resilient** under interruption/restart,
- **Consistent** across single-run, batched, and watcher execution paths.

---

## Current Architecture Snapshot

### Data Flow

1. Bronze raw Parquet (channel separated)
2. Silver feature transforms (`orderbook`, `trades`, `ticker`)
3. Gold bar aggregation (`run_bars.py`)

### Main Execution Modes

- **Single-run scripts** (`run_*_features.py`) for targeted transforms
- **Batch runner** (`run_orderbook_features_batch.py`) for memory-safe, parallel processing at scale
- **Watcher** (`run_watcher.py`) for continuous segment processing

### Core Contracts

- Directory-aligned partitioning must remain valid across ingestion and ETL.
- Stateful features require chronological processing per symbol.
- Trades alignment is required for TFI correctness.
- Quote normalization must be explicit and consistent.

---

## Active Revamp Themes

1. **Path Parity**: ensure output semantics are consistent across single-run, batch, and watcher paths.
2. **State Correctness**: strengthen guarantees around checkpoint/resume and staleness reset behavior.
3. **Operational Confidence**: improve targeted validation workflows for feature correctness and partition integrity.
4. **Documentation Discipline**: keep ETL docs synchronized with implementation, not aspirational design.

---

## Session Log

Record each ETL session here as a compact, auditable trail.

| Date | Agent Session Focus | Code Areas Touched | Validation Performed | Outcome | Next Step |
|------|---------------------|--------------------|----------------------|---------|-----------|
| 2026-02-19 | Established ETL agentic onboarding and continuity framework modeled after `research/` | `etl/README.md`, `etl/ETL_AGENT_PROMPT.md`, `etl/docs/INDEX.md`, `etl/docs/ETL_CONTEXT.md`, `etl/docs/IMPLEMENTATION_APPENDIX.md` | Documentation alignment pass against current `etl/` and `scripts/etl/` architecture | Baseline ETL handoff system created for fresh agent sessions | Start first revamp workstream: parity/correctness audit between `run_orderbook_features.py` and `run_orderbook_features_batch.py` |

---

## Open Questions / Risks

- Are there behavior differences between `run_orderbook_features.py` and `run_orderbook_features_batch.py` for edge partitions (missing trades, sparse snapshots, stale state boundaries)?
- Does `run_watcher.py` match the same output partitioning and transform semantics as batch/single-run paths?
- Which ETL docs under top-level `docs/` are now partially stale and should be consolidated or refreshed first?

---

## Rules for Updating This File

After every ETL work session:

1. Add one row in **Session Log**.
2. Update **Active Revamp Themes** if priorities changed.
3. Add/remove **Open Questions / Risks** based on findings.
4. Ensure statements reflect current code behavior, not planned behavior.
