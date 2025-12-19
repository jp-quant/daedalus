# CCXT Advanced Orderbook ETL Architecture

## Overview

This document details the architecture for processing high-frequency CCXT orderbook snapshots into engineered features and bar aggregates. The pipeline is designed to be **stateful**, **streaming-friendly**, and **storage-efficient**.

## Data Flow

```mermaid
graph TD
    A[Raw NDJSON Segment] -->|Read| B(RawParser)
    B -->|Normalized Record| C{CcxtAdvancedOrderbookProcessor}
    
    subgraph "Stateful Processing (Per Symbol)"
        C -->|Snapshot| D[SymbolState]
        D -->|Extract| E[Structural Features]
        D -->|Update| F[Rolling Stats]
        D -->|Update| G[Bar Builders]
    end
    
    D -->|Emit (10Hz)| H[HF Feature Row]
    D -->|Emit (On Close)| I[Bar Aggregate Row]
    
    H -->|Batch| J[Parquet Writer (HF)]
    I -->|Batch| K[Parquet Writer (Bars)]
```

## 1. Input Data (Ingestion)

*   **Source**: CCXT `watchOrderBook` stream.
*   **Format**: NDJSON (Newline Delimited JSON).
*   **Structure**:
    ```json
    {
      "type": "orderbook",
      "exchange": "binance",
      "symbol": "BTC/USDT",
      "data": {
        "bids": [[price, size], ...],
        "asks": [[price, size], ...],
        "timestamp": 1678886400000,
        "datetime": "2023-03-15T16:00:00Z",
        "nonce": 12345
      },
      "collected_at": 1678886400123
    }
    ```

## 2. Parsing Layer (`RawParser` & `CcxtParser`)

The `RawParser` delegates to `CcxtParser`, which normalizes the raw JSON into a flat dictionary structure used by the ETL processors.

*   **Output**:
    ```python
    {
        "channel": "orderbook",
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "timestamp": 1678886400000,
        "bids": [[...], ...],
        "asks": [[...], ...],
        ...
    }
    ```

## 3. Processing Layer (`CcxtAdvancedOrderbookProcessor`)

This is a **stateful** processor that maintains a `SymbolState` object for each unique `(exchange, symbol)` pair encountered in the stream.

### 3.1 Symbol State (`SymbolState`)

The core engine that persists across snapshots (within the process lifetime).

*   **Responsibility**:
    1.  **Feature Extraction**: Calls `extract_orderbook_features`.
    2.  **Delta Calculation**: Computes Returns and Order Flow Imbalance (OFI) by comparing with the *previous* snapshot.
    3.  **Rolling Statistics**: Updates `Welford` (Variance) and `RollingSum` (OFI) accumulators for horizons [1s, 5s, 30s, 60s].
    4.  **Bar Building**: Pushes updates to `BarBuilder` instances.
    5.  **Emission Control**: Decides when to emit a High-Frequency (HF) row based on `hf_emit_interval`.

### 3.2 Feature Engineering

#### A. Structural Features (Per Snapshot)
*   **Price/Spread**: Mid, Spread, Relative Spread, Microprice.
*   **Depth**: L1 Imbalance, Volume in bands (0-5bps, 5-10bps, etc.).
*   **Shape**: 50% Depth Level, Concentration (Herfindahl).
*   **Impact**: VWAP (Top 5), Smart Depth (Exp Decay), Lambda/Amihud proxies, Slopes.

#### B. Dynamic Features (Streaming)
*   **Returns**: Log returns over 1s, 5s, 30s, 60s.
*   **Volatility**: Realized Volatility (Std Dev of returns) over horizons.
*   **OFI**: Order Flow Imbalance sums over horizons.
*   **Regimes**: Spread regime (Tight/Wide) tracking.
*   **Drift**: Microprice - Mid Price.

#### C. Bar Aggregates (1s, 5s, 30s, 60s)
*   **OHLC**: Open, High, Low, Close (Mid Price).
*   **Microstructure Stats**: Mean Spread, Mean L1 Imbalance, Sum OFI, Realized Variance (within bar).

## 4. Output Layer (`MultiOutputETLPipeline`)

The pipeline splits the processor's output into two distinct Parquet streams.

### Stream 1: High-Frequency Features (`/hf`)
*   **Frequency**: Configurable (default 1.0s).
*   **Content**: Full snapshot feature set + current rolling stats.
*   **Use Case**: Training ML models that need granular orderbook state.

### Stream 2: Bar Aggregates (`/bars`)
*   **Frequency**: Event-driven (emitted when a bar closes).
*   **Content**: OHLCV + aggregated microstructure metrics.
*   **Use Case**: Standard time-series analysis, backtesting, visualization.

## 5. Operational Logic

### Continuous Watcher (`scripts/etl/run_watcher.py`)
*   **Persistence**: The `SymbolState` objects persist in memory as long as the watcher process runs. This ensures continuity of rolling stats across file segments.
*   **Gap Handling**: If data ingestion gaps occur, the time-based eviction logic in `SymbolState` automatically clears old buffer data, effectively resetting the stats after a "warm-up" period (equal to the max horizon, e.g., 60s).

### Batch Mode (`run_etl.py`)
*   **Scope**: State persists for the duration of the batch job.
*   **Note**: If processing a large date range in one go, state is maintained. If processing file-by-file in separate process invocations, state is reset each time.

## Configuration

Configuration is managed via `StateConfig` in `etl/features/state.py` and can be overridden via the main `config.yaml`.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `horizons` | `[1, 5, 30, 60]` | Windows for rolling stats (seconds) |
| `bar_durations` | `[1, 5, 30, 60]` | Bar sizes to aggregate (seconds) |
| `hf_emit_interval` | `1.0` | Downsampling interval for HF rows (seconds) |
| `bands_bps` | `[5, 10, 25, 50]` | Depth bands in basis points |
