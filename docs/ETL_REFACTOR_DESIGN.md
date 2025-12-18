# ETL Module Refactor Design

## Implementation Status: COMPLETED

This document describes the ETL refactor design and its implementation status.

---

## Implementation Summary

### What Was Done

1. **Created `etl/core/`** - New ETL framework foundation
   - `enums.py` - Type-safe enums (StorageBackendType, DataFormat, WriteMode, etc.)
   - `config.py` - Declarative I/O config (InputConfig, OutputConfig, TransformConfig)
   - `base.py` - BaseTransform, StatefulTransform, TransformContext
   - `executor.py` - TransformExecutor for running transforms
   - `registry.py` - Transform registry and @register_transform decorator

2. **Created `etl/utils/`** - Parquet utilities
   - `crud.py` - ParquetCRUD (moved from parquet_crud.py)
   - `compaction.py` - ParquetCompactor (extracted from repartitioner.py)
   - `repartition.py` - Repartitioner (trimmed from repartitioner.py)
   - `time_utils.py` - Timestamp parsing utilities

3. **Refactored `etl/features/`** - Feature extraction
   - `orderbook.py` - NEW: Vectorized Polars feature extraction
   - `stateful.py` - NEW: StatefulFeatureProcessor for batch OFI/MLOFI
   - `streaming.py` - UPDATED: Fixed imports, kept all algorithms
   - `snapshot.py` - KEPT: Row-by-row extraction for streaming
   - `state.py` - KEPT: SymbolState for real-time processing

4. **Created `etl/transforms/`** - Transform implementations
   - `orderbook.py` - OrderbookFeatureTransform (Bronze → Silver)

5. **Created `etl/legacy/`** - Archived old code
   - Moved: orchestrators/, parsers/, processors/, readers/, writers/, job.py
   - **NOTHING should import from legacy** - preserved for reference only

---

## Current State Analysis (Before Refactor)

### Problems Identified
1. **Redundant architectures**: Legacy `orchestrators/`, `parsers/`, `processors/`, `readers/`, `writers/` coexist with `parquet_etl_pipeline.py`
2. **Massive monolith**: `parquet_etl_pipeline.py` is 1700+ lines doing too many things
3. **Misplaced utilities**: `parquet_crud.py` and `repartitioner.py` are not ETL transforms, they're storage utilities
4. **Fragmented logic**: Same feature extraction logic duplicated in `snapshot.py` and `parquet_etl_pipeline.py`
5. **Unclear boundaries**: What's the difference between `etl/features/` and `etl/processors/ccxt/`?
6. **Config confusion**: `ParquetETLConfig`, `StateConfig` overlap and are created inconsistently

### What's Actually Used (New Framework)
- `parquet_etl_pipeline.py` → Main pipeline (but needs decomposition)
- `etl/features/snapshot.py` → Static orderbook features
- `etl/features/state.py` → SymbolState, BarBuilder, StateConfig
- `etl/features/streaming.py` → Rolling stats (VPIN, Kyle's Lambda, Welford)

### What's Legacy (NDJSON-based)
- `etl/orchestrators/` → Pipeline composition for NDJSON
- `etl/parsers/` → NDJSON parsing
- `etl/processors/` → Per-record processing
- `etl/readers/` → NDJSON reading
- `etl/writers/` → Parquet writing (but tightly coupled to legacy flow)
- `etl/job.py` → NDJSON segment job runner

---

## New Architecture (Implemented)

### Design Philosophy (Inspired by JPMorgan GlueETL)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ETL TRANSFORM FRAMEWORK                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  TRANSFORM DEFINITION (User-Defined)                                 │  │
│  │  ─────────────────────────────────────────────────────────────────── │  │
│  │  @transform(                                                         │  │
│  │      name="orderbook_features",                                      │  │
│  │      inputs={"orderbook": InputConfig(...), "trades": InputConfig(...)}│  │
│  │      outputs={"hf": OutputConfig(...), "bars": OutputConfig(...)},   │  │
│  │  )                                                                   │  │
│  │  def orderbook_features(inputs: Dict[str, pl.LazyFrame]) -> Dict:    │  │
│  │      # Your transformation logic here                                │  │
│  │      return {"hf": hf_df, "bars": bars_df}                          │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                    │                                        │
│                                    ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  TRANSFORM EXECUTOR (Framework)                                      │  │
│  │  ─────────────────────────────────────────────────────────────────── │  │
│  │  1. Resolve inputs (scan_parquet with filters, storage backend)      │  │
│  │  2. Execute transform function                                       │  │
│  │  3. Write outputs (partitioned Parquet, storage backend)             │  │
│  │  4. Track lineage, metrics, state                                    │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### New Directory Structure (Implemented)

```
etl/
├── __init__.py              # Public API exports
├── core/                    # Framework core (NEW)
│   ├── __init__.py
│   ├── enums.py             # Type-safe enums
│   ├── config.py            # InputConfig, OutputConfig, TransformConfig
│   ├── base.py              # BaseTransform, StatefulTransform, TransformContext
│   ├── executor.py          # TransformExecutor - runs transforms
│   └── registry.py          # Transform registry & @register_transform
│
├── features/                # Feature computation library (REFACTORED)
│   ├── __init__.py
│   ├── orderbook.py         # NEW: Vectorized Polars features
│   ├── stateful.py          # NEW: StatefulFeatureProcessor
│   ├── streaming.py         # Online algorithms (VPIN, Welford, Kyle's Lambda)
│   ├── snapshot.py          # Row-by-row extraction (original)
│   └── state.py             # SymbolState, BarBuilder (original)
│
├── transforms/              # Concrete transform implementations (NEW)
│   ├── __init__.py
│   └── orderbook.py         # OrderbookFeatureTransform
│
├── utils/                   # Parquet utilities (MOVED/REFACTORED)
│   ├── __init__.py
│   ├── crud.py              # ParquetCRUD
│   ├── compaction.py        # ParquetCompactor
│   ├── repartition.py       # Repartitioner
│   └── time_utils.py        # Timestamp parsing
│
├── legacy/                  # OLD STUFF (preserved, DO NOT IMPORT)
│   ├── __init__.py
│   ├── job.py
│   ├── orchestrators/
│   ├── parsers/
│   ├── processors/
│   ├── readers/
│   └── writers/
│
└── parquet_etl_pipeline.py  # STILL EXISTS - to be decomposed in Phase 2
```
    └── writers/
```

---

## Core Framework API

### 1. Transform Definition

```python
# etl/core/base.py

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any, Union, Literal
import polars as pl

@dataclass
class InputConfig:
    """Configuration for a transform input."""
    path: str                              # Path pattern (e.g., "raw/ready/ccxt/orderbook")
    format: Literal["parquet", "ndjson"] = "parquet"
    columns: Optional[List[str]] = None    # Columns to select (None = all)
    filter_expr: Optional[pl.Expr] = None  # Pushdown predicate
    # Runtime filters (applied at execution)
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None


@dataclass
class OutputConfig:
    """Configuration for a transform output."""
    path: str                              # Output path pattern
    partition_cols: List[str] = field(default_factory=lambda: ["exchange", "symbol", "date"])
    compression: str = "zstd"
    compression_level: int = 3
    mode: Literal["append", "overwrite", "merge"] = "append"


@dataclass
class TransformConfig:
    """Configuration for a transform."""
    name: str
    inputs: Dict[str, InputConfig]
    outputs: Dict[str, OutputConfig]
    # Feature engineering options (passed to transform function)
    options: Dict[str, Any] = field(default_factory=dict)
    # Scheduling
    schedule: Optional[str] = None  # Cron expression
    depends_on: List[str] = field(default_factory=list)  # Transform dependencies
    # Execution
    batch_size: Optional[int] = None  # For streaming/incremental
    checkpoint_enabled: bool = False


class BaseTransform:
    """Base class for ETL transforms."""
    
    config: TransformConfig
    
    def __init__(self, config: TransformConfig):
        self.config = config
    
    def transform(
        self, 
        inputs: Dict[str, pl.LazyFrame]
    ) -> Dict[str, pl.DataFrame]:
        """
        Execute the transform logic.
        
        Args:
            inputs: Dict mapping input names to LazyFrames
            
        Returns:
            Dict mapping output names to DataFrames
        """
        raise NotImplementedError
    
    def validate_inputs(self, inputs: Dict[str, pl.LazyFrame]) -> bool:
        """Validate input schemas."""
        return True
    
    def get_state(self) -> Optional[Dict[str, Any]]:
        """Get stateful transform state (for checkpointing)."""
        return None
    
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restore stateful transform state."""
        pass
```

### 2. Transform Executor

```python
# etl/core/executor.py

class TransformExecutor:
    """Executes transforms with storage backend support."""
    
    def __init__(
        self,
        storage_input: StorageBackend,
        storage_output: StorageBackend,
        base_input_path: str = "raw/ready",
        base_output_path: str = "processed",
    ):
        self.storage_input = storage_input
        self.storage_output = storage_output
        self.base_input_path = base_input_path
        self.base_output_path = base_output_path
    
    def execute(
        self,
        transform: BaseTransform,
        runtime_filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute a transform.
        
        Args:
            transform: Transform instance to execute
            runtime_filters: Optional runtime filters (exchange, symbol, date range)
            
        Returns:
            Execution statistics
        """
        # 1. Resolve inputs
        inputs = self._resolve_inputs(transform.config, runtime_filters)
        
        # 2. Execute transform
        outputs = transform.transform(inputs)
        
        # 3. Write outputs
        stats = self._write_outputs(transform.config, outputs)
        
        return stats
    
    def _resolve_inputs(
        self,
        config: TransformConfig,
        runtime_filters: Optional[Dict[str, Any]],
    ) -> Dict[str, pl.LazyFrame]:
        """Resolve input configs to LazyFrames."""
        inputs = {}
        
        for name, input_config in config.inputs.items():
            # Build full path
            path = f"{self.base_input_path}/{input_config.path}"
            
            # Get file list from storage
            if self.storage_input.backend_type == "local":
                full_path = self.storage_input.get_full_path(path)
                lf = pl.scan_parquet(f"{full_path}/**/*.parquet")
            else:
                # S3: Need to list files and scan
                files = self.storage_input.list_files(path, pattern="**/*.parquet")
                # ... handle S3 scanning
            
            # Apply column selection
            if input_config.columns:
                lf = lf.select(input_config.columns)
            
            # Apply filters
            if input_config.filter_expr:
                lf = lf.filter(input_config.filter_expr)
            
            # Apply runtime filters
            if runtime_filters:
                lf = self._apply_runtime_filters(lf, runtime_filters)
            
            inputs[name] = lf
        
        return inputs
    
    def _write_outputs(
        self,
        config: TransformConfig,
        outputs: Dict[str, pl.DataFrame],
    ) -> Dict[str, Any]:
        """Write outputs to storage."""
        stats = {}
        
        for name, df in outputs.items():
            output_config = config.outputs[name]
            output_path = f"{self.base_output_path}/{output_config.path}"
            
            # Write partitioned Parquet
            # ... implementation
            
            stats[name] = {"rows_written": len(df)}
        
        return stats
```

### 3. Concrete Transform Example

```python
# etl/transforms/orderbook.py

from etl.core.base import BaseTransform, TransformConfig, InputConfig, OutputConfig
from etl.features.orderbook import extract_structural_features, extract_stateful_features
from etl.features.bars import aggregate_to_bars
import polars as pl


class OrderbookFeatureTransform(BaseTransform):
    """
    Orderbook feature engineering transform.
    
    Inputs:
        - orderbook: Raw orderbook snapshots
        - trades: Raw trade data (for TFI)
    
    Outputs:
        - hf: High-frequency features (per snapshot)
        - bars: Bar aggregates (1m, 5m, 15m, 1h)
    """
    
    @classmethod
    def create(
        cls,
        source: str = "ccxt",
        horizons: List[int] = [5, 15, 60, 300, 900],
        bar_durations: List[int] = [60, 300, 900, 3600],
        max_levels: int = 20,
        **kwargs,
    ) -> "OrderbookFeatureTransform":
        """Factory method with sensible defaults."""
        config = TransformConfig(
            name=f"{source}_orderbook_features",
            inputs={
                "orderbook": InputConfig(path=f"{source}/orderbook"),
                "trades": InputConfig(path=f"{source}/trades"),
            },
            outputs={
                "hf": OutputConfig(path=f"{source}/orderbook/hf"),
                "bars": OutputConfig(path=f"{source}/orderbook/bars"),
            },
            options={
                "horizons": horizons,
                "bar_durations": bar_durations,
                "max_levels": max_levels,
                **kwargs,
            },
        )
        return cls(config)
    
    def transform(
        self,
        inputs: Dict[str, pl.LazyFrame],
    ) -> Dict[str, pl.DataFrame]:
        """Execute orderbook feature engineering."""
        orderbook_lf = inputs["orderbook"]
        trades_lf = inputs["trades"]
        
        options = self.config.options
        
        # 1. Extract structural features (vectorized)
        hf_df = extract_structural_features(
            orderbook_lf,
            max_levels=options.get("max_levels", 20),
            bands_bps=options.get("bands_bps", [5, 10, 25, 50, 100]),
        )
        
        # 2. Extract stateful features (hybrid)
        hf_df = extract_stateful_features(
            hf_df,
            trades_lf,
            horizons=options.get("horizons", [5, 15, 60, 300, 900]),
        )
        
        # 3. Aggregate to bars
        bars_df = aggregate_to_bars(
            hf_df,
            durations=options.get("bar_durations", [60, 300, 900, 3600]),
        )
        
        return {
            "hf": hf_df,
            "bars": bars_df,
        }
```

---

## Config Integration

### Updated `config/config.py`

```python
class TransformETLConfig(BaseModel):
    """Configuration for a single ETL transform."""
    enabled: bool = True
    inputs: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    outputs: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    options: Dict[str, Any] = Field(default_factory=dict)
    schedule: Optional[str] = None
    depends_on: List[str] = Field(default_factory=list)


class ETLConfig(BaseModel):
    """ETL layer configuration."""
    compression: str = "zstd"
    compression_level: int = 3
    delete_after_processing: bool = True
    
    # Transform configurations
    transforms: Dict[str, TransformETLConfig] = Field(default_factory=lambda: {
        "ccxt_ticker": TransformETLConfig(
            inputs={"ticker": {"path": "ccxt/ticker"}},
            outputs={"ticker": {"path": "ccxt/ticker", "partition_cols": ["exchange", "symbol", "date"]}},
        ),
        "ccxt_trades": TransformETLConfig(
            inputs={"trades": {"path": "ccxt/trades"}},
            outputs={"trades": {"path": "ccxt/trades", "partition_cols": ["exchange", "symbol", "date"]}},
        ),
        "ccxt_orderbook": TransformETLConfig(
            inputs={
                "orderbook": {"path": "ccxt/orderbook"},
                "trades": {"path": "ccxt/trades"},
            },
            outputs={
                "hf": {"path": "ccxt/orderbook/hf", "partition_cols": ["exchange", "symbol", "date"]},
                "bars": {"path": "ccxt/orderbook/bars", "partition_cols": ["exchange", "symbol", "date"]},
            },
            options={
                "horizons": [5, 15, 60, 300, 900],
                "bar_durations": [60, 300, 900, 3600],
                "max_levels": 20,
                "ofi_levels": 10,
                "enable_vpin": True,
            },
        ),
    })
```

### Example `config.yaml`

```yaml
etl:
  compression: "zstd"
  compression_level: 3
  delete_after_processing: true
  
  transforms:
    ccxt_orderbook:
      enabled: true
      inputs:
        orderbook:
          path: "ccxt/orderbook"
        trades:
          path: "ccxt/trades"
      outputs:
        hf:
          path: "ccxt/orderbook/hf"
          partition_cols: ["exchange", "symbol", "date"]
        bars:
          path: "ccxt/orderbook/bars"
          partition_cols: ["exchange", "symbol", "date"]
      options:
        horizons: [5, 15, 60, 300, 900]
        bar_durations: [60, 300, 900, 3600]
        max_levels: 20
        ofi_levels: 10
        enable_vpin: true
```

---

## Migration Plan

### Phase 1: Structural Changes (Now)
1. Create `etl/legacy/` and move old code
2. Create `etl/utils/` and move parquet utilities
3. Create `etl/core/` skeleton

### Phase 2: Core Framework
1. Implement `BaseTransform`, `InputConfig`, `OutputConfig`
2. Implement `TransformExecutor`
3. Implement transform registry

### Phase 3: Feature Consolidation
1. Consolidate `snapshot.py` + vectorized code into `features/orderbook.py`
2. Keep `streaming.py` as-is (it's good)
3. Extract bar logic into `features/bars.py`

### Phase 4: Transform Implementation
1. Implement `transforms/ticker.py`
2. Implement `transforms/trades.py`
3. Implement `transforms/orderbook.py`

### Phase 5: Integration
1. Update scripts (`run_parquet_etl.py`, etc.)
2. Update config models
3. Update documentation

---

## Benefits

1. **Clear separation**: Framework core vs. feature logic vs. transforms
2. **Declarative**: Define transforms via config, not code sprawl
3. **Extensible**: Add new transforms without touching framework
4. **Testable**: Each layer can be tested independently
5. **Future-proof**: Easy to add real-time support later
6. **JPMorgan-inspired**: Familiar pattern for data engineers

---

## Questions for Review

1. Should we keep any legacy pipelines for NDJSON processing, or fully deprecate?
2. Do we need real-time streaming support in v1, or can that be v2?
3. Should transforms be registered via decorators or explicit registration?
4. Do we want to support Spark/DuckDB as alternatives to Polars?
