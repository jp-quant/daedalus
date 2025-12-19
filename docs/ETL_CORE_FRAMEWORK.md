# ETL Core Framework Architecture

## Overview

The Daedalus ETL framework provides a declarative, config-driven architecture for transforming market data through the Medallion pipeline (Bronze → Silver → Gold). It's designed for:

- **Scalability**: Process millions of rows efficiently with Polars lazy evaluation
- **Extensibility**: Add new transforms by implementing a simple interface
- **Testability**: Dry-run mode, row limits, and isolated transform logic
- **Maintainability**: Clear separation between I/O orchestration and business logic

## Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        DAEDALUS ETL FRAMEWORK                              │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │                    ETL SCRIPTS (Thin Wrappers)                       │ │
│  │  scripts/etl/run_trades_features.py                                  │ │
│  │  scripts/etl/run_orderbook_features.py                               │ │
│  │  scripts/etl/run_ticker_features.py                                  │ │
│  │  scripts/etl/run_bars.py                                             │ │
│  │                                                                      │ │
│  │  Responsibilities:                                                   │ │
│  │  - Parse CLI arguments (--exchange, --symbol, --limit, --dry-run)    │ │
│  │  - Build TransformConfig and FilterSpec                              │ │
│  │  - Call executor.execute() and report results                        │ │
│  └────────────────────────────────────────────────────────────────────┬─┘ │
│                                                                        │   │
│                              executor.execute()                        │   │
│                                     │                                  │   │
│  ┌──────────────────────────────────▼───────────────────────────────────┐ │
│  │                    TRANSFORM EXECUTOR (Orchestrator)                 │ │
│  │  etl/core/executor.py                                                │ │
│  │                                                                      │ │
│  │  1. Create TransformContext (execution_id, feature_config, etc.)     │ │
│  │  2. Resolve inputs → read Parquet with FilterSpec partition pruning  │ │
│  │  3. Apply row limit if testing                                       │ │
│  │  4. Call transform.transform(inputs, context)                        │ │
│  │  5. Write outputs → Hive-partitioned Parquet with compression        │ │
│  │  6. Return execution statistics                                      │ │
│  └────────────────────────────────────────────────────────────────────┬─┘ │
│                                                                        │   │
│                         transform.transform(inputs, context)           │   │
│                                     │                                  │   │
│  ┌──────────────────────────────────▼───────────────────────────────────┐ │
│  │                    TRANSFORM IMPLEMENTATIONS                         │ │
│  │  etl/transforms/                                                     │ │
│  │  ├── trades.py    → TradesFeatureTransform                           │ │
│  │  ├── ticker.py    → TickerFeatureTransform                           │ │
│  │  ├── orderbook.py → OrderbookFeatureTransform (stateful)             │ │
│  │  └── bars.py      → BarAggregationTransform                          │ │
│  │                                                                      │ │
│  │  Responsibilities:                                                   │ │
│  │  - Receive resolved LazyFrames as inputs dict                        │ │
│  │  - Apply feature extraction / aggregation logic                      │ │
│  │  - Return outputs dict (LazyFrames, collected by executor)           │ │
│  └────────────────────────────────────────────────────────────────────┬─┘ │
│                                                                        │   │
│  ┌──────────────────────────────────▼───────────────────────────────────┐ │
│  │                    FEATURE EXTRACTION MODULES                        │ │
│  │  etl/features/                                                       │ │
│  │  ├── orderbook.py → extract_structural_features() (vectorized)       │ │
│  │  ├── stateful.py  → StatefulFeatureProcessor (OFI, MLOFI, VPIN)      │ │
│  │  ├── streaming.py → Rolling primitives (Welford, VPIN calculator)    │ │
│  │  └── ...                                                             │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. TransformExecutor (`etl/core/executor.py`)

The central orchestrator that handles the complete transform lifecycle:

```python
from etl.core.executor import TransformExecutor
from etl.core.config import FilterSpec

# Create executor (can also use TransformExecutor.from_config(config))
executor = TransformExecutor(
    feature_config=FeatureConfig(
        categories={FeatureCategory.STRUCTURAL, FeatureCategory.DYNAMIC},
        depth_levels=20,
    )
)

# Execute with partition filtering
result = executor.execute(
    transform=OrderbookFeatureTransform(config),
    filter_spec=FilterSpec(exchange="binanceus", symbol="BTC/USDT"),
    limit_rows=10000,  # For testing
    dry_run=False,
)

print(f"Processed {result['write_stats']['silver']['rows']} rows")
```

**Key Methods:**
- `execute()` - Main entry point for running transforms
- `from_config()` - Factory method to create from DaedalusConfig
- `_resolve_inputs()` - Read Parquet with filtering
- `_write_outputs()` - Write Hive-partitioned Parquet

### 2. BaseTransform (`etl/core/base.py`)

Abstract base class for all transforms:

```python
from etl.core.base import BaseTransform, TransformContext
from etl.core.registry import register_transform

@register_transform("my_features")
class MyFeatureTransform(BaseTransform):
    def transform(
        self,
        inputs: dict[str, pl.LazyFrame],
        context: TransformContext,
    ) -> dict[str, pl.LazyFrame]:
        # Access input by name
        bronze = inputs["bronze"]
        
        # Apply transformations
        silver = bronze.with_columns([...])
        
        # Return outputs by name
        return {"silver": silver}
```

**Key Methods:**
- `transform()` - Abstract method for transformation logic
- `validate_inputs()` - Verify required inputs present
- `validate_outputs()` - Verify expected outputs produced
- `initialize()` / `finalize()` - Lifecycle hooks

### 3. TransformConfig (`etl/core/config.py`)

Declarative configuration for transforms:

```python
from etl.core.config import TransformConfig, InputConfig, OutputConfig
from etl.core.enums import DataFormat, WriteMode, CompressionCodec

config = TransformConfig(
    name="trades_features",
    description="Extract features from bronze trades data",
    inputs={
        "trades": InputConfig(
            name="trades",
            path="data/raw/ready/ccxt/trades",
            format=DataFormat.PARQUET,
        ),
    },
    outputs={
        "silver": OutputConfig(
            name="silver",
            path="data/processed/silver/trades",
            format=DataFormat.PARQUET,
            partition_cols=["exchange", "symbol", "year", "month", "day"],
            mode=WriteMode.OVERWRITE_PARTITION,
            compression=CompressionCodec.ZSTD,
            compression_level=3,
        ),
    },
)
```

### 4. FilterSpec (`etl/core/config.py`)

Declarative filter for partition pruning:

```python
from etl.core.config import FilterSpec

# Exact partition match
filter_spec = FilterSpec(
    exchange="binanceus",
    symbol="BTC/USDT",
    year=2025,
    month=12,
    day=18,
)

# Date range (for batch processing multiple days)
filter_spec = FilterSpec(
    exchange="binanceus",
    start_date="2025-12-01",
    end_date="2025-12-15",
)

# Convert to Polars filter expression
expr = filter_spec.to_polars_filter()
# Result: (col("exchange") == "binanceus") & (col("capture_ts") >= ...) & ...
```

### 5. FeatureConfig (`etl/core/config.py`)

Configuration for feature computation:

```python
from etl.core.config import FeatureConfig
from etl.core.enums import FeatureCategory

feature_config = FeatureConfig(
    categories={
        FeatureCategory.STRUCTURAL,  # Static per-snapshot features
        FeatureCategory.DYNAMIC,     # Delta-based (OFI, returns)
        FeatureCategory.ROLLING,     # Time-windowed statistics
    },
    depth_levels=20,                 # Orderbook levels to process
    rolling_windows=[5, 15, 60, 300, 900],  # Rolling window sizes (seconds)
    ofi_decay_alpha=0.5,             # MLOFI decay parameter
    vpin_bucket_size=1.0,            # VPIN bucket volume
)
```

### 6. Enums (`etl/core/enums.py`)

Type-safe enumerations:

| Enum | Values | Description |
|------|--------|-------------|
| `DataTier` | BRONZE, SILVER, GOLD | Medallion architecture tiers |
| `DataFormat` | PARQUET, NDJSON, CSV | File formats |
| `WriteMode` | APPEND, OVERWRITE, OVERWRITE_PARTITION, MERGE | Write behaviors |
| `CompressionCodec` | ZSTD, SNAPPY, GZIP, LZ4 | Parquet compression |
| `FeatureCategory` | STRUCTURAL, DYNAMIC, ROLLING, BARS, ADVANCED | Feature types |
| `ProcessingMode` | BATCH, STREAMING, HYBRID | Transform modes |

## Data Flow

### Bronze → Silver (Feature Engineering)

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Bronze (Raw)      │    │  Feature Transform  │    │   Silver (Features) │
│                     │    │                     │    │                     │
│ data/raw/ready/     │───▶│ TradesFeatureTransf │───▶│ data/processed/     │
│   ccxt/trades/      │    │ OrderbookFeatureTr  │    │   silver/trades/    │
│   ccxt/orderbook/   │    │ TickerFeatureTrans  │    │   silver/orderbook/ │
│   ccxt/ticker/      │    │                     │    │   silver/ticker/    │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

**Features computed:**
- **Trades**: is_buy, dollar_volume, signed_volume, log_return, hour, day_of_week
- **Orderbook**: 60+ features (depth, spread, microprice, OFI, MLOFI, Kyle's Lambda, VPIN)
- **Ticker**: mid_price, spread, relative_spread, derived metrics

### Silver → Gold (Aggregation)

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Silver (Features) │    │   Bar Aggregation   │    │    Gold (Bars)      │
│                     │    │                     │    │                     │
│ data/processed/     │───▶│ BarAggregationTrans │───▶│ data/processed/     │
│   silver/trades/    │    │                     │    │   gold/trades_bars/ │
│   silver/orderbook/ │    │ run_bars.py         │    │   gold/orderbook_   │
│   silver/ticker/    │    │   --channel trades  │    │   gold/ticker_bars/ │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

**Bar durations**: 60s, 300s, 900s, 3600s
**Aggregations**: OHLC, volume, realized variance, trade count, buy/sell ratio

## ETL Scripts

### Feature Engineering Scripts

| Script | Input | Output | Features |
|--------|-------|--------|----------|
| `run_trades_features.py` | raw/trades | silver/trades | Direction, dollar volume, returns |
| `run_orderbook_features.py` | raw/orderbook | silver/orderbook | 60+ microstructure features |
| `run_ticker_features.py` | raw/ticker | silver/ticker | Mid, spread, derived metrics |

### Bar Aggregation Script

| Script | Input | Output | Description |
|--------|-------|--------|-------------|
| `run_bars.py` | silver/* | gold/*_bars | Time bars (1m, 5m, 15m, 1h) |

### Usage Examples

```bash
# Feature engineering (Bronze → Silver)
python -m scripts.etl.run_trades_features --limit 10000
python -m scripts.etl.run_orderbook_features --exchange binanceus --symbol BTC/USDT
python -m scripts.etl.run_ticker_features --dry-run

# Bar aggregation (Silver → Gold)
python -m scripts.etl.run_bars --channel trades --output data/processed
python -m scripts.etl.run_bars --channel orderbook --durations 60,300,900
```

## Configuration Integration

The ETL framework integrates with `config/config.yaml`:

```yaml
# In config.yaml
features:
  categories:
    - "structural"
    - "dynamic"
    - "rolling"
  depth_levels: 20
  rolling_windows: [5, 15, 60, 300, 900]
  bar_durations: [60, 300, 900, 3600]
  ofi_decay_alpha: 0.5
```

Load and use:

```python
from config.config import load_config
from etl.core.executor import TransformExecutor

config = load_config()
executor = TransformExecutor.from_config(config)
# executor.feature_config is now populated from config.yaml
```

## Extending the Framework

### Creating a New Transform

1. **Create transform class:**

```python
# etl/transforms/my_transform.py
from etl.core.base import BaseTransform
from etl.core.registry import register_transform

@register_transform("my_transform")
class MyTransform(BaseTransform):
    def transform(self, inputs, context):
        # Your logic here
        return {"output": result}
```

2. **Create runner script:**

```python
# scripts/etl/run_my_transform.py
from etl.core.executor import TransformExecutor
from etl.transforms.my_transform import MyTransform

config = create_transform_config(...)
transform = MyTransform(config)
executor = TransformExecutor()
result = executor.execute(transform)
```

### Adding New Features

Add to `etl/features/` and call from transform:

```python
# etl/features/my_features.py
def compute_my_features(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns([...])

# In transform
from etl.features.my_features import compute_my_features
silver = compute_my_features(bronze)
```

## State Management

For stateful transforms (e.g., OrderbookFeatureTransform):

```python
from etl.core.base import StatefulTransform

class MyStatefulTransform(StatefulTransform):
    def transform(self, inputs, context):
        # Access previous state
        prev_value = self.get_state_value("prev_value", default=0)
        
        # Compute with state
        result = compute_with_state(inputs, prev_value)
        
        # Update state for next batch
        self.update_state("prev_value", new_value)
        
        return {"output": result}
```

State can be persisted for checkpoint/resume via `--state-path` argument.

## Performance Considerations

1. **Lazy Evaluation**: All transforms work with `pl.LazyFrame` - computation is deferred until write
2. **Partition Pruning**: Use `FilterSpec` to avoid scanning unnecessary partitions
3. **Row Limits**: Use `--limit` flag for testing without processing entire dataset
4. **Compression**: ZSTD level 3 provides good balance of size/speed
5. **Parallel Execution**: Polars automatically parallelizes within transforms

## Testing

```bash
# Test with limited data
python -m scripts.etl.run_trades_features --limit 1000 --dry-run

# Test specific partition
python -m scripts.etl.run_orderbook_features \
    --exchange binanceus \
    --symbol BTC/USDT \
    --limit 5000

# Run unit tests
python -m pytest tests/test_etl_core.py -v
```

## File Structure

```
etl/
├── __init__.py           # Public API exports
├── core/
│   ├── __init__.py       # Core module exports
│   ├── base.py           # BaseTransform, StatefulTransform, TransformContext
│   ├── config.py         # TransformConfig, InputConfig, OutputConfig, FilterSpec, FeatureConfig
│   ├── enums.py          # Type-safe enums
│   ├── executor.py       # TransformExecutor
│   └── registry.py       # Transform registry and decorators
├── features/
│   ├── __init__.py       # Feature extraction exports
│   ├── orderbook.py      # Vectorized orderbook features
│   ├── stateful.py       # StatefulFeatureProcessor (OFI, MLOFI, VPIN)
│   ├── streaming.py      # Rolling primitives
│   ├── snapshot.py       # Per-row feature extraction
│   └── state.py          # SymbolState, BarBuilder
├── transforms/
│   ├── __init__.py       # Transform exports
│   ├── trades.py         # TradesFeatureTransform
│   ├── ticker.py         # TickerFeatureTransform
│   ├── orderbook.py      # OrderbookFeatureTransform
│   └── bars.py           # BarAggregationTransform
├── utils/
│   ├── crud.py           # ParquetCRUD
│   ├── compaction.py     # ParquetCompactor
│   └── ...
└── legacy/               # Archived NDJSON-based code (do not import)
```

## Related Documentation

- [PARQUET_ETL_ARCHITECTURE.md](PARQUET_ETL_ARCHITECTURE.md) - Detailed architecture decisions
- [ETL_REFACTOR_DESIGN.md](ETL_REFACTOR_DESIGN.md) - Refactor history and design rationale
- [PARQUET_ETL_FEATURES.md](PARQUET_ETL_FEATURES.md) - Feature extraction details
- [SYSTEM_ONBOARDING.md](SYSTEM_ONBOARDING.md) - Full system onboarding guide
