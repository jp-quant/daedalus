# New ETL Architecture - Directory Structure

## Before Refactoring
```
etl/
├── __init__.py
├── job.py                      # ❌ Monolithic - all logic in one file
├── parsers/
│   ├── __init__.py
│   └── coinbase_parser.py      # ✓ Good separation
└── writers/
    ├── __init__.py
    └── parquet_writer.py       # ❌ Too coupled to specific use case
```

## After Refactoring
```
etl/
├── __init__.py
├── job.py                      # ✅ Now orchestrates pipelines
│
├── readers/                    # ✅ NEW: Abstract data loading
│   ├── __init__.py
│   ├── base_reader.py         # Abstract interface
│   ├── ndjson_reader.py       # Read raw segments
│   └── parquet_reader.py      # Read Parquet (with Polars)
│
├── processors/                 # ✅ REFACTORED: Composable transforms
│   ├── __init__.py
│   ├── base_processor.py      # Abstract interface + chain
│   ├── raw_parser.py          # Parse NDJSON → structured
│   └── coinbase/              # Coinbase-specific processors
│       ├── __init__.py
│       ├── level2_processor.py    # LOB reconstruction + features
│       ├── trades_processor.py    # Trade processing
│       └── ticker_processor.py    # Ticker normalization
│
├── writers/                    # ✅ REFACTORED: Flexible output
│   ├── __init__.py
│   ├── base_writer.py         # Abstract interface
│   └── parquet_writer.py      # Schema-driven partitioning
│
├── orchestrators/              # ✅ NEW: Pipeline composition
│   ├── __init__.py
│   ├── pipeline.py            # Generic reader→processor→writer
│   └── coinbase_segment_pipeline.py    # Coinbase multi-channel processing
│
└── parsers/                    # ✅ UNCHANGED: Source-specific parsing
    ├── __init__.py
    └── coinbase_parser.py      # Coinbase message parsing
```

## Data Flow

### Old Architecture (Monolithic)
```
Segment File
    ↓
ETLJob.process_segment()
    ├─ Read file line-by-line
    ├─ Parse with CoinbaseParser
    ├─ Group by channel in memory
    └─ Write to Parquet (flat structure)
         ↓
    output/coinbase/channel/part_*.parquet
```

### New Architecture (Composable)
```
Segment File
    ↓
CoinbaseSegmentPipeline
    ├─ level2 Channel
    │   ├─ NDJSONReader
    │   │    ↓
    │   ├─ RawParser (coinbase, level2)
    │   │    ↓
    │   ├─ Level2Processor (LOB, features)
    │   │    ↓
    │   └─ ParquetWriter (partition by product_id, date)
    │        ↓
    │   output/level2/product_id=BTC-USD/date=2025-11-26/part_*.parquet
    │
    ├─ market_trades Channel
    │   ├─ NDJSONReader
    │   │    ↓
    │   ├─ RawParser (coinbase, market_trades)
    │   │    ↓
    │   ├─ TradesProcessor
    │   │    ↓
    │   └─ ParquetWriter (partition by product_id, date)
    │        ↓
    │   output/trades/product_id=BTC-USD/date=2025-11-26/part_*.parquet
    │
    └─ ticker Channel
        ├─ NDJSONReader
        │    ↓
        ├─ RawParser (coinbase, ticker)
        │    ↓
        ├─ TickerProcessor
        │    ↓
        └─ ParquetWriter (partition by date, hour)
             ↓
        output/ticker/date=2025-11-26/hour=14/part_*.parquet
```

## Component Responsibilities

### Readers
- **Purpose**: Load data from various sources
- **Responsibility**: Only I/O, no transformation
- **Types**: NDJSON, Parquet, CSV, Database (future)

```python
reader = NDJSONReader()
for record in reader.read("segment.ndjson"):
    # record is raw dict
    process(record)
```

### Processors
- **Purpose**: Transform data
- **Responsibility**: Only logic, no I/O
- **Composable**: Chain multiple processors

```python
processors = [
    RawParser(source="coinbase"),
    Level2Processor(reconstruct_lob=True),
    CustomFeatureProcessor(),
]
```

### Writers
- **Purpose**: Write data to sinks
- **Responsibility**: Only output, no transformation
- **Flexible**: Partition by any columns

```python
writer = ParquetWriter()
writer.write(
    data=records,
    output_path="output/level2",
    partition_cols=["product_id", "date"],
)
```

### Orchestrators
- **Purpose**: Compose reader → processors → writer
- **Responsibility**: Pipeline execution, error handling
- **Types**: Generic (ETLPipeline), Coinbase-specific (CoinbaseSegmentPipeline)

```python
pipeline = ETLPipeline(
    reader=reader,
    processors=processors,
    writer=writer,
)
pipeline.execute(input_path, output_path, partition_cols)
```

## Output Structure Comparison

### Before (Flat)
```
data/processed/coinbase/
├── level2/
│   ├── part_2025-11-22_abc123.parquet
│   ├── part_2025-11-22_def456.parquet
│   └── part_2025-11-23_ghi789.parquet
├── market_trades/
│   ├── part_2025-11-22_jkl012.parquet
│   └── part_2025-11-23_mno345.parquet
└── ticker/
    └── part_2025-11-22_pqr678.parquet

❌ Problems:
- Can't filter by product efficiently
- All data for all products in same file
- Queries scan entire dataset
```

### After (Partitioned)
```
data/processed/coinbase/
├── level2/
│   ├── product_id=BTC-USD/
│   │   ├── date=2025-11-22/
│   │   │   ├── part_001.parquet
│   │   │   └── part_002.parquet
│   │   └── date=2025-11-23/
│   │       └── part_001.parquet
│   └── product_id=ETH-USD/
│       └── date=2025-11-22/
│           └── part_001.parquet
├── market_trades/
│   ├── product_id=BTC-USD/
│   │   ├── date=2025-11-22/
│   │   │   └── part_001.parquet
│   │   └── date=2025-11-23/
│   │       └── part_001.parquet
│   └── product_id=ETH-USD/
│       └── date=2025-11-22/
│           └── part_001.parquet
└── ticker/
    └── date=2025-11-22/
        ├── hour=14/
        │   ├── part_001.parquet
        │   └── part_002.parquet
        └── hour=15/
            └── part_001.parquet

✅ Benefits:
- Query only specific product/date
- Partition pruning (10-100x faster)
- Parallel queries per partition
- Scales to TB+ of data
```

## Key Improvements

| Aspect | Before | After |
|--------|--------|-------|
| **Architecture** | Monolithic | Composable |
| **Extensibility** | Hard to extend | Easy to add processors |
| **Testability** | Coupled logic | Independent components |
| **Reusability** | Code duplication | Reusable processors |
| **Partitioning** | None | Product, date, hour |
| **Performance** | Pandas only | Pandas + Polars |
| **Query Speed** | Full scan | Partition pruning |
| **Scalability** | Limited | TB+ capable |

## Usage Comparison

### Old Way
```python
# One way to do it, inflexible
job = ETLJob(input_dir, output_dir)
job.process_all()
```

### New Way
```python
# Option 1: Simple (backwards compatible)
job = ETLJob(input_dir, output_dir)
job.process_all()

# Option 2: Custom configuration
job = ETLJob(
    input_dir, output_dir,
    channel_config={
        "level2": {
            "partition_cols": ["product_id", "date"],
            "processor_options": {"reconstruct_lob": True}
        }
    }
)

# Option 3: Custom pipeline
pipeline = ETLPipeline(
    reader=NDJSONReader(),
    processors=[RawParser(), CustomProcessor()],
    writer=ParquetWriter(),
)

# Option 4: Reprocessing
pipeline = ETLPipeline(
    reader=ParquetReader(use_polars=True),
    processors=[AdvancedProcessor()],
    writer=ParquetWriter(),
)
```

## Migration Path

✅ **Zero Breaking Changes!**

The existing `ETLJob` API is unchanged. Existing code continues to work, but now uses the new pipeline architecture internally.

```python
# This still works exactly as before
job = ETLJob(
    input_dir="data/raw/ready/coinbase",
    output_dir="data/processed",
)
job.process_all()
```

But now you can also do:

```python
# New capabilities
job = ETLJob(
    input_dir="data/raw/ready/coinbase",
    output_dir="data/processed",
    channel_config={
        "level2": {
            "partition_cols": ["product_id", "date"],
            "processor_options": {
                "reconstruct_lob": True,
                "compute_features": True,
            }
        }
    }
)
```

## Next Steps

1. **Test with real data**: `python scripts/run_etl.py --source coinbase --mode all`
2. **Try examples**: `python examples/etl_pipeline_examples.py`
3. **Query partitioned data**: See `docs/ETL_ARCHITECTURE.md`
4. **Build custom processors**: Extend `BaseProcessor`
5. **Add advanced features**: LOB reconstruction, microstructure, etc.
