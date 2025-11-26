# ETL Architecture - Usage Guide

## Overview

The new ETL architecture provides a flexible, composable framework for processing market data:

- **Readers**: Load data from various sources (NDJSON, Parquet, etc.)
- **Processors**: Transform data (parse, aggregate, derive features)
- **Writers**: Output data to sinks (Parquet with partitioning, etc.)
- **Orchestrators**: Compose readers → processors → writers into pipelines

## Quick Start

### Basic Usage (Existing ETL Job)

The existing `ETLJob` now uses the new pipeline architecture internally:

```python
from etl.job import ETLJob

# Create job (same as before, but now uses pipelines internally)
job = ETLJob(
    input_dir="data/raw/ready/coinbase",
    output_dir="data/processed",
    source="coinbase",
    delete_after_processing=True,
)

# Process all segments
job.process_all()
```

### Custom Pipeline Configuration

You can customize channel-specific processing:

```python
from etl.job import ETLJob

# Configure per-channel behavior
channel_config = {
    "level2": {
        "partition_cols": ["product_id", "date"],  # Partition by symbol and date
        "processor_options": {
            "add_derived_fields": True,
            "reconstruct_lob": True,  # Enable LOB reconstruction
            "compute_features": True,  # Enable microstructure features
        }
    },
    "market_trades": {
        "partition_cols": ["product_id", "date"],
        "processor_options": {
            "add_derived_fields": True,
            "infer_aggressor": False,
        }
    },
    "ticker": {
        "partition_cols": ["date", "hour"],  # Time-based partitioning
        "processor_options": {
            "add_derived_fields": True,
        }
    },
}

job = ETLJob(
    input_dir="data/raw/ready/coinbase",
    output_dir="data/processed",
    source="coinbase",
    channel_config=channel_config,
)

job.process_all()
```

## Advanced Usage

### 1. Build Custom Pipelines

Create your own processing pipelines for specialized use cases:

```python
from etl.readers import NDJSONReader
from etl.processors import RawParser
from etl.processors.coinbase import Level2Processor
from etl.writers import ParquetWriter
from etl.orchestrators import ETLPipeline

# Create custom pipeline
pipeline = ETLPipeline(
    reader=NDJSONReader(),
    processors=[
        RawParser(source="coinbase", channel="level2"),
        Level2Processor(
            reconstruct_lob=True,
            compute_features=True,
        ),
    ],
    writer=ParquetWriter(),
)

# Execute
pipeline.execute(
    input_path="data/segments/segment_001.ndjson",
    output_path="data/processed/coinbase/level2",
    partition_cols=["product_id", "date"],
)

# Get stats
print(pipeline.reader.get_stats())
print(pipeline.processor.get_stats())
print(pipeline.writer.get_stats())
```

### 2. Process Parquet → Parquet (Reprocessing)

Use ParquetReader for efficient reprocessing with Polars:

```python
from etl.readers import ParquetReader
from etl.processors.coinbase import Level2Processor
from etl.writers import ParquetWriter
from etl.orchestrators import ETLPipeline

# Reprocessing pipeline (uses Polars for efficiency)
pipeline = ETLPipeline(
    reader=ParquetReader(use_polars=True),  # Lazy evaluation
    processors=[
        Level2Processor(
            reconstruct_lob=True,
            compute_features=True,
        ),
    ],
    writer=ParquetWriter(),
)

# Reprocess existing data
pipeline.execute(
    input_path="data/processed/coinbase/level2",  # Read existing Parquet
    output_path="data/processed/coinbase/level2_features",
    partition_cols=["product_id", "date"],
)
```

### 3. Custom Processors

Create your own processors for domain-specific logic:

```python
from etl.processors import BaseProcessor
import polars as pl

class VWAPProcessor(BaseProcessor):
    """Compute VWAP from trades data."""
    
    def __init__(self, window_minutes: int = 5):
        super().__init__()
        self.window_minutes = window_minutes
    
    def process_batch(self, records: list[dict]) -> list[dict]:
        """Compute VWAP using Polars for efficiency."""
        df = pl.DataFrame(records)
        
        # Compute VWAP
        result = (
            df
            .with_columns([
                pl.col("timestamp").str.to_datetime(),
                (pl.col("price") * pl.col("size")).alias("value"),
            ])
            .groupby_dynamic(
                "timestamp",
                every=f"{self.window_minutes}m",
                by=["product_id"],
            )
            .agg([
                (pl.col("value").sum() / pl.col("size").sum()).alias("vwap"),
                pl.col("size").sum().alias("volume"),
                pl.col("price").min().alias("low"),
                pl.col("price").max().alias("high"),
                pl.col("price").first().alias("open"),
                pl.col("price").last().alias("close"),
            ])
        )
        
        self.stats["records_processed"] = len(records)
        self.stats["records_output"] = len(result)
        
        return result.to_dicts()

# Use in pipeline
from etl.readers import ParquetReader
from etl.orchestrators import ETLPipeline

pipeline = ETLPipeline(
    reader=ParquetReader(use_polars=True),
    processors=[VWAPProcessor(window_minutes=5)],
    writer=ParquetWriter(),
)

pipeline.execute(
    input_path="data/processed/coinbase/market_trades",
    output_path="data/processed/coinbase/vwap_5m",
    partition_cols=["product_id", "date"],
)
```

### 4. Chain Multiple Processors

Compose complex transformations:

```python
from etl.processors import ProcessorChain, RawParser
from etl.processors.coinbase import Level2Processor, TradesProcessor

# Create processor chain
chain = ProcessorChain([
    RawParser(source="coinbase"),
    Level2Processor(add_derived_fields=True),
    TradesProcessor(add_derived_fields=True),
])

# Use in pipeline
pipeline = ETLPipeline(
    reader=NDJSONReader(),
    processors=chain,
    writer=ParquetWriter(),
)
```

### 5. Segment Pipeline (Multi-Channel)

Process all channels at once:

```python
from etl.orchestrators import CoinbaseSegmentPipeline

# Create Coinbase segment pipeline
pipeline = CoinbaseSegmentPipeline(
    output_dir="data/processed",
    channel_config={
        "level2": {
            "partition_cols": ["product_id", "date"],
            "processor_options": {"reconstruct_lob": True},
        },
        "market_trades": {
            "partition_cols": ["product_id", "date"],
        },
        "ticker": {
            "partition_cols": ["date", "hour"],
        },
    },
)

# Process single segment (all channels)
pipeline.process_segment("data/segments/segment_001.ndjson")

# Or process specific channels only
pipeline.process_segment(
    "data/segments/segment_001.ndjson",
    channels=["level2", "market_trades"],
)
```

## Output Structure Examples

### Flat Write (No Partitioning)

```
data/processed/coinbase/ticker/
  part_20251126T140512_abc123.parquet
  part_20251126T140830_def456.parquet
```

### Date + Hour Partitioning

```
data/processed/coinbase/ticker/
  date=2025-11-26/
    hour=14/
      part_20251126T140512_abc123.parquet
      part_20251126T140830_def456.parquet
    hour=15/
      part_20251126T150120_ghi789.parquet
```

### Product + Date Partitioning

```
data/processed/coinbase/level2/
  product_id=BTC-USD/
    date=2025-11-26/
      part_20251126T140512_abc123.parquet
      part_20251126T140830_def456.parquet
  product_id=ETH-USD/
    date=2025-11-26/
      part_20251126T140520_jkl012.parquet
```

## Querying Partitioned Data

### Using DuckDB

```python
import duckdb

# Query with partition filtering
df = duckdb.query("""
    SELECT * 
    FROM 'data/processed/coinbase/level2/**/*.parquet'
    WHERE product_id = 'BTC-USD' 
      AND date = '2025-11-26'
""").to_df()
```

### Using Polars

```python
import polars as pl

# Lazy scan (efficient for large datasets)
lf = pl.scan_parquet("data/processed/coinbase/level2/**/*.parquet")

result = (
    lf
    .filter(pl.col("product_id") == "BTC-USD")
    .filter(pl.col("date") == "2025-11-26")
    .collect()
)
```

### Using Pandas

```python
import pandas as pd

# Read with partition filtering
df = pd.read_parquet(
    "data/processed/coinbase/level2",
    filters=[
        ("product_id", "==", "BTC-USD"),
        ("date", "==", "2025-11-26"),
    ]
)
```

## Performance Tips

1. **Use Polars for large datasets**: Set `use_polars=True` in ParquetReader
2. **Batch processing**: Adjust `batch_size` in pipeline.execute()
3. **Partition strategically**: High cardinality first (product_id), then date
4. **Lazy evaluation**: Use `scan_parquet()` instead of `read_parquet()` for large files
5. **Compression**: Use `zstd` for better compression than `snappy` (at cost of speed)

## Migration from Old Code

Old:
```python
# Inline processing in ETLJob
for record in records:
    parsed = parser.parse_record(record)
    writer.write(parsed, source, channel, date_str)
```

New:
```python
# Composable pipelines
pipeline = ETLPipeline(
    reader=NDJSONReader(),
    processors=[RawParser(source), ChannelProcessor()],
    writer=ParquetWriter(),
)
pipeline.execute(input_path, output_path, partition_cols)
```

## Future Enhancements

- [ ] Streaming processors (process records as they arrive)
- [ ] Parallel processing (multi-threaded/multi-process)
- [ ] Cloud writers (S3, GCS, Azure Blob)
- [ ] Delta Lake / Iceberg support
- [ ] Real-time feature computation
- [ ] Advanced LOB reconstruction with full book state
- [ ] Cross-channel processors (combine LOB + trades for features)
