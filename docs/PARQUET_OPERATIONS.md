# Parquet Operations Guide

Complete guide for managing Parquet datasets in FluxForge:
1. **Repartitioning** - Change partition schema
2. **Compaction** - Optimize file sizes
3. **CRUD Operations** - Delete, Update, Upsert

**Important:** FluxForge stores partition columns IN the Parquet data (not just in directory names).
This differs from strict Hive partitioning where columns are dropped. Our approach provides:
- Data integrity (files are standalone queryable)
- Directory-based partition pruning (query optimization)
- No need for `hive_partitioning=True` flag when querying

---

## Table of Contents
- [Terminology](#terminology)
- [Repartitioning](#repartitioning)
- [Compaction](#compaction)
- [CRUD Operations](#crud-operations)
- [Best Practices](#best-practices)
- [Common Workflows](#common-workflows)

---

## Terminology

### Repartitioning
**Changing the partition column structure** of a dataset.

**Example:** `['product_id', 'date']` → `['product_id', 'year', 'month', 'day', 'hour']`

**Why?**
- Finer-grained partitioning improves query performance
- Coarser partitioning reduces metadata overhead
- Add/remove partition columns for different access patterns

**When to use:**
- Initial partition schema doesn't match query patterns
- Want to add time granularity (hour, minute)
- Need to consolidate deeply nested partitions

### Compaction
**Consolidating many small files into fewer, larger files** within the same partition schema.

**Example:** Merge 100 x 1MB files → 1 x 100MB file

**Why?**
- Reduces file system metadata overhead
- Improves query planning speed
- Better compression ratios
- Fewer S3 API calls (if using cloud storage)

**When to use:**
- After many incremental writes (segment-by-segment ETL)
- Partitions have 10+ small files
- Query performance degrades due to file count

### CRUD Operations
**Modifying data within Parquet files** (immutable format requires read-modify-write).

**DELETE:** Remove rows matching condition  
**UPDATE:** Modify existing rows  
**UPSERT:** Insert or update based on key columns

---

## Repartitioning

### Basic Usage

```python
from etl.repartitioner import Repartitioner

repartitioner = Repartitioner(
    source_dir="F:/processed/coinbase/level2",
    target_dir="F:/processed/coinbase/level2_new",
    new_partition_cols=['product_id', 'year', 'month', 'day', 'hour'],
)

# Estimate before executing
estimates = repartitioner.estimate_size()
print(f"Will create {estimates['num_partitions']} partitions")

# Execute
stats = repartitioner.repartition(
    delete_source=False,  # Keep original (delete manually after verifying)
    validate=True,        # Verify row counts match
    dry_run=False,        # Set True to preview only
)
```

### With Data Transformation

Apply transformations during repartitioning:

```python
import polars as pl

def add_minute_field(df: pl.DataFrame) -> pl.DataFrame:
    """Add minute field during repartition."""
    return df.with_columns(
        pl.col("timestamp").dt.minute().alias("minute")
    )

stats = repartitioner.repartition(
    transform_fn=add_minute_field,
    delete_source=False,
    validate=True,
)
```

### Production Workflow

Safe migration with backup and rollback:

```python
from pathlib import Path
import shutil

source = Path("F:/processed/coinbase/level2")
temp = Path("F:/processed/coinbase/level2_temp")
backup = Path("F:/processed/coinbase/level2_backup")

# Step 1: Repartition to temp
repartitioner = Repartitioner(str(source), str(temp), new_partition_cols=[...])
stats = repartitioner.repartition(delete_source=False, validate=True)

if stats['errors'] > 0:
    print("❌ Repartitioning failed!")
    shutil.rmtree(temp)
    exit(1)

# Step 2: Backup original
source.rename(backup)

# Step 3: Move temp to production
temp.rename(source)

# Step 4: Verify queries work
import polars as pl
df = pl.scan_parquet(str(source / "**/*.parquet"))
count = df.select(pl.count()).collect().item()
print(f"✓ Query successful: {count:,} rows")

# Step 5: Delete backup (after confirming everything works)
# shutil.rmtree(backup)  # Manual cleanup
```

### Common Repartitioning Patterns

#### Pattern 1: Add Time Granularity
From: `['product_id', 'date']`  
To: `['product_id', 'year', 'month', 'day', 'hour']`

**Use case:** Hourly queries are common, finer partitioning improves performance.

#### Pattern 2: Add Product Partitioning
From: `['date', 'hour']`  
To: `['product_id', 'date', 'hour']`

**Use case:** Per-product backtests, want partition pruning by product_id.

#### Pattern 3: Simplify Nested Partitions
From: `['product_id', 'year', 'month', 'day', 'hour', 'minute']`  
To: `['product_id', 'date']`

**Use case:** Too many small partitions, causing metadata overhead.

---

## Compaction

### Basic Usage

```python
from etl.repartitioner import ParquetCompactor

compactor = ParquetCompactor(
    dataset_dir="F:/processed/coinbase/market_trades",
    target_file_size_mb=100,  # Target 100MB files
)

stats = compactor.compact(
    min_file_count=5,          # Only compact partitions with 5+ files
    max_file_size_mb=20,       # Only consolidate files < 20MB
    delete_source_files=True,  # Delete originals after compaction
    dry_run=False,
)

print(f"✓ Compacted {stats['files_before']} → {stats['files_after']} files")
print(f"  Space saved: {(stats['bytes_before'] - stats['bytes_after']) / (1024**2):.1f} MB")
```

### When to Compact

**Symptoms of fragmentation:**
- Query planning takes seconds (instead of milliseconds)
- `ls -l partition/` shows 100+ files
- Each segment ETL creates new small files

**Recommended schedule:**
- After bulk historical reprocessing
- Weekly for high-volume channels (level2, trades)
- Monthly for low-volume channels (ticker)

### Compaction vs Repartitioning

| Operation | Changes Schema? | Changes Partitions? | Use Case |
|-----------|----------------|---------------------|----------|
| **Compaction** | No | No | Merge small files |
| **Repartitioning** | Yes | Yes | Change partition columns |

**Can do both:** Repartition, then compact the new dataset.

---

## CRUD Operations

### DELETE

Remove rows matching filter condition.

```python
from etl.parquet_crud import ParquetCRUD

crud = ParquetCRUD(
    dataset_dir="F:/processed/coinbase/level2",
    backup_dir="F:/backups/level2",  # Optional backup
)

# Delete specific rows
stats = crud.delete(
    filter_expr=(
        (pl.col("product_id") == "BTC-USD") & 
        (pl.col("date") == "2025-11-26")
    ),
    partition_filter={"product_id": "BTC-USD", "date": "2025-11-26"},
    dry_run=False,
    create_backup=True,
)

print(f"✓ Deleted {stats['rows_deleted']:,} rows")
```

**Partition filter** narrows down which partitions to scan (performance optimization).

### UPDATE

Modify existing rows.

```python
# Update literal value
stats = crud.update(
    filter_expr=pl.col("side") == "offer",
    update_expr={"side_normalized": "ask"},
    partition_filter={"product_id": "BTC-USD"},
    dry_run=False,
    create_backup=True,
)

# Update with expression
stats = crud.update(
    filter_expr=pl.col("spread").is_null(),
    update_expr={
        "spread": pl.col("best_ask") - pl.col("best_bid"),
        "spread_bps": ((pl.col("best_ask") - pl.col("best_bid")) / pl.col("mid_price")) * 10000,
    },
    partition_filter={"date": "2025-11-26"},
)
```

### UPSERT

Insert or update based on key columns.

```python
import polars as pl

# New/corrected data
new_trades = pl.DataFrame({
    "product_id": ["BTC-USD", "BTC-USD"],
    "trade_id": ["12345", "99999"],  # 12345 exists, 99999 new
    "price": [50000.0, 50100.0],
    "size": [1.5, 2.0],
    "date": ["2025-11-26", "2025-11-26"],
})

stats = crud.upsert(
    new_data=new_trades,
    key_cols=["product_id", "trade_id"],  # Unique identifier
    partition_cols=["product_id", "date"],
    update_cols=["price", "size"],  # Only update these columns
    dry_run=False,
    create_backup=True,
)

print(f"✓ Inserted: {stats['rows_inserted']}, Updated: {stats['rows_updated']}")
```

**Key columns:** Determine uniqueness (e.g., `trade_id`, `sequence_num`)  
**Update columns:** Which columns to modify on conflict (None = all non-key columns)

---

## Best Practices

### 1. Always Use Backups

```python
crud = ParquetCRUD(
    dataset_dir="F:/processed/coinbase/level2",
    backup_dir="F:/backups/level2",  # ✓ Enable backups
)

stats = crud.delete(..., create_backup=True)  # ✓ Create backup per operation
```

### 2. Use Dry Run First

```python
# Preview changes without executing
stats = crud.delete(filter_expr=..., dry_run=True)
print(f"Would delete {stats['rows_deleted']} rows")

# Confirm, then execute
stats = crud.delete(filter_expr=..., dry_run=False)
```

### 3. Leverage Partition Filters

```python
# ❌ Slow - scans ALL partitions
stats = crud.delete(
    filter_expr=pl.col("product_id") == "BTC-USD",
)

# ✓ Fast - only scans BTC-USD partitions
stats = crud.delete(
    filter_expr=pl.col("product_id") == "BTC-USD",
    partition_filter={"product_id": "BTC-USD"},  # ✓ Partition pruning
)
```

### 4. Validate Row Counts

```python
# Before operation
df_before = pl.scan_parquet(dataset_dir / "**/*.parquet")
count_before = df_before.select(pl.count()).collect().item()

# Perform operation
stats = crud.delete(...)

# After operation
df_after = pl.scan_parquet(dataset_dir / "**/*.parquet")
count_after = df_after.select(pl.count()).collect().item()

assert count_after == count_before - stats['rows_deleted']
```

### 5. Monitor File Counts

```python
from pathlib import Path

def count_files(dataset_dir):
    """Count parquet files per partition."""
    partitions = {}
    for f in Path(dataset_dir).rglob("*.parquet"):
        partition = f.parent
        partitions[partition] = partitions.get(partition, 0) + 1
    return partitions

# Check fragmentation
file_counts = count_files("F:/processed/coinbase/level2")
fragmented = {k: v for k, v in file_counts.items() if v > 10}
print(f"Partitions with 10+ files: {len(fragmented)}")
```

---

## Common Workflows

### Workflow 1: Fix Data Quality Issues

```python
crud = ParquetCRUD(dataset_dir="...", backup_dir="...")

# Step 1: Delete outliers
crud.delete(
    filter_expr=pl.col("spread_bps") > 500,
    partition_filter={"product_id": "BTC-USD"},
    create_backup=True,
)

# Step 2: Fix categorical values
crud.update(
    filter_expr=pl.col("side") == "offer",
    update_expr={"side_normalized": "ask"},
    partition_filter={"product_id": "BTC-USD"},
)

# Step 3: Recalculate derived fields
crud.update(
    filter_expr=pl.col("spread").is_not_null(),
    update_expr={
        "spread": pl.col("best_ask") - pl.col("best_bid")
    },
)
```

### Workflow 2: Deduplication

```python
# Read and find duplicates
df = pl.scan_parquet(dataset_dir / "**/*.parquet")
duplicates = df.groupby(["product_id", "trade_id"]).agg(pl.count().alias("count"))
duplicates = duplicates.filter(pl.col("count") > 1).collect()

print(f"Found {len(duplicates)} duplicate trade IDs")

# Deduplicate
df_deduped = df.unique(subset=["product_id", "trade_id"], keep="first").collect()

# Upsert deduplicated data (replaces duplicates)
crud.upsert(
    new_data=df_deduped,
    key_cols=["product_id", "trade_id"],
    partition_cols=["product_id", "date"],
    create_backup=True,
)
```

### Workflow 3: Incremental Data Loading

```python
# Load new data from external source
new_data = pl.read_parquet("external_data.parquet")

# Upsert (insert new, update existing)
crud.upsert(
    new_data=new_data,
    key_cols=["product_id", "sequence_num"],
    partition_cols=["product_id", "date"],
    update_cols=["price", "volume"],  # Only update these fields
)
```

### Workflow 4: Schema Migration

```python
# Repartition with transformation
def add_hour_minute(df):
    return df.with_columns([
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.minute().alias("minute"),
    ])

repartitioner = Repartitioner(
    source_dir="F:/processed/coinbase/level2",
    target_dir="F:/processed/coinbase/level2_new",
    new_partition_cols=['product_id', 'year', 'month', 'day', 'hour'],
)

stats = repartitioner.repartition(
    transform_fn=add_hour_minute,
    delete_source=False,
    validate=True,
)

# Compact new dataset
compactor = ParquetCompactor(
    dataset_dir="F:/processed/coinbase/level2_new",
    target_file_size_mb=100,
)

compactor.compact(min_file_count=5, delete_source_files=True)
```

---

## Performance Tips

### 1. Partition Filters Are Critical

**Slow (scans all partitions):**
```python
crud.delete(filter_expr=pl.col("product_id") == "BTC-USD")
```

**Fast (only scans BTC-USD partitions):**
```python
crud.delete(
    filter_expr=pl.col("product_id") == "BTC-USD",
    partition_filter={"product_id": "BTC-USD"},
)
```

### 2. Batch Operations

Instead of:
```python
for product in products:
    crud.delete(filter_expr=pl.col("product_id") == product)
```

Use:
```python
crud.delete(filter_expr=pl.col("product_id").is_in(products))
```

### 3. Use Polars Lazy Evaluation

```python
# Lazy scan (doesn't load into memory)
df = pl.scan_parquet(dataset_dir / "**/*.parquet")

# Operations are lazy (query optimization)
filtered = df.filter(pl.col("product_id") == "BTC-USD")

# Only loads data when needed
result = filtered.collect()
```

### 4. Monitor Memory Usage

For large datasets:
```python
# Process in batches
repartitioner = Repartitioner(
    source_dir="...",
    target_dir="...",
    new_partition_cols=[...],
    batch_size=1_000_000,  # Process 1M rows at a time
)
```

---

## Troubleshooting

### Issue: "Partition columns not found in data"

**Cause:** Partition columns missing in DataFrame.

**Solution:** Ensure columns exist before repartitioning/upserting.

```python
# Check columns
print(df.columns)

# Add missing columns
df = df.with_columns([
    pl.col("timestamp").dt.year().alias("year"),
    pl.col("timestamp").dt.month().alias("month"),
])
```

### Issue: "Row count mismatch after repartition"

**Cause:** Data loss during transformation or filter applied accidentally.

**Solution:** Check transform_fn, ensure it doesn't filter rows.

```python
# Debug transform
def debug_transform(df):
    print(f"Input rows: {len(df)}")
    df_out = df.with_columns(...)
    print(f"Output rows: {len(df_out)}")
    return df_out
```

### Issue: "Out of memory during compaction"

**Cause:** Partition too large to fit in memory.

**Solution:** Split partition or use streaming compaction (not yet implemented).

```python
# Check partition size
df = pl.read_parquet(partition_dir / "*.parquet")
print(f"Rows: {len(df):,}, Memory: {df.estimated_size() / (1024**2):.1f} MB")
```

---

## API Reference

### Repartitioner

```python
Repartitioner(
    source_dir: str,              # Existing dataset
    target_dir: str,              # New dataset location
    new_partition_cols: List[str],# New partition schema
    compression: str = "snappy",  # Compression codec
    batch_size: int = 1_000_000,  # Rows per batch
)

.repartition(
    delete_source: bool = False,  # Delete source after success
    validate: bool = True,        # Verify row counts
    dry_run: bool = False,        # Preview only
    transform_fn: Callable = None,# Optional transformation
) -> Dict[str, Any]

.estimate_size() -> Dict[str, Any]
```

### ParquetCompactor

```python
ParquetCompactor(
    dataset_dir: str,                # Dataset to compact
    target_file_size_mb: int = 100, # Target file size
    compression: str = "snappy",     # Compression codec
)

.compact(
    min_file_count: int = 2,         # Min files to trigger
    max_file_size_mb: int = None,    # Only compact small files
    delete_source_files: bool = True,# Delete after compaction
    dry_run: bool = False,           # Preview only
) -> Dict[str, Any]
```

### ParquetCRUD

```python
ParquetCRUD(
    dataset_dir: str,              # Dataset directory
    compression: str = "snappy",   # Compression codec
    backup_dir: str = None,        # Optional backup location
)

.delete(
    filter_expr: pl.Expr,          # Rows to delete
    partition_filter: Dict = None, # Partition pruning
    dry_run: bool = False,         # Preview only
    create_backup: bool = True,    # Backup before delete
) -> Dict[str, Any]

.update(
    filter_expr: pl.Expr,          # Rows to update
    update_expr: Dict,             # column → value/expression
    partition_filter: Dict = None, # Partition pruning
    dry_run: bool = False,         # Preview only
    create_backup: bool = True,    # Backup before update
) -> Dict[str, Any]

.upsert(
    new_data: pl.DataFrame,        # Data to insert/update
    key_cols: List[str],           # Unique identifier columns
    partition_cols: List[str],     # Partition columns
    update_cols: List[str] = None, # Columns to update (None = all)
    dry_run: bool = False,         # Preview only
    create_backup: bool = True,    # Backup before upsert
) -> Dict[str, Any]
```

---

## See Also

- [ETL_ARCHITECTURE.md](ETL_ARCHITECTURE.md) - Overall ETL system design
- [examples/repartition_examples.py](../examples/repartition_examples.py) - Runnable examples
- [examples/parquet_crud_examples.py](../examples/parquet_crud_examples.py) - CRUD examples
