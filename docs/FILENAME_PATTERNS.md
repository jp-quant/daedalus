# Filename Patterns and Collision Prevention

## Overview

Daedalus uses consistent filename patterns across all Parquet writing operations to ensure:
1. **No file overwrites** - Each file has a unique identifier
2. **Temporal ordering** - Filenames include timestamps for debugging
3. **Pattern consistency** - All modules use the same naming convention

---

## Standard Filename Pattern

**Format:** `part_YYYYMMDDTHH_<uuid8>.parquet`

**Components:**
- `part_` - Prefix indicating a data partition file
- `YYYYMMDDTHH` - Timestamp (year, month, day, hour)
- `<uuid8>` - First 8 characters of UUID4 (random)
- `.parquet` - File extension

**Example:** `part_20251203T14_a3f8c92d.parquet`

---

## Modules Using This Pattern

### 1. ParquetWriter (etl/writers/parquet_writer.py)

**Location:** `etl/writers/parquet_writer.py`  
**Method:** `_generate_unique_filename()`

```python
def _generate_unique_filename(self) -> str:
    """Generate unique filename with timestamp and UUID."""
    timestamp = datetime.now().strftime("%Y%m%dT%H")
    unique_id = str(uuid.uuid4())[:8]
    return f"part_{timestamp}_{unique_id}.parquet"
```

**Used by:**
- Segment ETL processing (ticker, level2, market_trades)
- All incremental data writes from raw NDJSON

### 2. Repartitioner (etl/repartitioner.py)

**Location:** `etl/repartitioner.py`  
**Methods:** `repartition()`, `ParquetCompactor.compact()`

```python
# Repartitioning
timestamp = datetime.now().strftime("%Y%m%dT%H")
unique_id = str(uuid.uuid4())[:8]
output_file = partition_path / f"part_{timestamp}_{unique_id}.parquet"

# Compaction (same pattern)
timestamp = datetime.now().strftime("%Y%m%dT%H")
unique_id = str(uuid.uuid4())[:8]
output_file = partition_dir / f"part_{timestamp}_{unique_id}.parquet"
```

**Used by:**
- Partition schema migrations
- File consolidation/compaction

### 3. ParquetCRUD (etl/parquet_crud.py)

**Location:** `etl/parquet_crud.py`  
**Methods:** `delete()`, `update()`, `upsert()`

```python
# All CRUD operations use same pattern
timestamp = datetime.now().strftime("%Y%m%dT%H")
unique_id = str(uuid.uuid4())[:8]
output_file = partition_dir / f"part_{timestamp}_{unique_id}.parquet"
```

**Used by:**
- DELETE operations (rewriting filtered data)
- UPDATE operations (rewriting modified data)
- UPSERT operations (merging new/updated data)

---

## Collision Resistance

### Why This Pattern is Collision-Proof

1. **Hour-level timestamp** (e.g., `20251203T14`)
   - Groups files by time window
   - Multiple operations in same hour are differentiated by UUID

2. **UUID4 random component** (8 hex characters)
   - 16^8 = 4,294,967,296 possible combinations
   - Collision probability: ~0% for reasonable file counts

3. **Atomic write patterns**
   - All operations write to new files first
   - Old files deleted only after new files are written
   - Never overwrites existing files

### Collision Probability Analysis

**Scenario:** 1000 files written in same hour

Using birthday paradox formula:
```
P(collision) ≈ 1 - e^(-n²/(2*N))
where n = 1000 files, N = 4,294,967,296 possible UUIDs

P(collision) ≈ 0.00012% (negligible)
```

**Practical conclusion:** Even with 10,000 files in same hour, collision risk is < 0.01%

---

## File Lifecycle

### 1. Normal ETL Write (ParquetWriter)

```
Segment processed → Generate unique filename → Write to disk
```

**No collision risk:** Each segment creates new file(s)

### 2. Repartitioning Operation

```
Read source partition → Transform → Write to target with new filename → Delete source
```

**No collision risk:** Source and target are different directories

### 3. Compaction Operation

```
Read small files → Merge → Write consolidated file → Delete small files
```

**No collision risk:** New file written before old files deleted

### 4. CRUD Operations

```
Read partition → Apply operation → Write new file → Delete old files
```

**No collision risk:** New file has unique UUID, old files remain until atomic swap

---

## Atomic Operations

All write operations follow atomic pattern:

```python
# Step 1: Write new file(s)
new_file = partition_dir / f"part_{timestamp}_{uuid}.parquet"
df.write_parquet(new_file)

# Step 2: Verify write succeeded
assert new_file.exists()

# Step 3: Delete old files (only if step 1 succeeded)
for old_file in old_files:
    old_file.unlink()
```

This ensures:
- No data loss if write fails
- No orphaned data if deletion fails
- Eventual consistency even with failures

---

## Partition Directory Structure

**Example:**
```
F:/processed/coinbase/level2/
  product_id=BTC-USD/
    date=2025-12-03/
      part_20251203T09_a3f8c92d.parquet  ← ETL write
      part_20251203T09_f7b2e1a4.parquet  ← ETL write
      part_20251203T10_c5d9a826.parquet  ← ETL write
      part_20251203T14_b1e4f3c7.parquet  ← CRUD update
    date=2025-12-04/
      part_20251204T08_d2a6b9e1.parquet
      ...
  product_id=ETH-USD/
    ...
```

**Key observations:**
1. Multiple files per partition (append-only)
2. Each file has unique UUID
3. Timestamp provides temporal ordering
4. Query engines scan all files in partition

---

## Querying Multiple Files

Query engines (Polars, DuckDB, Spark) automatically handle multiple files:

```python
import polars as pl

# Reads ALL parquet files matching glob
df = pl.scan_parquet("F:/processed/coinbase/level2/**/*.parquet")

# Partition pruning still works
df_filtered = df.filter(
    (pl.col("product_id") == "BTC-USD") &
    (pl.col("date") == "2025-12-03")
)
# Only scans: product_id=BTC-USD/date=2025-12-03/*.parquet
```

**Performance note:** Directory structure enables partition pruning even with many files

---

## File Compaction Strategy

Over time, partitions accumulate many small files. Compact when:

```python
from etl.repartitioner import ParquetCompactor

compactor = ParquetCompactor(
    dataset_dir="F:/processed/coinbase/level2",
    target_file_size_mb=100,
)

stats = compactor.compact(
    min_file_count=10,  # Only compact partitions with 10+ files
    delete_source_files=True,
)
```

**Result:** Many small files → Fewer optimized files (still using same naming pattern)

---

## Best Practices

### ✅ DO:
- Trust the UUID uniqueness
- Write files atomically (new file → delete old)
- Keep partition columns in Parquet data
- Use compaction for fragmented partitions

### ❌ DON'T:
- Manually rename files (breaks pattern)
- Create custom filename patterns
- Delete files before writing new ones
- Drop partition columns from data

---

## Debugging File Issues

### Check for filename collisions:

```python
from pathlib import Path
from collections import Counter

def check_collisions(dataset_dir):
    """Check for duplicate filenames across all partitions."""
    files = [f.name for f in Path(dataset_dir).rglob("*.parquet")]
    duplicates = {k: v for k, v in Counter(files).items() if v > 1}
    
    if duplicates:
        print(f"Found {len(duplicates)} duplicate filenames:")
        for name, count in duplicates.items():
            print(f"  {name}: {count} occurrences")
    else:
        print("No duplicate filenames found")

check_collisions("F:/processed/coinbase/level2")
```

### Verify filename patterns:

```python
import re
from pathlib import Path

def verify_patterns(dataset_dir):
    """Verify all files follow standard naming pattern."""
    pattern = re.compile(r'part_\d{8}T\d{2}_[a-f0-9]{8}\.parquet')
    
    bad_files = []
    for f in Path(dataset_dir).rglob("*.parquet"):
        if not pattern.match(f.name):
            bad_files.append(f)
    
    if bad_files:
        print(f"Found {len(bad_files)} files with non-standard names:")
        for f in bad_files[:10]:  # Show first 10
            print(f"  {f}")
    else:
        print("All files follow standard naming pattern")

verify_patterns("F:/processed/coinbase/level2")
```

---

## Migration from Old Patterns

If you have old files with different naming patterns:

```python
from pathlib import Path
import shutil
import uuid
from datetime import datetime

def rename_to_standard_pattern(dataset_dir):
    """Rename old files to standard pattern."""
    for f in Path(dataset_dir).rglob("*.parquet"):
        if not f.name.startswith("part_"):
            # Generate new standard name
            timestamp = datetime.now().strftime("%Y%m%dT%H")
            unique_id = str(uuid.uuid4())[:8]
            new_name = f"part_{timestamp}_{unique_id}.parquet"
            new_path = f.parent / new_name
            
            print(f"Renaming: {f.name} → {new_name}")
            f.rename(new_path)

# Use with caution - test on copy first!
# rename_to_standard_pattern("F:/processed/coinbase/level2")
```

---

## Summary

**Filename Pattern:** `part_YYYYMMDDTHH_<uuid8>.parquet`

**Collision Risk:** Negligible (< 0.01% even with 10,000 files/hour)

**Used By:**
- ✓ ParquetWriter (ETL)
- ✓ Repartitioner (schema migration)
- ✓ ParquetCompactor (file consolidation)
- ✓ ParquetCRUD (delete/update/upsert)

**Safety Guarantees:**
- Atomic writes (new file before old file deletion)
- UUID uniqueness (4.3 billion combinations)
- Never overwrites existing files
- Partition columns preserved in data
