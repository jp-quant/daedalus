# Daedalus Unified Storage Architecture

## Overview

Daedalus now uses a unified storage abstraction that works seamlessly with both local filesystem and AWS S3. This enables flexible deployment scenarios including all-local, all-S3, or hybrid configurations.

## Key Changes

### 1. **Unified Storage Backend**
- **`StorageBackend`** base class: Abstract interface for all storage operations
- **`LocalStorage`**: Local filesystem implementation
- **`S3Storage`**: AWS S3 implementation
- All paths are relative to a storage root (local base_dir or S3 bucket)

### 2. **Simplified Configuration**
- Single `storage.base_dir` concept (works for both local and S3)
- Customizable path structure via `storage.paths`
- No more hardcoded "F:/" or "raw/ready/" paths
- Ingestion and ETL layers use the same storage configuration

### 3. **Unified Writers**
- `LogWriter`: Works with any `StorageBackend` (local or S3)
- No separate `S3LogWriter` needed
- Same code paths for both storage types

### 4. **Orchestrator Pattern**
- `IngestionPipeline` orchestrator (`ingestion/orchestrators/`)
- Matches ETL orchestrator pattern for consistency
- Cleaner separation of concerns

## Configuration Examples

### Example 1: All-Local Storage

```yaml
storage:
  backend: "local"
  base_dir: "F:/"  # Or "./data" for relative path
  
  paths:
    raw_dir: "raw"
    active_subdir: "active"
    ready_subdir: "ready"
    processing_subdir: "processing"
    processed_dir: "processed"

# Result structure:
# F:/raw/active/coinbase/segment_*.ndjson
# F:/raw/ready/coinbase/segment_*.ndjson
# F:/processed/coinbase/ticker/*.parquet
```

### Example 2: All-S3 Storage

```yaml
storage:
  backend: "s3"
  base_dir: "my-datalake"  # S3 bucket name (base_dir acts as bucket when s3)
  
  paths:
    raw_dir: "raw"
    active_subdir: "active"
    ready_subdir: "ready"
    processing_subdir: "processing"
    processed_dir: "processed"
  
  s3:
    bucket: "my-datalake"  # Must match base_dir
    region: "us-east-1"
    aws_access_key_id: null  # Uses environment/IAM role
    aws_secret_access_key: null

# Result structure:
# s3://my-datalake/raw/active/coinbase/segment_*.ndjson
# s3://my-datalake/raw/ready/coinbase/segment_*.ndjson
# s3://my-datalake/processed/coinbase/ticker/*.parquet
```

### Example 3: Hybrid - Ingest Local, ETL to S3

This advanced scenario requires two storage backends. Implementation requires:
1. Separate storage configs for ingestion and ETL
2. ETL reads from local ready/ and writes to S3

**NOTE**: This requires additional configuration structure (to be implemented):

```yaml
# Proposed structure for hybrid storage:
ingestion:
  storage:
    backend: "local"
    base_dir: "F:/"
    paths:
      raw_dir: "raw"
      active_subdir: "active"
      ready_subdir: "ready"

etl:
  storage_input:  # Read from local
    backend: "local"
    base_dir: "F:/"
    paths:
      raw_dir: "raw"
      ready_subdir: "ready"
  
  storage_output:  # Write to S3
    backend: "s3"
    base_dir: "my-datalake"
    paths:
      processed_dir: "processed"
    s3:
      bucket: "my-datalake"
      region: "us-east-1"
```

### Example 4: Custom Path Structure

```yaml
storage:
  backend: "local"
  base_dir: "./data"
  
  paths:
    raw_dir: "ingested"           # Instead of "raw"
    active_subdir: "streaming"     # Instead of "active"
    ready_subdir: "completed"      # Instead of "ready"
    processing_subdir: "transforming"
    processed_dir: "curated"       # Instead of "processed"

# Result structure:
# ./data/ingested/streaming/coinbase/segment_*.ndjson
# ./data/ingested/completed/coinbase/segment_*.ndjson
# ./data/curated/coinbase/ticker/*.parquet
```

## Usage Examples

### Running Ingestion

```python
from config import load_config
from ingestion.orchestrators import IngestionPipeline

# Load config (reads storage.backend setting)
config = load_config()

# Create pipeline (automatically uses correct storage)
pipeline = IngestionPipeline(config)

# Start ingestion
await pipeline.start()
await pipeline.wait_for_shutdown()
await pipeline.stop()
```

**No code changes needed** - just update config.yaml to switch between local and S3!

### Running ETL

```python
from config import load_config
from storage.factory import create_storage_backend, get_etl_input_path, get_etl_output_path
from etl.job import ETLJob

config = load_config()

# Create storage backend
storage = create_storage_backend(config)

# Get paths (automatically correct for local or S3)
input_path = get_etl_input_path(config, "coinbase")
output_path = get_etl_output_path(config, "coinbase")

# Create ETL job
job = ETLJob(
    storage=storage,
    input_path=input_path,
    output_path=output_path,
    delete_after_processing=config.etl.delete_after_processing,
)

# Process segments
job.process_all_segments()
```

## Migration Guide

### From Old Architecture → New Architecture

**1. Update config.yaml:**

OLD:
```yaml
ingestion:
  output_dir: "F://raw"

etl:
  input_dir: "F://raw/ready/coinbase"
  output_dir: "F://processed"
  processing_dir: "F://raw/processing"
```

NEW:
```yaml
storage:
  backend: "local"
  base_dir: "F:/"
  paths:
    raw_dir: "raw"
    active_subdir: "active"
    ready_subdir: "ready"
    processing_subdir: "processing"
    processed_dir: "processed"

ingestion:
  # output_dir removed - uses storage.paths

etl:
  # input_dir, output_dir, processing_dir removed - uses storage.paths
```

**2. Update imports in your code:**

OLD:
```python
from ingestion.writers import LogWriter, S3LogWriter
from storage.factory import StorageFactory

# Create writer based on backend
if config.storage.backend == "local":
    writer = LogWriter(output_dir="F:/raw", ...)
else:
    writer = S3LogWriter(s3_manager=s3_mgr, ...)
```

NEW:
```python
from storage.factory import create_storage_backend, get_ingestion_path
from ingestion.writers import LogWriter

# Create storage backend (local or S3)
storage = create_storage_backend(config)

# Get paths
active_path = get_ingestion_path(config, "coinbase", state="active")
ready_path = get_ingestion_path(config, "coinbase", state="ready")

# Create unified writer (works with both!)
writer = LogWriter(
    storage=storage,
    active_path=active_path,
    ready_path=ready_path,
    source_name="coinbase",
    ...
)
```

**3. Run scripts:**

Scripts are already updated to use the new architecture:
- `scripts/run_ingestion.py` uses `IngestionPipeline` orchestrator
- `scripts/etl/run_watcher.py` is the current continuous ETL watcher

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                      Configuration                           │
│  storage.backend: "local" | "s3"                             │
│  storage.base_dir: "F:/" | "my-bucket"                       │
│  storage.paths: {raw_dir, active_subdir, ...}                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       v
┌─────────────────────────────────────────────────────────────┐
│              Storage Factory (factory.py)                    │
│  create_storage_backend() → LocalStorage | S3Storage         │
│  get_ingestion_path(), get_etl_input_path()                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
         ┌─────────────┴─────────────┐
         v                           v
┌─────────────────────┐    ┌──────────────────────┐
│   LocalStorage      │    │     S3Storage         │
│  - base_path: "F:/" │    │  - bucket: "my-lake"  │
│  - Uses Path/open() │    │  - Uses boto3/s3fs    │
└─────────────────────┘    └──────────────────────┘
         │                           │
         └─────────────┬─────────────┘
                       │
                       v
         ┌─────────────────────────────┐
         │  LogWriter (unified)         │
         │  - storage: StorageBackend   │
         │  - active_path: relative     │
         │  - ready_path: relative      │
         └─────────────────────────────┘
                       │
                       v
              ┌────────────────┐
              │ IngestionPipeline│
              │ (orchestrator)   │
              └────────────────┘
```

## Path Resolution Examples

### Local Storage
```python
base_dir = "F:/"
active_path = "raw/active/coinbase"

# LogWriter creates segments at:
# F:/raw/active/coinbase/segment_20251203T14_00001.ndjson

# After rotation, moves to:
# F:/raw/ready/coinbase/segment_20251203T14_00001.ndjson
```

### S3 Storage
```python
bucket = "my-datalake"
active_path = "raw/active/coinbase"

# LogWriter creates segments at:
# s3://my-datalake/raw/active/coinbase/segment_20251203T14_00001.ndjson

# After rotation, writes directly to:
# s3://my-datalake/raw/ready/coinbase/segment_20251203T14_00001.ndjson
```

## Benefits of New Architecture

1. **Flexibility**: Switch between local and S3 with one config line
2. **Consistency**: Same code paths for both storage types
3. **Simplicity**: No separate LogWriter vs S3LogWriter
4. **Maintainability**: Single source of truth for storage logic
5. **Testability**: Easy to mock StorageBackend for testing
6. **Extensibility**: Add Azure, GCS, etc. by implementing StorageBackend

## Implementation Status

All core unified storage functionality is complete:

- ✅ `StorageBackend` abstraction (`storage/base.py`)
- ✅ `LocalStorage` and `S3Storage` implementations
- ✅ `LogWriter` unified implementation (local + S3)
- ✅ `ParquetWriter` unified implementation (local + S3)
- ✅ `IngestionPipeline` orchestrator
- ✅ `ETLJob` uses unified storage
- ✅ Hybrid storage support (separate ingestion/ETL configs)
- ✅ Configuration system with explicit per-layer storage

## Testing

### Test Local Storage
```bash
# Edit config/config.yaml:
# storage.backend: "local"
# storage.base_dir: "./test_data"

python scripts/run_ingestion.py
```

### Test S3 Storage
```bash
# Edit config/config.yaml:
# storage.backend: "s3"
# storage.base_dir: "my-test-bucket"
# Configure AWS credentials

python scripts/run_ingestion.py
```

### Verify Path Structure
```python
from config import load_config
from storage.factory import create_storage_backend, get_ingestion_path

config = load_config()
storage = create_storage_backend(config)

active = get_ingestion_path(config, "coinbase", "active")
ready = get_ingestion_path(config, "coinbase", "ready")

print(f"Backend: {storage.backend_type}")
print(f"Active:  {storage.get_full_path(active)}")
print(f"Ready:   {storage.get_full_path(ready)}")
```
