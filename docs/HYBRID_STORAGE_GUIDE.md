# Hybrid Storage Architecture Guide

## Overview

FluxForge uses **explicit storage configuration** where each layer (ingestion, ETL input, ETL output) can have its own storage backend. This provides maximum flexibility to optimize for performance, cost, and durability.

## Architecture

### Explicit Configuration

All storage is configured per-layer in `config/config.yaml`:

```yaml
storage:
  # Where ingestion writes raw segments
  ingestion_storage:
    backend: "local"  # or "s3"
    base_dir: "F:/"   # or bucket name
  
  # Where ETL reads raw segments from
  etl_storage_input:
    backend: "local"
    base_dir: "F:/"
  
  # Where ETL writes processed data
  etl_storage_output:
    backend: "local"
    base_dir: "F:/"
```

This explicit approach eliminates confusion and gives full control.

---

## Common Storage Patterns

### Pattern 1: All Local (Development/Testing)

**Use Case:** Fast development, testing, local analysis

**Configuration:**
```yaml
storage:
  ingestion_storage:
    backend: "local"
    base_dir: "F:/"
  
  etl_storage_input:
    backend: "local"
    base_dir: "F:/"
  
  etl_storage_output:
    backend: "local"
    base_dir: "F:/"
```

**Benefits:**
- ✅ No cloud costs
- ✅ Fast performance
- ✅ Easy debugging
- ✅ No network dependency

**Drawbacks:**
- ❌ Limited by disk space
- ❌ No durability guarantees
- ❌ Single machine only

---

### Pattern 2: Ingest Local → ETL to S3 (RECOMMENDED)

**Use Case:** Production - fast local ingestion, durable cloud storage

**Configuration:**
```yaml
storage:
  ingestion_storage:
    backend: "local"
    base_dir: "F:/"
  
  etl_storage_input:
    backend: "local"
    base_dir: "F:/"
  
  etl_storage_output:
    backend: "s3"
    base_dir: "market-data-vault"
    s3:
      bucket: "market-data-vault"
      region: "us-east-1"
```

**Benefits:**
- ✅ Fast ingestion (no network latency)
- ✅ Durable processed data
- ✅ Cost-effective (fewer S3 writes)
- ✅ Cloud query capabilities

**How it works:**
1. Ingestion writes raw NDJSON segments to local `F:/raw/active/` and `F:/raw/ready/`
2. ETL reads from local `F:/raw/ready/`
3. ETL writes processed Parquet to `s3://market-data-vault/processed/`

---

### Pattern 3: All S3 (Fully Cloud)

**Use Case:** Distributed systems, serverless, multi-region

**Configuration:**
```yaml
storage:
  ingestion_storage:
    backend: "s3"
    base_dir: "market-data-vault"
    s3:
      bucket: "market-data-vault"
      region: "us-east-1"
  
  etl_storage_input:
    backend: "s3"
    base_dir: "market-data-vault"
    s3:
      bucket: "market-data-vault"
      region: "us-east-1"
  
  etl_storage_output:
    backend: "s3"
    base_dir: "market-data-vault"
    s3:
      bucket: "market-data-vault"
      region: "us-east-1"
```

**Benefits:**
- ✅ Fully cloud-native
- ✅ Can run ingestion/ETL on different machines
- ✅ Multi-region support
- ✅ No local disk requirements

**Drawbacks:**
- ❌ Network latency on ingestion
- ❌ Higher S3 costs (PUT requests for raw segments)

---

## Usage

### Start Ingestion

```bash
python scripts/run_ingestion.py --source ccxt
```

Configuration is read automatically from `config/config.yaml`.

### Start ETL

```bash
python scripts/run_etl_watcher.py --poll-interval 30
```

The ETL watcher reads from `etl_storage_input` and writes to `etl_storage_output`.

---

## Directory Structure

### Local Storage
```
F:/
├── raw/
│   ├── active/ccxt/
│   │   └── segment_20251209T14_00001.ndjson
│   ├── ready/ccxt/
│   │   └── segment_20251209T14_00002.ndjson
│   └── processing/ccxt/
│       └── (temp during ETL)
└── processed/ccxt/
    ├── ticker/exchange=binanceus/symbol=BTC-USDT/date=2025-12-09/
    ├── trades/...
    └── orderbook/
        ├── hf/...
        └── bars/...
```

### S3 Storage
```
s3://market-data-vault/
├── raw/
│   ├── active/ccxt/
│   ├── ready/ccxt/
│   └── processing/ccxt/
└── processed/ccxt/
    ├── ticker/exchange=binanceus/symbol=BTC-USDT/date=2025-12-09/
    ├── trades/...
    └── orderbook/
        ├── hf/...
        └── bars/...
```

---

## S3 Configuration Details

### AWS Credentials

Configure AWS credentials via environment variables or IAM roles:

```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID="your-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"

# Option 2: AWS config file (~/.aws/credentials)
# Option 3: IAM role (recommended for EC2/ECS)
```

### S3 Bucket Setup

```bash
# Create bucket
aws s3 mb s3://market-data-vault --region us-east-1

# Enable versioning (optional but recommended)
aws s3api put-bucket-versioning \
    --bucket market-data-vault \
    --versioning-configuration Status=Enabled
```

---

## Path Resolution

The storage factory handles path resolution automatically:

```python
from config import load_config
from storage.factory import (
    create_storage_backend,
    get_ingestion_path,
    get_etl_input_path,
    get_etl_output_path,
)

config = load_config()

# Ingestion paths
ingestion_storage = create_storage_backend(config.storage.ingestion_storage)
active_path = get_ingestion_path(config, "ccxt", state="active")
ready_path = get_ingestion_path(config, "ccxt", state="ready")

# ETL paths
etl_input_storage = create_storage_backend(config.storage.etl_storage_input)
etl_output_storage = create_storage_backend(config.storage.etl_storage_output)
input_path = get_etl_input_path(config, "ccxt")
output_path = get_etl_output_path(config, "ccxt")
```

---

## Benefits of Explicit Configuration

1. **Clarity**: Each layer's storage is explicitly defined
2. **Flexibility**: Mix and match local/S3 per layer
3. **Cost Optimization**: Keep raw data local, processed in cloud
4. **Performance**: Optimize for latency vs durability
5. **Debugging**: Clear separation of concerns
