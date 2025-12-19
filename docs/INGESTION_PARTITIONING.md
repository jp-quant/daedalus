# Ingestion Partitioning Guide

The ingestion pipeline supports Hive-style partitioning for raw data, enabling efficient downstream ETL processing.

## Overview

Raw data is partitioned during ingestion using a configurable directory structure:

```
raw/ready/ccxt/{channel}/exchange={exchange}/symbol={symbol}/year={y}/month={m}/day={d}/segment_*.parquet
```

### Benefits

1. **Efficient ETL**: ETL can read only relevant partitions instead of scanning all data
2. **Parallel Processing**: Different partitions can be processed independently
3. **Cost Savings**: On S3, partition pruning reduces data scanned significantly
4. **Organized Storage**: Data is naturally organized by source and time

## Configuration

Configure partitioning in `config.yaml`:

```yaml
ingestion:
  # Partition columns (order matters for path structure)
  partition_by:
    - "exchange"   # First level: exchange=binanceus
    - "symbol"     # Second level: symbol=BTC~USDT
  
  # Add time partitions (year/month/day)
  enable_date_partition: true
```

### Partition Options

| Option | Default | Description |
|--------|---------|-------------|
| `partition_by` | `["exchange", "symbol"]` | List of partition columns |
| `enable_date_partition` | `true` | Add year/month/day partitions |

### Example Directory Structures

**Full partitioning (default)**:
```
raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC~USDT/year=2025/month=6/day=19/segment_*.parquet
```

**Exchange-only + date**:
```yaml
partition_by: ["exchange"]
enable_date_partition: true
```
```
raw/ready/ccxt/orderbook/exchange=binanceus/year=2025/month=6/day=19/segment_*.parquet
```

**No date partitions**:
```yaml
partition_by: ["exchange", "symbol"]
enable_date_partition: false
```
```
raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC~USDT/segment_*.parquet
```

## How It Works

### Partition Key Extraction

For each record, the writer extracts:
1. **Channel**: From `type` field (orderbook, ticker, trades)
2. **Exchange**: From `exchange` field 
3. **Symbol**: From `symbol` field (sanitized: `/` → `~`)
4. **Date**: From `timestamp` or `capture_ts` field

### Symbol Sanitization

Symbols containing `/` are converted to `~` for filesystem compatibility:
- `BTC/USDT` → `BTC~USDT`
- `ETH/USD` → `ETH~USD`

### Date Extraction

The date is extracted from records in priority order:
1. `capture_ts` field (ISO format or datetime)
2. `timestamp` field (epoch milliseconds)
3. Current UTC time (fallback)

## ETL Integration

The batched ETL processor reads partitioned data efficiently:

```python
# Read only BTC/USDT from binanceus for a specific date
df = pl.scan_parquet(
    "raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC~USDT/year=2025/month=6/day=19/*.parquet"
).collect()
```

### Partition Discovery

Use the ETL batched processor to automatically discover and process partitions:

```bash
python scripts/run_orderbook_features_batched.py --discover
```

## Testing

Run the partitioning tests:

```bash
python scripts/test_ingestion_partitioning.py
```

This validates:
- Partition path building
- Partition key extraction from records
- Symbol sanitization
- Date parsing from multiple formats

## Migration

### From Unpartitioned Data

If you have existing unpartitioned data, you can:

1. **Option A**: Let new data accumulate in partitioned format, process old data separately
2. **Option B**: Use a migration script to reorganize existing data (not provided)

### Backward Compatibility

The changes are backward compatible:
- If `partition_by` is empty, data is written without partitions
- If `enable_date_partition` is `false`, no date partitions are created
- ETL can read both partitioned and unpartitioned data with glob patterns
