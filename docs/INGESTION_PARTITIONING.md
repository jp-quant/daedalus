# Ingestion Partitioning Guide

The ingestion pipeline supports **Directory-Aligned Partitioning** for raw data, enabling efficient downstream ETL processing.

## Overview

### Directory-Aligned Partitioning (NOT Traditional Hive)

Our partitioning approach differs from traditional Hive-style partitioning:

| Aspect | Traditional Hive | Our Approach (Directory-Aligned) |
|--------|-----------------|----------------------------------|
| Partition columns in data | NO - derived from path | YES - stored in Parquet |
| Partition value sanitization | Path only | BOTH path AND data |
| Date/time columns | Derived from path | Stored as year/month/day/hour integers |
| Data-path consistency | Not guaranteed | **Guaranteed** |

**Key Guarantee**: Partition column values **EXIST in the Parquet data** and **MATCH the directory partition values exactly**.

This means:
- If the path has `symbol=BTC-USD`, the data column `symbol` contains `"BTC-USD"`
- If the path has `exchange=binanceus`, the data column `exchange` contains `"binanceus"`
- If the path has `year=2025/month=12/day=19/hour=14`, the data has matching columns
- No ambiguity between path and stored data

### Directory Structure

Raw data is partitioned during ingestion:

```
raw/ready/ccxt/{channel}/exchange={exchange}/symbol={symbol}/year={y}/month={m}/day={d}/hour={h}/segment_*.parquet
```

### Benefits

1. **Efficient ETL**: ETL can read only relevant partitions instead of scanning all data
2. **Parallel Processing**: Different partitions can be processed independently
3. **Cost Savings**: On S3, partition pruning reduces data scanned significantly
4. **Data Consistency**: Partition values match stored data exactly
5. **Hourly Granularity**: Hour-level partitioning enables fine-grained analysis
6. **No Ambiguity**: Path and data are always consistent

## Configuration

Configure partitioning in `config.yaml`:

```yaml
ingestion:
  # Partition columns (order matters for path structure)
  # Default: ["exchange", "symbol", "year", "month", "day", "hour"]
  partition_by:
    - "exchange"   # First level: exchange=binanceus
    - "symbol"     # Second level: symbol=BTC-USD (sanitized)
    - "year"       # Third level: year=2025
    - "month"      # Fourth level: month=12
    - "day"        # Fifth level: day=19
    - "hour"       # Sixth level: hour=14
```

### Supported Partition Columns

| Column | Type | Description |
|--------|------|-------------|
| `exchange` | string | Exchange identifier (sanitized) |
| `symbol` | string | Trading pair symbol (sanitized: `/` → `-`) |
| `year` | int32 | Year from timestamp (e.g., 2025) |
| `month` | int32 | Month from timestamp (1-12) |
| `day` | int32 | Day from timestamp (1-31) |
| `hour` | int32 | Hour from timestamp (0-23) |

### Default Partition Configuration

The default is full partitioning with all columns:

```python
partition_by = ["exchange", "symbol", "year", "month", "day", "hour"]
```

This creates:
```
raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC-USD/year=2025/month=12/day=19/hour=14/segment_*.parquet
```

### Custom Partition Examples

**Exchange + Symbol only (no time partitions)**:
```yaml
partition_by: ["exchange", "symbol"]
```
```
raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC-USD/segment_*.parquet
```

**Daily partitions (no hour)**:
```yaml
partition_by: ["exchange", "symbol", "year", "month", "day"]
```
```
raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC-USD/year=2025/month=12/day=19/segment_*.parquet
```

**Monthly aggregation**:
```yaml
partition_by: ["exchange", "symbol", "year", "month"]
```
```
raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC-USD/year=2025/month=12/segment_*.parquet
```

## How It Works

### Partition Value Sanitization

**ALL** partition column values are sanitized for filesystem compatibility. The following characters are replaced with `-`:
- `/` (forward slash)
- `\` (backslash)
- `:` (colon)
- `*` (asterisk)
- `?` (question mark)
- `"` (double quote)
- `<` and `>` (angle brackets)
- `|` (pipe)

Examples:
- `BTC/USD` → `BTC-USD`
- `ETH/USDT` → `ETH-USDT`
- `exchange:test` → `exchange-test`

**IMPORTANT**: This sanitization applies to **BOTH the path AND the data**. All partition columns in the Parquet file contain sanitized values.

### Partition Key Extraction

For each record, the writer extracts:
1. **Channel**: From `type` field (orderbook, ticker, trades)
2. **Exchange**: From `exchange` field (sanitized)
3. **Symbol**: From `symbol` field (sanitized)
4. **Year/Month/Day/Hour**: From `timestamp` or `capture_ts` field

### DateTime Extraction

Date/time values are extracted from records in priority order:
1. `capture_ts` field (ISO format or datetime object)
2. `timestamp` field (epoch milliseconds)
3. Current UTC time (fallback)

## Parquet Schema

All schemas include partition columns. These columns ALWAYS exist in the data regardless of `partition_by` configuration:

```python
# Ticker schema example
pa.schema([
    pa.field("collected_at", pa.int64()),
    pa.field("capture_ts", pa.timestamp("us", tz="UTC")),
    pa.field("exchange", pa.string()),    # Sanitized partition column
    pa.field("symbol", pa.string()),      # Sanitized partition column
    pa.field("year", pa.int32()),         # Partition column
    pa.field("month", pa.int32()),        # Partition column
    pa.field("day", pa.int32()),          # Partition column
    pa.field("hour", pa.int32()),         # Partition column
    # ... other fields
])
```

## ETL Integration

The batched ETL processor reads partitioned data efficiently:

```python
# Read only BTC-USD from binanceus for a specific hour
df = pl.scan_parquet(
    "raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC-USD/year=2025/month=12/day=19/hour=14/*.parquet"
).collect()

# The data columns match the path exactly:
assert df["symbol"].unique().to_list() == ["BTC-USD"]
assert df["year"].unique().to_list() == [2025]
assert df["month"].unique().to_list() == [12]
assert df["day"].unique().to_list() == [19]
assert df["hour"].unique().to_list() == [14]
```

### Partition Discovery

Use the ETL batched processor to automatically discover and process partitions:

```bash
python scripts/run_orderbook_features_batched.py --discover
```

## Migration

### From Unpartitioned Data

Use the migration script to reorganize existing unpartitioned data:

```bash
# Dry run (default) - see what would be done
python scripts/migrate_to_partitioned.py

# Execute migration
python scripts/migrate_to_partitioned.py --execute

# Execute and delete source files after successful migration
python scripts/migrate_to_partitioned.py --execute --delete-source

# Migrate specific channel only
python scripts/migrate_to_partitioned.py --channel orderbook --execute
```

The migration script:
1. Reads existing unpartitioned Parquet files
2. Sanitizes ALL partition values (including exchange and symbol)
3. Adds `year`, `month`, `day`, `hour` columns derived from timestamps
4. Writes to partitioned directory structure
5. Ensures data values match partition path values exactly

### Backward Compatibility

The schema changes affect how data is stored:
- **New columns**: `year`, `month`, `day`, `hour` (int32)
- **Sanitized values**: Exchange and symbol columns contain sanitized strings

Existing code that reads:
- **Unpartitioned data**: Still works with glob patterns
- **New partitioned data**: Gets additional columns and sanitized partition values

## Testing

Run the partitioning validation test:

```bash
python scripts/test_partition_alignment.py
```

This validates:
- Schema includes all partition columns (year, month, day, hour)
- Partition values in data match partition path values
- Symbol sanitization applied correctly
- All partition values are sanitized

## Summary

| Feature | Description |
|---------|-------------|
| Default partitions | `["exchange", "symbol", "year", "month", "day", "hour"]` |
| Sanitized characters | `/ \ : * ? " < > \|` → `-` |
| Data-path match | **Guaranteed** - partition values exist in data |
| Configurable | Yes - customize `partition_by` list |
| Hour granularity | Yes - enables hourly analysis |
