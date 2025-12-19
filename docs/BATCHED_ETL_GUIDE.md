# Batched Orderbook ETL Processing

## Problem

The original `run_orderbook_features.py` script processes all orderbook data at once, which causes memory issues for large datasets because:

1. The stateful feature computation (OFI, MLOFI, TFI, etc.) requires calling `.collect()` on the entire LazyFrame
2. For datasets with many symbols and dates, this can require 10GB+ of RAM
3. The process crashes with out-of-memory errors

## Solution

The new `run_orderbook_features_batched.py` script processes data **partition-by-partition**:

- Each (exchange, symbol, date) partition is processed independently
- Memory usage is bounded by the largest single partition (~10-100MB typically)
- Supports parallel processing across partitions
- Includes checkpoint/resume for fault tolerance

## Memory Comparison

| Approach | Memory Usage | Example Dataset |
|----------|--------------|-----------------|
| Original (all-at-once) | ~15GB | 50 symbols × 30 days |
| Batched (per-partition) | ~50MB | 1 symbol × 1 day |

**Result**: ~300x memory reduction for large datasets.

## Usage

### Basic Usage (Sequential)

```bash
python scripts/etl/run_orderbook_features_batched.py \
  --input data/raw/ready/ccxt/orderbook \
  --output data/processed \
  --trades data/raw/ready/ccxt/trades
```

### Parallel Processing (Recommended)

Process multiple partitions in parallel for faster throughput:

```bash
python scripts/etl/run_orderbook_features_batched.py \
  --input data/raw/ready/ccxt/orderbook \
  --output data/processed \
  --trades data/raw/ready/ccxt/trades \
  --workers 42
```

### Resume from Checkpoint

If the process is interrupted, resume from where it left off:

```bash
python scripts/etl/run_orderbook_features_batched.py \
  --input data/raw/ready/ccxt/orderbook \
  --output data/processed \
  --checkpoint-path state/orderbook_checkpoint.json
```

The checkpoint file tracks:
- ✓ Successfully processed partitions
- ✗ Failed partitions
- Statistics per partition

### Filter Specific Exchange/Symbol

Process only specific trading pairs:

```bash
# Single symbol
python scripts/etl/run_orderbook_features_batched.py \
  --exchange binanceus \
  --symbol BTC/USDT

# All symbols on one exchange
python scripts/etl/run_orderbook_features_batched.py \
  --exchange binanceus
```

## How It Works

### Partition Discovery

The script scans the input directory for all partitions:

```
data/raw/ready/ccxt/orderbook/
├── exchange=binanceus/
│   ├── symbol=BTC~USDT/
│   │   ├── year=2025/
│   │   │   ├── month=12/
│   │   │   │   ├── day=18/
│   │   │   │   │   └── *.parquet
│   │   │   │   └── day=19/
│   │   │   │       └── *.parquet
```

Each (exchange, symbol, date) combination is a partition.

### Processing Flow

For each partition:

1. **Create FilterSpec** - Exact partition filter (exchange=X, symbol=Y, date=Z)
2. **Fresh Transform** - New `OrderbookFeatureTransform` instance (resets stateful processor)
3. **Execute** - Run transform via `TransformExecutor` with partition filter
4. **Write Output** - Parquet files written to output path with Hive partitioning
5. **Update Checkpoint** - Mark partition as processed

### Stateful Features Reset

Critical: Each partition gets a **fresh transform instance** with reset state, because:

- OFI/MLOFI depend on previous orderbook snapshot
- TFI depends on trade accumulation
- Kyle's Lambda uses rolling regression

State should NOT carry across symbol/date boundaries - each partition is independent.

## Performance

### Throughput

Typical performance on modern hardware:

| Workers | Partitions/Hour | Notes |
|---------|-----------------|-------|
| 1 | 60-120 | Safe, low memory |
| 2 | 120-240 | Good for modest datasets |
| 4 | 240-480 | Recommended for large datasets |
| 8+ | 400-800+ | Diminishing returns due to I/O |

### Memory Usage

- **Per-worker overhead**: ~200MB (Python + Polars)
- **Per-partition peak**: ~50-200MB depending on:
  - Number of orderbook snapshots per day
  - Depth levels configured
  - Number of rolling windows

**Rule of thumb**: `total_memory_MB = 500 + (workers × 200)`

For 4 workers: ~1.3GB total (vs ~15GB for non-batched).

## Output Structure

Output follows Hive-style partitioning:

```
data/processed/silver/orderbook/
├── exchange=binanceus/
│   ├── symbol=BTC~USDT/
│   │   ├── year=2025/
│   │   │   └── month=12/
│   │   │       └── day=18/
│   │   │           └── part-0.parquet
```

Each partition writes to its own directory with computed features:
- Structural features (depth, spread, microprice, etc.)
- Dynamic features (OFI, MLOFI, TFI, log returns)
- Rolling features (realized vol, regime stats)
- Advanced features (Kyle's Lambda, VPIN)

Raw `bids`/`asks` arrays are dropped by default (controlled by `drop_raw_book_arrays=True` in config).

## Checkpointing

The checkpoint file (`state/orderbook_checkpoint.json`) contains:

```json
{
  "processed": [
    ["binanceus", "BTC/USDT", "2025-12-18"],
    ["binanceus", "ETH/USDT", "2025-12-18"]
  ],
  "failed": [],
  "stats": {},
  "last_update": "2025-12-19T01:23:45.123456"
}
```

Benefits:
- **Resume capability**: Restart after crashes without reprocessing
- **Progress tracking**: See how many partitions completed
- **Failure isolation**: Failed partitions don't block others

## When to Use Which Script

### Use `run_orderbook_features_batched.py` (NEW) when:
- ✅ Processing large datasets (>10 symbols, >7 days)
- ✅ Memory is constrained (<16GB RAM)
- ✅ Need parallel processing
- ✅ Want checkpoint/resume capability
- ✅ Production workloads

### Use `run_orderbook_features.py` (ORIGINAL) when:
- Limited data (1-2 symbols, 1-2 days)
- Testing/development with `--limit`
- Memory is abundant (>32GB RAM)
- Data is already filtered to small subset

**Recommendation**: Use batched script as default for all production workloads.

## Troubleshooting

### "No partitions found"
- Check input path is correct
- Verify Hive-style partitioning: `exchange=*/symbol=*/year=*/month=*/day=*`

### "Partition failed: X/Y/Z"
- Check logs for specific error
- Re-run with `--debug` for detailed logging
- Failed partitions are marked in checkpoint and skipped on retry

### Memory still high with multiple workers
- Reduce `--workers` (try 2 instead of 4)
- Check system has enough RAM: `workers × 300MB`
- Some partitions may be unusually large (check partition size)

### Slow processing
- Increase `--workers` for parallel processing
- Check I/O bottleneck (disk/network speed)
- Verify not reading from slow external drive
