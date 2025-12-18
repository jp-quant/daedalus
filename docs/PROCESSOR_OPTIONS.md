# Processor Options Configuration Guide

## Overview
This document explains how to configure processor behavior through `processor_options` in `config.yaml`.

## Configuration Flow

```
config.yaml
    ↓
ETLConfig (Pydantic)
    ↓
scripts/run_etl_v2.py (batch runner)
  ↓
etl/features/orderbook.py (vectorized structural + rolling)
etl/features/stateful.py (sequential stateful features, optional)
```

## Orderbook Processor Options

The orderbook path consumes the following keys via `processor_options`:

### Feature Extraction Settings

- **`max_levels`** (int, default: 20)
  - Number of depth levels to include in feature extraction
  - Higher values capture more of the order book but increase computation
  - Example: `max_levels: 20`

- **`bands_bps`** (List[int], default: `[5, 10, 25, 50, 100]`)
  - Basis point bands for liquidity analysis
  - Measures available liquidity at different price distances
  - Example: `bands_bps: [5, 10, 25, 50, 100]`

### Rolling / Aggregation

- **`horizons`** (List[int], default: `[5, 15, 60, 300, 900]`)
  - Rolling window sizes in seconds
  - Used by `etl/features/orderbook.py::compute_rolling_features`

- **`bar_durations`** (List[int], default: `[60, 300, 900, 3600]`)
  - Bar window sizes in seconds
  - Used by `etl/transforms/bars.py` aggregation

### Stateful Feature Settings (Optional)

- **`enable_stateful`** (bool, default: `true`)
  - If true, `scripts/run_etl_v2.py` will compute sequential stateful features via `etl/features/stateful.py`

- **`ofi_levels`** (int, default: 10)
  - Number of levels used for Order Flow Imbalance (OFI) calculation
  - Typically fewer than `max_levels` since OFI focuses on top-of-book

- **`ofi_decay_alpha`** (float, default: `0.5`)
  - Exponential decay parameter for multi-level OFI (MLOFI)

- **`use_dynamic_spread_regime`** (bool, default: `true`)
- **`spread_regime_window`** (int, default: `300`)
- **`spread_tight_percentile`** (float, default: `0.2`)
- **`spread_wide_percentile`** (float, default: `0.8`)
- **`tight_spread_threshold`** (float, default: `0.0001`)
  - Static fallback threshold (used if dynamic regime is disabled)

- **`kyle_lambda_window`** (int, default: `300`)
  - Window size (seconds) for Kyle’s Lambda estimator

- **`enable_vpin`** (bool, default: `true`)
- **`vpin_bucket_volume`** (float, default: `1.0`)
- **`vpin_window_buckets`** (int, default: `50`)

## Example Configuration

```yaml
etl:
  channels:
    orderbook:
      partition_cols:
        - "exchange"
        - "symbol"
        - "date"
      processor_options:
        compute_features: true
        max_levels: 20
        bands_bps: [5, 10, 25, 50, 100]
        horizons: [5, 15, 60, 300, 900]
        bar_durations: [60, 300, 900, 3600]

        enable_stateful: true
        ofi_levels: 10
        ofi_decay_alpha: 0.5
        use_dynamic_spread_regime: true
        spread_regime_window: 300
        spread_tight_percentile: 0.2
        spread_wide_percentile: 0.8
        tight_spread_threshold: 0.0001
        kyle_lambda_window: 300
        enable_vpin: true
        vpin_bucket_volume: 1.0
        vpin_window_buckets: 50
```

## Testing Configuration

Run the test script to verify your configuration:

```bash
python scripts/test_config_flow.py
```

This will:
1. Load your `config.yaml`
2. Extract `processor_options` for orderbook channel
3. Verify all parameters are recognized
4. Create a `StateConfig` instance
5. Initialize the processor with your settings
6. Confirm all values match

## Implementation Details

### Parameter Mapping

The processor supports backward compatibility for renamed parameters:

- `hf_sample_interval` → `hf_emit_interval` (auto-mapped)

### Code Locations

- **Configuration model**: `config/config.py` - `ChannelETLConfig.processor_options`
- **Batch runner**: `scripts/run_etl_v2.py`
- **Vectorized orderbook features**: `etl/features/orderbook.py`
- **Stateful orderbook features**: `etl/features/stateful.py`

## Other Channels

Ticker and trades channels also accept a `processor_options` dict, but current transforms primarily use defaults.

## Debugging

If your configuration isn't being applied:

1. Verify parameter names match exactly (case-sensitive)

2. Run the config flow script:
   ```bash
   python scripts/test_config_flow.py
   ```

3. Enable DEBUG logging to see parameter extraction:
   ```yaml
   log_level: "DEBUG"
   ```

## Performance Considerations

- **`max_levels`**: Higher values increase memory and computation linearly
- **`bar_durations`**: More durations = more output files
- **`horizons`**: Affects rolling statistic computation but minimal overhead
- **`enable_stateful`**: If true, adds a Python loop over snapshots (slower than fully vectorized)

## Recommendations

### For Low-Latency Trading
```yaml
processor_options:
  max_levels: 10
  horizons: [5, 15, 60]
  bar_durations: [60, 300]
  enable_stateful: false
```

### For Research/Analysis
```yaml
processor_options:
  max_levels: 50
  horizons: [5, 15, 60, 300, 900]
  bar_durations: [60, 300, 900, 3600]
  enable_stateful: true
```

### For Storage Efficiency
```yaml
processor_options:
  max_levels: 10
  bar_durations: [60, 300]  # Only 1min and 5min bars
  enable_stateful: false
```
