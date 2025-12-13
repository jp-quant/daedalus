# Processor Options Configuration Guide

## Overview
This document explains how to configure processor behavior through `processor_options` in `config.yaml`.

## Configuration Flow

```
config.yaml
    ↓
ETLConfig (Pydantic)
    ↓
ETLJob.run()
    ↓
CcxtSegmentPipeline.__init__(channel_config=...)
    ↓
CcxtAdvancedOrderbookProcessor(**processor_options)
    ↓
StateConfig (dataclass)
```

## Orderbook Processor Options

The `CcxtAdvancedOrderbookProcessor` accepts the following parameters via `processor_options`:

### Feature Extraction Settings

- **`max_levels`** (int, default: 10)
  - Number of depth levels to include in feature extraction
  - Higher values capture more of the order book but increase computation
  - Example: `max_levels: 20`

- **`ofi_levels`** (int, default: 5)
  - Number of levels used for Order Flow Imbalance (OFI) calculation
  - Typically fewer than `max_levels` since OFI focuses on top-of-book
  - Example: `ofi_levels: 5`

- **`bands_bps`** (List[int], default: [5, 10, 25, 50])
  - Basis point bands for liquidity analysis
  - Measures available liquidity at different price distances
  - Example: `bands_bps: [5, 10, 25, 50]`

### High-Frequency Output Settings

- **`hf_emit_interval`** (float, default: 1.0)
  - Emit high-frequency feature snapshots every N seconds
  - Controls sampling rate for the HF output stream
  - Example: `hf_emit_interval: 1.0` (emit every second)
  - Note: Previously named `hf_sample_interval` (still supported for backward compatibility)

### Bar Aggregation Settings

- **`bar_durations`** (List[int], default: [1, 5, 30, 60])
  - Bar window sizes in seconds
  - Generates OHLCV + microstructure statistics for each duration
  - Example: `bar_durations: [5, 15, 60]` (5s, 15s, 60s bars)

- **`horizons`** (List[int], default: [1, 5, 30, 60])
  - Rolling window sizes in seconds for streaming statistics
  - Used for computing moving averages, volatility, etc.
  - Example: `horizons: [1, 5, 30, 60]`

### Advanced Options

- **`keep_raw_arrays`** (bool, default: false)
  - Whether to include raw bid/ask price/quantity arrays in output
  - Warning: Increases output size significantly
  - Example: `keep_raw_arrays: false`

- **`tight_spread_threshold`** (float, default: 0.0001)
  - Spread threshold in decimal for regime classification
  - Example: `0.0001` = 1 basis point = 0.01%
  - Used to identify "tight spread" vs "wide spread" regimes

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
        # Feature extraction
        max_levels: 20
        ofi_levels: 5
        bands_bps: [5, 10, 25, 50]
        
        # HF output (1s snapshots)
        hf_emit_interval: 1.0
        
        # Bar aggregation (5s, 15s, 60s)
        bar_durations: [5, 15, 60]
        horizons: [1, 5, 30, 60]
        
        # Advanced
        keep_raw_arrays: false
        tight_spread_threshold: 0.0001
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
- **Pipeline routing**: `etl/orchestrators/ccxt_segment_pipeline.py` - Line 99
- **Processor initialization**: `etl/processors/ccxt/advanced_orderbook_processor.py` - Line 21-39
- **State configuration**: `etl/features/state.py` - `StateConfig` dataclass

## Other Processors

Currently only the `CcxtAdvancedOrderbookProcessor` accepts `processor_options`. Other processors (ticker, trades) use default behavior but support basic options:

### Ticker Processor
```yaml
ticker:
  processor_options:
    # Currently no custom options, uses defaults
```

### Trades Processor
```yaml
trades:
  processor_options:
    # Currently no custom options, uses defaults
```

The framework supports extending these processors to accept options in the future by:
1. Adding parameters to their `__init__(**kwargs)` method
2. Defining which parameters they accept
3. Using try/except fallback for backward compatibility

## Debugging

If your configuration isn't being applied:

1. Check logs for the initialization message:
   ```
   [CcxtAdvancedOrderbookProcessor] Initialized with config: StateConfig(...)
   ```

2. Verify parameter names match exactly (case-sensitive)

3. Run the test script to validate the flow:
   ```bash
   python scripts/test_config_flow.py
   ```

4. Enable DEBUG logging to see parameter extraction:
   ```yaml
   log_level: "DEBUG"
   ```

## Changes Made

### Files Modified

1. **`etl/orchestrators/ccxt_segment_pipeline.py`**
   - Added debug logging for processor options
   - Added fallback for processors that don't accept options

2. **`etl/processors/ccxt/advanced_orderbook_processor.py`**
   - Added parameter name mapping (backward compatibility)
   - Improved initialization logging

3. **`config/config.yaml`**
   - Updated orderbook processor_options with proper parameter names
   - Added comprehensive documentation for each parameter

### Files Added

4. **`scripts/test_config_flow.py`**
   - New test script to verify configuration flow
   - Validates parameters from config → processor → StateConfig

5. **`docs/PROCESSOR_OPTIONS.md`**
   - This comprehensive guide

## Performance Considerations

- **`max_levels`**: Higher values increase memory and computation linearly
- **`bar_durations`**: More durations = more output files
- **`hf_emit_interval`**: Lower values = more HF snapshots = larger output
- **`horizons`**: Affects rolling statistic computation but minimal overhead
- **`keep_raw_arrays`**: Can 10x the output size, use sparingly

## Recommendations

### For Low-Latency Trading
```yaml
processor_options:
  max_levels: 10
  hf_emit_interval: 0.1  # 100ms snapshots
  bar_durations: [1, 5, 15]
```

### For Research/Analysis
```yaml
processor_options:
  max_levels: 50
  hf_emit_interval: 1.0
  bar_durations: [1, 5, 15, 30, 60, 300]
  keep_raw_arrays: true
```

### For Storage Efficiency
```yaml
processor_options:
  max_levels: 10
  hf_emit_interval: 5.0  # Only every 5s
  bar_durations: [60, 300]  # Only 1min and 5min bars
  keep_raw_arrays: false
```
