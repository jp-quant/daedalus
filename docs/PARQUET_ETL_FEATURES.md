# Parquet ETL Feature Engineering Guide

## Overview

This document describes the comprehensive feature engineering capabilities in `etl/parquet_etl_pipeline.py`. The pipeline processes channel-separated Parquet files (orderbook, trades, ticker) and computes 100+ features for quantitative analysis.

## Architecture

```
Channel-Separated Parquet Files
├── orderbook/product=BTC-USD/year=2024/...
├── trades/product=BTC-USD/year=2024/...
└── ticker/product=BTC-USD/year=2024/...
        │
        ▼
┌─────────────────────────────────────────────┐
│  ParquetETLPipeline                         │
│  ┌─────────────────────────────────────┐    │
│  │ extract_structural_features_vectorized │  │  ◄── Polars LazyFrame (60+ features)
│  └─────────────────────────────────────┘    │
│  ┌─────────────────────────────────────┐    │
│  │ OrderbookState (per symbol)          │   │  ◄── Stateful processing (40+ features)
│  │  - OFI/MLOFI                         │   │
│  │  - VPIN (toxicity)                   │   │
│  │  - Kyle's Lambda                     │   │
│  │  - Rolling statistics                │   │
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
        │
        ▼
   Enriched Parquet with 100+ features
```

## Configuration

```python
from etl.parquet_etl_pipeline import ParquetETLConfig

config = ParquetETLConfig(
    # Orderbook depth
    max_levels=20,           # L0-L19 price levels
    
    # OFI computation
    ofi_levels=5,            # Levels for multi-level OFI
    mlofi_decay=0.7,         # Exponential decay for MLOFI
    
    # Rolling windows (seconds)
    horizons=[1, 5, 30, 60], # For rolling stats
    
    # Band-based depth
    bands_bps=[5, 10, 25, 50, 100],  # Basis point bands
    
    # VPIN (Volume-Synchronized Probability of Informed Trading)
    enable_vpin=True,        # Critical for volatility prediction
    vpin_bucket_volume=1.0,  # Volume per bucket
    vpin_window_buckets=50,  # Rolling window
)
```

## Feature Categories

### 1. Structural Features (Vectorized, Per-Snapshot)

These are computed using Polars LazyFrames for maximum efficiency.

#### 1.1 Price & Spread
| Feature | Formula | Description |
|---------|---------|-------------|
| `mid_price` | (bid₀ + ask₀) / 2 | Midpoint price |
| `spread` | ask₀ - bid₀ | Absolute spread |
| `spread_bps` | spread / mid × 10000 | Spread in basis points |
| `microprice` | (bid₀×ask_vol₀ + ask₀×bid_vol₀) / (bid_vol₀ + ask_vol₀) | Volume-weighted mid |
| `micro_minus_mid` | microprice - mid_price | Buying pressure indicator |

#### 1.2 Depth & Imbalance
| Feature | Formula | Description |
|---------|---------|-------------|
| `bid_depth_L5` | Σ bid_size[0:5] | Total bid volume top 5 levels |
| `ask_depth_L5` | Σ ask_size[0:5] | Total ask volume top 5 levels |
| `imb_L1` | (bid_vol₀ - ask_vol₀) / (bid_vol₀ + ask_vol₀) | L1 imbalance |
| `imb_L5` | (bid_depth_L5 - ask_depth_L5) / total | Multi-level imbalance |
| `imb_signed` | imb_L5 × sign(micro_minus_mid) | Directional imbalance |

#### 1.3 Band-Based Depth (using `bands_bps`)
| Feature | Description |
|---------|-------------|
| `bid_vol_band_0_5bps` | Bid volume within 0-5 bps of mid |
| `ask_vol_band_0_5bps` | Ask volume within 0-5 bps of mid |
| `imb_band_0_5bps` | Imbalance within 0-5 bps band |
| `bid_vol_band_5_10bps` | Bid volume 5-10 bps from mid |
| ... | Similar for 10-25, 25-50, 50-100 bps |

#### 1.4 Shape & Concentration
| Feature | Formula | Description |
|---------|---------|-------------|
| `bid_herfindahl` | Σ(vol_i/total)² | Concentration (1=all at one level) |
| `ask_herfindahl` | Same for asks | Ask-side concentration |
| `bid_gini` | Gini coefficient | Inequality measure |
| `ask_gini` | Same for asks | |
| `depth_50pct_bid` | Level with 50% cumulative volume | Shape measure |
| `depth_50pct_ask` | Same for asks | |

#### 1.5 Impact & Liquidity
| Feature | Formula | Description |
|---------|---------|-------------|
| `bid_vwap_L5` | Σ(price×vol) / Σvol | Volume-weighted avg price |
| `ask_vwap_L5` | Same for asks | |
| `smart_bid_depth` | Σ vol × exp(-0.1×level) | Exponentially weighted depth |
| `smart_ask_depth` | Same for asks | |
| `kyle_lambda_proxy` | log(L5_depth) | Price impact proxy |
| `amihud_proxy` | spread / total_depth | Illiquidity proxy |
| `slippage_proxy` | (vwap - best) / mid | Execution cost estimate |

#### 1.6 Slope & Decay
| Feature | Formula | Description |
|---------|---------|-------------|
| `bid_slope_simple` | (price₀ - price_N) / N | Simple slope |
| `ask_slope_simple` | Same for asks | |
| `bid_slope_regression` | Linear regression β | More robust slope |
| `bid_exp_decay_rate` | Fitted λ in vol = A×e^(-λ×dist) | Decay rate |
| `ask_exp_decay_rate` | Same for asks | |

#### 1.7 Center of Gravity & Pressure
| Feature | Formula | Description |
|---------|---------|-------------|
| `bid_cog` | Σ(price×vol) / Σvol | Volume-weighted center |
| `ask_cog` | Same for asks | |
| `cog_vs_mid` | (bid_cog - ask_cog) / spread | Pressure direction |
| `bid_wall_level` | Level with max volume | Wall detection |
| `ask_wall_level` | Same for asks | |

### 2. Stateful Features (OrderbookState)

These require cross-snapshot state and are computed row-by-row.

#### 2.1 Order Flow Imbalance (OFI)
| Feature | Formula | Description |
|---------|---------|-------------|
| `ofi` | Cont's OFI at L1 | Order flow imbalance |
| `ofi_step` | Same, per-snapshot delta | |
| `mlofi_decay` | Multi-level OFI with decay | Weighted by level |
| `ofi_sum_{h}s` | Rolling sum over h seconds | |
| `mlofi_sum_{h}s` | Rolling multi-level sum | |

**Cont's OFI Formula:**
```
OFI_t = I(bid_t ≥ bid_{t-1}) × ΔBidVol - I(ask_t ≤ ask_{t-1}) × ΔAskVol
```

#### 2.2 Returns & Volatility
| Feature | Formula | Description |
|---------|---------|-------------|
| `log_return` | log(mid_t / mid_{t-1}) | Log return |
| `rv_{h}s` | √Var(returns over h seconds) | Realized volatility |
| `mid_velocity` | Δmid / Δt | Price velocity |
| `mid_accel` | Δvelocity / Δt | Price acceleration |

#### 2.3 Trade Flow Imbalance (TFI)
| Feature | Formula | Description |
|---------|---------|-------------|
| `tfi` | (buy_vol - sell_vol) / total | Trade imbalance |
| `buy_volume` | Accumulated buy volume | |
| `sell_volume` | Accumulated sell volume | |
| `tfi_{h}s` | Rolling TFI over h seconds | |
| `trade_vol_{h}s` | Rolling total volume | |

#### 2.4 VPIN (Order Flow Toxicity)

**VPIN = Volume-Synchronized Probability of Informed Trading**

Critical for predicting volatility spikes and flash crashes.

| Feature | Description |
|---------|-------------|
| `vpin` | Toxicity probability [0, 1] |
| `order_flow_toxicity` | Same as VPIN |

**Algorithm:**
1. Accumulate trades into volume buckets (fixed volume per bucket)
2. Classify each bucket as buy/sell (tick rule or explicit side)
3. VPIN = rolling average of |BuyVol - SellVol| / TotalVol

**Reference:** Easley, López de Prado, O'Hara (2012)

#### 2.5 Kyle's Lambda (Price Impact)
| Feature | Formula | Description |
|---------|---------|-------------|
| `kyle_lambda` | β from regression: return ~ OFI | Price impact coefficient |
| `kyle_lambda_r2` | R² of regression | Fit quality |

**Interpretation:**
- High λ = Low liquidity, high price impact per unit order flow
- R² measures how well order flow explains returns

#### 2.6 Spread Regime
| Feature | Description |
|---------|-------------|
| `spread_percentile` | Current spread's percentile (0-1) |
| `spread_regime` | "tight" / "normal" / "wide" |
| `spread_regime_tight_frac` | Fraction of time in tight regime |
| `spread_regime_wide_frac` | Fraction of time in wide regime |

#### 2.7 Rolling Shape Stats
| Feature | Description |
|---------|-------------|
| `bid_slope_mean_60s` | Rolling mean of bid slope |
| `bid_slope_std_60s` | Rolling std of bid slope |
| `cog_momentum_mean_60s` | Rolling mean of CoG momentum |
| `depth_variance_5m` | Variance of near-touch depth |

## Usage Example

```python
from etl.parquet_etl_pipeline import ParquetETLPipeline, ParquetETLConfig
import polars as pl

# Configure
config = ParquetETLConfig(
    enable_vpin=True,
    horizons=[1, 5, 30, 60],
    bands_bps=[5, 10, 25, 50, 100],
)

# Initialize pipeline
pipeline = ParquetETLPipeline(config)

# Process orderbook data
orderbook_df = pl.scan_parquet("data/orderbook/*.parquet")

# Extract features
enriched = pipeline.process_orderbooks(orderbook_df)

# Join with trades for TFI/VPIN
trades_df = pl.scan_parquet("data/trades/*.parquet")
final = pipeline.process_trades(trades_df, enriched)

final.collect().write_parquet("output/features.parquet")
```

## Research References

- **VPIN:** Easley, López de Prado, O'Hara (2012) "Flow Toxicity and Liquidity in a High Frequency World"
- **OFI:** Cont, Kukanov, Stoikov (2014) "The Price Impact of Order Book Events"
- **Kyle's Lambda:** Kyle (1985) "Continuous Auctions and Insider Trading"
- **Microprice:** Gatheral & Oomen (2010) "Zero-Intelligence Realized Variance Estimation"

## Notes

1. **Band-based depth** uses level-to-bps mapping approximation in vectorized mode. For exact price-distance calculation, use stateful processor.

2. **VPIN bucket_volume** should be calibrated per asset (e.g., 100 BTC for BTC/USD, 1000 ETH for ETH/USD).

3. **Horizons** affect memory usage - each horizon requires rolling state accumulators.

4. **max_levels** beyond 20 provides diminishing returns for most features.
