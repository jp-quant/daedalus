# Deep Research Prompt: Orderbook Modeling for Mid-Frequency Crypto Quant Trading

## Research Objective

Conduct comprehensive research into state-of-the-art quantitative methodologies for modeling cryptocurrency orderbook data to generate predictive signals and alpha factors. The target trading frequency is **mid-frequency** (seconds to minutes to hours), NOT high-frequency. We acknowledge we cannot compete with institutional HFT infrastructure, so focus on strategies that exploit **information asymmetry, microstructure patterns, and statistical edges** that persist over longer horizons.

---

## Current System Overview

### Trading Context
- **Asset Class**: Cryptocurrency (multi-exchange: Coinbase, Binance US, etc.)
- **Target Frequency**: 5 seconds to 4 hours (mid-frequency)
- **Data Sources**: L2 orderbook snapshots, trades, ticker data
- **Execution**: Not co-located; retail/prosumer infrastructure (Raspberry Pi to cloud)
- **Goal**: Generate alpha signals for systematic trading, not market-making

### Current Feature Extraction Pipeline

We have implemented a streaming orderbook processor with the following capabilities:

#### 1. Structural/Static Features (per snapshot)
```
- Best bid/ask prices and sizes (L0)
- Multi-level prices and sizes (L0 to L{max_levels})
- Mid price, spread, relative spread (bps)
- Microprice (volume-weighted fair value)
- Order imbalance at each level (imbalance_L0, L1, ...)
- Cumulative depth at price bands (0-5bps, 5-10bps, 10-25bps, 25-50bps)
- Total bid/ask volume within bands
- Book depth (number of levels)
- Weighted mid price
```

#### 2. Flow/Dynamic Features (delta-based)
```
- Log returns (mid price)
- Order Flow Imbalance (OFI) - L1 using Cont's method
- Multi-level OFI (sum across top N levels)
- Mid price velocity (dp/dt)
- Mid price acceleration (d²p/dt²)
```

#### 3. Rolling/Streaming Statistics
```
- Realized volatility (Welford online algorithm) over horizons [1s, 5s, 30s, 60s]
- Rolling OFI sum over horizons
- Trade Flow Imbalance (TFI) = (buy_vol - sell_vol) / total_vol
- Rolling trade volume
- Depth variance (0-5bps band)
- Spread regime fraction (tight vs wide regime over 30s window)
```

#### 4. Time Features
```
- Hour of day, minute of hour
- Day of week, is_weekend flag
```

#### 5. Bar Aggregation (OHLCV+)
```
- OHLC of mid price
- Mean/min/max spread
- Mean relative spread
- Sum OFI
- Mean L1 imbalance
- Realized variance within bar
- Durations: [1s, 5s, 15s, 60s] (configurable)
```

### Current Configuration Parameters (StateConfig)

```python
@dataclass
class StateConfig:
    # Horizons for rolling stats (seconds)
    horizons: List[int] = [1, 5, 30, 60]
    
    # Bar durations to build (seconds)
    bar_durations: List[int] = [1, 5, 30, 60]
    
    # High-frequency emission interval (seconds)
    hf_emit_interval: float = 1.0
    
    # Feature extraction settings
    max_levels: int = 10          # Orderbook depth to process
    ofi_levels: int = 5           # Levels for multi-level OFI
    bands_bps: List[int] = [5, 10, 25, 50]  # Price bands in basis points
    keep_raw_arrays: bool = False  # Store raw bid/ask arrays
    
    # Spread regime thresholds
    tight_spread_threshold: float = 0.0001  # 1 bp
```

---

## Research Questions

### Part 1: Optimal Configuration Parameters

For mid-frequency crypto trading (5s to 4h holding periods), research and recommend optimal values for:

1. **`horizons`**: What rolling window sizes are most predictive?
   - Current: [1, 5, 30, 60] seconds
   - Should we add longer horizons (5min, 15min, 1hr)?
   - What does academic literature say about optimal lookback windows for orderbook features?

2. **`bar_durations`**: What aggregation intervals capture meaningful patterns?
   - Current: [1, 5, 30, 60] seconds
   - Is 1s redundant if we emit HF at 1s?
   - Should we add [300, 900, 3600] (5m, 15m, 1h)?

3. **`max_levels`**: How many orderbook levels contain useful signal?
   - Current: 10 levels
   - Research shows diminishing returns - where is the cutoff for crypto?
   - Does this vary by liquidity tier (BTC vs altcoins)?

4. **`ofi_levels`**: How many levels for OFI calculation?
   - Current: 5 levels
   - Is multi-level OFI better than L1 OFI for mid-frequency?
   - What weighting scheme (uniform, decay, volume-weighted)?

5. **`bands_bps`**: What price bands capture liquidity structure?
   - Current: [5, 10, 25, 50] bps
   - Should bands be asset-specific (tighter for BTC, wider for altcoins)?
   - What does research say about "near-touch" vs "deep" liquidity relevance?

6. **`hf_emit_interval`**: What sampling rate balances signal vs noise?
   - Current: 1.0 seconds
   - Is sub-second (0.1s, 0.5s) useful for mid-frequency?
   - What's the Nyquist frequency for orderbook signal in crypto?

7. **`tight_spread_threshold`**: How to define spread regimes?
   - Current: 0.0001 (1 bp)
   - Should this be dynamic (percentile-based)?
   - What other regime classifications are useful?

---

### Part 2: Missing Features to Implement

Research and identify additional features we should implement:

#### 2.1 Orderbook Shape Features
- **Queue position / priority value**
- **Book slope / elasticity** (price impact per unit volume)
- **Depth imbalance decay** (how imbalance changes across levels)
- **Volume-weighted depth** (alternative to cumulative)
- **Orderbook convexity/concavity**

#### 2.2 Advanced Flow Features
- **Signed OFI** (direction-adjusted)
- **OFI acceleration** (rate of change of OFI)
- **Kyle's Lambda** (price impact coefficient) - rolling estimate
- **Amihud illiquidity measure** (adapted for orderbooks)
- **Trade arrival intensity** (Poisson/Hawkes process features)
- **Volume clock** (volume-based sampling instead of time-based)

#### 2.3 Cross-Asset / Cross-Exchange Features
- **Lead-lag relationships** (BTC leads alts?)
- **Correlation structure** (rolling pairwise correlations)
- **Funding rate integration** (perpetual vs spot)
- **Exchange-specific features** (arb opportunities, fragmentation)

#### 2.4 Information Asymmetry Features
- **VPIN (Volume-Synchronized Probability of Informed Trading)**
- **PIN (Probability of Informed Trading)** - adapted
- **Toxic flow indicators**
- **Unusual size detection** (z-score of trade/quote sizes)

#### 2.5 Regime / State Features
- **Hidden Markov Model regimes** (trending, mean-reverting, volatile)
- **Volatility regime** (low/medium/high)
- **Liquidity regime** (thin/normal/thick)
- **Momentum regime** (breakout vs range-bound)

#### 2.6 Machine Learning-Derived Features
- **Autoencoder latent features** (compress orderbook state)
- **LSTM/Transformer embeddings** (learned representations)
- **PCA components** of rolling orderbook snapshots
- **Clustering-based regime labels**

---

### Part 3: Signal Generation Methodologies

Research state-of-the-art approaches for generating predictive signals from orderbook data:

#### 3.1 Statistical Methods
- **Granger causality** (OFI → returns)
- **Cointegration** (pairs/baskets)
- **Kalman filtering** for fair value estimation
- **GARCH/realized volatility forecasting**

#### 3.2 Machine Learning Models
- **Gradient Boosting** (XGBoost, LightGBM, CatBoost) - feature importance
- **Neural Networks** (MLP, LSTM, Temporal Fusion Transformer)
- **Attention mechanisms** for orderbook data
- **Graph Neural Networks** (order flow as graph)
- **Reinforcement Learning** for dynamic signal weighting

#### 3.3 Signal Combination
- **Ensemble methods**
- **Stacking / meta-learning**
- **Online learning** (adaptive models)
- **Bayesian model averaging**

---

### Part 4: Academic Literature Review

Please review and summarize key findings from:

1. **"Price Impact of Order Book Events"** (Cont, Stoikov, Talreja, 2010)
   - OFI methodology, queue dynamics

2. **"High-Frequency Trading and Price Discovery"** (Brogaard et al.)
   - How HFT affects orderbook, implications for slower traders

3. **"Order Flow and Exchange Rate Dynamics"** (Evans & Lyons)
   - Flow-based models, applicable insights

4. **"Market Microstructure Invariance"** (Kyle & Obizhaeva)
   - Scaling laws, bet size, volatility relationships

5. **"Deep Learning for Limit Order Books"** (Zhang et al., 2019)
   - DeepLOB architecture, feature engineering

6. **"The Volume Clock"** (Easley, López de Prado, O'Hara)
   - VPIN, volume-synchronized sampling

7. **"Advances in Financial Machine Learning"** (López de Prado)
   - Meta-labeling, fractional differentiation, feature importance

8. **Crypto-specific research**:
   - Orderbook dynamics in 24/7 markets
   - Cross-exchange arbitrage patterns
   - Funding rate / perpetual basis signals

---

### Part 5: Implementation Recommendations

Based on your research, provide:

1. **Recommended StateConfig defaults** for mid-frequency crypto trading:
```python
@dataclass
class StateConfig:
    horizons: List[int] = ???
    bar_durations: List[int] = ???
    hf_emit_interval: float = ???
    max_levels: int = ???
    ofi_levels: int = ???
    bands_bps: List[int] = ???
    keep_raw_arrays: bool = ???
    tight_spread_threshold: float = ???
    # New parameters to add?
```

2. **Priority-ranked list of features to implement** (effort vs expected alpha)

3. **Suggested model architectures** for:
   - Short-term (5s-5m) direction prediction
   - Medium-term (5m-1h) trend following
   - Volatility forecasting
   - Regime detection

4. **Data quality / preprocessing recommendations**:
   - Outlier handling
   - Gap filling
   - Normalization strategies
   - Stationarity transformations

5. **Backtesting considerations**:
   - Lookahead bias pitfalls
   - Transaction cost modeling
   - Slippage estimation from orderbook data

---

## Output Format

Please structure your research output as:

1. **Executive Summary** (key findings, recommended config)
2. **Detailed Parameter Analysis** (with citations)
3. **Feature Recommendations** (prioritized, with implementation notes)
4. **Model Architecture Recommendations**
5. **Literature Summary Table**
6. **Code Snippets / Pseudocode** for complex features
7. **References / Bibliography**

---

## Constraints & Context

- **Infrastructure**: Not co-located, latency 50-500ms to exchange
- **Capital**: Retail/prosumer level ($10K-$1M)
- **Compute**: Can run ML inference in real-time, but not GPU-intensive models
- **Data**: Have full L2 orderbook, trades, 1s+ granularity
- **Edge**: Must come from **information processing**, not speed
- **Risk**: Conservative, focus on Sharpe > 2, max drawdown < 20%

---

*This research will directly inform the configuration and feature expansion of a production trading system. Please prioritize actionable, evidence-based recommendations over theoretical possibilities.*
