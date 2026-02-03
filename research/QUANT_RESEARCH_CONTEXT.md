# Quantitative Crypto Market Research - Context & Goals

## Researcher Profile

**Role**: Senior Quantitative Researcher - Crypto Markets  
**Focus**: Alpha extraction from high-frequency orderbook microstructure data  
**Exchange**: Coinbase Advanced (US-regulated)  
**Data Source**: Real-time L2 orderbook snapshots + trades (CCXT WebSocket)

---

## Research Objectives

### Primary Goals

1. **Short-term Price Direction Prediction**
   - Predict price movement direction over horizons: 1s, 5s, 30s, 1m, 5m
   - Binary classification (up/down) and regression (return magnitude)
   - Target: AUC > 0.55, Sharpe > 2.0 after transaction costs

2. **Volatility Regime Classification**
   - Identify high/medium/low volatility regimes from orderbook features
   - Use for dynamic position sizing and risk management
   - Target: 3-class classification with F1 > 0.7

3. **Liquidity-Adjusted Execution Signals**
   - Predict optimal execution timing based on liquidity conditions
   - Minimize market impact for larger orders
   - Feature importance: spread, depth, Kyle's Lambda

4. **Cross-Asset Correlation Discovery**
   - Find leading/lagging relationships between crypto pairs
   - Build pair trading signals from orderbook divergence

### Secondary Goals

- **Feature Importance Analysis**: Identify which orderbook features carry alpha
- **Model Interpretability**: Understand WHY models predict (SHAP, partial dependence)
- **Regime-Conditional Models**: Different models for different market conditions

---

## Data Architecture

### Feature Hierarchy

```
Bronze (Raw)          →    Silver (Features)       →    Gold (Aggregated)
─────────────────────────────────────────────────────────────────────────
orderbook snapshots        Structural features          OHLCV bars
trades                     Dynamic features             Aggregated signals
                          Rolling features             Model predictions
                          Advanced features
```

### Feature Categories (197 total features)

| Category | Count | Examples | Use Case |
|----------|-------|----------|----------|
| **Structural** | 109 | L0-L19 prices/sizes, microprice, imbalance | Static book state |
| **Dynamic** | 23 | OFI, MLOFI, velocity, acceleration | Order flow momentum |
| **Rolling** | 13 | realized_vol, mean_spread, regime_fraction | Time-aggregated stats |
| **Advanced** | 4 | Kyle's Lambda, VPIN, toxicity | Market quality metrics |

### Key Features for Alpha

| Feature | Description | Academic Reference |
|---------|-------------|-------------------|
| `ofi` | Order Flow Imbalance | Cont et al. (2014) |
| `mlofi_*` | Multi-level OFI with decay | Extension of Cont |
| `microprice` | Volume-weighted fair value | - |
| `imbalance_L1` | Top-of-book imbalance | - |
| `kyle_lambda` | Price impact coefficient | Kyle (1985) |
| `vpin` | Volume-sync. PIN | Easley et al. (2012) |
| `smart_depth_imbalance` | Distance-weighted depth | - |
| `realized_vol_*` | Rolling volatility | - |

---

## Modeling Approach

### AutoML Strategy (AutoGluon)

We use AutoGluon's TabularPredictor for rapid model screening:

```python
from autogluon.tabular import TabularPredictor

# Binary classification: predict up/down
predictor = TabularPredictor(
    label='target_direction',
    problem_type='binary',
    eval_metric='roc_auc',
)

predictor.fit(
    train_data=train_df,
    presets='best_quality',  # Ensemble of GBM, NN, etc.
    time_limit=3600,
)
```

### Model Zoo

AutoGluon automatically trains and ensembles:
- **LightGBM**: Fast gradient boosting
- **CatBoost**: Handles categoricals well  
- **XGBoost**: Robust baseline
- **Neural Networks**: TabNet, MLP
- **Ensemble**: Weighted combination

### Target Engineering

| Target | Formula | Horizon | Problem Type |
|--------|---------|---------|--------------|
| `ret_1s` | `log(mid[t+1s] / mid[t])` | 1 second | Regression |
| `ret_5s` | `log(mid[t+5s] / mid[t])` | 5 seconds | Regression |
| `dir_1s` | `sign(ret_1s)` | 1 second | Binary |
| `dir_5s` | `sign(ret_5s)` | 5 seconds | Binary |
| `vol_regime` | Quantile of realized_vol | N/A | Multiclass |

---

## File Structure

```
research/
├── QUANT_RESEARCH_CONTEXT.md      # This file - research goals & context
├── 01_data_exploration.ipynb       # EDA, feature distributions
├── 02_target_engineering.ipynb     # Label creation, horizon analysis
├── 03_feature_selection.ipynb      # Correlation, importance, redundancy
├── 04_model_training.ipynb         # AutoGluon training
├── 05_backtesting.ipynb            # Strategy simulation
├── 06_interpretability.ipynb       # SHAP, feature importance
└── models/                         # Saved model artifacts
```

---

## Quick Start for New Agents

### 0. Environment Setup

```bash
# Activate virtual environment
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install AutoGluon (recommended for ML modeling)
pip install autogluon

# Or for lighter install (tabular only):
pip install autogluon.tabular

# Install visualization packages
pip install plotly seaborn
```

### 1. Load Features Data

```python
import polars as pl

# Single partition (1 hour)
df = pl.read_parquet(
    "data/processed/silver/orderbook/exchange=coinbaseadvanced/"
    "symbol=BTC-USD/year=2026/month=1/day=26/hour=9/*.parquet"
)

# Full dataset (scan lazily)
lf = pl.scan_parquet(
    "data/processed/silver/orderbook/exchange=coinbaseadvanced/"
    "symbol=BTC-USD/**/*.parquet"
)
```

### 2. Create Targets

```python
# Forward returns at various horizons
df = df.with_columns([
    (pl.col("mid_price").shift(-1) / pl.col("mid_price") - 1).alias("ret_1tick"),
    (pl.col("mid_price").shift(-5) / pl.col("mid_price") - 1).alias("ret_5tick"),
])

# Binary direction
df = df.with_columns([
    (pl.col("ret_5tick") > 0).cast(pl.Int8).alias("dir_5tick"),
])
```

### 3. Train with AutoGluon

```python
from autogluon.tabular import TabularPredictor

# Select features (exclude targets, meta columns)
feature_cols = [c for c in df.columns if c not in ['timestamp', 'symbol', ...]]

predictor = TabularPredictor(label='dir_5tick', path='models/dir_5tick')
predictor.fit(train_df[feature_cols + ['dir_5tick']], time_limit=600)
```

### 4. Evaluate

```python
# Leaderboard
predictor.leaderboard(test_df, silent=True)

# Feature importance
predictor.feature_importance(test_df)
```

---

## Research Log

| Date | Researcher | Finding |
|------|------------|---------|
| 2026-01-31 | Initial | Created feature pipeline with 197 features |
| 2026-01-31 | EDA Phase | Completed comprehensive EDA (1 hour sample) |
| 2026-02-02 | Full Day Analysis | AutoGluon modeling on 613K records |
| **2026-02-02** | **Walk-Forward & Backtest** | **Complete validation pipeline** |

### Walk-Forward Validation & Backtest (2026-02-02) ⭐ LATEST

**Walk-Forward Cross-Validation (5 folds):**
| Metric | Mean ± Std | Interpretation |
|--------|------------|----------------|
| ROC-AUC | **0.601 ± 0.041** | Modest predictive power |
| Accuracy | 0.636 ± 0.058 | Above baseline |
| Precision | 0.469 ± 0.108 | **Below break-even** |
| F1 Score | 0.474 ± 0.110 | Needs improvement |
| **Stability** | ❌ UNSTABLE | AUC range 0.11 across folds |

**Critical Finding: Model is UNSTABLE**
- Best fold: #2 (AUC=0.655)
- Worst fold: #1 (AUC=0.548)
- Performance varies significantly with time period

**Threshold Optimization:**
- Precision never exceeds 42% even at high thresholds (0.8)
- Edge per trade is **negative** at all thresholds
- No threshold achieves profitability

**Backtest Results (with 130 bps round-trip costs):**
| Threshold | Trades | Win Rate | Avg Return | Total Return |
|-----------|--------|----------|------------|--------------|
| 0.50 | ~244K | 38% | -98 bps | **Massive loss** |
| 0.55 | ~184K | 37% | -104 bps | Massive loss |
| 0.60 | ~141K | 38% | -101 bps | Massive loss |
| 0.65 | ~102K | 37% | -106 bps | Massive loss |

**🚨 CRITICAL CONCLUSION: Model cannot overcome transaction costs!**
- With 40% precision and 130 bps costs, every trade loses money
- Need precision > 55% to break even after costs
- Current model is NOT profitable

**Feature Selection Results:**
| N Features | ROC-AUC | Precision | Notes |
|------------|---------|-----------|-------|
| 5 | 0.554 | 42% | Too sparse |
| 10 | 0.602 | 49% | **Recommended min** |
| 15 | 0.602 | 50% | Good trade-off |
| **20** | **0.617** | **50%** | **Best precision** |
| Full (27) | 0.601 | 47% | Overfitting? |

**Top 10 Features for Production:**
1. `imbalance_L1`
2. `relative_spread`
3. `imbalance_L5`
4. `total_imbalance`
5. `bid_concentration`
6. `imbalance_L10`
7. `imb_band_5_10bps`
8. `mlofi`
9. `ofi`
10. `hour_cos`

### Artifacts Created:
- `research/enhanced_findings.json` - Complete validation results
- `research/walk_forward_results.csv` - Fold-by-fold metrics
- `research/backtest_results.csv` - Threshold comparison
- `research/threshold_analysis.csv` - Precision-recall curve

---

### Path to Profitability - Research Agenda

Given current results showing **unprofitable** model, priority research:

1. **Longer Horizons**: Try `dir_60tick` or `dir_120tick` for larger price moves
2. **Volatility Filtering**: Only trade during high-vol regimes (bigger moves)
3. **Cost Reduction**: Use maker orders (40 bps vs 60 bps) with limit orders
4. **Multi-Day Validation**: Current 1-day may not be representative
5. **Feature Engineering**: Try lag features, interaction terms, regime indicators
6. **Alternative Targets**: Predict volatility instead of direction

---

### Full Day Analysis (2026-02-02)

**Data Characteristics (Full Day - 2026-01-26):**
- 613,480 records (24 hours of trading)
- Event-driven data: 54ms median, 141ms mean inter-arrival
- **NOT fixed-frequency** - irregular timestamps!
- Price range: $86,412 - $88,767 (2.73% daily range)

**⚠️ Critical: Event-Driven Data Handling:**
Added time features to account for irregular timestamps:
- `delta_seconds` - Time since last event
- `hour_of_day`, `hour_sin`, `hour_cos` - Intraday patterns  
- `is_gap_1s`, `is_gap_5s` - Gap indicators

**Target Selection (dir_30tick chosen):**
| Horizon | % Up | Recommendation |
|---------|------|----------------|
| 1-tick | 7% | ❌ Too imbalanced |
| 5-tick | 15% | ⚠️ Still imbalanced |
| **30-tick** | **33%** | ✅ **Best balance** |
| 60-tick | 40% | ✅ Good alternative |

**AutoGluon Model Results:**
| Metric | Value |
|--------|-------|
| Best Model | LightGBM_BAG_L1 |
| Test ROC-AUC | **0.629** |
| Test Accuracy | 61.9% |
| Training Time | ~6 minutes |

**Top Features by Model Importance (Permutation):**
| Feature | Importance | Correlation |
|---------|------------|-------------|
| `imbalance_L1` | **0.028** | +0.030 |
| `relative_spread` | **0.019** | +0.015 |
| `imbalance_L5` | 0.006 | +0.057 |
| `total_imbalance` | 0.006 | +0.087 |
| `bid_concentration` | 0.005 | +0.040 |

**Key Insight**: `imbalance_L1` (top-of-book imbalance) is the most predictive feature, even though it had lower raw correlation than depth imbalances!

**Artifacts Created:**
- `research/analysis_findings.json` - Complete analysis results
- `research/feature_importance_autogluon.csv` - Model feature importance
- `research/model_leaderboard.csv` - All model performances
- `research/models/autogluon_BTC_USD_dir_30tick/` - Trained model

### Previous EDA Phase Findings (2026-01-31)

**Data Characteristics (1 Hour Sample):**
- High-frequency data: ~18 updates/sec (54ms median inter-arrival)
- 28,138 records per hour, 207 columns
- Very tight spreads: 0.005 bps mean (liquid market)

**Multicollinearity Warning:**
- `total_imbalance` ≈ `book_pressure` ≈ `smart_depth_imbalance` (r=1.0)
- AutoGluon automatically handles by ignoring redundant features

---

## References

1. Cont, R., Kukanov, A., & Stoikov, S. (2014). "The Price Impact of Order Book Events"
2. Kyle, A. S. (1985). "Continuous Auctions and Insider Trading"
3. Easley, D., López de Prado, M., & O'Hara, M. (2012). "Flow Toxicity and Liquidity"
4. López de Prado, M. (2018). "Advances in Financial Machine Learning"
