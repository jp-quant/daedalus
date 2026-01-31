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
| | | |

---

## References

1. Cont, R., Kukanov, A., & Stoikov, S. (2014). "The Price Impact of Order Book Events"
2. Kyle, A. S. (1985). "Continuous Auctions and Insider Trading"
3. Easley, D., López de Prado, M., & O'Hara, M. (2012). "Flow Toxicity and Liquidity"
4. López de Prado, M. (2018). "Advances in Financial Machine Learning"
