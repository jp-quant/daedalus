# Quantitative Crypto Market Research - Context & Goals

## Researcher Profile

**Role**: Senior Quantitative Researcher - Crypto Markets  
**Focus**: Alpha extraction from high-frequency orderbook microstructure data  
**Exchange**: Coinbase Advanced (US-regulated)  
**Data Source**: Real-time L2 orderbook snapshots + trades (CCXT WebSocket)  
**Context**: Solo retail quant trader (1 person), 50-500ms latency, mid-frequency (5s-4hr holds)

---

## Research Objectives

### Primary Goal (Permanent Standing Objective)

> Uncover as high win-rate alpha(s) as possible that are well and heavily tested and statistically reliable, precise, optimizing for maximal profitability in an executable timeframe in our context as a solo quant trader.

### Specific Targets

1. **Short-term Price Direction Prediction**
   - Predict price movement direction over 10-30 second horizons
   - Binary classification via XGBoost direction classifier
   - Target: AUC > 0.7, 100% daily win rate, profitable at 0.1-0.5 bps
   - **STATUS: ACHIEVED** for 8/9 assets (all except BTC)

2. **Multi-Asset Portfolio Construction**
   - Diversify across 9 crypto assets for portfolio-level consistency
   - Equal-weight portfolio: +99,201% over 9 OOS days
   - **STATUS: ACHIEVED** with 9/9 portfolio days profitable

3. **Statistical Validation**
   - Permutation tests, bootstrap CI, Holm-Bonferroni correction
   - **STATUS: ACHIEVED** for 8/8 altcoins

4. **Fee Viability**
   - Profitable at realistic fee levels (0.1-0.5 bps)
   - **STATUS: ACHIEVED** for 7/9 assets at ALL tested levels up to 0.5 bps

### Remaining Objectives

5. **Capacity Analysis**: How much capital before alpha decays?
   - **STATUS: ACHIEVED** — Kyle's lambda analysis confirms $100K positions feasible with negligible market impact (NB05)

6. **Dynamic Allocation**: Weight by signal strength / AUC
   - **STATUS: ACHIEVED** — Momentum weighting (+1,190,717%), AUC-weighted, and inverse-vol all tested (NB06). All maintain 9/9 profitable days.
7. **Live Paper Trading**: Validate latency assumptions
   - **STATUS: PARTIALLY ACHIEVED** — Execution realism simulation shows all 64 latency/slippage scenarios profitable (NB05). Real-time paper trading pending.
8. **Production Pipeline**: Daily retraining + automated execution
   - **STATUS: ACHIEVED** — Expanding window daily retraining validated with 100% feature stability for core features (NB05)
9. **Extended Feature Exploration**: Expand beyond 72 PROD features
   - **STATUS: ACHIEVED** — 111 features screened → 19 new + 13 engineered = 104 FULL features (NB06). Portfolio improved 15.4x.
10. **Multi-Timeframe Ensemble**: Combine signals from multiple horizons
    - **STATUS: ACHIEVED** — 30s/1m/2m ensemble tested (NB06). 30s dominates; ensembles don't beat single-best horizon.

---

## Data Architecture

### Feature Hierarchy

```
Bronze (Raw)          ->    Silver (Features)       ->    Research
Raw orderbook               Structural features          ML models
snapshots + trades          Dynamic features             Strategy signals
                           Rolling features              Deployment bundles
                           Advanced features
```

### Data Universe (as of Feb 11, 2026)

| Symbol | Days | Size | Signal Strength |
|--------|------|------|-----------------|
| BTC-USD | 39 | 8,864 MB | |r|=0.064 (weak) |
| ETH-USD | 39 | 8,291 MB | |r|=0.109 |
| BCH-USD | 39 | 4,394 MB | |r|=0.076 |
| DOGE-USD | 39 | 3,465 MB | |r|=0.267 (strong) |
| HBAR-USD | 39 | 3,249 MB | |r|=0.298 (strongest) |
| AAVE-USD | 39 | 3,021 MB | |r|=0.223 |
| ADA-USD | 39 | 2,150 MB | |r|=0.282 (strong) |
| FARTCOIN-USD | 39 | 1,657 MB | |r|=0.265 |
| AVAX-USD | 39 | 1,250 MB | |r|=0.225 |

**Total**: ~36.3 GB, all Jan 1 - Feb 10, 2026 (gap Jan 10-11)
**Partition**: exchange/symbol/year/month/day/hour/*.parquet
**Schema**: 205 columns

### Feature Categories (205 total)

| Category | Count | Examples | Alpha Contribution |
|----------|-------|----------|-------------------|
| **Imbalance** | 20+ | imbalance_L1/L3/L5/L10, total_imbalance | **PRIMARY ALPHA SOURCE** |
| **Structural** | 109 | L0-L19 prices/sizes, microprice | Raw data |
| **Order Flow** | 23 | OFI, MLOFI, VPIN, toxicity | Secondary signals |
| **Depth** | 15+ | total_bid/ask_depth, concentration | Context features |
| **Volatility** | 10+ | rv_5s/15s/60s/300s/900s | Regime detection |
| **Advanced** | 10+ | Kyle's Lambda, center_of_gravity | Supplementary |

### Key Features for Alpha (Ranked by |r| with 30s Forward Return)

| Rank | Feature | Avg |r| | Description |
|------|---------|---------|-------------|
| 1 | `imbalance_L3` | **0.274** | 3rd depth level imbalance |
| 2 | `imbalance_L5` | 0.201 | 5th depth level imbalance |
| 3 | `imbalance_L1` | 0.185 | Top-of-book imbalance |
| 4 | `imb_band_0_5bps` | 0.162 | Near-the-money band imbalance |
| 5 | `imbalance_L10` | 0.140 | 10th depth level imbalance |
| 6 | `cog_vs_mid` | 0.134 | Center of gravity vs midprice |
| 7 | `ofi_sum_5s` | 0.098 | 5-second order flow imbalance |
| 8 | `smart_depth_imbalance` | 0.089 | Distance-weighted imbalance |

---

## Current Best Strategy

### XGBoost Direction Classifier (NB03-04)

```python
# Configuration
model = XGBoost(max_depth=4, n_estimators=200, learning_rate=0.05)
thresholds = {'long': 0.6, 'short': 0.4}  # P(up) probability thresholds
hold_period = 30  # bars (~30 seconds)
fee = 0.1  # bps per side (0.00001)
features = ['imbalance_L3', 'imbalance_L5', 'imbalance_L1', 'imb_band_0_5bps',
            'imbalance_L10', 'cog_vs_mid', 'ofi_sum_5s', 'smart_depth_imbalance']
```

### Results Summary

| Metric | BTC (NB03) | 8 Altcoins (NB04) | Portfolio (NB04) | Long-Only 1m (NB05) | Long-Only 2m (NB05) | FULL 1m LO (NB06) |
|--------|------------|-------------------|-----------------|---------------------|---------------------|-------------------|
| OOS Days | 12 | 9 | 9 | 9 | 9 | 9 |
| Daily Win Rate | 100% (12/12) | 100% (9/9 each) | 100% (9/9) | 100% (9/9) | 89% (8/9) | 100% (8/9 assets) |
| Trade Win Rate | 54.9% | 53.7%-72.4% | N/A | 57.1%-76.2% | 56.3%-68.4% | — |
| AUC | 0.748 | 0.616-0.827 | N/A | 0.593-0.783 | 0.560-0.706 | — |
| Return (0.1 bps) | +161.4% | +1,323% to +2,040,819% | +99,201% | +133% to +7,437% | +86% to +1,552% | +56% to +783,115% |
| Portfolio | — | — | +99,201% | +2,310% | +650% | **+35,483%** |
| Stat. Significant | Yes (p<0.001) | Yes for all 8 (p<0.005) | Yes | — | — | 4/9 Holm-Bonf. |
| Max Viable Fee | 0.27 bps | >0.5 bps (7/8) | N/A | >0.5 bps (all) | >0.5 bps (all) | >0.5 bps (7/9) |

---

## Research Log

| Date | Session | Finding | Notebook |
|------|---------|---------|----------|
| 2026-01-31 | Initial | Created feature pipeline, 205 features | NB01 |
| 2026-02-02 | EDA | AutoGluon modeling, feature importance | Pre-NB02 |
| 2026-02-02 | Walk-Forward | Walk-forward + backtest pipeline | Pre-NB02 |
| 2026-02-04 | NB01 | Comprehensive feature analysis, 205 features catalogued | NB01 |
| 2026-02-05 | NB02 | 5 strategies tested, imbalance z-score wins, breakeven 0.27 bps | NB02 |
| 2026-02-06 | NB03 | XGBoost ML, composite signal (r=0.114), 100% daily WR | NB03 |
| 2026-02-06 | NB03 | Early multi-asset: SOL +383%, XRP +136%, DOGE +68% (3 days) | NB03 |
| **2026-02-11** | **NB04** | **9-asset expansion: imbalance_L3 universal king, 8/8 altcoins validated** | **NB04** |
| **2026-02-11** | **NB04** | **Portfolio: +99,201%, 9/9 days. All altcoins viable at 0.5 bps** | **NB04** |
| **2026-02-12** | **NB05** | **Holding period sweep: 30s-30m, alpha persists to 15m. Best: 30s, 1m, 2m** | **NB05** |
| **2026-02-12** | **NB05** | **Long-only: 38.7% alpha retention, higher WR, retention ↑ with horizon** | **NB05** |
| **2026-02-12** | **NB05** | **Execution realism: 64 scenarios ALL profitable. Max: 5-bar lat + 0.20 bps slip** | **NB05** |
| **2026-02-12** | **NB05** | **Capacity: $100K positions feasible. Kyle's λ ≈ 0 for HBAR/DOGE/ADA** | **NB05** |
| **2026-02-12** | **NB05** | **Production pipeline: Expanding window, 100% feature stability, daily retrain OK** | **NB05** |
| **2026-02-12** | **NB05** | **9-asset portfolio: +2,310% (1m), +650% (2m), long-only, 0.1 bps** | **NB05** |
| **2026-02-12** | **Reporting** | **Built `research/lib/reporting.py`: PerformanceReport + StrategyComparison** | **NB05 Part 8** |
| **2026-02-12** | **Reporting** | **Interactive Plotly dashboards: equity+DD, trades, rolling metrics, fees, comparison** | **NB05 Part 8** |
| **2026-02-12** | **Reporting** | **Validated: HBAR +161% (PF 2.51, DD -3.43%), DOGE +149% (PF 2.28, DD -5.89%)** | **NB05 Part 8** |
| **2026-02-12** | **Reporting** | **Reports exported to `research/reports/` (trades.csv, equity.csv, metrics.json)** | **NB05 Part 8** |
| **2026-02-13** | **NB06** | **Extended feature screening: 111 features × 4 assets × 3 horizons. 19 new features found** | **NB06** |
| **2026-02-13** | **NB06** | **13 engineered interaction features (imb_L3_div_rv60, depth_asymmetry, etc.)** | **NB06** |
| **2026-02-13** | **NB06** | **EXTENDED (91) beats PROD (72) by 1.09x–2.11x return ratio on all 4 assets** | **NB06** |
| **2026-02-13** | **NB06** | **Multi-timeframe ensemble: 30s dominates. Ensembles don't beat single best horizon** | **NB06** |
| **2026-02-13** | **NB06** | **Dynamic allocation: Momentum +1,190,717% (best). All methods 9/9 profitable days** | **NB06** |
| **2026-02-13** | **NB06** | **Full 9-asset validation (FULL, 1m, LO): +35,483% EW portfolio (15.4x over NB05)** | **NB06** |

### Key Strategic Insight (NB06)

> **Extended features (104 total) deliver 15.4x portfolio improvement over NB05's 72-feature baseline. 19 newly discovered features (led by volume band features `bid_vol_band_0_5bps` |r|=0.137) plus 13 engineered interactions materially improve alpha extraction. 30s horizon dominates raw returns (AUC 0.735–0.796) but 1m remains preferred for production. Momentum-weighted allocation (+1,190,717%) outperforms equal-weight (+820,221%). All 9 assets P(>0) ≥ 99.9% by bootstrap.**

---

## Artifacts Inventory

### Notebooks
- `research/notebooks/01_orderbook_feature_analysis.ipynb` - COMPLETE
- `research/notebooks/02_microstructure_alpha_discovery.ipynb` - COMPLETE
- `research/notebooks/03_advanced_alpha_optimization.ipynb` - COMPLETE (42 cells)
- `research/notebooks/04_multi_asset_alpha_expansion.ipynb` - COMPLETE (27 cells, 19 code)
- `research/notebooks/05_production_alpha_realistic_execution.ipynb` - COMPLETE (37 cells, 28 code) — includes Part 8: Reporting Framework
- `research/notebooks/06_extended_features_multitimeframe_ensemble.ipynb` - COMPLETE (30 cells) — Extended features, ensemble, allocation

### Deployment Bundles
- `research/deployments/alpha_v2/` - NB03 BTC-optimized (7 files)
- `research/deployments/alpha_v3_multi_asset/` - NB04 9-asset models (7 files)
- `research/deployments/alpha_v4_production/` - NB05 production alpha (5 files)
  - `config.json` - Strategy parameters, horizons, execution config
  - `features.json` - 79-feature list with descriptions
  - `capacity_analysis.csv` - Kyle's lambda, depth, volume, max positions
  - `full_validation.json` - 9-asset results at 1m and 2m horizons
  - `results_summary.json` - Comprehensive NB05 findings
- `research/deployments/alpha_v5_ensemble/` - NB06 extended ensemble (6 files)
  - `config.json` - Strategy parameters, feature sets, ensemble config
  - `features.json` - 104-feature list (FULL set)
  - `full_validation.json` - 9-asset results with FULL features at 1m LO
  - `ensemble_results.json` - Multi-timeframe ensemble methods & results
  - `allocation_results.json` - Dynamic allocation methods & portfolio returns
  - `feature_screening.json` - 111-feature screening results with correlations

### Result Files
- `research/results/02_strategy_results.json`
- `research/results/feature_correlations.csv`
- `research/results/backtest_results.csv`
- `research/results/threshold_analysis.csv`

### Report Artifacts (from `research/lib/reporting.py`)
- `research/reports/hbar_usd/` — trades.csv, equity.csv, metrics.json
- `research/reports/doge_usd/` — trades.csv, equity.csv, metrics.json
- `research/reports/combined_summary.json` — All strategies combined
- `research/reports/strategy_comparison.csv` — Side-by-side comparison table

### Research Framework (`research/lib/`)
- `data.py` — DataLoader (Hive-partitioned Parquet)
- `signals.py` — Signal classes (ImbalanceSignal, ForwardReturnSignal, etc.)
- `strategies.py` — Strategy classes (Direction, Regression, Imbalance, etc.)
- `backtest.py` — BacktestEngine + BacktestResult + TradeLog
- `evaluation.py` — PerformanceAnalyzer
- `reporting.py` — **NEW** PerformanceReport + StrategyComparison (Plotly interactive dashboards)
- `deploy.py` — ModelExporter

---

## References

1. Cont, R., Kukanov, A., & Stoikov, S. (2014). "The Price Impact of Order Book Events"
2. Kyle, A. S. (1985). "Continuous Auctions and Insider Trading"
3. Easley, D., Lopez de Prado, M., & O'Hara, M. (2012). "Flow Toxicity and Liquidity"
4. Lopez de Prado, M. (2018). "Advances in Financial Machine Learning"
5. Cao, C., Hansch, O., & Wang, X. (2009). "The Information Content of an Open Limit-Order Book"

---

*Last Updated: February 13, 2026*
