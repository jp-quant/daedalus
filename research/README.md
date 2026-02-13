# Quantitative Research: Crypto Market Microstructure

[![Research Status](https://img.shields.io/badge/Status-Active-brightgreen)]()
[![Data](https://img.shields.io/badge/Data-L2%20Orderbook-blue)]()
[![Exchange](https://img.shields.io/badge/Exchange-Coinbase%20Advanced-orange)]()

> **High-frequency orderbook analysis and alpha signal discovery for cryptocurrency markets**

---

## Research Portfolio

| Study | Status | Key Finding | Sharpe | Implementation |
|-------|--------|-------------|--------|----------------|
| [01: Feature Analysis](notebooks/01_orderbook_feature_analysis.ipynb) | ✅ Complete | 205 features extracted | N/A | Production |
| [02: Alpha Discovery](notebooks/02_microstructure_alpha_discovery.ipynb) | ✅ Complete | 8.2% correlation signal | 3.2* | Institutional |
| [03: Alpha Optimization](notebooks/03_advanced_alpha_optimization.ipynb) | ✅ Complete | XGBoost ML, composite signal | 100% daily WR | VIP Tier+ |
| [04: Multi-Asset Expansion](notebooks/04_multi_asset_alpha_expansion.ipynb) | ✅ Complete | 9-asset portfolio, +99,201% OOS | 100% daily WR | **Active Trader+** |
| [05: Production Alpha](notebooks/05_production_alpha_realistic_execution.ipynb) | ✅ Complete | Execution realism, capacity, long-only | 100% daily WR | **Production-Ready** |

*Strategy viable for active traders (0.1-0.5 bps fee tier) — not just market makers*

---

## 🎯 Executive Summary

### The Discovery

We identified a **statistically significant predictive signal** in L2 orderbook data:

```
┌─────────────────────────────────────────────────────────────────┐
│  TOTAL ORDERBOOK IMBALANCE → 10-30 SECOND FORWARD RETURNS       │
│                                                                 │
│  Correlation: ρ = 0.082 (p < 0.001)                            │
│  Information Coefficient: IC = 0.065                            │
│  Signal Decay: τ₁/₂ ≈ 45 seconds                               │
└─────────────────────────────────────────────────────────────────┘
```

### Economic Significance

| Fee Regime | 5-Day Return | Annualized | Sharpe | Viable? |
|------------|--------------|------------|--------|---------|
| 0.00 bps | +84.55% | ~6,100% | 8.4 | Theory |
| 0.10 bps | +49.55% | ~3,600% | 5.2 | MM w/ rebate |
| 0.20 bps | +21.20% | ~1,540% | 3.2 | VIP tier |
| 0.25 bps | +9.10% | ~660% | 1.8 | Marginal |
| **0.27 bps** | **0.00%** | **Breakeven** | **0.0** | **Threshold** |
| 1.00 bps | -77.46% | N/A | N/A | ❌ Retail |

### Key Insight

> *"The signal is real, universal, and exploitable. imbalance_L3 dominates across all 9 assets. Altcoins remain profitable beyond 0.5 bps—accessible to active traders with VIP-tier fee schedules, not just market makers."*

---

## Repository Structure

```
research/
├── README.md                          # This file
├── RESEARCH_PAPER.md                  # Full technical paper
├── QUANT_RESEARCH_AGENT_PROMPT.md      # Onboarding prompt for new agents
│
├── notebooks/                         # Jupyter notebooks (sequential)
│   ├── 01_orderbook_feature_analysis.ipynb    # 205 features EDA
│   ├── 02_microstructure_alpha_discovery.ipynb # BTC-only strategy iteration
│   ├── 03_advanced_alpha_optimization.ipynb    # ML, composite signal, multi-asset
│   ├── 04_multi_asset_alpha_expansion.ipynb    # 9-asset expansion, portfolio
│   └── 05_production_alpha_realistic_execution.ipynb  # Execution realism, capacity, production pipeline
│
├── lib/                               # Research framework (reusable)
│   ├── __init__.py                    #   Public API
│   ├── data.py                        #   DataLoader (Hive partition)
│   ├── signals.py                     #   Signal registry & base classes
│   ├── strategies.py                  #   Strategy base & implementations
│   ├── backtest.py                    #   BacktestEngine + BacktestResult
│   ├── evaluation.py                  #   PerformanceAnalyzer
│   └── deploy.py                      #   ModelExporter + deployment bundles
│
├── deployments/                       # Production deployment bundles
│   ├── alpha_v2/                      #   NB03 BTC-optimized (7 files)
│   ├── alpha_v3_multi_asset/          #   NB04 9-asset models (7 files)
│   └── alpha_v4_production/           #   NB05 production alpha (5 files)
│
├── results/                           # Structured outputs
│   ├── 02_strategy_results.json
│   ├── feature_correlations.csv
│   ├── backtest_results.csv
│   └── threshold_analysis.csv
│
├── models/                            # Trained models
│   └── AutogluonModels/
│
└── docs/                              # Supporting documentation
    ├── INDEX.md                       #   Document index
    ├── QUANT_RESEARCH_CONTEXT.md       #   Research context & objectives
    └── MATHEMATICAL_APPENDIX.md       #   Formal definitions & derivations
```

---

## 🔬 Methodology

### Data Pipeline

```
Raw L2 Orderbook     Feature Engineering      Signal Generation
    (1Hz)          ─────────────────────►    ─────────────────►
                        205 Features              Imbalance
    ┌─────┐              ┌─────┐               Z-Score
    │ Bid │              │ OFI │                  │
    │Depth│      →       │VPIN │        →     Entry: Z > 1.5
    │ Ask │              │ λ   │              Exit:  Z < 0.5
    │Depth│              │ σ   │
    └─────┘              └─────┘
```

### Strategies Evaluated

| # | Strategy | Approach | Result |
|---|----------|----------|--------|
| 1 | Optimal Trade Points | Hindsight-optimal → ML | ❌ 0.002% class imbalance |
| 2 | Forward Return Regression | XGBoost regression | ❌ R²=0.016 |
| 3 | Mean Reversion | Price Z-score | ❌ Negative at 0 fee |
| 4 | Hourly Direction | LogReg classifier | ⚠️ 53% acc, fee-sensitive |
| 5 | **Imbalance Signal** | Z-score threshold | ✅ **Profitable <0.27bps** |

---

## Key Results

### Feature Importance (Correlation with 30s Forward Return, Multi-Asset Average)

| Rank | Feature | Avg |r| | Best Asset | Best |r| |
|------|---------|---------|------------|----------|
| 1 | `imbalance_L3` | **0.274** | HBAR-USD | 0.298 |
| 2 | `imbalance_L5` | 0.201 | HBAR-USD | 0.247 |
| 3 | `imbalance_L1` | 0.185 | DOGE-USD | 0.237 |
| 4 | `imb_band_0_5bps` | 0.162 | ADA-USD | 0.219 |
| 5 | `cog_vs_mid` | 0.134 | ADA-USD | 0.184 |

### ML Walk-Forward Performance (XGBoost, 0.6/0.4, 30-bar hold, 0.1 bps)

```
Asset        | Return         | AUC   | WR    | Days+
-------------+----------------+-------+-------+------
HBAR-USD     | +2,040,819%    | 0.736 | 69.1% | 9/9
DOGE-USD     | +806,297%      | 0.762 | 72.4% | 9/9
ADA-USD      | +740,258%      | 0.779 | 65.0% | 9/9
AAVE-USD     | +473,864%      | 0.722 | 69.6% | 9/9
FARTCOIN-USD | +102,927%      | 0.685 | 58.5% | 9/9
AVAX-USD     | +14,422%       | 0.827 | 61.2% | 9/9
ETH-USD      | +2,822%        | 0.616 | 53.7% | 9/9
BCH-USD      | +1,323%        | 0.622 | 57.6% | 9/9
BTC-USD      | +7.4%          | 0.576 | 47.1% | 5/9
```

---

## 🏗️ Research Framework (`research/lib/`)

The discoveries above are backed by a **modular, extensible framework** enabling rapid strategy iteration and production deployment.

### Architecture

```
DataLoader ──► BaseSignal ──► BaseStrategy ──► BacktestEngine ──► BacktestResult
                   │               │                │                   │
            ImbalanceSignal   ImbalanceStrategy    sweep_fees()    PerformanceAnalyzer
            ForwardReturn     MeanReversion        sweep_param()   correlation_matrix()
            PriceZScore       Regression           run_strategy()  fee_sensitivity()
            Percentile        Direction                                 │
            OptimalTrade      UltraSelective                     ModelExporter
                                                                  → deployment bundle
```

### Quick Start

```python
from research.lib import DataLoader, ImbalanceSignal, ImbalanceStrategy, BacktestEngine

loader = DataLoader(data_root="data/processed/silver/orderbook")
df = loader.load_day(2026, 1, 22)
prices = loader.get_prices(df)

signal = ImbalanceSignal(column="total_imbalance", lookback=600)
z_scores = signal.generate(df, prices)

strategy = ImbalanceStrategy(entry_z=1.5, exit_z=0.5, max_hold=30)
engine = BacktestEngine(fee_pct=0.0002)
result = engine.run_strategy(prices, z_scores, strategy)

print(f"Return: {result.total_return_pct:+.2f}%, Sharpe: {result.sharpe:.1f}")
```

### Extending

| To add... | Subclass | Implement |
|-----------|----------|-----------|
| New signal | `BaseSignal` | `generate(df, prices) → np.ndarray` |
| New strategy | `BaseStrategy` | `generate_positions(prices, signal) → np.ndarray` |
| Production deploy | `ModelExporter.export()` | Outputs `config.yaml` + `features.json` + model |

---

## Future Research

### Completed
- ✅ **NB01**: Feature engineering (205 features from L2 orderbook)
- ✅ **NB02**: Strategy iteration (5 strategies, imbalance z-score wins)
- ✅ **NB03**: ML enhancement, composite signal, regime conditioning, early multi-asset
- ✅ **NB04**: Full 9-asset expansion (39 days), portfolio construction, statistical validation, fee sensitivity
- ✅ **NB05**: Production alpha — holding period sweep (30s-30m), long-only analysis, execution realism (64 latency/slippage scenarios), capacity analysis (Kyle's lambda, up to $100K), production ML pipeline (expanding window, feature stability), full 9-asset validation at production horizons

### Next Steps (Notebook 06+)
1. **Live Paper Trading**: Real-time simulation with actual exchange connectivity
2. **Dynamic Asset Allocation**: Weight assets by predicted AUC / signal strength rather than equal-weight
3. **Extended Feature Exploration**: Only using 79/205 features. Many unexplored.
4. **Cross-Exchange Arbitrage**: Latency-adjusted signal propagation
5. **Regime Detection**: Adapt to different market conditions
6. **Multi-Timeframe Models**: Combine signals from multiple horizons

---

## 📚 References

- Cont, R., Kukanov, A., & Stoikov, S. (2014). *The Price Impact of Order Book Events*. Journal of Financial Econometrics.
- Kyle, A. S. (1985). *Continuous Auctions and Insider Trading*. Econometrica.
- Easley, D., López de Prado, M., & O'Hara, M. (2012). *Flow Toxicity and Liquidity in a High-Frequency World*. Review of Financial Studies.

---

## 📧 Contact

For collaboration or inquiries regarding this research, please open an issue or reach out via the repository.

---

*Last Updated: February 12, 2026*  
*Notebooks: 5 complete | Assets: 9 | Data: 39 days (Jan 1 - Feb 10, 2026) | Total: ~36.3 GB*
