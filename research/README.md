# Quantitative Research: Crypto Market Microstructure

[![Research Status](https://img.shields.io/badge/Status-Active-brightgreen)]()
[![Data](https://img.shields.io/badge/Data-L2%20Orderbook-blue)]()
[![Exchange](https://img.shields.io/badge/Exchange-Coinbase%20Advanced-orange)]()

> **High-frequency orderbook analysis and alpha signal discovery for cryptocurrency markets**

---

## 📊 Research Portfolio

| Study | Status | Key Finding | Sharpe | Implementation |
|-------|--------|-------------|--------|----------------|
| [Orderbook Feature Analysis](notebooks/01_orderbook_feature_analysis.ipynb) | ✅ Complete | 205 features extracted | N/A | Production |
| [Microstructure Alpha Discovery](notebooks/02_microstructure_alpha_discovery.ipynb) | ✅ Complete | 8.2% correlation signal | 3.2* | Institutional |

*\*At zero fees; economically viable only for market makers*

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

> *"The signal is real. The alpha is there. But the edge is measured in hundredths of basis points—a playground for market makers, not retail."*

---

## 📁 Repository Structure

```
research/
├── README.md                          # This file
├── RESEARCH_PAPER.md                  # Full technical paper
│
├── notebooks/                         # Jupyter notebooks
│   ├── 01_orderbook_feature_analysis.ipynb
│   └── 02_microstructure_alpha_discovery.ipynb
│
├── lib/                               # Research framework (reusable)
│   ├── __init__.py                    #   Public API
│   ├── data.py                        #   DataLoader (Hive partition)
│   ├── signals.py                     #   Signal registry & base classes
│   ├── strategies.py                  #   Strategy base & implementations
│   ├── backtest.py                    #   BacktestEngine → BacktestResult
│   ├── evaluation.py                  #   PerformanceAnalyzer
│   └── deploy.py                      #   ModelExporter → deployment bundles
│
├── deployments/                       # Production deployment bundles
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
    └── QUANT_RESEARCH_CONTEXT.md
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

## 📈 Key Results

### Feature Importance (Correlation with 10s Forward Return)

| Rank | Feature | ρ | Category |
|------|---------|---|----------|
| 1 | `total_imbalance` | 8.13% | Structural |
| 2 | `smart_depth_imbalance` | 8.18% | Structural |
| 3 | `imbalance_L5` | 5.75% | Structural |
| 4 | `imbalance_L3` | 4.12% | Structural |
| 5 | `imbalance_L1` | 2.30% | Structural |

### Backtest Performance (Z=1.5 Entry)

```
Fee Level    │ Return │ Win Rate │ Trades │ Sharpe
─────────────┼────────┼──────────┼────────┼────────
0.00 bps     │ +84.6% │   53.9%  │ 10,513 │   8.4
0.10 bps     │ +49.6% │   46.7%  │ 10,513 │   5.2
0.20 bps     │ +21.2% │   41.0%  │ 10,513 │   3.2
0.25 bps     │  +9.1% │   37.9%  │ 10,513 │   1.8
0.30 bps     │  -1.8% │   34.6%  │ 10,513 │  -0.3
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

## 🚀 Future Research

1. **Multi-Asset Extension**: Test on ETH, SOL, other liquid pairs
2. **Regime Conditioning**: Separate models for high/low volatility
3. **Order Flow Toxicity**: Integrate VPIN, Kyle's Lambda
4. **Execution Optimization**: Optimal order placement using signals
5. **Cross-Exchange Arbitrage**: Latency-adjusted signal propagation

---

## 📚 References

- Cont, R., Kukanov, A., & Stoikov, S. (2014). *The Price Impact of Order Book Events*. Journal of Financial Econometrics.
- Kyle, A. S. (1985). *Continuous Auctions and Insider Trading*. Econometrica.
- Easley, D., López de Prado, M., & O'Hara, M. (2012). *Flow Toxicity and Liquidity in a High-Frequency World*. Review of Financial Studies.

---

## 📧 Contact

For collaboration or inquiries regarding this research, please open an issue or reach out via the repository.

---

*Last Updated: February 2026*
