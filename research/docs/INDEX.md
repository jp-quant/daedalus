# Research Documentation Index

## Core Documents

| Document | Description | Audience |
|----------|-------------|----------|
| [README.md](../README.md) | Executive summary and portfolio overview | All |
| [RESEARCH_PAPER.md](../RESEARCH_PAPER.md) | Full technical paper with methodology and results (NB01-04) | Quants, Researchers |
| [QUANT_RESEARCH_AGENT_PROMPT.md](../QUANT_RESEARCH_AGENT_PROMPT.md) | Onboarding prompt for new QR agents | Agents |
| [MATHEMATICAL_APPENDIX.md](MATHEMATICAL_APPENDIX.md) | Formal definitions and derivations | Quants, Academics |
| [QUANT_RESEARCH_CONTEXT.md](QUANT_RESEARCH_CONTEXT.md) | Research context, log, and objectives | Team members |

## Notebooks

| Notebook | Description | Key Outputs |
|----------|-------------|-------------|
| [01_orderbook_feature_analysis](../notebooks/01_orderbook_feature_analysis.ipynb) | Feature engineering and EDA | 205 features defined |
| [02_microstructure_alpha_discovery](../notebooks/02_microstructure_alpha_discovery.ipynb) | Iterative strategy research & alpha discovery | Imbalance signal (rho=0.082), breakeven 0.27 bps |
| [03_advanced_alpha_optimization](../notebooks/03_advanced_alpha_optimization.ipynb) | ML enhancement, composite signal, multi-asset | XGBoost 100% daily WR, +161.4% OOS |
| [04_multi_asset_alpha_expansion](../notebooks/04_multi_asset_alpha_expansion.ipynb) | 9-asset expansion, portfolio, statistical validation | +99,201% portfolio, 8/8 altcoins validated |
| [05_production_alpha_realistic_execution](../notebooks/05_production_alpha_realistic_execution.ipynb) | Holding period sweep, long-only, execution realism, capacity, production pipeline | All 9 assets profitable at 1m/2m long-only |

## Deployment Bundles

| Bundle | Notebook | Contents | Description |
|--------|----------|----------|-------------|
| [alpha_v2/](../deployments/alpha_v2/) | NB03 | 7 files | BTC-optimized XGBoost direction classifier |
| [alpha_v3_multi_asset/](../deployments/alpha_v3_multi_asset/) | NB04 | 7 files | 9-asset models, fee sensitivity, portfolio results |
| [alpha_v4_production/](../deployments/alpha_v4_production/) | NB05 | 5 files | Production config, capacity, full validation, results |

### alpha_v3_multi_asset/ Contents
- `config.json` - Strategy parameters, symbols, features
- `ml_results_by_asset.csv` - Per-asset AUC, returns, win rates
- `feature_importance_by_asset.csv` - XGBoost feature importance matrix
- `fee_sensitivity.csv` - Returns at 7 fee levels per asset
- `portfolio_daily.csv` - Daily portfolio returns
- `signal_correlations_by_asset.csv` - Feature-return correlation matrix
- `per_asset_features.json` - Top features per asset with correlations

## Result Files

| File | Description | Format |
|------|-------------|--------|
| [02_strategy_results.json](../results/02_strategy_results.json) | Complete strategy backtest results | JSON |
| [feature_correlations.csv](../results/feature_correlations.csv) | Feature-return correlations | CSV |
| [backtest_results.csv](../results/backtest_results.csv) | Trade-by-trade backtest | CSV |
| [threshold_analysis.csv](../results/threshold_analysis.csv) | Threshold optimization results | CSV |

## Research Framework (`research/lib/`)

| Module | Description | Key Classes |
|--------|-------------|-------------|
| [data.py](../lib/data.py) | Data loading with Hive partition filtering | `DataLoader` |
| [signals.py](../lib/signals.py) | Signal generation & registry | `BaseSignal`, `ImbalanceSignal`, `SignalRegistry` |
| [strategies.py](../lib/strategies.py) | Strategy definitions | `BaseStrategy`, `ImbalanceStrategy`, `DirectionStrategy` |
| [backtest.py](../lib/backtest.py) | Backtesting engine | `BacktestEngine`, `BacktestResult`, `TradeLog` |
| [evaluation.py](../lib/evaluation.py) | Performance analysis | `PerformanceAnalyzer` |
| [deploy.py](../lib/deploy.py) | Production deployment export | `ModelExporter` |

## Quick Links

- **Best Strategy**: XGBoost Direction (0.6/0.4, 30-bar hold) - all 8 altcoins 100% daily WR
- **Best Asset**: HBAR-USD (|r|=0.298, AUC=0.736, +2,040,819% at 0.1 bps)
- **Portfolio Result**: +99,201% over 9 OOS days (L+S, 30s); +2,310% (Long-Only, 1m); +650% (Long-Only, 2m)
- **Universal Feature**: `imbalance_L3` (avg |r|=0.274, 100% stable across daily retrains)
- **Fee Viability**: 7/9 assets profitable at ALL fee levels up to 0.5 bps (both L+S and Long-Only)
- **Execution Realism**: All 64 latency/slippage scenarios profitable (max 5-bar delay + 0.20 bps)
- **Capacity**: All focus assets profitable at $100K positions (negligible market impact)
- **Production Pipeline**: Expanding window retraining validates daily retrain stability
- **Key Insight**: BTC is WORST asset; 1m-2m horizons offer best production trade-off

---

*Last Updated: February 12, 2026*
