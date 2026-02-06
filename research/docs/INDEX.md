# Research Documentation Index

## Core Documents

| Document | Description | Audience |
|----------|-------------|----------|
| [README.md](../README.md) | Executive summary and portfolio overview | All |
| [RESEARCH_PAPER.md](../RESEARCH_PAPER.md) | Full technical paper with methodology and results | Quants, Researchers |
| [MATHEMATICAL_APPENDIX.md](MATHEMATICAL_APPENDIX.md) | Formal definitions and derivations | Quants, Academics |
| [QUANT_RESEARCH_CONTEXT.md](QUANT_RESEARCH_CONTEXT.md) | Research context and objectives | Team members |

## Notebooks

| Notebook | Description | Key Outputs |
|----------|-------------|-------------|
| [01_orderbook_feature_analysis](../notebooks/01_orderbook_feature_analysis.ipynb) | Feature engineering and EDA | 205 features defined |
| [02_microstructure_alpha_discovery](../notebooks/02_microstructure_alpha_discovery.ipynb) | Iterative strategy research & alpha discovery | Imbalance signal (ρ=0.082) |

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
| [strategies.py](../lib/strategies.py) | Strategy definitions | `BaseStrategy`, `ImbalanceStrategy`, `MeanReversionStrategy` |
| [backtest.py](../lib/backtest.py) | Backtesting engine | `BacktestEngine`, `BacktestResult`, `TradeLog` |
| [evaluation.py](../lib/evaluation.py) | Performance analysis | `PerformanceAnalyzer` |
| [deploy.py](../lib/deploy.py) | Production deployment export | `ModelExporter` |

## Quick Links

- **Best Result**: +84.55% return (5 days, 0 fee) → [Strategy Results](../results/02_strategy_results.json)
- **Key Finding**: 8.2% correlation → [Research Paper §4.1](../RESEARCH_PAPER.md#41-correlation-analysis)
- **Breakeven Fee**: 0.27 bps → [Mathematical Appendix §4.3](MATHEMATICAL_APPENDIX.md#43-breakeven-fee)
