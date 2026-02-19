"""
Daedalus Research Framework
============================

Modular, extensible research platform for quantitative strategy development.

Architecture:
    research/lib/
    ├── __init__.py          # This file - public API
    ├── data.py              # Data loading & caching
    ├── signals.py           # Signal registry & base classes
    ├── strategies.py        # Strategy base class & implementations
    ├── backtest.py          # Backtesting engine
    ├── evaluation.py        # Performance metrics & analysis
    ├── reporting.py         # Portfolio & strategy performance reporting
    └── deploy.py            # Model export & production deployment

Usage:
    from research.lib import (
        DataLoader,
        SignalRegistry,
        BacktestEngine,
        ImbalanceSignal,
        ImbalanceStrategy,
    )

    # Load data
    loader = DataLoader(data_root="data/processed/silver/orderbook")
    df = loader.load_day(2026, 1, 20, exchange="coinbaseadvanced", symbol="BTC-USD")

    # Generate signals
    signal = ImbalanceSignal(column="total_imbalance", lookback=600)
    z_scores = signal.generate(df)

    # Backtest
    strategy = ImbalanceStrategy(entry_z=1.5, exit_z=0.5, max_hold=300)
    engine = BacktestEngine(fee_pct=0.0001)
    results = engine.run(df, strategy)
"""

__version__ = "0.1.0"

from research.lib.data import DataLoader
from research.lib.signals import (
    BaseSignal,
    ImbalanceSignal,
    ForwardReturnSignal,
    PriceZScoreSignal,
    SignalRegistry,
)
from research.lib.strategies import (
    BaseStrategy,
    ImbalanceStrategy,
    MeanReversionStrategy,
    RegressionStrategy,
    DirectionStrategy,
    UltraSelectiveStrategy,
)
from research.lib.backtest import BacktestEngine, BacktestResult, TradeLog
from research.lib.evaluation import PerformanceAnalyzer
from research.lib.reporting import PerformanceReport, StrategyComparison
from research.lib.deploy import ModelExporter

__all__ = [
    # Data
    "DataLoader",
    # Signals
    "BaseSignal",
    "ImbalanceSignal",
    "ForwardReturnSignal",
    "PriceZScoreSignal",
    "SignalRegistry",
    # Strategies
    "BaseStrategy",
    "ImbalanceStrategy",
    "MeanReversionStrategy",
    "RegressionStrategy",
    "DirectionStrategy",
    "UltraSelectiveStrategy",
    # Backtest
    "BacktestEngine",
    "BacktestResult",
    "TradeLog",
    # Evaluation
    "PerformanceAnalyzer",
    # Reporting
    "PerformanceReport",
    "StrategyComparison",
    # Deploy
    "ModelExporter",
]
