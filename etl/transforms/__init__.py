"""
Transforms Module
=================

Transform implementations for the ETL framework.

Transforms follow the pattern:
    INPUTS dict → transform() → OUTPUTS dict

Each transform:
1. Declares its inputs and outputs via TransformConfig
2. Implements the transform() method
3. Optionally maintains state for stateful processing

Available transforms:
- OrderbookFeatureTransform: Bronze orderbook → Silver features
- TickerFeatureTransform: Bronze ticker → Silver features
- TradesFeatureTransform: Bronze trades → Silver features
- BarAggregationTransform: Silver HF → Gold bars
"""

from etl.transforms.bars import BarAggregationTransform, aggregate_bars_vectorized
from etl.transforms.orderbook import OrderbookFeatureTransform
from etl.transforms.ticker import TickerFeatureTransform, compute_ticker_features
from etl.transforms.trades import TradesFeatureTransform, compute_trades_features

__all__ = [
    # Transforms
    "OrderbookFeatureTransform",
    "TickerFeatureTransform",
    "TradesFeatureTransform",
    "BarAggregationTransform",
    # Utility functions
    "compute_ticker_features",
    "compute_trades_features",
    "aggregate_bars_vectorized",
]
