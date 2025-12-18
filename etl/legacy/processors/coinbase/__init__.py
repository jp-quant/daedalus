"""Coinbase-specific processors for ETL pipelines."""

from .level2_processor import Level2Processor
from .trades_processor import TradesProcessor
from .ticker_processor import TickerProcessor

__all__ = [
    "Level2Processor",
    "TradesProcessor",
    "TickerProcessor",
]
