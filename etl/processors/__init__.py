"""ETL processors for data transformation."""
from .base_processor import BaseProcessor, ProcessorChain
from .raw_parser import RawParser

# Coinbase-specific processors (aliased for explicit naming)
from .coinbase import Level2Processor as CoinbaseLevel2Processor
from .coinbase import TradesProcessor as CoinbaseTradesProcessor
from .coinbase import TickerProcessor as CoinbaseTickerProcessor

__all__ = [
    "BaseProcessor",
    "ProcessorChain",
    "RawParser",
    # Coinbase processors
    "CoinbaseLevel2Processor",
    "CoinbaseTradesProcessor",
    "CoinbaseTickerProcessor",
]
