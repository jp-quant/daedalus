"""
Pytest configuration and fixtures for all tests.
"""
import pytest
from pathlib import Path
import tempfile


@pytest.fixture
def temp_dir(tmp_path):
    """
    Provide a temporary directory for tests.
    
    This wraps pytest's built-in tmp_path fixture for compatibility
    with existing test code that expects a `temp_dir` fixture.
    """
    return tmp_path


@pytest.fixture
def sample_orderbook_data():
    """Sample orderbook data for testing."""
    return {
        "timestamp": [1000, 2000, 3000],
        "capture_ts": [
            "2025-01-01 00:00:00",
            "2025-01-01 00:00:01",
            "2025-01-01 00:00:02",
        ],
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT", "BTC/USDT"],
        "bids": [
            [{"price": 100.0, "size": 10.0}, {"price": 99.0, "size": 20.0}],
            [{"price": 101.0, "size": 11.0}, {"price": 100.0, "size": 21.0}],
            [{"price": 102.0, "size": 12.0}, {"price": 101.0, "size": 22.0}],
        ],
        "asks": [
            [{"price": 101.0, "size": 10.0}, {"price": 102.0, "size": 20.0}],
            [{"price": 102.0, "size": 11.0}, {"price": 103.0, "size": 21.0}],
            [{"price": 103.0, "size": 12.0}, {"price": 104.0, "size": 22.0}],
        ],
    }


@pytest.fixture
def sample_trades_data():
    """Sample trades data for testing."""
    return {
        "timestamp": [1000, 2000, 3000],
        "price": [100.0, 101.0, 102.0],
        "amount": [10.0, 11.0, 12.0],
        "side": ["buy", "sell", "buy"],
        "capture_ts": [
            "2025-01-01 00:00:00",
            "2025-01-01 00:00:01",
            "2025-01-01 00:00:02",
        ],
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT", "BTC/USDT"],
    }


@pytest.fixture
def sample_ticker_data():
    """Sample ticker data for testing."""
    return {
        "timestamp": [1000, 2000, 3000],
        "bid": [100.0, 101.0, 102.0],
        "ask": [101.0, 102.0, 103.0],
        "bid_volume": [10.0, 11.0, 12.0],
        "ask_volume": [9.0, 10.0, 11.0],
        "last": [100.5, 101.5, 102.5],
        "capture_ts": [
            "2025-01-01 00:00:00",
            "2025-01-01 00:00:01",
            "2025-01-01 00:00:02",
        ],
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT", "BTC/USDT"],
    }
