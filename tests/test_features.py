"""
Unit tests for feature engineering mathematical calculations.

Tests the following modules:
- etl/features/snapshot.py - Structural orderbook features
- etl/features/streaming.py - Rolling statistics (Welford, RollingSum, etc.)
- etl/features/state.py - BarBuilder and SymbolState

These tests verify the correctness of mathematical computations used in
high-frequency trading feature engineering.
"""
import pytest
import math
import numpy as np
from typing import Dict, Any, List


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def simple_orderbook() -> Dict[str, Any]:
    """Simple orderbook with 5 bid/ask levels."""
    return {
        "symbol": "BTC/USDT",
        "exchange": "binance",
        "timestamp": 1700000000000,
        "date": "2025-11-14",
        "data": {
            "bids": [
                [50000.0, 1.0],   # L0
                [49990.0, 2.0],   # L1
                [49980.0, 3.0],   # L2
                [49970.0, 4.0],   # L3
                [49960.0, 5.0],   # L4
            ],
            "asks": [
                [50010.0, 1.5],   # L0
                [50020.0, 2.5],   # L1
                [50030.0, 3.5],   # L2
                [50040.0, 4.5],   # L3
                [50050.0, 5.5],   # L4
            ],
            "timestamp": 1700000000000,
        }
    }


@pytest.fixture
def symmetric_orderbook() -> Dict[str, Any]:
    """Symmetric orderbook (equal bid/ask volumes) for imbalance testing."""
    return {
        "symbol": "ETH/USDT",
        "exchange": "binance",
        "timestamp": 1700000000000,
        "date": "2025-11-14",
        "data": {
            "bids": [
                [3000.0, 10.0],
                [2990.0, 10.0],
            ],
            "asks": [
                [3010.0, 10.0],
                [3020.0, 10.0],
            ],
            "timestamp": 1700000000000,
        }
    }


@pytest.fixture
def imbalanced_orderbook() -> Dict[str, Any]:
    """Imbalanced orderbook (bid-heavy) for imbalance testing."""
    return {
        "symbol": "SOL/USDT",
        "exchange": "binance",
        "timestamp": 1700000000000,
        "date": "2025-11-14",
        "data": {
            "bids": [
                [100.0, 100.0],  # Much larger bid volume
                [99.0, 50.0],
            ],
            "asks": [
                [101.0, 10.0],   # Small ask volume
                [102.0, 10.0],
            ],
            "timestamp": 1700000000000,
        }
    }


@pytest.fixture
def empty_orderbook() -> Dict[str, Any]:
    """Empty orderbook for edge case testing."""
    return {
        "symbol": "TEST/USDT",
        "exchange": "test",
        "timestamp": 1700000000000,
        "date": "2025-11-14",
        "data": {
            "bids": [],
            "asks": [],
            "timestamp": 1700000000000,
        }
    }


# =============================================================================
# Tests for snapshot.py - extract_orderbook_features()
# =============================================================================

class TestExtractOrderbookFeatures:
    """Tests for structural feature extraction from orderbook snapshots."""

    def test_mid_price_calculation(self, simple_orderbook):
        """Test mid price = (best_bid + best_ask) / 2."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook)
        
        expected_mid = (50000.0 + 50010.0) / 2.0  # 50005.0
        assert features["mid_price"] == pytest.approx(expected_mid)
        
    def test_spread_calculation(self, simple_orderbook):
        """Test spread = best_ask - best_bid."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook)
        
        expected_spread = 50010.0 - 50000.0  # 10.0
        assert features["spread"] == pytest.approx(expected_spread)
        
    def test_relative_spread_calculation(self, simple_orderbook):
        """Test relative_spread = spread / mid_price."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook)
        
        expected_spread = 10.0
        expected_mid = 50005.0
        expected_rel_spread = expected_spread / expected_mid
        assert features["relative_spread"] == pytest.approx(expected_rel_spread)
        
    def test_microprice_calculation(self, simple_orderbook):
        """Test microprice = (bid*ask_vol + ask*bid_vol) / (bid_vol + ask_vol)."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook)
        
        best_bid = 50000.0
        best_ask = 50010.0
        bid_vol = 1.0
        ask_vol = 1.5
        expected_microprice = (best_bid * ask_vol + best_ask * bid_vol) / (bid_vol + ask_vol)
        assert features["microprice"] == pytest.approx(expected_microprice)

    def test_l1_imbalance_symmetric(self, symmetric_orderbook):
        """Test L1 imbalance is 0 for symmetric orderbook."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(symmetric_orderbook)
        
        # (bid_vol - ask_vol) / (bid_vol + ask_vol) = 0
        assert features["imbalance_L1"] == pytest.approx(0.0)
        
    def test_l1_imbalance_bid_heavy(self, imbalanced_orderbook):
        """Test L1 imbalance is positive for bid-heavy orderbook."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(imbalanced_orderbook)
        
        bid_vol = 100.0
        ask_vol = 10.0
        expected_imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol)  # 0.818...
        assert features["imbalance_L1"] == pytest.approx(expected_imbalance)
        assert features["imbalance_L1"] > 0  # Bid-heavy = positive

    def test_level_extraction(self, simple_orderbook):
        """Test individual level price/size extraction."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook, max_levels=5)
        
        # Check bid levels
        assert features["bid_price_L0"] == pytest.approx(50000.0)
        assert features["bid_size_L0"] == pytest.approx(1.0)
        assert features["bid_price_L4"] == pytest.approx(49960.0)
        assert features["bid_size_L4"] == pytest.approx(5.0)
        
        # Check ask levels
        assert features["ask_price_L0"] == pytest.approx(50010.0)
        assert features["ask_size_L0"] == pytest.approx(1.5)
        assert features["ask_price_L4"] == pytest.approx(50050.0)
        assert features["ask_size_L4"] == pytest.approx(5.5)

    def test_vwap_bid_calculation(self, simple_orderbook):
        """Test VWAP bid = sum(price * vol) / sum(vol) for top 5 levels."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook)
        
        # Top 5 bids
        prices = [50000.0, 49990.0, 49980.0, 49970.0, 49960.0]
        vols = [1.0, 2.0, 3.0, 4.0, 5.0]
        expected_vwap = sum(p * v for p, v in zip(prices, vols)) / sum(vols)
        assert features["vwap_bid_5"] == pytest.approx(expected_vwap)

    def test_vwap_ask_calculation(self, simple_orderbook):
        """Test VWAP ask = sum(price * vol) / sum(vol) for top 5 levels."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook)
        
        # Top 5 asks
        prices = [50010.0, 50020.0, 50030.0, 50040.0, 50050.0]
        vols = [1.5, 2.5, 3.5, 4.5, 5.5]
        expected_vwap = sum(p * v for p, v in zip(prices, vols)) / sum(vols)
        assert features["vwap_ask_5"] == pytest.approx(expected_vwap)

    def test_concentration_herfindahl(self, simple_orderbook):
        """Test concentration = sum(share^2) where share = vol / total_vol."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook, max_levels=5)
        
        # Bid concentration
        bid_vols = [1.0, 2.0, 3.0, 4.0, 5.0]
        total = sum(bid_vols)
        expected_concentration = sum((v / total) ** 2 for v in bid_vols)
        assert features["bid_concentration"] == pytest.approx(expected_concentration)

    def test_slope_calculation(self, simple_orderbook):
        """Test slope = |price_L4 - price_L0| / cumulative_volume."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook)
        
        # Bid slope
        price_diff = abs(49960.0 - 50000.0)  # 40.0
        cum_vol = 1.0 + 2.0 + 3.0 + 4.0 + 5.0  # 15.0
        expected_slope = price_diff / cum_vol
        assert features["bid_slope"] == pytest.approx(expected_slope)

    def test_empty_orderbook_handling(self, empty_orderbook):
        """Test graceful handling of empty orderbook."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(empty_orderbook)
        
        # Should return NaN for calculated features
        assert math.isnan(features["mid_price"])
        assert math.isnan(features["spread"])
        assert math.isnan(features["microprice"])

    def test_depth_bands(self, simple_orderbook):
        """Test depth aggregation in basis point bands."""
        from etl.features.snapshot import extract_orderbook_features
        
        # Use custom bands
        features = extract_orderbook_features(simple_orderbook, bands_bps=[5, 10, 25, 50])
        
        # Mid price = 50005.0
        # Check that band keys exist
        assert "bid_vol_band_0_5bps" in features
        assert "ask_vol_band_0_5bps" in features
        assert "imb_band_0_5bps" in features


# =============================================================================
# Tests for streaming.py - Rolling statistics
# =============================================================================

class TestWelford:
    """Tests for Welford's online mean/variance algorithm."""

    def test_single_value(self):
        """Test with single value."""
        from etl.features.streaming import Welford
        
        w = Welford()
        w.update(10.0)
        
        assert w.mean == pytest.approx(10.0)
        assert w.variance == pytest.approx(0.0)  # Single value = no variance

    def test_two_values(self):
        """Test with two values."""
        from etl.features.streaming import Welford
        
        w = Welford()
        w.update(10.0)
        w.update(20.0)
        
        expected_mean = 15.0
        expected_var = 50.0  # ((10-15)^2 + (20-15)^2) / (2-1) = 50
        
        assert w.mean == pytest.approx(expected_mean)
        assert w.variance == pytest.approx(expected_var)

    def test_known_sequence(self):
        """Test with known sequence and compare to numpy."""
        from etl.features.streaming import Welford
        
        values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        
        w = Welford()
        for v in values:
            w.update(v)
        
        expected_mean = np.mean(values)
        expected_var = np.var(values, ddof=1)  # Sample variance
        
        assert w.mean == pytest.approx(expected_mean, rel=1e-10)
        assert w.variance == pytest.approx(expected_var, rel=1e-10)

    def test_standard_deviation(self):
        """Test std = sqrt(variance)."""
        from etl.features.streaming import Welford
        
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        
        w = Welford()
        for v in values:
            w.update(v)
        
        expected_std = np.std(values, ddof=1)
        assert w.std == pytest.approx(expected_std, rel=1e-10)


class TestRollingWelford:
    """Tests for time-based rolling Welford variance."""

    def test_values_within_window(self):
        """Test that values within window are included."""
        from etl.features.streaming import RollingWelford
        
        rw = RollingWelford(window_seconds=10.0)
        
        # All within 10 second window
        rw.update(10.0, timestamp=0.0)
        rw.update(20.0, timestamp=5.0)
        rw.update(30.0, timestamp=9.0)
        
        expected_mean = 20.0
        expected_var = np.var([10.0, 20.0, 30.0], ddof=1)
        
        assert rw.mean == pytest.approx(expected_mean)
        assert rw.variance == pytest.approx(expected_var, rel=1e-10)

    def test_eviction_old_values(self):
        """Test that old values are evicted from rolling window."""
        from etl.features.streaming import RollingWelford
        
        rw = RollingWelford(window_seconds=10.0)
        
        rw.update(100.0, timestamp=0.0)   # Will be evicted
        rw.update(20.0, timestamp=5.0)    # Will be evicted
        rw.update(30.0, timestamp=15.0)   # Current
        rw.update(40.0, timestamp=20.0)   # Current
        
        # At t=20, window is [10, 20], so only values at t=15 and t=20 remain
        expected_mean = 35.0
        expected_var = np.var([30.0, 40.0], ddof=1)
        
        assert rw.mean == pytest.approx(expected_mean)
        assert rw.variance == pytest.approx(expected_var, rel=1e-10)


class TestTimeBasedRollingSum:
    """Tests for time-based rolling sum."""

    def test_sum_within_window(self):
        """Test sum of values within window."""
        from etl.features.streaming import TimeBasedRollingSum
        
        rs = TimeBasedRollingSum(window_seconds=10.0)
        
        rs.update(10.0, timestamp=0.0)
        rs.update(20.0, timestamp=5.0)
        rs.update(30.0, timestamp=9.0)
        
        assert rs.sum == pytest.approx(60.0)
        assert rs.mean == pytest.approx(20.0)

    def test_eviction_sum(self):
        """Test sum after eviction."""
        from etl.features.streaming import TimeBasedRollingSum
        
        rs = TimeBasedRollingSum(window_seconds=5.0)
        
        rs.update(100.0, timestamp=0.0)   # Will be evicted at t=6
        rs.update(10.0, timestamp=3.0)    # Will remain
        rs.update(20.0, timestamp=6.0)    # Current, evicts t=0
        
        # Only values at t=3 and t=6 remain
        assert rs.sum == pytest.approx(30.0)


class TestRegimeStats:
    """Tests for regime tracking statistics."""

    def test_single_regime(self):
        """Test tracking a single regime."""
        from etl.features.streaming import RegimeStats
        
        rs = RegimeStats(window_seconds=10.0)
        
        rs.update("tight", timestamp=0.0)
        rs.update("tight", timestamp=5.0)
        rs.update("tight", timestamp=10.0)
        
        # All time in "tight" regime
        assert rs.get_regime_fraction("tight") == pytest.approx(1.0)
        assert rs.get_regime_fraction("wide") == pytest.approx(0.0)

    def test_regime_transition(self):
        """Test tracking regime transitions."""
        from etl.features.streaming import RegimeStats
        
        rs = RegimeStats(window_seconds=100.0)  # Large window
        
        rs.update("tight", timestamp=0.0)
        rs.update("tight", timestamp=5.0)  # 5s in tight
        rs.update("wide", timestamp=5.0)
        rs.update("wide", timestamp=10.0)  # 5s in wide
        
        # 50% tight, 50% wide
        assert rs.get_regime_fraction("tight") == pytest.approx(0.5)
        assert rs.get_regime_fraction("wide") == pytest.approx(0.5)


# =============================================================================
# Tests for state.py - BarBuilder
# =============================================================================

class TestBarBuilder:
    """Tests for OHLC bar building."""

    def test_ohlc_single_bar(self):
        """Test OHLC calculation for a single bar."""
        from etl.features.state import BarBuilder
        
        bb = BarBuilder(duration=10)  # 10-second bars
        
        # Simulate updates within a single bar
        result = bb.update(timestamp=0.0, mid=100.0, spread=0.1, rel_spread=0.001, 
                          ofi=0.0, l1_imb=0.0, log_ret=0.0)
        assert result is None  # Bar not complete
        
        result = bb.update(timestamp=3.0, mid=105.0, spread=0.2, rel_spread=0.002,
                          ofi=0.5, l1_imb=0.1, log_ret=0.05)
        assert result is None
        
        result = bb.update(timestamp=7.0, mid=102.0, spread=0.15, rel_spread=0.0015,
                          ofi=-0.3, l1_imb=-0.05, log_ret=-0.03)
        assert result is None
        
        # Complete the bar by moving to next period
        result = bb.update(timestamp=10.5, mid=98.0, spread=0.1, rel_spread=0.001,
                          ofi=0.0, l1_imb=0.0, log_ret=-0.04)
        
        assert result is not None
        assert result["open"] == pytest.approx(100.0)
        assert result["high"] == pytest.approx(105.0)
        assert result["low"] == pytest.approx(100.0)  # min of [100, 105, 102] = 100
        assert result["close"] == pytest.approx(102.0)  # last price before bar close
        assert result["count"] == 3

    def test_mean_spread_in_bar(self):
        """Test mean spread calculation within bar."""
        from etl.features.state import BarBuilder
        
        bb = BarBuilder(duration=10)
        
        bb.update(timestamp=0.0, mid=100.0, spread=10.0, rel_spread=0.001,
                 ofi=0.0, l1_imb=0.0, log_ret=0.0)
        bb.update(timestamp=3.0, mid=100.0, spread=20.0, rel_spread=0.002,
                 ofi=0.0, l1_imb=0.0, log_ret=0.0)
        bb.update(timestamp=6.0, mid=100.0, spread=30.0, rel_spread=0.003,
                 ofi=0.0, l1_imb=0.0, log_ret=0.0)
        
        result = bb.update(timestamp=10.0, mid=100.0, spread=0.0, rel_spread=0.0,
                          ofi=0.0, l1_imb=0.0, log_ret=0.0)
        
        expected_mean_spread = (10.0 + 20.0 + 30.0) / 3
        assert result["mean_spread"] == pytest.approx(expected_mean_spread)

    def test_sum_ofi_in_bar(self):
        """Test OFI sum within bar."""
        from etl.features.state import BarBuilder
        
        bb = BarBuilder(duration=10)
        
        bb.update(timestamp=0.0, mid=100.0, spread=0.1, rel_spread=0.001,
                 ofi=1.0, l1_imb=0.0, log_ret=0.0)
        bb.update(timestamp=5.0, mid=100.0, spread=0.1, rel_spread=0.001,
                 ofi=-0.5, l1_imb=0.0, log_ret=0.0)
        
        result = bb.update(timestamp=10.0, mid=100.0, spread=0.1, rel_spread=0.001,
                          ofi=0.0, l1_imb=0.0, log_ret=0.0)
        
        expected_sum_ofi = 1.0 + (-0.5)
        assert result["sum_ofi"] == pytest.approx(expected_sum_ofi)

    def test_realized_variance_in_bar(self):
        """Test realized variance calculation within bar."""
        from etl.features.state import BarBuilder
        
        bb = BarBuilder(duration=10)
        
        log_returns = [0.01, -0.02, 0.03, -0.01, 0.02]
        
        for i, lr in enumerate(log_returns):
            bb.update(timestamp=float(i), mid=100.0, spread=0.1, rel_spread=0.001,
                     ofi=0.0, l1_imb=0.0, log_ret=lr)
        
        result = bb.update(timestamp=10.0, mid=100.0, spread=0.1, rel_spread=0.001,
                          ofi=0.0, l1_imb=0.0, log_ret=0.0)
        
        expected_var = np.var(log_returns, ddof=1)
        assert result["realized_variance"] == pytest.approx(expected_var, rel=1e-10)


# =============================================================================
# Mathematical Property Tests
# =============================================================================

class TestMathematicalProperties:
    """Test mathematical properties and invariants."""

    def test_imbalance_bounds(self, simple_orderbook):
        """Test that imbalance is always in [-1, 1]."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook)
        
        assert -1.0 <= features["imbalance_L1"] <= 1.0

    def test_concentration_bounds(self, simple_orderbook):
        """Test that concentration (Herfindahl) is in [1/n, 1]."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook, max_levels=5)
        
        n = 5  # max_levels
        assert 1.0 / n <= features["bid_concentration"] <= 1.0
        assert 1.0 / n <= features["ask_concentration"] <= 1.0

    def test_spread_always_positive(self, simple_orderbook):
        """Test that spread is always positive (ask > bid)."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook)
        
        assert features["spread"] > 0

    def test_microprice_between_bid_ask(self, simple_orderbook):
        """Test that microprice is between bid and ask."""
        from etl.features.snapshot import extract_orderbook_features
        
        features = extract_orderbook_features(simple_orderbook)
        
        assert features["best_bid"] <= features["microprice"] <= features["best_ask"]

    def test_variance_non_negative(self):
        """Test that variance is always non-negative."""
        from etl.features.streaming import Welford, RollingWelford
        
        # Test Welford
        w = Welford()
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            w.update(v)
        assert w.variance >= 0
        
        # Test RollingWelford
        rw = RollingWelford(window_seconds=10.0)
        for i, v in enumerate([1.0, 2.0, 3.0, 4.0, 5.0]):
            rw.update(v, timestamp=float(i))
        assert rw.variance >= 0


# =============================================================================
# Edge Case Tests
# =============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_level_orderbook(self):
        """Test orderbook with only one level."""
        from etl.features.snapshot import extract_orderbook_features
        
        snapshot = {
            "symbol": "TEST",
            "exchange": "test",
            "timestamp": 1000,
            "data": {
                "bids": [[100.0, 1.0]],
                "asks": [[101.0, 1.0]],
            }
        }
        
        features = extract_orderbook_features(snapshot, max_levels=10)
        
        # Should still compute basic features
        assert features["mid_price"] == pytest.approx(100.5)
        assert features["spread"] == pytest.approx(1.0)
        
        # Higher levels should be NaN
        assert math.isnan(features["bid_price_L1"])
        assert math.isnan(features["ask_price_L1"])

    def test_very_small_numbers(self):
        """Test with very small price/volume values."""
        from etl.features.snapshot import extract_orderbook_features
        
        snapshot = {
            "symbol": "SHIB",
            "exchange": "test",
            "timestamp": 1000,
            "data": {
                "bids": [[0.00001, 1000000.0]],
                "asks": [[0.00002, 1000000.0]],
            }
        }
        
        features = extract_orderbook_features(snapshot)
        
        assert features["mid_price"] == pytest.approx(0.000015)
        assert features["spread"] == pytest.approx(0.00001)

    def test_very_large_numbers(self):
        """Test with very large price/volume values."""
        from etl.features.snapshot import extract_orderbook_features
        
        snapshot = {
            "symbol": "BTC",
            "exchange": "test",
            "timestamp": 1000,
            "data": {
                "bids": [[1000000.0, 0.001]],
                "asks": [[1000001.0, 0.001]],
            }
        }
        
        features = extract_orderbook_features(snapshot)
        
        assert features["mid_price"] == pytest.approx(1000000.5)

    def test_rolling_window_empty(self):
        """Test rolling statistics with empty window."""
        from etl.features.streaming import RollingWelford, TimeBasedRollingSum
        
        rw = RollingWelford(window_seconds=1.0)
        # Add value at t=0, then query at t=100 (well past window)
        rw.update(10.0, timestamp=0.0)
        rw.update(20.0, timestamp=100.0)  # Evicts first value
        
        # Only second value remains
        assert rw.mean == pytest.approx(20.0)
        assert rw.count == 1

    def test_bar_gap_handling(self):
        """Test bar builder handles time gaps correctly."""
        from etl.features.state import BarBuilder
        
        bb = BarBuilder(duration=10)
        
        # First bar
        bb.update(timestamp=0.0, mid=100.0, spread=0.1, rel_spread=0.001,
                 ofi=0.0, l1_imb=0.0, log_ret=0.0)
        
        # Jump to much later (gap)
        result = bb.update(timestamp=50.0, mid=105.0, spread=0.2, rel_spread=0.002,
                          ofi=0.0, l1_imb=0.0, log_ret=0.0)
        
        # Should have completed the first bar
        assert result is not None
        assert result["close"] == pytest.approx(100.0)


# =============================================================================
# Numerical Stability Tests
# =============================================================================

class TestNumericalStability:
    """Test numerical stability of algorithms."""

    def test_welford_catastrophic_cancellation(self):
        """Test Welford avoids catastrophic cancellation."""
        from etl.features.streaming import Welford
        
        # Large mean, small variance - naive algorithm would fail
        values = [1000000.0 + i * 0.0001 for i in range(1000)]
        
        w = Welford()
        for v in values:
            w.update(v)
        
        expected_var = np.var(values, ddof=1)
        
        # Should match numpy within reasonable tolerance
        # Using rel=1e-5 due to floating point precision differences
        assert w.variance == pytest.approx(expected_var, rel=1e-5)

    def test_many_updates(self):
        """Test stability with many updates."""
        from etl.features.streaming import Welford
        
        w = Welford()
        np.random.seed(42)
        values = np.random.randn(10000)
        
        for v in values:
            w.update(v)
        
        expected_mean = np.mean(values)
        expected_var = np.var(values, ddof=1)
        
        assert w.mean == pytest.approx(expected_mean, rel=1e-10)
        assert w.variance == pytest.approx(expected_var, rel=1e-6)

    def test_rolling_many_evictions(self):
        """Test rolling stats with many evictions."""
        from etl.features.streaming import RollingWelford
        
        rw = RollingWelford(window_seconds=10.0)
        
        # Add many values, causing many evictions
        np.random.seed(42)
        for i in range(1000):
            rw.update(np.random.randn(), timestamp=float(i))
        
        # Should maintain reasonable statistics
        assert not math.isnan(rw.mean)
        assert not math.isnan(rw.variance)
        assert rw.variance >= 0
