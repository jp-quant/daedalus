"""
Unit Tests for ETL Features Module
==================================

Tests for orderbook feature extraction, stateful processing, and streaming utilities.
"""

import pytest
import math

import polars as pl

from etl.features.orderbook import (
    extract_structural_features,
    compute_rolling_features,
)
from etl.features.stateful import (
    StatefulFeatureProcessor,
    StatefulProcessorConfig,
)
from etl.features.streaming import (
    TimeBasedRollingSum,
    RollingWelford,
    RollingPercentile,
    KyleLambdaEstimator,
    VPINCalculator,
)


class TestTimeBasedRollingSum:
    """Tests for TimeBasedRollingSum accumulator."""
    
    def test_basic_sum(self):
        """Test basic sum accumulation."""
        rs = TimeBasedRollingSum(window_seconds=10)
        
        rs.update(1.0, 1.0)  # value=1, ts=1
        rs.update(2.0, 2.0)  # value=2, ts=2
        rs.update(3.0, 3.0)  # value=3, ts=3
        
        assert rs.sum == 6.0  # 1+2+3
    
    def test_window_expiry(self):
        """Test that old values expire from window."""
        rs = TimeBasedRollingSum(window_seconds=5)
        
        # Add values within window
        rs.update(10.0, 1.0)  # value=10, ts=1
        rs.update(20.0, 2.0)  # value=20, ts=2
        rs.update(30.0, 3.0)  # value=30, ts=3
        
        # Add value that triggers window cleanup
        # At ts=7, threshold=7-5=2, values with ts<=2 are evicted
        rs.update(70.0, 7.0)  # value=70, ts=7
        
        # ts=1 and ts=2 are expired (both <= threshold)
        # Remaining: 30+70 = 100
        assert rs.sum == 100.0
    
    def test_empty_sum(self):
        """Test sum of empty accumulator."""
        rs = TimeBasedRollingSum(window_seconds=10)
        assert rs.sum == 0.0


class TestRollingWelford:
    """Tests for RollingWelford (mean/variance) accumulator."""
    
    def test_mean_calculation(self):
        """Test mean calculation."""
        rw = RollingWelford(window_seconds=100)  # Large window to not expire
        
        # Add values 1-5
        for i in range(1, 6):
            rw.update(float(i), float(i))  # value, timestamp
        
        # Mean of 1,2,3,4,5 = 3
        assert rw.mean == pytest.approx(3.0, rel=1e-6)
    
    def test_variance_calculation(self):
        """Test variance calculation."""
        rw = RollingWelford(window_seconds=100)
        
        # Add values 1-5
        for i in range(1, 6):
            rw.update(float(i), float(i))
        
        # Sample variance of 1,2,3,4,5 = 2.5
        assert rw.variance == pytest.approx(2.5, rel=1e-6)
    
    def test_std_calculation(self):
        """Test standard deviation calculation."""
        rw = RollingWelford(window_seconds=100)
        
        for i in range(1, 6):
            rw.update(float(i), float(i))
        
        # std = sqrt(2.5) ≈ 1.58
        assert rw.std == pytest.approx(math.sqrt(2.5), rel=1e-6)
    
    def test_empty_stats(self):
        """Test stats of empty accumulator."""
        rw = RollingWelford(window_seconds=10)
        
        assert rw.mean == 0.0
        assert rw.variance == 0.0
        assert rw.std == 0.0


class TestStatefulFeatureProcessor:
    """Tests for StatefulFeatureProcessor."""
    
    def create_orderbook_record(
        self,
        timestamp: int = 1000,
        bids: list = None,
        asks: list = None,
    ) -> dict:
        """Create a test orderbook record."""
        if bids is None:
            bids = [
                {"price": 100.0, "size": 10.0},
                {"price": 99.0, "size": 20.0},
                {"price": 98.0, "size": 30.0},
            ]
        if asks is None:
            asks = [
                {"price": 101.0, "size": 10.0},
                {"price": 102.0, "size": 20.0},
                {"price": 103.0, "size": 30.0},
            ]
        return {
            "timestamp": timestamp,
            "bids": bids,
            "asks": asks,
        }
    
    def test_processor_initialization(self):
        """Test processor initialization."""
        config = StatefulProcessorConfig(
            horizons=[5, 15, 60],
            ofi_levels=5,
        )
        processor = StatefulFeatureProcessor(config)
        
        assert processor.config.horizons == [5, 15, 60]
        assert processor.config.ofi_levels == 5
        assert processor.prev_mid is None
    
    def test_first_orderbook_no_ofi(self):
        """Test that first orderbook returns zero OFI (no previous state)."""
        config = StatefulProcessorConfig(horizons=[5])
        processor = StatefulFeatureProcessor(config)
        
        record = self.create_orderbook_record(timestamp=1000)
        features = processor.process_orderbook(record)
        
        # First record should have OFI=0 (no previous state)
        assert features.get("ofi", 0) == 0.0
    
    def test_mid_price_tracking(self):
        """Test mid price is tracked correctly."""
        config = StatefulProcessorConfig(horizons=[5])
        processor = StatefulFeatureProcessor(config)
        
        record = self.create_orderbook_record()
        processor.process_orderbook(record)
        
        # Mid = (100 + 101) / 2 = 100.5
        assert processor.prev_mid == pytest.approx(100.5, rel=1e-6)
    
    def test_trade_accumulation(self):
        """Test trade volume accumulation for TFI."""
        config = StatefulProcessorConfig(horizons=[5])
        processor = StatefulFeatureProcessor(config)
        
        # Process some trades
        processor.process_trade({"side": "buy", "amount": 10.0, "price": 100.0})
        processor.process_trade({"side": "sell", "amount": 5.0, "price": 100.0})
        processor.process_trade({"side": "buy", "amount": 8.0, "price": 100.0})
        
        assert processor.pending_buy_volume == 18.0  # 10 + 8
        assert processor.pending_sell_volume == 5.0


class TestOrderbookFeatureExtraction:
    """Tests for orderbook vectorized feature extraction."""
    
    def create_test_orderbook_df(self) -> pl.LazyFrame:
        """Create a test orderbook DataFrame."""
        # Create nested bid/ask structure
        data = {
            "timestamp": [1000, 2000, 3000],
            "capture_ts": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:01",
                "2025-01-01 00:00:02",
            ],
            "exchange": ["binance", "binance", "binance"],
            "symbol": ["BTC/USDT", "BTC/USDT", "BTC/USDT"],
            "bids": [
                [
                    {"price": 100.0, "size": 10.0},
                    {"price": 99.0, "size": 20.0},
                    {"price": 98.0, "size": 30.0},
                ],
                [
                    {"price": 101.0, "size": 11.0},
                    {"price": 100.0, "size": 21.0},
                    {"price": 99.0, "size": 31.0},
                ],
                [
                    {"price": 102.0, "size": 12.0},
                    {"price": 101.0, "size": 22.0},
                    {"price": 100.0, "size": 32.0},
                ],
            ],
            "asks": [
                [
                    {"price": 101.0, "size": 10.0},
                    {"price": 102.0, "size": 20.0},
                    {"price": 103.0, "size": 30.0},
                ],
                [
                    {"price": 102.0, "size": 11.0},
                    {"price": 103.0, "size": 21.0},
                    {"price": 104.0, "size": 31.0},
                ],
                [
                    {"price": 103.0, "size": 12.0},
                    {"price": 104.0, "size": 22.0},
                    {"price": 105.0, "size": 32.0},
                ],
            ],
        }
        
        return pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
    
    def test_extract_basic_features(self):
        """Test extraction of basic orderbook features."""
        df = self.create_test_orderbook_df()
        
        result = extract_structural_features(df, max_levels=3)
        result_df = result.collect()
        
        # Check mid_price
        assert "mid_price" in result_df.columns
        # First row: (100 + 101) / 2 = 100.5
        assert result_df["mid_price"][0] == pytest.approx(100.5, rel=1e-6)
    
    def test_extract_spread(self):
        """Test spread calculation."""
        df = self.create_test_orderbook_df()
        
        result = extract_structural_features(df, max_levels=3)
        result_df = result.collect()
        
        # First row: 101 - 100 = 1
        assert result_df["spread"][0] == pytest.approx(1.0, rel=1e-6)
    
    def test_extract_imbalance(self):
        """Test imbalance calculation."""
        df = self.create_test_orderbook_df()
        
        result = extract_structural_features(df, max_levels=3)
        result_df = result.collect()
        
        # First row imbalance: (10 - 10) / (10 + 10) = 0
        assert result_df["imbalance_L1"][0] == pytest.approx(0.0, rel=1e-6)
    
    def test_extract_microprice(self):
        """Test microprice calculation."""
        df = self.create_test_orderbook_df()
        
        result = extract_structural_features(df, max_levels=3)
        result_df = result.collect()
        
        # Microprice = (bid*ask_size + ask*bid_size) / (bid_size + ask_size)
        # First row: (100*10 + 101*10) / (10+10) = 2010/20 = 100.5
        assert result_df["microprice"][0] == pytest.approx(100.5, rel=1e-6)
    
    def test_handles_missing_levels(self):
        """Test that missing levels are handled gracefully."""
        # Create data with fewer levels than requested
        data = {
            "timestamp": [1000],
            "capture_ts": ["2025-01-01 00:00:00"],
            "exchange": ["test"],
            "symbol": ["TEST"],
            "bids": [[{"price": 100.0, "size": 10.0}]],  # Only 1 level
            "asks": [[{"price": 101.0, "size": 10.0}]],  # Only 1 level
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        # Should not raise error even when requesting 10 levels
        result = extract_structural_features(df, max_levels=10)
        result_df = result.collect()
        
        # Should have filled missing levels with 0
        assert result_df["bid_price_L5"][0] == 0.0


class TestVPINCalculator:
    """Tests for VPIN calculator."""
    
    def test_vpin_initialization(self):
        """Test VPIN calculator initialization."""
        vpin = VPINCalculator(bucket_volume=100.0, window_buckets=50)
        
        assert vpin.bucket_volume == 100.0
        assert vpin.window_buckets == 50
        assert vpin.vpin == 0.0
    
    def test_vpin_bucket_completion(self):
        """Test VPIN bucket completion logic."""
        vpin = VPINCalculator(bucket_volume=10.0, window_buckets=5)
        
        # Add trades to fill one bucket
        vpin.process_trade(100.0, 5.0, "buy")
        vpin.process_trade(100.0, 5.0, "sell")  # Should complete bucket
        
        # Bucket should be complete, VPIN might still be 0 (not enough buckets)
        # The test is that it doesn't crash
        vpin.vpin


class TestKyleLambdaEstimator:
    """Tests for Kyle's Lambda estimator."""
    
    def test_estimator_initialization(self):
        """Test Kyle's Lambda estimator initialization."""
        estimator = KyleLambdaEstimator(window_seconds=300)
        
        assert estimator.window_seconds == 300
        assert estimator.lambda_estimate == 0.0
    
    def test_estimator_update(self):
        """Test updating Kyle's Lambda estimator."""
        estimator = KyleLambdaEstimator(window_seconds=300)
        
        # Add some price impact observations
        estimator.update(
            timestamp=1.0,
            price_return=0.01,
            signed_flow=100.0,
        )
        estimator.update(
            timestamp=2.0,
            price_return=0.02,
            signed_flow=200.0,
        )
        
        # Should have some lambda value (might be 0 if not enough data)
        lambda_val = estimator.lambda_estimate
        assert isinstance(lambda_val, float)
    
    def test_estimator_r_squared(self):
        """Test R-squared calculation."""
        estimator = KyleLambdaEstimator(window_seconds=300)
        
        # With no data, r_squared should be 0
        assert estimator.r_squared == 0.0
        
        # Add observations with clear correlation
        for i in range(10):
            estimator.update(
                timestamp=float(i),
                price_return=0.01 * (i + 1),
                signed_flow=100.0 * (i + 1),
            )
        
        # Should have some r_squared value
        r2 = estimator.r_squared
        assert isinstance(r2, float)
        assert 0.0 <= r2 <= 1.0
    
    def test_estimator_window_eviction(self):
        """Test that old observations are evicted from window."""
        estimator = KyleLambdaEstimator(window_seconds=10)
        
        # Add observations within window
        estimator.update(1.0, 0.01, 100.0)
        estimator.update(5.0, 0.02, 200.0)
        
        # Add observation that triggers eviction of first
        estimator.update(15.0, 0.03, 300.0)  # ts=1 should be evicted (15-10=5 threshold)
        
        # Should still work
        assert isinstance(estimator.lambda_estimate, float)


class TestWelford:
    """Tests for Welford (infinite history) mean/variance tracker."""
    
    def test_initial_values(self):
        """Test initial values are zero."""
        from etl.features.streaming import Welford
        
        w = Welford()
        assert w.count == 0
        assert w.mean == 0.0
        assert w.variance == 0.0
        assert w.std == 0.0
    
    def test_single_value(self):
        """Test single value produces correct mean, zero variance."""
        from etl.features.streaming import Welford
        
        w = Welford()
        w.update(5.0)
        
        assert w.count == 1
        assert w.mean == 5.0
        assert w.variance == 0.0  # Variance undefined with n=1
    
    def test_mean_calculation(self):
        """Test mean is correctly calculated."""
        from etl.features.streaming import Welford
        
        w = Welford()
        for x in [1, 2, 3, 4, 5]:
            w.update(float(x))
        
        assert w.mean == pytest.approx(3.0, rel=1e-6)
    
    def test_variance_calculation(self):
        """Test variance is correctly calculated."""
        from etl.features.streaming import Welford
        
        w = Welford()
        for x in [1, 2, 3, 4, 5]:
            w.update(float(x))
        
        # Sample variance of [1,2,3,4,5] = 2.5
        assert w.variance == pytest.approx(2.5, rel=1e-6)


class TestRegimeStats:
    """Tests for RegimeStats time-in-regime tracker."""
    
    def test_initialization(self):
        """Test RegimeStats initialization."""
        from etl.features.streaming import RegimeStats
        
        rs = RegimeStats(window_seconds=60)
        assert rs.window_seconds == 60
        assert rs.total_duration == 0.0
    
    def test_single_regime(self):
        """Test tracking single regime."""
        from etl.features.streaming import RegimeStats
        
        rs = RegimeStats(window_seconds=60)
        rs.update("tight", 0.0)
        rs.update("tight", 10.0)  # 10 seconds in tight
        
        assert rs.get_regime_fraction("tight") > 0.0
    
    def test_multiple_regimes(self):
        """Test tracking multiple regimes."""
        from etl.features.streaming import RegimeStats
        
        rs = RegimeStats(window_seconds=60)
        rs.update("tight", 0.0)
        rs.update("normal", 10.0)  # 10s in tight
        rs.update("wide", 20.0)   # 10s in normal
        rs.update("wide", 30.0)   # 10s in wide
        
        # Should have fractions adding up to ~1
        total = (
            rs.get_regime_fraction("tight") +
            rs.get_regime_fraction("normal") +
            rs.get_regime_fraction("wide")
        )
        assert total == pytest.approx(1.0, rel=0.1)
    
    def test_regime_window_expiry(self):
        """Test that old regime durations expire."""
        from etl.features.streaming import RegimeStats
        
        rs = RegimeStats(window_seconds=20)
        rs.update("tight", 0.0)
        rs.update("normal", 10.0)
        rs.update("wide", 30.0)  # Should evict some old data
        
        # Should not crash
        rs.get_regime_fraction("tight")


class TestRollingPercentile:
    """Tests for RollingPercentile tracker."""
    
    def test_initialization(self):
        """Test RollingPercentile initialization."""
        rp = RollingPercentile(window_seconds=60)
        assert rp.window_seconds == 60
        assert rp.get_percentile(0.5) == 0.0  # Empty
    
    def test_percentile_calculation(self):
        """Test percentile calculation."""
        rp = RollingPercentile(window_seconds=100)
        
        # Add values 1-10
        for i in range(1, 11):
            rp.update(float(i), float(i))
        
        # Median should be around 5-6
        median = rp.get_percentile(0.5)
        assert 4.0 <= median <= 6.0
    
    def test_get_current_percentile(self):
        """Test get_current_percentile returns rank of value."""
        rp = RollingPercentile(window_seconds=100)
        
        # Add values 1-10
        for i in range(1, 11):
            rp.update(float(i), float(i))
        
        # Value below min should have low percentile
        low_rank = rp.get_current_percentile(0.5)
        assert low_rank < 0.2
        
        # Value above max should have high percentile
        high_rank = rp.get_current_percentile(11.0)
        assert high_rank >= 0.9
    
    def test_window_expiry(self):
        """Test that old values expire from window."""
        rp = RollingPercentile(window_seconds=5)
        
        # Add values
        rp.update(10.0, 1.0)
        rp.update(20.0, 2.0)
        rp.update(30.0, 8.0)  # ts=1 and ts=2 should be evicted
        
        # Median should now be 30 (only value left)
        assert rp.get_percentile(0.5) == 30.0


class TestVolumeBarBuilder:
    """Tests for VolumeBarBuilder."""
    
    def test_initialization(self):
        """Test VolumeBarBuilder initialization."""
        from etl.features.streaming import VolumeBarBuilder
        
        vbb = VolumeBarBuilder(volume_threshold=100.0)
        assert vbb.volume_threshold == 100.0
        assert vbb.current_volume == 0.0
    
    def test_small_trade_no_bar(self):
        """Test that small trade doesn't emit a bar."""
        from etl.features.streaming import VolumeBarBuilder
        
        vbb = VolumeBarBuilder(volume_threshold=100.0)
        bars = vbb.process_trade(1.0, 100.0, 10.0, "buy")
        
        assert len(bars) == 0
        assert vbb.current_volume == 10.0
    
    def test_trade_completes_bar(self):
        """Test that trade completing threshold emits a bar."""
        from etl.features.streaming import VolumeBarBuilder
        
        vbb = VolumeBarBuilder(volume_threshold=100.0)
        
        # First trade
        bars = vbb.process_trade(1.0, 100.0, 50.0, "buy")
        assert len(bars) == 0
        
        # Second trade completes bar
        bars = vbb.process_trade(2.0, 101.0, 50.0, "sell")
        assert len(bars) == 1
        
        bar = bars[0]
        assert bar['open'] == 100.0
        assert bar['close'] == 101.0
        assert bar['volume'] == 100.0
        assert bar['buy_volume'] == 50.0
        assert bar['sell_volume'] == 50.0
    
    def test_large_trade_multiple_bars(self):
        """Test that large trade can emit multiple bars."""
        from etl.features.streaming import VolumeBarBuilder
        
        vbb = VolumeBarBuilder(volume_threshold=100.0)
        
        # Large trade that should emit 2 bars
        bars = vbb.process_trade(1.0, 100.0, 250.0, "buy")
        
        assert len(bars) == 2
        for bar in bars:
            assert bar['volume'] == 100.0
    
    def test_bar_vwap_calculation(self):
        """Test VWAP is correctly calculated in bar."""
        from etl.features.streaming import VolumeBarBuilder
        
        vbb = VolumeBarBuilder(volume_threshold=100.0)
        
        # Two trades at different prices
        vbb.process_trade(1.0, 100.0, 60.0, "buy")  # 60 * 100 = 6000
        bars = vbb.process_trade(2.0, 120.0, 40.0, "sell")  # 40 * 120 = 4800
        
        assert len(bars) == 1
        bar = bars[0]
        # VWAP = (6000 + 4800) / 100 = 108
        assert bar['vwap'] == pytest.approx(108.0, rel=1e-6)
    
    def test_bar_high_low(self):
        """Test high/low are tracked correctly."""
        from etl.features.streaming import VolumeBarBuilder
        
        vbb = VolumeBarBuilder(volume_threshold=100.0)
        
        vbb.process_trade(1.0, 100.0, 30.0, "buy")
        vbb.process_trade(2.0, 95.0, 30.0, "sell")   # Low
        vbb.process_trade(3.0, 110.0, 30.0, "buy")  # High
        bars = vbb.process_trade(4.0, 105.0, 10.0, "sell")  # Complete
        
        assert len(bars) == 1
        bar = bars[0]
        assert bar['high'] == 110.0
        assert bar['low'] == 95.0


class TestStatefulFeatureProcessorAdvanced:
    """Advanced tests for StatefulFeatureProcessor."""
    
    def test_processor_reset(self):
        """Test reset clears all state."""
        config = StatefulProcessorConfig(horizons=[5])
        processor = StatefulFeatureProcessor(config)
        
        # Build up some state
        record = {
            "timestamp": 1000,
            "bids": [{"price": 100.0, "size": 10.0}],
            "asks": [{"price": 101.0, "size": 10.0}],
        }
        processor.process_orderbook(record)
        processor.process_trade({"side": "buy", "amount": 10.0, "price": 100.0})
        
        assert processor.prev_mid is not None
        assert processor.pending_buy_volume > 0
        
        # Reset
        processor.reset()
        
        assert processor.prev_mid is None
        assert processor.pending_buy_volume == 0.0
        assert processor.pending_sell_volume == 0.0
    
    def test_processor_state_serialization(self):
        """Test get_state and set_state for checkpointing."""
        config = StatefulProcessorConfig(horizons=[5])
        processor = StatefulFeatureProcessor(config)
        
        # Build up some state
        record = {
            "timestamp": 1000,
            "bids": [{"price": 100.0, "size": 10.0}],
            "asks": [{"price": 101.0, "size": 10.0}],
        }
        processor.process_orderbook(record)
        
        # Get state
        state = processor.get_state()
        assert "prev_mid" in state
        assert state["prev_mid"] == pytest.approx(100.5, rel=1e-6)
        
        # Create new processor and restore state
        processor2 = StatefulFeatureProcessor(config)
        processor2.set_state(state)
        
        assert processor2.prev_mid == pytest.approx(100.5, rel=1e-6)
    
    def test_multiple_orderbook_ofi_computation(self):
        """Test OFI is computed correctly across multiple snapshots."""
        config = StatefulProcessorConfig(horizons=[5])
        processor = StatefulFeatureProcessor(config)
        
        # First snapshot
        record1 = {
            "timestamp": 1000,
            "bids": [{"price": 100.0, "size": 10.0}],
            "asks": [{"price": 101.0, "size": 10.0}],
        }
        features1 = processor.process_orderbook(record1)
        # First record uses "ofi_step" key, should be 0 (no previous state)
        assert features1.get("ofi_step", 0) == 0.0
        
        # Second snapshot with changed sizes
        record2 = {
            "timestamp": 2000,
            "bids": [{"price": 100.0, "size": 15.0}],  # +5
            "asks": [{"price": 101.0, "size": 8.0}],   # -2
        }
        features2 = processor.process_orderbook(record2)
        # OFI = Δbid_size - Δask_size = 5 - (-2) = 7
        assert features2["ofi_step"] == pytest.approx(7.0, rel=1e-6)
    
    def test_tfi_computation(self):
        """Test TFI (Trade Flow Imbalance) computation."""
        config = StatefulProcessorConfig(horizons=[5])
        processor = StatefulFeatureProcessor(config)
        
        # Process some trades
        processor.process_trade({"side": "buy", "amount": 10.0, "price": 100.0})
        processor.process_trade({"side": "sell", "amount": 3.0, "price": 100.0})
        
        # Process orderbook which should compute TFI
        record = {
            "timestamp": 1000,
            "bids": [{"price": 100.0, "size": 10.0}],
            "asks": [{"price": 101.0, "size": 10.0}],
        }
        features = processor.process_orderbook(record)
        
        # TFI = (buy_vol - sell_vol) / (buy_vol + sell_vol) = (10-3)/(10+3) ≈ 0.538
        # The actual key is "tfi_{h}s" where h is from horizons (5)
        assert "tfi_5s" in features
        assert features["tfi_5s"] == pytest.approx(7.0 / 13.0, rel=1e-6)
    
    def test_vpin_integration(self):
        """Test VPIN is computed when enabled."""
        config = StatefulProcessorConfig(
            horizons=[5],
            enable_vpin=True,
            vpin_bucket_volume=10.0,
            vpin_window_buckets=5,
        )
        processor = StatefulFeatureProcessor(config)
        
        # Process trades to build VPIN buckets
        for i in range(20):
            processor.process_trade({
                "side": "buy" if i % 2 == 0 else "sell",
                "amount": 5.0,
                "price": 100.0 + i * 0.1,
            })
        
        # Process orderbook to get VPIN in features
        record = {
            "timestamp": 1000,
            "bids": [{"price": 100.0, "size": 10.0}],
            "asks": [{"price": 101.0, "size": 10.0}],
        }
        features = processor.process_orderbook(record)
        
        # VPIN should be present
        assert "vpin" in features
        assert isinstance(features["vpin"], float)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
