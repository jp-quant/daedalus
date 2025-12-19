"""
Unit Tests for ETL Transforms Module
====================================

Tests for the transform classes: orderbook, ticker, trades, bars.
"""

import pytest
import uuid

import polars as pl

from etl.core.config import TransformConfig, FeatureConfig
from etl.core.base import TransformContext
from etl.transforms import (
    OrderbookFeatureTransform,
    TickerFeatureTransform,
    TradesFeatureTransform,
    BarAggregationTransform,
    compute_ticker_features,
    compute_trades_features,
    aggregate_bars_vectorized,
)


class TestOrderbookFeatureTransform:
    """Tests for OrderbookFeatureTransform."""
    
    def test_transform_instantiation(self):
        """Test transform can be instantiated."""
        config = TransformConfig(name="test_orderbook")
        transform = OrderbookFeatureTransform(config)
        
        # Name comes from config, not class name
        assert transform.name == "test_orderbook"
    
    def test_transform_with_data(self):
        """Test transform with actual orderbook data."""
        from etl.core.config import InputConfig, OutputConfig
        
        config = TransformConfig(
            name="test_orderbook",
            inputs={"orderbook": InputConfig(name="orderbook", path="test/orderbook")},
            outputs={"silver_features": OutputConfig(name="silver_features", path="test/silver")},
        )
        transform = OrderbookFeatureTransform(config)
        
        # Create test data
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
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        feature_config = FeatureConfig(depth_levels=5)
        context = TransformContext(
            execution_id=str(uuid.uuid4()),
            partition_values={"exchange": "binance", "symbol": "BTC/USDT"},
            feature_config=feature_config,
        )
        
        inputs = {"orderbook": df}
        outputs = transform.transform(inputs, context)
        
        assert "silver_features" in outputs
        result_df = outputs["silver_features"].collect()
        
        assert "mid_price" in result_df.columns
        assert "spread" in result_df.columns
        assert len(result_df) == 3


class TestTickerFeatureTransform:
    """Tests for TickerFeatureTransform."""
    
    def test_transform_instantiation(self):
        """Test transform can be instantiated."""
        config = TransformConfig(name="test_ticker")
        transform = TickerFeatureTransform(config)
        
        assert transform.name == "test_ticker"
    
    def test_compute_ticker_features(self):
        """Test compute_ticker_features function."""
        data = {
            "timestamp": [1000, 2000, 3000],
            "bid": [100.0, 101.0, 102.0],
            "ask": [101.0, 102.0, 103.0],
            "bid_volume": [10.0, 11.0, 12.0],  # Correct column name
            "ask_volume": [9.0, 10.0, 11.0],   # Correct column name
            "last": [100.5, 101.5, 102.5],
            "capture_ts": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:01",
                "2025-01-01 00:00:02",
            ],
            "exchange": ["binance"] * 3,
            "symbol": ["BTC/USDT"] * 3,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        result = compute_ticker_features(df)
        result_df = result.collect()
        
        assert "spread" in result_df.columns
        assert "mid_price" in result_df.columns
        assert "relative_spread" in result_df.columns
        assert "volume_imbalance" in result_df.columns
        
        # Check calculations
        assert result_df["spread"][0] == pytest.approx(1.0, rel=1e-6)
        assert result_df["mid_price"][0] == pytest.approx(100.5, rel=1e-6)


class TestTradesFeatureTransform:
    """Tests for TradesFeatureTransform."""
    
    def test_transform_instantiation(self):
        """Test transform can be instantiated."""
        config = TransformConfig(name="test_trades")
        transform = TradesFeatureTransform(config)
        
        assert transform.name == "test_trades"
    
    def test_compute_trades_features(self):
        """Test compute_trades_features function."""
        data = {
            "timestamp": [1000, 2000, 3000],
            "price": [100.0, 101.0, 102.0],
            "amount": [10.0, 11.0, 12.0],
            "side": ["buy", "sell", "buy"],
            "capture_ts": [  # Required for time features
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:01",
                "2025-01-01 00:00:02",
            ],
            "exchange": ["binance"] * 3,
            "symbol": ["BTC/USDT"] * 3,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        result = compute_trades_features(df)
        result_df = result.collect()
        
        assert "is_buy" in result_df.columns
        assert "dollar_volume" in result_df.columns
        assert "signed_volume" in result_df.columns
        
        # Check calculations
        assert result_df["is_buy"][0] == 1
        assert result_df["is_buy"][1] == 0
        assert result_df["dollar_volume"][0] == pytest.approx(1000.0, rel=1e-6)
        assert result_df["signed_volume"][0] == pytest.approx(10.0, rel=1e-6)
        assert result_df["signed_volume"][1] == pytest.approx(-11.0, rel=1e-6)


class TestBarAggregationTransform:
    """Tests for BarAggregationTransform."""
    
    def test_transform_instantiation(self):
        """Test transform can be instantiated."""
        config = TransformConfig(name="test_bars")
        transform = BarAggregationTransform(config)
        
        assert transform.name == "test_bars"
    
    def test_aggregate_bars_vectorized(self):
        """Test aggregate_bars_vectorized function."""
        data = {
            "capture_ts": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:30",
                "2025-01-01 00:01:00",
                "2025-01-01 00:01:30",
                "2025-01-01 00:02:00",
            ],
            "mid_price": [100.0, 100.5, 101.0, 101.5, 102.0],
            "spread": [1.0, 1.1, 1.2, 0.9, 0.8],
            "exchange": ["binance"] * 5,
            "symbol": ["BTC/USDT"] * 5,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        # Aggregate into 60-second bars
        result_dict = aggregate_bars_vectorized(df, durations=[60])
        assert 60 in result_dict
        
        result_df = result_dict[60].collect()
        
        assert "open" in result_df.columns
        assert "high" in result_df.columns
        assert "low" in result_df.columns
        assert "close" in result_df.columns
        assert "mean_spread" in result_df.columns
        assert "tick_count" in result_df.columns


class TestTransformIntegration:
    """Integration tests for transform pipeline."""
    
    def test_orderbook_to_bars_pipeline(self):
        """Test full pipeline from orderbook to bars."""
        # Create orderbook data
        orderbook_data = {
            "timestamp": [1000, 1500, 2000, 2500],
            "capture_ts": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:30",
                "2025-01-01 00:01:00",
                "2025-01-01 00:01:30",
            ],
            "exchange": ["binance"] * 4,
            "symbol": ["BTC/USDT"] * 4,
            "bids": [
                [{"price": 100.0, "size": 10.0}],
                [{"price": 100.5, "size": 10.5}],
                [{"price": 101.0, "size": 11.0}],
                [{"price": 101.5, "size": 11.5}],
            ],
            "asks": [
                [{"price": 101.0, "size": 10.0}],
                [{"price": 101.5, "size": 10.5}],
                [{"price": 102.0, "size": 11.0}],
                [{"price": 102.5, "size": 11.5}],
            ],
        }
        df = pl.LazyFrame(orderbook_data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        # Step 1: Extract features
        from etl.features.orderbook import extract_structural_features
        silver_df = extract_structural_features(df, max_levels=5)
        
        # Step 2: Aggregate to bars
        bars_dict = aggregate_bars_vectorized(silver_df, durations=[60])
        result = bars_dict[60].collect()
        
        assert len(result) > 0
        assert "open" in result.columns
        assert "close" in result.columns


class TestTickerRollingFeatures:
    """Tests for compute_ticker_rolling_features function."""
    
    def test_ticker_rolling_features_columns_exist(self):
        """Test compute_ticker_rolling_features adds expected columns."""
        from etl.transforms.ticker import compute_ticker_rolling_features
        
        # Create test data with enough rows and time spread
        # Each row is 1 second apart
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(10)],
            "mid_price": [100.0 + i * 0.1 for i in range(10)],
            "spread": [1.0 + (i % 3) * 0.1 for i in range(10)],
            "volume_imbalance": [(i % 5 - 2) * 0.1 for i in range(10)],
            "exchange": ["binance"] * 10,
            "symbol": ["BTC/USDT"] * 10,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        # Use a 5-second window
        result = compute_ticker_rolling_features(df, windows=[5])
        result_df = result.collect()
        
        # Check that rolling columns were added
        assert "mid_price_mean_5s" in result_df.columns
        assert "mid_price_std_5s" in result_df.columns
        assert "spread_mean_5s" in result_df.columns
        assert "vol_imbalance_mean_5s" in result_df.columns
    
    def test_ticker_rolling_mean_calculation(self):
        """Test that rolling mean is calculated correctly."""
        from etl.transforms.ticker import compute_ticker_rolling_features
        
        # Create simple test data where we can verify the math
        # 10 rows, 1 second apart
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(10)],
            "mid_price": [100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0, 114.0, 116.0, 118.0],
            "spread": [1.0] * 10,
            "volume_imbalance": [0.0] * 10,
            "exchange": ["binance"] * 10,
            "symbol": ["BTC/USDT"] * 10,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        result = compute_ticker_rolling_features(df, windows=[3])
        result_df = result.collect()
        
        # The rolling window should produce valid means
        # Not all values may have rolling stats (depending on window alignment)
        # but the ones that do should be reasonable
        means = result_df["mid_price_mean_3s"].drop_nulls()
        assert len(means) > 0
        # All rolling means should be between min and max of mid_price
        assert means.min() >= 100.0
        assert means.max() <= 118.0
    
    def test_ticker_rolling_multiple_windows(self):
        """Test multiple rolling windows."""
        from etl.transforms.ticker import compute_ticker_rolling_features
        
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(15)],
            "mid_price": [100.0 + i for i in range(15)],
            "spread": [1.0] * 15,
            "volume_imbalance": [0.0] * 15,
            "exchange": ["binance"] * 15,
            "symbol": ["BTC/USDT"] * 15,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        result = compute_ticker_rolling_features(df, windows=[3, 5])
        result_df = result.collect()
        
        # Should have columns for both windows
        assert "mid_price_mean_3s" in result_df.columns
        assert "mid_price_mean_5s" in result_df.columns
        assert "spread_mean_3s" in result_df.columns
        assert "spread_mean_5s" in result_df.columns


class TestTradesAggregates:
    """Tests for compute_trades_aggregates function."""
    
    def test_trades_aggregates_columns_exist(self):
        """Test compute_trades_aggregates adds expected columns."""
        from etl.transforms.trades import compute_trades_aggregates, compute_trades_features
        
        # Create test data - 10 trades, 1 second apart
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(10)],
            "price": [100.0 + i * 0.5 for i in range(10)],
            "amount": [1.0 + (i % 3) * 0.5 for i in range(10)],
            "side": ["buy" if i % 2 == 0 else "sell" for i in range(10)],
            "exchange": ["binance"] * 10,
            "symbol": ["BTC/USDT"] * 10,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        # First compute basic features
        df = compute_trades_features(df)
        
        # Then compute rolling aggregates with 5-second window
        result = compute_trades_aggregates(df, windows=[5])
        result_df = result.collect()
        
        # Check that rolling columns were added
        assert "vwap_5s" in result_df.columns
        assert "volume_5s" in result_df.columns
        assert "dollar_volume_5s" in result_df.columns
        assert "trade_count_5s" in result_df.columns
        assert "buy_ratio_5s" in result_df.columns
    
    def test_trades_aggregates_vwap_calculation(self):
        """Test VWAP calculation is correct."""
        from etl.transforms.trades import compute_trades_aggregates, compute_trades_features
        
        # Simple test: all trades at same price
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(5)],
            "price": [100.0] * 5,  # All same price
            "amount": [1.0, 2.0, 3.0, 4.0, 5.0],
            "side": ["buy"] * 5,
            "exchange": ["binance"] * 5,
            "symbol": ["BTC/USDT"] * 5,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        df = compute_trades_features(df)
        result = compute_trades_aggregates(df, windows=[10])
        result_df = result.collect()
        
        # VWAP should be 100.0 when all prices are 100.0
        vwap_values = result_df["vwap_10s"].drop_nulls()
        if len(vwap_values) > 0:
            for vwap in vwap_values:
                assert vwap == pytest.approx(100.0, rel=1e-6)
    
    def test_trades_aggregates_buy_ratio(self):
        """Test buy ratio calculation."""
        from etl.transforms.trades import compute_trades_aggregates, compute_trades_features
        
        # 3 buys, 2 sells = 60% buy ratio
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(5)],
            "price": [100.0] * 5,
            "amount": [1.0] * 5,
            "side": ["buy", "buy", "buy", "sell", "sell"],
            "exchange": ["binance"] * 5,
            "symbol": ["BTC/USDT"] * 5,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        df = compute_trades_features(df)
        result = compute_trades_aggregates(df, windows=[10])
        result_df = result.collect()
        
        # Buy ratio should be 0.6 (3 buys / 5 total)
        buy_ratios = result_df["buy_ratio_10s"].drop_nulls()
        if len(buy_ratios) > 0:
            # Should be around 0.6
            assert buy_ratios[-1] == pytest.approx(0.6, rel=0.1)
    
    def test_trades_aggregates_volume_sum(self):
        """Test volume is summed correctly."""
        from etl.transforms.trades import compute_trades_aggregates, compute_trades_features
        
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(5)],
            "price": [100.0] * 5,
            "amount": [1.0, 2.0, 3.0, 4.0, 5.0],  # Total = 15
            "side": ["buy"] * 5,
            "exchange": ["binance"] * 5,
            "symbol": ["BTC/USDT"] * 5,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        df = compute_trades_features(df)
        result = compute_trades_aggregates(df, windows=[10])
        result_df = result.collect()
        
        # Volume should sum to 15
        volumes = result_df["volume_10s"].drop_nulls()
        if len(volumes) > 0:
            assert volumes[-1] == pytest.approx(15.0, rel=1e-6)


class TestAggregateTradesToBars:
    """Tests for aggregate_trades_to_bars function."""
    
    def test_aggregate_trades_to_bars(self):
        """Test aggregate_trades_to_bars creates time bars."""
        from etl.transforms.trades import aggregate_trades_to_bars, compute_trades_features
        
        # Create test data with trades
        timestamps = [f"2025-01-01 00:0{i // 6}:{(i % 6) * 10:02d}" for i in range(18)]
        data = {
            "capture_ts": timestamps,
            "price": [100.0 + i * 0.5 for i in range(18)],
            "amount": [1.0 + (i % 3) * 0.5 for i in range(18)],
            "side": ["buy" if i % 2 == 0 else "sell" for i in range(18)],
            "exchange": ["binance"] * 18,
            "symbol": ["BTC/USDT"] * 18,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        # Compute features first
        df = compute_trades_features(df)
        
        # Aggregate to bars
        bars_dict = aggregate_trades_to_bars(df, durations=[60])
        
        assert 60 in bars_dict
        result = bars_dict[60].collect()
        
        assert "open" in result.columns
        assert "high" in result.columns
        assert "low" in result.columns
        assert "close" in result.columns
        assert "volume" in result.columns
        assert "vwap" in result.columns
        assert "buy_volume" in result.columns
        assert "sell_volume" in result.columns
        assert "trade_imbalance" in result.columns
    
    def test_bars_have_correct_ohlc(self):
        """Test bars have correct OHLC values."""
        from etl.transforms.trades import aggregate_trades_to_bars, compute_trades_features
        
        data = {
            "capture_ts": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:10",
                "2025-01-01 00:00:20",
                "2025-01-01 00:00:30",
            ],
            "price": [100.0, 105.0, 95.0, 102.0],  # Open=100, High=105, Low=95, Close=102
            "amount": [1.0, 1.0, 1.0, 1.0],
            "side": ["buy"] * 4,
            "exchange": ["binance"] * 4,
            "symbol": ["BTC/USDT"] * 4,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        df = compute_trades_features(df)
        bars_dict = aggregate_trades_to_bars(df, durations=[60])
        result = bars_dict[60].collect()
        
        assert len(result) == 1
        assert result["open"][0] == 100.0
        assert result["high"][0] == 105.0
        assert result["low"][0] == 95.0
        assert result["close"][0] == 102.0


class TestAggregateOrderbookBars:
    """Tests for aggregate_orderbook_bars function."""
    
    def test_aggregate_orderbook_bars_columns_exist(self):
        """Test aggregate_orderbook_bars creates expected columns."""
        from etl.transforms.bars import aggregate_orderbook_bars
        
        # Create test data with orderbook features
        data = {
            "capture_ts": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:30",
                "2025-01-01 00:01:00",
                "2025-01-01 00:01:30",
            ],
            "mid_price": [100.0, 100.5, 101.0, 101.5],
            "spread": [1.0, 1.1, 1.2, 1.1],
            "relative_spread": [0.01, 0.011, 0.012, 0.011],
            "microprice": [100.2, 100.7, 101.2, 101.7],
            "imbalance_L1": [0.1, -0.1, 0.2, -0.05],
            "total_imbalance": [0.05, -0.05, 0.1, -0.02],
            "total_bid_depth": [100.0, 110.0, 120.0, 115.0],
            "total_ask_depth": [95.0, 100.0, 105.0, 108.0],
            "smart_depth_imbalance": [0.02, 0.05, 0.07, 0.03],
            "ofi": [0.0, 5.0, 3.0, -2.0],
            "mlofi": [0.0, 4.0, 2.5, -1.5],
            "log_return": [0.0, 0.005, 0.005, 0.005],
            "exchange": ["binance"] * 4,
            "symbol": ["BTC/USDT"] * 4,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        bars_dict = aggregate_orderbook_bars(df, durations=[60])
        
        assert 60 in bars_dict
        result = bars_dict[60].collect()
        
        # Check expected columns exist
        assert "open" in result.columns
        assert "high" in result.columns
        assert "low" in result.columns
        assert "close" in result.columns
        assert "mean_spread" in result.columns
        assert "mean_microprice" in result.columns
        assert "mean_l1_imbalance" in result.columns
        assert "sum_ofi" in result.columns
        assert "mean_bid_depth" in result.columns
        assert "snapshot_count" in result.columns
        assert "bar_duration_sec" in result.columns
        assert "return" in result.columns
    
    def test_aggregate_orderbook_bars_ohlc_values(self):
        """Test OHLC values are calculated correctly."""
        from etl.transforms.bars import aggregate_orderbook_bars
        
        # Create data where we know exact OHLC
        data = {
            "capture_ts": [
                "2025-01-01 00:00:00",  # Open
                "2025-01-01 00:00:15",  # High
                "2025-01-01 00:00:30",  # Low
                "2025-01-01 00:00:45",  # Close
            ],
            "mid_price": [100.0, 110.0, 90.0, 105.0],  # O=100, H=110, L=90, C=105
            "spread": [1.0] * 4,
            "relative_spread": [0.01] * 4,
            "microprice": [100.0] * 4,
            "imbalance_L1": [0.0] * 4,
            "total_imbalance": [0.0] * 4,
            "total_bid_depth": [100.0] * 4,
            "total_ask_depth": [100.0] * 4,
            "smart_depth_imbalance": [0.0] * 4,
            "ofi": [0.0] * 4,
            "mlofi": [0.0] * 4,
            "log_return": [0.0] * 4,
            "exchange": ["binance"] * 4,
            "symbol": ["BTC/USDT"] * 4,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        bars_dict = aggregate_orderbook_bars(df, durations=[60])
        result = bars_dict[60].collect()
        
        assert len(result) == 1
        assert result["open"][0] == 100.0
        assert result["high"][0] == 110.0
        assert result["low"][0] == 90.0
        assert result["close"][0] == 105.0
    
    def test_aggregate_orderbook_bars_ofi_sum(self):
        """Test OFI is summed correctly over the bar."""
        from etl.transforms.bars import aggregate_orderbook_bars
        
        data = {
            "capture_ts": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:15",
                "2025-01-01 00:00:30",
                "2025-01-01 00:00:45",
            ],
            "mid_price": [100.0] * 4,
            "spread": [1.0] * 4,
            "relative_spread": [0.01] * 4,
            "microprice": [100.0] * 4,
            "imbalance_L1": [0.0] * 4,
            "total_imbalance": [0.0] * 4,
            "total_bid_depth": [100.0] * 4,
            "total_ask_depth": [100.0] * 4,
            "smart_depth_imbalance": [0.0] * 4,
            "ofi": [10.0, 5.0, -3.0, 8.0],  # Sum = 20
            "mlofi": [5.0, 2.0, -1.0, 4.0],  # Sum = 10
            "log_return": [0.0] * 4,
            "exchange": ["binance"] * 4,
            "symbol": ["BTC/USDT"] * 4,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        bars_dict = aggregate_orderbook_bars(df, durations=[60])
        result = bars_dict[60].collect()
        
        assert result["sum_ofi"][0] == pytest.approx(20.0, rel=1e-6)
        assert result["sum_mlofi"][0] == pytest.approx(10.0, rel=1e-6)


class TestOrderbookComputeRollingFeatures:
    """Tests for compute_rolling_features function in orderbook.py."""
    
    def test_rolling_features_columns_exist(self):
        """Test compute_rolling_features adds expected columns."""
        from etl.features.orderbook import compute_rolling_features
        
        # Create test data with enough rows and time spread
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(15)],
            "mid_price": [100.0 + i * 0.1 for i in range(15)],
            "relative_spread": [0.001 + i * 0.0001 for i in range(15)],
            "exchange": ["binance"] * 15,
            "symbol": ["BTC/USDT"] * 15,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        # Use a small horizon for testing
        result = compute_rolling_features(df, horizons=[5])
        result_df = result.collect()
        
        # Check columns were added
        assert "log_return" in result_df.columns
        assert "rv_5s" in result_df.columns  # realized volatility
        assert "mean_return_5s" in result_df.columns
        assert "mean_spread_5s" in result_df.columns
    
    def test_log_return_calculation(self):
        """Test log return is calculated correctly."""
        from etl.features.orderbook import compute_rolling_features
        import math
        
        # Simple test: prices 100, 110, 121 (log returns are ln(1.1) each)
        data = {
            "capture_ts": ["2025-01-01 00:00:00", "2025-01-01 00:00:01", "2025-01-01 00:00:02"],
            "mid_price": [100.0, 110.0, 121.0],
            "relative_spread": [0.001, 0.001, 0.001],
            "exchange": ["binance"] * 3,
            "symbol": ["BTC/USDT"] * 3,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        result = compute_rolling_features(df, horizons=[5])
        result_df = result.collect()
        
        log_returns = result_df["log_return"].to_list()
        
        # First value is NaN (no previous price)
        assert log_returns[0] is None or math.isnan(log_returns[0])
        
        # Second value: ln(110/100) = ln(1.1) ≈ 0.0953
        expected_log_return_1 = math.log(110.0 / 100.0)
        assert log_returns[1] == pytest.approx(expected_log_return_1, rel=1e-6)
        
        # Third value: ln(121/110) = ln(1.1) ≈ 0.0953
        expected_log_return_2 = math.log(121.0 / 110.0)
        assert log_returns[2] == pytest.approx(expected_log_return_2, rel=1e-6)
    
    def test_rolling_mean_spread_calculation(self):
        """Test rolling mean spread is calculated correctly."""
        from etl.features.orderbook import compute_rolling_features
        
        # Create data with known spreads
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(10)],
            "mid_price": [100.0] * 10,  # Constant price - no log returns
            "relative_spread": [0.001, 0.002, 0.003, 0.001, 0.002, 0.003, 0.001, 0.002, 0.003, 0.001],
            "exchange": ["binance"] * 10,
            "symbol": ["BTC/USDT"] * 10,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        result = compute_rolling_features(df, horizons=[5])
        result_df = result.collect()
        
        mean_spreads = result_df["mean_spread_5s"].drop_nulls()
        
        # The rolling means should be reasonable averages
        assert len(mean_spreads) > 0
        # Mean of 0.001, 0.002, 0.003 repeated = 0.002
        for m in mean_spreads:
            assert 0.001 <= m <= 0.003  # Should be within bounds of actual spreads
    
    def test_rolling_volatility_calculation(self):
        """Test rolling realized volatility calculation."""
        from etl.features.orderbook import compute_rolling_features
        import math
        
        # Create data with consistent returns (same log return each step)
        # If all returns are the same, std should be close to 0
        base_price = 100.0
        multiplier = 1.01  # 1% return each time
        prices = [base_price * (multiplier ** i) for i in range(10)]
        
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(10)],
            "mid_price": prices,
            "relative_spread": [0.001] * 10,
            "exchange": ["binance"] * 10,
            "symbol": ["BTC/USDT"] * 10,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        result = compute_rolling_features(df, horizons=[5])
        result_df = result.collect()
        
        # Check log returns are constant ~ln(1.01)
        log_returns = result_df["log_return"].drop_nulls().to_list()
        expected_log_return = math.log(multiplier)
        for lr in log_returns:
            assert lr == pytest.approx(expected_log_return, rel=1e-6)
        
        # Rolling std of constant values should be 0
        rvs = result_df["rv_5s"].drop_nulls().to_list()
        for rv in rvs:
            if rv is not None and not math.isnan(rv):
                assert rv == pytest.approx(0.0, abs=1e-10)
    
    def test_multiple_horizons(self):
        """Test compute_rolling_features with multiple horizons."""
        from etl.features.orderbook import compute_rolling_features
        
        data = {
            "capture_ts": [f"2025-01-01 00:00:{i:02d}" for i in range(20)],
            "mid_price": [100.0 + i * 0.5 for i in range(20)],
            "relative_spread": [0.001] * 20,
            "exchange": ["binance"] * 20,
            "symbol": ["BTC/USDT"] * 20,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        result = compute_rolling_features(df, horizons=[5, 10, 15])
        result_df = result.collect()
        
        # Should have columns for all horizons
        assert "rv_5s" in result_df.columns
        assert "rv_10s" in result_df.columns
        assert "rv_15s" in result_df.columns
        assert "mean_return_5s" in result_df.columns
        assert "mean_return_10s" in result_df.columns
        assert "mean_return_15s" in result_df.columns
        assert "mean_spread_5s" in result_df.columns
        assert "mean_spread_10s" in result_df.columns
        assert "mean_spread_15s" in result_df.columns


class TestCreateOrderbookFeatureConfig:
    """Tests for create_orderbook_feature_config function."""
    
    def test_create_orderbook_feature_config_defaults(self):
        """Test create_orderbook_feature_config with required args."""
        from etl.transforms.orderbook import create_orderbook_feature_config
        
        config = create_orderbook_feature_config(
            orderbook_path="data/bronze/orderbook",
            silver_path="data/silver/orderbook",
        )
        
        assert config.name == "orderbook_features"
        assert "orderbook" in config.inputs
        assert "silver" in config.outputs
        assert config.inputs["orderbook"].path == "data/bronze/orderbook"
        assert config.outputs["silver"].path == "data/silver/orderbook"
    
    def test_create_orderbook_feature_config_with_trades(self):
        """Test create_orderbook_feature_config with trades path for TFI."""
        from etl.transforms.orderbook import create_orderbook_feature_config
        
        config = create_orderbook_feature_config(
            orderbook_path="data/bronze/orderbook",
            silver_path="data/silver/orderbook",
            trades_path="data/bronze/trades",
        )
        
        assert "orderbook" in config.inputs
        assert "trades" in config.inputs
        assert config.inputs["trades"].path == "data/bronze/trades"
    
    def test_create_orderbook_feature_config_custom_partitions(self):
        """Test create_orderbook_feature_config with custom partition columns."""
        from etl.transforms.orderbook import create_orderbook_feature_config
        
        config = create_orderbook_feature_config(
            orderbook_path="data/bronze/orderbook",
            silver_path="data/silver/orderbook",
            partition_cols=["exchange", "year", "month"],
        )
        
        assert config.inputs["orderbook"].partition_cols == ["exchange", "year", "month"]


class TestBarAggregationTransformMethods:
    """Tests for BarAggregationTransform class methods."""
    
    def test_transform_with_custom_durations(self):
        """Test BarAggregationTransform with custom bar durations."""
        from etl.core.config import InputConfig, OutputConfig, FeatureConfig
        
        config = TransformConfig(
            name="test_bars",
            inputs={"hf": InputConfig(name="hf", path="test/hf")},
            outputs={"bars": OutputConfig(name="bars", path="test/bars")},
        )
        transform = BarAggregationTransform(config)
        
        # Create test data
        data = {
            "capture_ts": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:30",
                "2025-01-01 00:01:00",
            ],
            "mid_price": [100.0, 100.5, 101.0],
            "spread": [1.0, 1.1, 1.2],
            "exchange": ["binance"] * 3,
            "symbol": ["BTC/USDT"] * 3,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        # Create context with custom bar durations
        feature_config = FeatureConfig(depth_levels=5)
        feature_config.bar_durations = [30, 60]
        context = TransformContext(
            execution_id=str(uuid.uuid4()),
            feature_config=feature_config,
        )
        
        inputs = {"hf": df}
        outputs = transform.transform(inputs, context)
        
        # Should have bars for both durations
        assert "bars_30s" in outputs
        assert "bars_60s" in outputs


class TestTradesFeatureTransformMethods:
    """Tests for TradesFeatureTransform class methods."""
    
    def test_transform_full_pipeline(self):
        """Test TradesFeatureTransform full pipeline."""
        from etl.core.config import InputConfig, OutputConfig
        
        config = TransformConfig(
            name="test_trades",
            inputs={"bronze": InputConfig(name="bronze", path="test/bronze")},
            outputs={"silver": OutputConfig(name="silver", path="test/silver")},
        )
        transform = TradesFeatureTransform(config)
        
        data = {
            "timestamp": [1000, 2000, 3000],
            "price": [100.0, 101.0, 102.0],
            "amount": [10.0, 11.0, 12.0],
            "side": ["buy", "sell", "buy"],
            "capture_ts": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:01",
                "2025-01-01 00:00:02",
            ],
            "exchange": ["binance"] * 3,
            "symbol": ["BTC/USDT"] * 3,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        context = TransformContext(execution_id=str(uuid.uuid4()))
        inputs = {"bronze": df}
        outputs = transform.transform(inputs, context)
        
        assert "silver" in outputs
        result = outputs["silver"].collect()
        
        assert "is_buy" in result.columns
        assert "dollar_volume" in result.columns
        assert "signed_volume" in result.columns
        assert "log_return" in result.columns


class TestTickerFeatureTransformMethods:
    """Tests for TickerFeatureTransform class methods."""
    
    def test_transform_full_pipeline(self):
        """Test TickerFeatureTransform full pipeline."""
        from etl.core.config import InputConfig, OutputConfig
        
        config = TransformConfig(
            name="test_ticker",
            inputs={"bronze": InputConfig(name="bronze", path="test/bronze")},
            outputs={"silver": OutputConfig(name="silver", path="test/silver")},
        )
        transform = TickerFeatureTransform(config)
        
        data = {
            "bid": [100.0, 101.0, 102.0],
            "ask": [101.0, 102.0, 103.0],
            "bid_volume": [10.0, 11.0, 12.0],
            "ask_volume": [9.0, 10.0, 11.0],
            "capture_ts": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:01",
                "2025-01-01 00:00:02",
            ],
            "exchange": ["binance"] * 3,
            "symbol": ["BTC/USDT"] * 3,
        }
        df = pl.LazyFrame(data).with_columns([
            pl.col("capture_ts").str.to_datetime()
        ])
        
        context = TransformContext(execution_id=str(uuid.uuid4()))
        inputs = {"bronze": df}
        outputs = transform.transform(inputs, context)
        
        assert "silver" in outputs
        result = outputs["silver"].collect()
        
        assert "spread" in result.columns
        assert "mid_price" in result.columns
        assert "relative_spread" in result.columns
        assert "volume_imbalance" in result.columns
        assert "hour" in result.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
