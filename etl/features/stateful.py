"""
Stateful Orderbook Feature Processor
====================================

Maintains state for computing path-dependent features that require
history across snapshots (OFI, MLOFI, TFI, Kyle's Lambda, etc.).

For batch processing of Parquet files where records are processed
sequentially, this class tracks the necessary state to compute
features that depend on previous values.

For real-time streaming, use the SymbolState class in state.py
which includes additional functionality for HF emission and bar building.

Features computed:
- Order Flow Imbalance (OFI) - Cont's method
- Multi-level OFI with exponential decay (MLOFI)
- Trade Flow Imbalance (TFI)
- Kyle's Lambda (price impact coefficient)
- Mid velocity and acceleration
- Spread regime (dynamic percentile-based)
- Rolling slope and CoG statistics
- VPIN (Volume-Synchronized Probability of Informed Trading)

References:
- Cont et al. (2014): OFI price impact
- Kyle & Obizhaeva (2016): Microstructure invariance
- Xu et al. (2019): Multi-level OFI
- Easley et al. (2012): VPIN
"""

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple

from etl.features.streaming import (
    KyleLambdaEstimator,
    RegimeStats,
    RollingPercentile,
    RollingSum,
    RollingWelford,
    VPINCalculator,
)


@dataclass
class StatefulProcessorConfig:
    """
    Configuration for stateful feature processor.
    
    All parameters should come from the application config, not hardcoded.
    """
    # Horizons for rolling stats (seconds)
    horizons: List[int] = field(default_factory=lambda: [5, 15, 60, 300, 900])
    
    # OFI computation parameters
    ofi_levels: int = 10
    ofi_decay_alpha: float = 0.5  # Exponential decay for MLOFI
    
    # Spread regime detection
    spread_regime_window: int = 300  # 5 min window
    spread_tight_percentile: float = 0.2
    spread_wide_percentile: float = 0.8
    use_dynamic_spread_regime: bool = True
    tight_spread_threshold: float = 0.0001  # Fallback static threshold
    
    # Kyle's Lambda window
    kyle_lambda_window: int = 300
    
    # VPIN configuration
    enable_vpin: bool = True
    vpin_bucket_volume: float = 1.0
    vpin_window_buckets: int = 50
    
    # State staleness threshold (seconds)
    # If time gap between prev_timestamp and current exceeds this,
    # reset prev_* state to avoid computing OFI against stale data.
    # Default: 300s (5 minutes) - reasonable for hour-by-hour processing
    max_state_staleness_seconds: float = 300.0


class StatefulFeatureProcessor:
    """
    Maintains state for computing path-dependent orderbook features.
    
    This processor is designed for batch processing where records
    are processed sequentially. It tracks previous snapshot state
    and accumulates rolling statistics.
    
    Usage:
        processor = StatefulFeatureProcessor(config)
        
        for record in records:
            # Process trades first (if available)
            for trade in record.get('trades', []):
                processor.process_trade(trade)
            
            # Then process orderbook
            features = processor.process_orderbook(record)
            record.update(features)
    """
    
    def __init__(self, config: StatefulProcessorConfig):
        """
        Initialize processor with configuration.
        
        Args:
            config: Processor configuration
        """
        self.config = config
        
        # Previous snapshot state
        self.prev_mid = None
        self.prev_best_bid = None
        self.prev_best_ask = None
        self.prev_bid_size = None
        self.prev_ask_size = None
        self.prev_bids: Optional[List[Tuple[float, float]]] = None
        self.prev_asks: Optional[List[Tuple[float, float]]] = None
        self.prev_timestamp = None
        
        # Trade accumulator for TFI
        self.pending_buy_volume = 0.0
        self.pending_sell_volume = 0.0
        
        # Mid price history for velocity/acceleration
        self.mid_history: Deque[Tuple[float, float]] = deque(maxlen=3)
        
        # Rolling statistics
        self._init_rolling_stats()
    
    def _init_rolling_stats(self):
        """Initialize all rolling statistics containers."""
        horizons = self.config.horizons
        
        # Core rolling stats
        self.rolling_ofi = {h: RollingSum(h) for h in horizons}
        self.rolling_mlofi = {h: RollingSum(h) for h in horizons}
        self.rolling_buy_vol = {h: RollingSum(h) for h in horizons}
        self.rolling_sell_vol = {h: RollingSum(h) for h in horizons}
        self.rolling_returns = {h: RollingWelford(h) for h in horizons}
        
        # Advanced rolling stats
        self.rolling_depth_var = RollingWelford(self.config.spread_regime_window)
        self.rolling_bid_slope = RollingWelford(60)
        self.rolling_ask_slope = RollingWelford(60)
        self.rolling_cog_momentum = RollingWelford(60)
        
        # Spread regime tracking
        self.spread_percentile_tracker = RollingPercentile(self.config.spread_regime_window)
        self.spread_regime = RegimeStats(self.config.spread_regime_window)
        
        # Kyle's Lambda estimator
        self.kyle_lambda = KyleLambdaEstimator(self.config.kyle_lambda_window)
        
        # VPIN calculator
        if self.config.enable_vpin:
            self.vpin_calculator = VPINCalculator(
                bucket_volume=self.config.vpin_bucket_volume,
                window_buckets=self.config.vpin_window_buckets,
            )
        else:
            self.vpin_calculator = None
    
    def reset(self):
        """Reset all state. Call between processing different symbols."""
        self._reset_prev_state()
        self.pending_buy_volume = 0.0
        self.pending_sell_volume = 0.0
        self.mid_history.clear()
        self._init_rolling_stats()
    
    def _reset_prev_state(self):
        """
        Reset only the prev_* state used for OFI/MLOFI computation.
        
        Called when:
        - Time gap exceeds max_state_staleness_seconds
        - Symbol changes during processing
        - Manual reset() call
        
        Does NOT reset rolling stats (they handle their own time-based expiry).
        """
        self.prev_mid = None
        self.prev_best_bid = None
        self.prev_best_ask = None
        self.prev_bid_size = None
        self.prev_ask_size = None
        self.prev_bids = None
        self.prev_asks = None
        # Note: prev_timestamp is NOT reset here so we can still track gaps
    
    def process_trade(self, trade: Dict[str, Any]):
        """
        Process a trade to accumulate for TFI calculation.
        
        Call this for each trade before processing the next orderbook snapshot.
        
        Args:
            trade: Trade dict with 'amount', 'side', 'price' fields
        """
        amount = trade.get("amount", 0) or 0
        side = (trade.get("side") or "").lower()
        price = trade.get("price", 0) or 0
        
        if side == "buy":
            self.pending_buy_volume += amount
        elif side == "sell":
            self.pending_sell_volume += amount
        
        # Update VPIN if enabled
        if self.vpin_calculator and price > 0:
            self.vpin_calculator.process_trade(price, amount, side if side else None)
    
    def process_orderbook(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process orderbook snapshot and compute all stateful features.
        
        Args:
            record: Orderbook record with 'bids', 'asks', 'timestamp' fields.
                    bids/asks can be:
                    - List[dict] with 'price', 'size' keys
                    - List[tuple/list] with [price, size]
        
        Returns:
            Dictionary of computed features to merge with structural features.
        
        Note:
            If the time gap between this snapshot and the previous one exceeds
            max_state_staleness_seconds, prev_* state is reset to avoid computing
            OFI/MLOFI against stale orderbook data. This handles:
            - First snapshot after loading stale state
            - Data gaps from ingestion downtime
            - Resuming processing after long pauses
        """
        features: Dict[str, Any] = {}
        
        ts_ms = record.get("timestamp") or record.get("collected_at", 0)
        ts_sec = ts_ms / 1000.0
        
        # Check for stale state and reset prev_* if gap is too large
        # This prevents computing OFI against orderbook data from hours ago
        if self.prev_timestamp is not None:
            time_gap = ts_sec - self.prev_timestamp
            if time_gap > self.config.max_state_staleness_seconds:
                # Reset only prev_* state (not rolling stats which handle their own expiry)
                self._reset_prev_state()
        
        # Extract bids/asks
        bids = record.get("bids", [])
        asks = record.get("asks", [])
        
        if not bids or not asks:
            return features
        
        # Normalize to list of tuples
        current_bids = self._normalize_levels(bids, self.config.ofi_levels)
        current_asks = self._normalize_levels(asks, self.config.ofi_levels)
        
        if not current_bids or not current_asks:
            return features
        
        best_bid = current_bids[0][0]
        best_ask = current_asks[0][0]
        bid_size = current_bids[0][1]
        ask_size = current_asks[0][1]
        mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
        spread = best_ask - best_bid if best_bid and best_ask else 0
        rel_spread = spread / mid if mid > 0 else 0
        
        # Initialize feature values
        ofi = 0.0
        mlofi = 0.0
        log_ret = 0.0
        
        # Compute velocity and acceleration
        features.update(self._compute_velocity_acceleration(ts_sec, mid))
        
        # Compute OFI and MLOFI if we have previous state
        if self.prev_mid is not None and mid > 0 and self.prev_mid > 0:
            log_ret = math.log(mid / self.prev_mid)
            ofi = self._compute_ofi(best_bid, best_ask, bid_size, ask_size)
            mlofi = self._compute_mlofi(current_bids, current_asks)
        
        # Primary feature names (used by downstream code)
        features["ofi"] = ofi
        features["mlofi"] = mlofi
        features["log_return"] = log_ret
        
        # Also provide step-suffixed versions for clarity in analysis
        features["ofi_step"] = ofi
        features["mlofi_step"] = mlofi
        features["log_return_step"] = log_ret
        
        # Update rolling OFI and returns
        for h, tracker in self.rolling_ofi.items():
            tracker.update(ofi, ts_sec)
            features[f"ofi_sum_{h}s"] = tracker.sum
        
        for h, tracker in self.rolling_mlofi.items():
            tracker.update(mlofi, ts_sec)
            features[f"mlofi_sum_{h}s"] = tracker.sum
        
        for h, tracker in self.rolling_returns.items():
            tracker.update(log_ret, ts_sec)
            features[f"rv_{h}s"] = tracker.std
        
        # TFI features
        features.update(self._compute_tfi(ts_sec))
        
        # Spread regime
        features.update(self._compute_spread_regime(ts_sec, rel_spread))
        
        # Kyle's Lambda
        self.kyle_lambda.update(ts_sec, log_ret, ofi)
        features["kyle_lambda"] = self.kyle_lambda.lambda_estimate
        features["kyle_lambda_r2"] = self.kyle_lambda.r_squared
        
        # Rolling slope stats
        bid_slope = record.get("bid_slope_regression")
        ask_slope = record.get("ask_slope_regression")
        features.update(self._update_slope_stats(ts_sec, bid_slope, ask_slope))
        
        # CoG momentum
        cog_vs_mid = record.get("cog_vs_mid")
        features.update(self._update_cog_momentum(ts_sec, cog_vs_mid))
        
        # Depth variance
        bid_vol_0_5 = record.get("bid_vol_band_0_5bps", 0) or 0
        ask_vol_0_5 = record.get("ask_vol_band_0_5bps", 0) or 0
        depth_0_5 = bid_vol_0_5 + ask_vol_0_5
        self.rolling_depth_var.update(depth_0_5, ts_sec)
        features["depth_0_5bps_sigma"] = self.rolling_depth_var.std
        
        # VPIN
        if self.vpin_calculator:
            features["vpin"] = self.vpin_calculator.vpin
            features["order_flow_toxicity"] = self.vpin_calculator.order_flow_toxicity
        
        # Update state for next snapshot
        self.prev_mid = mid
        self.prev_best_bid = best_bid
        self.prev_best_ask = best_ask
        self.prev_bid_size = bid_size
        self.prev_ask_size = ask_size
        self.prev_bids = current_bids
        self.prev_asks = current_asks
        self.prev_timestamp = ts_sec
        
        # Reset trade accumulators
        self.pending_buy_volume = 0.0
        self.pending_sell_volume = 0.0
        
        return features
    
    def _normalize_levels(
        self, 
        levels: List[Any], 
        max_levels: int
    ) -> List[Tuple[float, float]]:
        """Normalize bid/ask levels to list of (price, size) tuples."""
        result = []
        for level in levels[:max_levels]:
            if isinstance(level, dict):
                result.append((level["price"], level["size"]))
            elif isinstance(level, (list, tuple)):
                result.append((level[0], level[1]))
            else:
                break
        return result
    
    def _compute_velocity_acceleration(
        self, 
        ts_sec: float, 
        mid: float
    ) -> Dict[str, float]:
        """Compute mid price velocity and acceleration."""
        self.mid_history.append((ts_sec, mid))
        
        mid_velocity = 0.0
        mid_accel = 0.0
        
        if len(self.mid_history) >= 2:
            t1, p1 = self.mid_history[-1]
            t0, p0 = self.mid_history[-2]
            dt = t1 - t0
            if dt > 0:
                mid_velocity = (p1 - p0) / dt
        
        if len(self.mid_history) == 3:
            t2, p2 = self.mid_history[-1]
            t1, p1 = self.mid_history[-2]
            t0, p0 = self.mid_history[-3]
            dt1 = t2 - t1
            dt0 = t1 - t0
            if dt1 > 0 and dt0 > 0:
                v1 = (p2 - p1) / dt1
                v0 = (p1 - p0) / dt0
                mid_accel = (v1 - v0) / ((dt1 + dt0) / 2.0)
        
        return {
            "mid_velocity": mid_velocity,
            "mid_accel": mid_accel,
        }
    
    def _compute_ofi(
        self,
        best_bid: float,
        best_ask: float,
        bid_size: float,
        ask_size: float,
    ) -> float:
        """
        Compute Order Flow Imbalance using Cont's method.
        
        Reference: Cont et al. (2014) "Price Impact of Order Book Events"
        
        Note: This method should only be called when prev_* values are set.
        """
        # Type assertions for mypy - caller ensures these are not None
        assert self.prev_best_bid is not None
        assert self.prev_best_ask is not None
        assert self.prev_bid_size is not None
        assert self.prev_ask_size is not None
        
        # Bid side OFI
        if best_bid > self.prev_best_bid:
            ofi_bid = bid_size
        elif best_bid < self.prev_best_bid:
            ofi_bid = -self.prev_bid_size
        else:
            ofi_bid = bid_size - self.prev_bid_size
        
        # Ask side OFI
        if best_ask > self.prev_best_ask:
            ofi_ask = -self.prev_ask_size
        elif best_ask < self.prev_best_ask:
            ofi_ask = ask_size
        else:
            ofi_ask = ask_size - self.prev_ask_size
        
        return ofi_bid - ofi_ask
    
    def _compute_mlofi(
        self,
        current_bids: List[Tuple[float, float]],
        current_asks: List[Tuple[float, float]],
    ) -> float:
        """
        Compute Multi-Level OFI with exponential decay weighting.
        
        Reference: Xu et al. (2019) "Multi-Level Order-Flow Imbalance"
        """
        if not self.prev_bids or not self.prev_asks:
            return 0.0
        
        alpha = self.config.ofi_decay_alpha
        mlofi_bid_decay = 0.0
        mlofi_ask_decay = 0.0
        
        # Bids
        for i in range(min(len(current_bids), len(self.prev_bids))):
            curr_p, curr_s = current_bids[i]
            prev_p, prev_s = self.prev_bids[i]
            
            if curr_p > prev_p:
                level_ofi = curr_s
            elif curr_p < prev_p:
                level_ofi = -prev_s
            else:
                level_ofi = curr_s - prev_s
            
            weight = math.exp(-alpha * i)
            mlofi_bid_decay += level_ofi * weight
        
        # Asks
        for i in range(min(len(current_asks), len(self.prev_asks))):
            curr_p, curr_s = current_asks[i]
            prev_p, prev_s = self.prev_asks[i]
            
            if curr_p > prev_p:
                level_ofi = -prev_s
            elif curr_p < prev_p:
                level_ofi = curr_s
            else:
                level_ofi = curr_s - prev_s
            
            weight = math.exp(-alpha * i)
            mlofi_ask_decay += level_ofi * weight
        
        return mlofi_bid_decay - mlofi_ask_decay
    
    def _compute_tfi(self, ts_sec: float) -> Dict[str, float]:
        """Compute Trade Flow Imbalance features."""
        features = {}
        
        # Update rolling trade volumes
        for h in self.config.horizons:
            self.rolling_buy_vol[h].update(self.pending_buy_volume, ts_sec)
            self.rolling_sell_vol[h].update(self.pending_sell_volume, ts_sec)
            
            buy_vol = self.rolling_buy_vol[h].sum
            sell_vol = self.rolling_sell_vol[h].sum
            total_vol = buy_vol + sell_vol
            
            tfi = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0.0
            features[f"tfi_{h}s"] = tfi
            features[f"trade_vol_{h}s"] = total_vol
        
        return features
    
    def _compute_spread_regime(
        self, 
        ts_sec: float, 
        rel_spread: float
    ) -> Dict[str, Any]:
        """Compute spread regime features."""
        features = {}
        
        if not math.isnan(rel_spread):
            self.spread_percentile_tracker.update(rel_spread, ts_sec)
        
        if self.config.use_dynamic_spread_regime:
            spread_pctile = self.spread_percentile_tracker.get_current_percentile(rel_spread)
            features["spread_percentile"] = spread_pctile
            
            if spread_pctile <= self.config.spread_tight_percentile:
                regime = "tight"
            elif spread_pctile >= self.config.spread_wide_percentile:
                regime = "wide"
            else:
                regime = "normal"
        else:
            regime = "tight" if rel_spread <= self.config.tight_spread_threshold else "wide"
            features["spread_percentile"] = float("nan")
        
        features["spread_regime"] = regime
        
        self.spread_regime.update(regime, ts_sec)
        features["spread_regime_tight_frac"] = self.spread_regime.get_regime_fraction("tight")
        features["spread_regime_wide_frac"] = self.spread_regime.get_regime_fraction("wide")
        
        return features
    
    def _update_slope_stats(
        self,
        ts_sec: float,
        bid_slope: Optional[float],
        ask_slope: Optional[float],
    ) -> Dict[str, float]:
        """Update rolling slope statistics."""
        if bid_slope is not None and not math.isnan(bid_slope):
            self.rolling_bid_slope.update(bid_slope, ts_sec)
        if ask_slope is not None and not math.isnan(ask_slope):
            self.rolling_ask_slope.update(ask_slope, ts_sec)
        
        return {
            "bid_slope_mean_60s": self.rolling_bid_slope.mean,
            "bid_slope_std_60s": self.rolling_bid_slope.std,
            "ask_slope_mean_60s": self.rolling_ask_slope.mean,
            "ask_slope_std_60s": self.rolling_ask_slope.std,
        }
    
    def _update_cog_momentum(
        self,
        ts_sec: float,
        cog_vs_mid: Optional[float],
    ) -> Dict[str, float]:
        """Update center of gravity momentum statistics."""
        if cog_vs_mid is not None and not math.isnan(cog_vs_mid):
            self.rolling_cog_momentum.update(cog_vs_mid, ts_sec)
        
        return {
            "cog_momentum_mean_60s": self.rolling_cog_momentum.mean,
            "cog_momentum_std_60s": self.rolling_cog_momentum.std,
        }
    
    def get_state(self) -> Dict[str, Any]:
        """
        Get serializable state for checkpointing.
        
        Returns:
            Dictionary of state values that can be serialized.
        """
        return {
            "prev_mid": self.prev_mid,
            "prev_best_bid": self.prev_best_bid,
            "prev_best_ask": self.prev_best_ask,
            "prev_bid_size": self.prev_bid_size,
            "prev_ask_size": self.prev_ask_size,
            "prev_bids": self.prev_bids,
            "prev_asks": self.prev_asks,
            "prev_timestamp": self.prev_timestamp,
            "mid_history": list(self.mid_history),
        }
    
    def set_state(self, state: Dict[str, Any]):
        """
        Restore state from checkpoint.
        
        Args:
            state: State dictionary from get_state()
        """
        self.prev_mid = state.get("prev_mid")
        self.prev_best_bid = state.get("prev_best_bid")
        self.prev_best_ask = state.get("prev_best_ask")
        self.prev_bid_size = state.get("prev_bid_size")
        self.prev_ask_size = state.get("prev_ask_size")
        self.prev_bids = state.get("prev_bids")
        self.prev_asks = state.get("prev_asks")
        self.prev_timestamp = state.get("prev_timestamp")
        
        mid_history = state.get("mid_history", [])
        self.mid_history.clear()
        for item in mid_history:
            self.mid_history.append(tuple(item))
