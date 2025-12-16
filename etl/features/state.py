"""
Symbol state management and bar building.

This is the CORE of Daedalus's feature engineering system. It orchestrates:
1. Static feature extraction (via snapshot.py)
2. Dynamic feature computation (OFI, returns, velocity, acceleration)
3. Rolling statistics (realized volatility, OFI sums, TFI)
4. Bar aggregation (OHLCV + microstructure stats)
5. High-frequency feature emission

Key Components:

1. StateConfig (dataclass)
   - Configuration for feature engineering
   - All parameters customizable via config.yaml → processor_options
   - Controls horizons, bar durations, sampling rate, depth
   
2. BarBuilder (class)
   - Accumulates data for a single time bar
   - OHLCV + mean/min/max spread + sum OFI + realized variance
   - Emits completed bars when duration boundary crossed
   
3. SymbolState (class)
   - Maintains state for a single symbol (e.g., BTC/USDT)
   - Tracks previous snapshot for delta computation
   - Manages rolling statistics (Welford, RollingSum, RegimeStats)
   - Owns BarBuilder instances for each configured duration
   - process_snapshot() is called for each new orderbook update
   - process_trade() is called for each trade (for TFI calculation)

Data Flow:
    Raw orderbook snapshot
        ↓ (extract_orderbook_features)
    Static features (60+ columns)
        ↓ (SymbolState.process_snapshot)
    + Dynamic features (OFI, returns, velocity, acceleration)
    + Rolling statistics (volatility, OFI sums, TFI)
    + Time features (hour, day, weekend)
        ↓ (if hf_emit_interval elapsed)
    High-frequency feature row (emitted)
        ↓ (BarBuilder.update)
    Bar aggregates (when duration boundary crossed)

Configuration Example (config.yaml):
    etl:
      channels:
        orderbook:
          processor_options:
            horizons: [1, 5, 30, 60]       # Rolling stat windows (seconds)
            bar_durations: [1, 5, 30, 60]  # Bar intervals (seconds)
            hf_emit_interval: 1.0          # HF sampling rate
            max_levels: 10                 # Orderbook depth
            ofi_levels: 5                  # Levels for multi-level OFI
            bands_bps: [5, 10, 25, 50]     # Liquidity bands

Output:
    1. HF features (emitted every hf_emit_interval seconds):
       - All static features from snapshot.py
       - All dynamic features (OFI, returns, velocity, etc.)
       - All rolling statistics (rv_1s, ofi_sum_5s, tfi_30s, etc.)
       - Time features (hour, day, weekend)
       
    2. Bar aggregates (emitted when bar closes):
       - timestamp, duration
       - OHLC (of mid price)
       - mean/min/max spread
       - mean L1 imbalance
       - sum OFI
       - realized variance
       - count (snapshots in bar)

Usage:
    from etl.features.state import SymbolState, StateConfig
    
    config = StateConfig(
        horizons=[1, 5, 30, 60],
        bar_durations=[1, 5, 30, 60],
        hf_emit_interval=1.0,
        max_levels=20,
        ofi_levels=5,
    )
    
    state = SymbolState(symbol="BTC/USDT", exchange="binanceus", config=config)
    
    # Process each orderbook snapshot
    hf_row, completed_bars = state.process_snapshot(orderbook_snapshot)
    
    # Process each trade (for TFI)
    state.process_trade(trade_message)

Note: This module is stateful - SymbolState maintains history across calls.
      One SymbolState instance per symbol is required.

Key Algorithms:
- OFI: Cont's method ("Price Impact of Order Book Events", 2010)
- Variance: Welford's online algorithm (numerically stable)
- Rolling stats: Time-based decay with RollingSum
- Regime detection: RegimeStats with configurable thresholds
"""
import math
import logging
from typing import Dict, List, Optional, Any, Tuple, Deque
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timezone
import numpy as np

from etl.processors.time_utils import add_time_fields
from .snapshot import extract_orderbook_features
from .streaming import RollingWelford, TimeBasedRollingSum, RollingSum, RegimeStats

logger = logging.getLogger(__name__)

@dataclass
class StateConfig:
    """
    Configuration for SymbolState feature engineering.
    
    Research-optimized defaults based on:
    - Cont et al. (2014): OFI price impact
    - Kyle & Obizhaeva (2016): Microstructure invariance
    - López de Prado (2018): Volume clocks
    - Zhang et al. (2019): DeepLOB deep book features
    
    Key insights for mid-frequency (5s to 4h):
    - Longer horizons (5min, 15min) capture institutional flow patterns
    - 20 levels captures "walls" and hidden liquidity
    - Multi-level OFI with decay reduces noise by 15-70%
    - 1s bars redundant if emitting HF at 1s; use 1min+ bars
    """
    # Horizons for rolling stats (seconds)
    # Research: Include multi-minute windows for mid-frequency
    # [5s micro, 15s short, 60s standard, 300s (5min) medium, 900s (15min) trend]
    horizons: List[int] = field(default_factory=lambda: [5, 15, 60, 300, 900])
    
    # Bar durations to build (seconds)
    # Research: Skip sub-minute bars (redundant with HF), focus on 1min+ for patterns
    # [60s standard, 300s (5min), 900s (15min), 3600s (1hr) trend]
    bar_durations: List[int] = field(default_factory=lambda: [60, 300, 900, 3600])
    
    # High-frequency emission interval (seconds)
    # Research: 1.0s is good balance; sub-second mostly noise for mid-frequency
    hf_emit_interval: float = 1.0
    
    # Feature extraction settings
    # Research: 20 levels captures institutional "walls" and improves prediction
    max_levels: int = 20
    
    # OFI levels with decay weighting
    # Research: 10 levels with decay reduces forecast error by 15-70%
    ofi_levels: int = 10
    
    # OFI decay alpha for exponential weighting: w_i = exp(-alpha * i)
    # Higher alpha = more focus on L1, lower = more uniform across levels
    ofi_decay_alpha: float = 0.5
    
    # Liquidity bands in basis points
    # Research: Wider bands [5, 10, 25, 50, 100] for crypto volatility
    bands_bps: List[int] = field(default_factory=lambda: [5, 10, 25, 50, 100])
    
    keep_raw_arrays: bool = False
    
    # Spread regime detection
    # Research: Use dynamic percentile-based thresholds
    tight_spread_threshold: float = 0.0001  # 1 bp fallback
    use_dynamic_spread_regime: bool = True  # Enable percentile-based regime
    spread_regime_window: int = 300  # 5 min window for percentile calculation
    spread_tight_percentile: float = 0.2  # Bottom 20% = tight
    spread_wide_percentile: float = 0.8   # Top 20% = wide
    
    # Kyle's Lambda (price impact) settings
    kyle_lambda_window: int = 300  # 5 min rolling window for lambda estimation
    
    # VPIN settings (Volume-Synchronized Probability of Informed Trading)
    vpin_bucket_count: int = 50  # Number of volume buckets for VPIN
    vpin_window_buckets: int = 50  # Rolling window in buckets

class BarBuilder:
    """
    Accumulates data for a single time bar (OHLCV + stats).
    """
    def __init__(self, duration: int):
        self.duration = duration
        self.current_bar_start: Optional[float] = None
        self.bar_stats: Dict[str, Any] = {}
        self._reset()

    def _reset(self):
        self.bar_stats = {
            'open': None, 'high': -float('inf'), 'low': float('inf'), 'close': None,
            'count': 0,
            'sum_spread': 0.0,
            'sum_rel_spread': 0.0,
            'sum_ofi': 0.0,
            'sum_l1_imb': 0.0,
            'min_spread': float('inf'),
            'max_spread': -float('inf'),
            'rv_welford': RollingWelford(self.duration * 2), # Just for variance calc within bar
        }
        # Reset welford manually since we use it as an accumulator here
        self.bar_stats['rv_welford'].count = 0
        self.bar_stats['rv_welford'].mean = 0.0
        self.bar_stats['rv_welford'].M2 = 0.0

    def update(self, timestamp: float, mid: float, spread: float, rel_spread: float, ofi: float, l1_imb: float, log_ret: float) -> Optional[Dict[str, Any]]:
        """
        Update bar state. Returns a completed bar dict if the bar closed.
        """
        # Initialize start time if needed (align to duration grid)
        if self.current_bar_start is None:
            self.current_bar_start = (math.floor(timestamp / self.duration)) * self.duration

        # Check if we moved to a new bar
        completed_bar = None
        if timestamp >= self.current_bar_start + self.duration:
            # Finalize current bar
            if self.bar_stats['count'] > 0:
                completed_bar = self._finalize_bar(self.current_bar_start)
            
            # Start new bar (handling gaps if necessary, but for now just snap to current)
            self.current_bar_start = (math.floor(timestamp / self.duration)) * self.duration
            self._reset()

        # Update stats
        s = self.bar_stats
        if s['open'] is None:
            s['open'] = mid
        s['high'] = max(s['high'], mid)
        s['low'] = min(s['low'], mid)
        s['close'] = mid
        s['count'] += 1
        
        if not math.isnan(spread):
            s['sum_spread'] += spread
            s['min_spread'] = min(s['min_spread'], spread)
            s['max_spread'] = max(s['max_spread'], spread)
        
        if not math.isnan(rel_spread):
            s['sum_rel_spread'] += rel_spread
            
        if not math.isnan(ofi):
            s['sum_ofi'] += ofi
            
        if not math.isnan(l1_imb):
            s['sum_l1_imb'] += l1_imb
            
        # Update realized variance within bar
        # We use RollingWelford but just as a standard Welford accumulator here
        # We pass timestamp but it doesn't matter for accumulation if we don't evict
        s['rv_welford']._add_new(log_ret, timestamp)

        return completed_bar

    def _finalize_bar(self, start_time: float) -> Dict[str, Any]:
        s = self.bar_stats
        count = s['count']
        return {
            'timestamp': start_time,
            'duration': self.duration,
            'open': s['open'],
            'high': s['high'],
            'low': s['low'],
            'close': s['close'],
            'mean_spread': s['sum_spread'] / count if count > 0 else None,
            'mean_relative_spread': s['sum_rel_spread'] / count if count > 0 else None,
            'mean_l1_imbalance': s['sum_l1_imb'] / count if count > 0 else None,
            'sum_ofi': s['sum_ofi'],
            'min_spread': s['min_spread'] if count > 0 else None,
            'max_spread': s['max_spread'] if count > 0 else None,
            'realized_variance': s['rv_welford'].variance,
            'count': count
        }

class SymbolState:
    """
    Manages state for a single symbol:
    - Last snapshot info
    - Rolling statistics (Returns, OFI, Volatility)
    - Bar builders
    """
    def __init__(self, symbol: str, exchange: str, config: StateConfig):
        self.symbol = symbol
        self.exchange = exchange
        self.config = config
        
        # State
        self.last_snapshot_time = 0.0
        self.last_emit_time = 0.0
        self.prev_mid = None
        
        # L1 State
        self.prev_best_bid = None
        self.prev_best_ask = None
        self.prev_bid_size = None
        self.prev_ask_size = None
        
        # Multi-level State (Arrays of [price, size])
        self.prev_bids = None
        self.prev_asks = None
        
        # Acceleration State
        self.mid_history = deque(maxlen=3)  # t, t-1, t-2
        
        # Rolling Stats Containers
        # Map: horizon_sec -> RollingObject
        self.rolling_returns: Dict[int, RollingWelford] = {
            h: RollingWelford(h) for h in config.horizons
        }
        self.rolling_ofi: Dict[int, RollingSum] = {
            h: RollingSum(h) for h in config.horizons
        }
        
        # NEW: Rolling MLOFI (Multi-Level OFI with decay)
        self.rolling_mlofi: Dict[int, RollingSum] = {
            h: RollingSum(h) for h in config.horizons
        }
        
        # Trade Flow Imbalance (TFI)
        self.rolling_buy_vol: Dict[int, RollingSum] = {
            h: RollingSum(h) for h in config.horizons
        }
        self.rolling_sell_vol: Dict[int, RollingSum] = {
            h: RollingSum(h) for h in config.horizons
        }
        
        # Spread Regime Stats
        # Use configurable window instead of hardcoded 30s
        self.spread_regime = RegimeStats(config.spread_regime_window)
        
        # NEW: Dynamic spread regime tracking (percentile-based)
        from .streaming import RollingPercentile
        self.spread_percentile_tracker = RollingPercentile(config.spread_regime_window)
        
        # Rolling depth variance for 0-5bps band
        self.rolling_depth_var = RollingWelford(config.spread_regime_window)
        
        # NEW: Kyle's Lambda estimator (price impact)
        from .streaming import KyleLambdaEstimator
        self.kyle_lambda = KyleLambdaEstimator(config.kyle_lambda_window)
        
        # NEW: Book slope tracking for trend detection
        self.rolling_bid_slope = RollingWelford(60)  # 1-minute rolling slope stats
        self.rolling_ask_slope = RollingWelford(60)
        
        # NEW: Center of gravity momentum
        self.rolling_cog_momentum = RollingWelford(60)

        # Bar Builders
        self.bar_builders = [BarBuilder(d) for d in config.bar_durations]

    def process_trade(self, trade: Dict[str, Any]):
        """
        Process a trade message to update rolling trade volume stats.
        """
        try:
            ts_ms = trade.get('timestamp') or trade.get('collected_at')
            if not ts_ms:
                return
            
            ts_sec = ts_ms / 1000.0
            amount = float(trade.get('amount', 0))
            side = trade.get('side', '').lower() # 'buy' or 'sell'
            
            if side == 'buy':
                for tracker in self.rolling_buy_vol.values():
                    tracker.update(amount, ts_sec)
                # Update sell trackers with 0 to keep time moving? 
                # RollingSum handles time decay on update, but if no update comes, it might be stale.
                # Ideally we update all trackers with 0 if no event, but here we just update the relevant one.
                # The RollingSum implementation usually handles decay on read or update.
                # Let's assume we just update the active side.
                for tracker in self.rolling_sell_vol.values():
                    tracker.update(0, ts_sec)
                    
            elif side == 'sell':
                for tracker in self.rolling_sell_vol.values():
                    tracker.update(amount, ts_sec)
                for tracker in self.rolling_buy_vol.values():
                    tracker.update(0, ts_sec)
                    
        except Exception as e:
            logger.error(f"Error processing trade: {e}")

    def process_snapshot(self, snapshot: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Process a new orderbook snapshot.
        
        Returns:
            hf_row: High-frequency feature row (or None if not time to emit)
            bar_rows: List of completed bars
        """
        # 1. Extract Structural Features
        features = extract_orderbook_features(
            snapshot, 
            max_levels=self.config.max_levels,
            bands_bps=self.config.bands_bps,
            keep_raw_arrays=self.config.keep_raw_arrays
        )
        
        # Normalize timestamp to seconds for internal logic
        ts_ms = features['timestamp']
        ts_sec = ts_ms / 1000.0
        
        mid = features['mid_price']
        best_bid = features['best_bid']
        best_ask = features['best_ask']
        bid_size = features['bid_size_L0']
        ask_size = features['ask_size_L0']
        microprice = features['microprice']
        
        # 2. Calculate Deltas (Returns, OFI)
        log_ret = 0.0
        ofi = 0.0
        ofi_multi = 0.0
        mlofi_decay = 0.0  # NEW: MLOFI with exponential decay weighting
        
        # Reconstruct current top levels for Multi-level OFI
        current_bids = []
        current_asks = []
        for i in range(self.config.ofi_levels):
            bp = features.get(f'bid_price_L{i}')
            bs = features.get(f'bid_size_L{i}')
            ap = features.get(f'ask_price_L{i}')
            as_ = features.get(f'ask_size_L{i}')
            
            if bp is not None and not math.isnan(bp):
                current_bids.append((bp, bs))
            if ap is not None and not math.isnan(ap):
                current_asks.append((ap, as_))

        if self.prev_mid is not None and mid > 0 and self.prev_mid > 0:
            log_ret = math.log(mid / self.prev_mid)
            
            # L1 OFI Calculation (Cont's method)
            # Bid side
            if best_bid > self.prev_best_bid:
                ofi_bid = bid_size
            elif best_bid < self.prev_best_bid:
                ofi_bid = -self.prev_bid_size
            else: # Equal
                ofi_bid = bid_size - self.prev_bid_size
                
            # Ask side
            if best_ask > self.prev_best_ask:
                ofi_ask = -self.prev_ask_size
            elif best_ask < self.prev_best_ask:
                ofi_ask = ask_size
            else: # Equal
                ofi_ask = ask_size - self.prev_ask_size
                
            ofi = ofi_bid - ofi_ask # Standard: e_t = e_b_t - e_a_t
            
            # Multi-level OFI with DECAY WEIGHTING (Research Enhancement)
            # Reference: Xu et al. (2019) "Multi-Level Order-Flow Imbalance"
            # MLOFI = Σ w_i * OFI_i where w_i = exp(-alpha * i)
            if self.prev_bids and self.prev_asks:
                ofi_multi_bid = 0.0
                ofi_multi_ask = 0.0
                mlofi_bid_decay = 0.0
                mlofi_ask_decay = 0.0
                
                alpha = self.config.ofi_decay_alpha
                
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
                    
                    # Uniform weight for ofi_multi
                    ofi_multi_bid += level_ofi
                    # Decay weight for mlofi_decay
                    weight = math.exp(-alpha * i)
                    mlofi_bid_decay += level_ofi * weight
                        
                # Asks
                for i in range(min(len(current_asks), len(self.prev_asks))):
                    curr_p, curr_s = current_asks[i]
                    prev_p, prev_s = self.prev_asks[i]
                    
                    if curr_p > prev_p:
                        level_ofi = -prev_s  # Price moved up (away)
                    elif curr_p < prev_p:
                        level_ofi = curr_s   # Price moved down (closer)
                    else:
                        level_ofi = curr_s - prev_s
                        
                    ofi_multi_ask += level_ofi
                    weight = math.exp(-alpha * i)
                    mlofi_ask_decay += level_ofi * weight

                ofi_multi = ofi_multi_bid - ofi_multi_ask
                mlofi_decay = mlofi_bid_decay - mlofi_ask_decay

        # 3. Acceleration & Velocity
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

        features['mid_velocity'] = mid_velocity
        features['mid_accel'] = mid_accel
        features['ofi_multi'] = ofi_multi
        
        # NEW: MLOFI with decay weighting (Research Priority Feature)
        features['mlofi_decay'] = mlofi_decay

        # 4. Update Rolling Stats & TFI
        for h, tracker in self.rolling_returns.items():
            tracker.update(log_ret, ts_sec)
            features[f'rv_{h}s'] = tracker.std
            
        for h, tracker in self.rolling_ofi.items():
            tracker.update(ofi, ts_sec)
            features[f'ofi_sum_{h}s'] = tracker.sum
        
        # NEW: Rolling MLOFI with decay
        for h, tracker in self.rolling_mlofi.items():
            tracker.update(mlofi_decay, ts_sec)
            features[f'mlofi_sum_{h}s'] = tracker.sum
            
        # TFI Features
        for h in self.config.horizons:
            buy_vol = self.rolling_buy_vol[h].sum
            sell_vol = self.rolling_sell_vol[h].sum
            total_vol = buy_vol + sell_vol
            tfi = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0.0
            features[f'tfi_{h}s'] = tfi
            features[f'trade_vol_{h}s'] = total_vol

        # Time Features
        # Use robust time_utils logic to add all time components
        # We use the 'datetime' field if available (ISO string), or fallback to 'timestamp' (ms int)
        # Note: extract_orderbook_features already ensures 'timestamp' is present
        
        # Create a temporary record for time parsing
        time_record = {
            'timestamp': features.get('timestamp'),
            'datetime': features.get('datetime') # Might be None if not in snapshot
        }
        
        # Enrich with time fields
        # time_utils.add_time_fields now handles int/float timestamps directly
        add_time_fields(time_record, 'datetime', 'timestamp')
        
        # Copy relevant time fields back to features
        # We want specific cyclic/categorical features for ML
        features['hour_of_day'] = time_record.get('hour')
        features['day_of_week'] = time_record.get('day_of_week')
        features['is_weekend'] = 1 if time_record.get('is_weekend') else 0
        features['minute_of_hour'] = time_record.get('minute')
        
        # Update depth variance (example: 0-5bps band)
        depth_0_5 = features.get('bid_vol_band_0_5bps', 0) + features.get('ask_vol_band_0_5bps', 0)
        self.rolling_depth_var.update(depth_0_5, ts_sec)
        features['depth_0_5bps_sigma'] = self.rolling_depth_var.std
        
        # ===============================================================
        # NEW RESEARCH FEATURES (Dynamic / State-based)
        # ===============================================================
        
        # --- Dynamic Spread Regime (Percentile-based) ---
        rel_spread = features.get('relative_spread', 0)
        if not math.isnan(rel_spread):
            self.spread_percentile_tracker.update(rel_spread, ts_sec)
        
        if self.config.use_dynamic_spread_regime:
            # Use percentile-based regime detection
            spread_pctile = self.spread_percentile_tracker.get_current_percentile(rel_spread)
            features['spread_percentile'] = spread_pctile
            
            if spread_pctile <= self.config.spread_tight_percentile:
                regime = "tight"
            elif spread_pctile >= self.config.spread_wide_percentile:
                regime = "wide"
            else:
                regime = "normal"
            features['spread_regime'] = regime
        else:
            # Fallback to static threshold
            regime = "tight" if rel_spread <= self.config.tight_spread_threshold else "wide"
            features['spread_percentile'] = np.nan
            features['spread_regime'] = regime
        
        self.spread_regime.update(regime, ts_sec)
        features['spread_regime_tight_frac'] = self.spread_regime.get_regime_fraction("tight")
        features['spread_regime_wide_frac'] = self.spread_regime.get_regime_fraction("wide")
        
        # --- Kyle's Lambda (Price Impact Coefficient) ---
        # Update estimator with return and signed flow
        signed_flow = ofi  # Use OFI as proxy for signed order flow
        self.kyle_lambda.update(ts_sec, log_ret, signed_flow)
        features['kyle_lambda'] = self.kyle_lambda.lambda_estimate
        features['kyle_lambda_r2'] = self.kyle_lambda.r_squared
        
        # --- Book Slope Rolling Stats ---
        bid_slope = features.get('bid_slope_regression', np.nan)
        ask_slope = features.get('ask_slope_regression', np.nan)
        
        if not math.isnan(bid_slope):
            self.rolling_bid_slope.update(bid_slope, ts_sec)
        if not math.isnan(ask_slope):
            self.rolling_ask_slope.update(ask_slope, ts_sec)
        
        features['bid_slope_mean_60s'] = self.rolling_bid_slope.mean
        features['bid_slope_std_60s'] = self.rolling_bid_slope.std
        features['ask_slope_mean_60s'] = self.rolling_ask_slope.mean
        features['ask_slope_std_60s'] = self.rolling_ask_slope.std
        
        # --- Center of Gravity Momentum ---
        cog_vs_mid = features.get('cog_vs_mid', np.nan)
        if not math.isnan(cog_vs_mid):
            self.rolling_cog_momentum.update(cog_vs_mid, ts_sec)
        
        features['cog_momentum_mean_60s'] = self.rolling_cog_momentum.mean
        features['cog_momentum_std_60s'] = self.rolling_cog_momentum.std
        
        # --- Microprice drift (already calculated) ---
        if mid > 0:
            features['micro_minus_mid'] = microprice - mid
        else:
            features['micro_minus_mid'] = np.nan

        # 5. Update State
        self.prev_mid = mid
        self.prev_best_bid = best_bid
        self.prev_best_ask = best_ask
        self.prev_bid_size = bid_size
        self.prev_ask_size = ask_size
        self.prev_bids = current_bids
        self.prev_asks = current_asks
        self.last_snapshot_time = ts_sec

        # 6. Bar Building
        completed_bars = []
        for builder in self.bar_builders:
            bar = builder.update(
                ts_sec, 
                mid, 
                features['spread'], 
                features['relative_spread'], 
                ofi,
                features['imbalance_L1'],
                log_ret
            )
            if bar:
                bar['symbol'] = self.symbol
                bar['exchange'] = self.exchange
                bar['date'] = features.get('date')
                # Add MLOFI to bars as well
                bar['sum_mlofi_decay'] = mlofi_decay
                completed_bars.append(bar)

        # 7. HF Emission Check
        hf_row = None
        if ts_sec - self.last_emit_time >= self.config.hf_emit_interval:
            hf_row = features
            # Add derived rolling features to the emitted row
            hf_row['log_return_step'] = log_ret
            hf_row['ofi_step'] = ofi
            hf_row['mlofi_step'] = mlofi_decay
            self.last_emit_time = ts_sec

        return hf_row, completed_bars
