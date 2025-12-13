"""
Streaming statistics module.
Provides online algorithms for rolling means, variances, and sums over time-based windows.
"""
from collections import deque
from typing import Deque, Tuple, Optional, Dict
import math

class Welford:
    """
    Numerically stable online mean and variance tracker (infinite history).
    """
    def __init__(self):
        self.count = 0
        self.mean = 0.0
        self.M2 = 0.0

    def update(self, x: float):
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.M2 += delta * delta2

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        # Clamp M2 to 0 to avoid negative variance due to floating point errors
        return max(0.0, self.M2) / (self.count - 1)

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)


class TimeBasedRolling:
    """
    Base class for time-based rolling statistics.
    Manages a deque of (timestamp, value) and handles eviction.
    """
    def __init__(self, window_seconds: float):
        self.window_seconds = window_seconds
        self.deque: Deque[Tuple[float, float]] = deque()

    def _evict(self, current_time: float):
        threshold = current_time - self.window_seconds
        while self.deque and self.deque[0][0] <= threshold:
            self._remove_oldest()

    def _remove_oldest(self):
        # To be implemented by subclasses to update stats before popping
        self.deque.popleft()

    def update(self, value: float, timestamp: float):
        self._evict(timestamp)
        self._add_new(value, timestamp)

    def _add_new(self, value: float, timestamp: float):
        self.deque.append((timestamp, value))


class TimeBasedRollingSum(TimeBasedRolling):
    """
    Rolling sum and mean over a time window.
    """
    def __init__(self, window_seconds: float):
        super().__init__(window_seconds)
        self.current_sum = 0.0

    def _remove_oldest(self):
        ts, val = self.deque.popleft()
        self.current_sum -= val

    def _add_new(self, value: float, timestamp: float):
        self.deque.append((timestamp, value))
        self.current_sum += value

    @property
    def sum(self) -> float:
        return self.current_sum

    @property
    def mean(self) -> float:
        if not self.deque:
            return 0.0
        return self.current_sum / len(self.deque)


class RollingWelford(TimeBasedRolling):
    """
    Rolling mean and variance over a time window using Welford's algorithm
    with support for removing expired values.
    """
    def __init__(self, window_seconds: float):
        super().__init__(window_seconds)
        self.count = 0
        self.mean = 0.0
        self.M2 = 0.0

    def _add_new(self, x: float, timestamp: float):
        self.deque.append((timestamp, x))
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.M2 += delta * delta2

    def _remove_oldest(self):
        ts, x = self.deque.popleft()
        if self.count <= 1:
            self.count = 0
            self.mean = 0.0
            self.M2 = 0.0
            return

        # Welford removal logic
        # old_mean = mean_prev
        # new_mean = (n * old_mean - x) / (n - 1)
        # M2_new = M2_old - (x - old_mean) * (x - new_mean)
        
        old_mean = self.mean
        self.mean = (self.count * old_mean - x) / (self.count - 1)
        self.M2 -= (x - old_mean) * (x - self.mean)
        self.count -= 1

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        # Clamp M2 to 0 to avoid negative variance due to floating point errors
        return max(0.0, self.M2) / (self.count - 1)

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)


class RollingSum(TimeBasedRollingSum):
    """Alias for TimeBasedRollingSum."""
    pass


class RegimeStats(TimeBasedRolling):
    """
    Tracks time spent in different regimes (e.g. tight vs wide spread).
    """
    def __init__(self, window_seconds: float):
        super().__init__(window_seconds)
        self.total_duration = 0.0
        self.regime_durations: Dict[str, float] = {}
        self.last_update_time: Optional[float] = None
        self.current_regime: Optional[str] = None

    def update(self, regime: str, timestamp: float):
        self._evict(timestamp)
        
        if self.last_update_time is not None:
            duration = timestamp - self.last_update_time
            if duration > 0 and self.current_regime is not None:
                self._add_duration(self.current_regime, duration, timestamp)
        
        self.current_regime = regime
        self.last_update_time = timestamp

    def _add_duration(self, regime: str, duration: float, timestamp: float):
        self.deque.append((timestamp, (regime, duration)))
        self.regime_durations[regime] = self.regime_durations.get(regime, 0.0) + duration
        self.total_duration += duration

    def _remove_oldest(self):
        ts, (regime, duration) = self.deque.popleft()
        self.regime_durations[regime] -= duration
        self.total_duration -= duration
        if self.regime_durations[regime] <= 0:
            del self.regime_durations[regime]

    def get_regime_fraction(self, regime: str) -> float:
        if self.total_duration <= 0:
            return 0.0
        return self.regime_durations.get(regime, 0.0) / self.total_duration


class RollingPercentile(TimeBasedRolling):
    """
    Rolling percentile tracker over a time window.
    Useful for dynamic threshold calculation (e.g., spread regime detection).
    """
    def __init__(self, window_seconds: float):
        super().__init__(window_seconds)
    
    def _remove_oldest(self):
        self.deque.popleft()
    
    def _add_new(self, value: float, timestamp: float):
        self.deque.append((timestamp, value))
    
    def get_percentile(self, p: float) -> float:
        """
        Get the p-th percentile (0 to 1) of values in the window.
        """
        if not self.deque:
            return 0.0
        values = sorted([v for _, v in self.deque])
        n = len(values)
        idx = int(p * (n - 1))
        return values[idx]
    
    def get_current_percentile(self, value: float) -> float:
        """
        Get the percentile rank of a value within the current distribution.
        Returns 0-1 indicating where value falls in the distribution.
        """
        if not self.deque:
            return 0.5
        values = [v for _, v in self.deque]
        n = len(values)
        below = sum(1 for v in values if v < value)
        return below / n


class KyleLambdaEstimator:
    """
    Rolling estimator for Kyle's Lambda (price impact coefficient).
    
    λ = Cov(ΔP, SignedFlow) / Var(SignedFlow)
    
    Lambda measures how much price moves per unit of signed order flow.
    Higher lambda = more illiquid/fragile market, easier to move price.
    
    Reference: Kyle (1985), Kyle & Obizhaeva (2016)
    """
    def __init__(self, window_seconds: float = 300):
        self.window_seconds = window_seconds
        self.data: Deque[Tuple[float, float, float]] = deque()  # (timestamp, return, flow)
        
        # Running statistics for incremental calculation
        self.sum_ret = 0.0
        self.sum_flow = 0.0
        self.sum_ret_sq = 0.0
        self.sum_flow_sq = 0.0
        self.sum_ret_flow = 0.0
        self.count = 0
    
    def _evict(self, current_time: float):
        threshold = current_time - self.window_seconds
        while self.data and self.data[0][0] <= threshold:
            ts, ret, flow = self.data.popleft()
            self.sum_ret -= ret
            self.sum_flow -= flow
            self.sum_ret_sq -= ret * ret
            self.sum_flow_sq -= flow * flow
            self.sum_ret_flow -= ret * flow
            self.count -= 1
    
    def update(self, timestamp: float, price_return: float, signed_flow: float):
        """
        Update with new observation.
        
        Args:
            timestamp: Unix timestamp in seconds
            price_return: Log return or price change
            signed_flow: Signed order flow (positive = buy pressure)
        """
        self._evict(timestamp)
        
        self.data.append((timestamp, price_return, signed_flow))
        self.sum_ret += price_return
        self.sum_flow += signed_flow
        self.sum_ret_sq += price_return * price_return
        self.sum_flow_sq += signed_flow * signed_flow
        self.sum_ret_flow += price_return * signed_flow
        self.count += 1
    
    @property
    def lambda_estimate(self) -> float:
        """
        Get current Kyle's lambda estimate.
        λ = Cov(ret, flow) / Var(flow)
        """
        if self.count < 2:
            return 0.0
        
        n = self.count
        mean_ret = self.sum_ret / n
        mean_flow = self.sum_flow / n
        
        # Covariance: E[XY] - E[X]E[Y]
        cov = (self.sum_ret_flow / n) - (mean_ret * mean_flow)
        
        # Variance: E[X^2] - E[X]^2
        var_flow = (self.sum_flow_sq / n) - (mean_flow * mean_flow)
        
        if var_flow < 1e-10:
            return 0.0
        
        return cov / var_flow
    
    @property
    def r_squared(self) -> float:
        """
        Get R-squared of the lambda regression.
        Higher R² means lambda estimate is more reliable.
        """
        if self.count < 2:
            return 0.0
        
        n = self.count
        mean_ret = self.sum_ret / n
        mean_flow = self.sum_flow / n
        
        var_ret = (self.sum_ret_sq / n) - (mean_ret * mean_ret)
        var_flow = (self.sum_flow_sq / n) - (mean_flow * mean_flow)
        
        if var_ret < 1e-10 or var_flow < 1e-10:
            return 0.0
        
        cov = (self.sum_ret_flow / n) - (mean_ret * mean_flow)
        correlation = cov / math.sqrt(var_ret * var_flow)
        
        return correlation * correlation


class VPINCalculator:
    """
    Volume-Synchronized Probability of Informed Trading (VPIN).
    
    VPIN estimates order flow toxicity - the probability that trades are
    from informed traders. High VPIN often precedes volatility spikes.
    
    Algorithm:
    1. Accumulate trades into volume buckets (fixed volume per bucket)
    2. Classify each bucket's volume as buy/sell using BVC
    3. VPIN = rolling average of |BuyVol - SellVol| / TotalVol
    
    Reference: Easley, López de Prado, O'Hara (2012)
    """
    def __init__(self, bucket_volume: float = 1.0, window_buckets: int = 50):
        """
        Args:
            bucket_volume: Volume threshold for each bucket (e.g., 100 BTC)
            window_buckets: Number of buckets in rolling VPIN window
        """
        self.bucket_volume = bucket_volume
        self.window_buckets = window_buckets
        
        # Current bucket state
        self.current_bucket_vol = 0.0
        self.current_bucket_buy_vol = 0.0
        self.current_bucket_sell_vol = 0.0
        self.last_price: Optional[float] = None
        
        # Completed buckets
        self.buckets: Deque[Tuple[float, float]] = deque(maxlen=window_buckets)
        # Each bucket: (buy_vol, sell_vol)
    
    def process_trade(self, price: float, volume: float, side: Optional[str] = None) -> Optional[float]:
        """
        Process a trade and potentially emit updated VPIN.
        
        Args:
            price: Trade price
            volume: Trade volume
            side: Optional 'buy' or 'sell'. If not provided, uses tick rule.
        
        Returns:
            Updated VPIN if a bucket was completed, else None
        """
        # Classify trade if side not provided (Bulk Volume Classification)
        if side is None:
            if self.last_price is not None:
                # Tick rule: price up = buy, price down = sell
                if price > self.last_price:
                    buy_vol, sell_vol = volume, 0.0
                elif price < self.last_price:
                    buy_vol, sell_vol = 0.0, volume
                else:
                    # Price unchanged: split 50/50
                    buy_vol, sell_vol = volume / 2, volume / 2
            else:
                buy_vol, sell_vol = volume / 2, volume / 2
        else:
            if side.lower() == 'buy':
                buy_vol, sell_vol = volume, 0.0
            else:
                buy_vol, sell_vol = 0.0, volume
        
        self.last_price = price
        
        # Add to current bucket
        remaining_vol = volume
        vpin_updated = None
        
        while remaining_vol > 0:
            space_in_bucket = self.bucket_volume - self.current_bucket_vol
            
            if remaining_vol >= space_in_bucket:
                # Fill current bucket and complete it
                fill_ratio = space_in_bucket / volume if volume > 0 else 0
                self.current_bucket_buy_vol += buy_vol * fill_ratio
                self.current_bucket_sell_vol += sell_vol * fill_ratio
                self.current_bucket_vol = self.bucket_volume
                
                # Complete bucket
                self.buckets.append((self.current_bucket_buy_vol, self.current_bucket_sell_vol))
                
                # Reset for new bucket
                self.current_bucket_vol = 0.0
                self.current_bucket_buy_vol = 0.0
                self.current_bucket_sell_vol = 0.0
                
                remaining_vol -= space_in_bucket
                buy_vol *= (1 - fill_ratio)
                sell_vol *= (1 - fill_ratio)
                
                vpin_updated = self.vpin
            else:
                # Partial fill
                ratio = remaining_vol / volume if volume > 0 else 0
                self.current_bucket_buy_vol += buy_vol * ratio
                self.current_bucket_sell_vol += sell_vol * ratio
                self.current_bucket_vol += remaining_vol
                remaining_vol = 0
        
        return vpin_updated
    
    @property
    def vpin(self) -> float:
        """
        Calculate current VPIN from completed buckets.
        VPIN = Average(|BuyVol - SellVol|) / BucketVolume
        """
        if not self.buckets:
            return 0.0
        
        total_imbalance = sum(abs(b - s) for b, s in self.buckets)
        total_volume = len(self.buckets) * self.bucket_volume
        
        return total_imbalance / total_volume if total_volume > 0 else 0.0
    
    @property
    def order_flow_toxicity(self) -> float:
        """
        Alias for VPIN - order flow toxicity measure.
        Higher values indicate more informed/toxic flow.
        """
        return self.vpin
    
    def set_bucket_volume(self, bucket_volume: float):
        """Dynamically adjust bucket volume (e.g., based on daily volume)."""
        self.bucket_volume = bucket_volume


class VolumeBarBuilder:
    """
    Volume-based bar generator (Volume Clock).
    
    Instead of fixed-time bars, emit bars after a fixed volume is traded.
    This synchronizes with information arrival and produces more 
    statistically stable return distributions.
    
    Reference: López de Prado (2018), Easley et al. (2012)
    """
    def __init__(self, volume_threshold: float = 100.0):
        """
        Args:
            volume_threshold: Volume required to complete a bar
        """
        self.volume_threshold = volume_threshold
        self.current_bar = self._new_bar()
        self.current_volume = 0.0
    
    def _new_bar(self) -> Dict[str, Any]:
        return {
            'open': None,
            'high': -float('inf'),
            'low': float('inf'),
            'close': None,
            'volume': 0.0,
            'buy_volume': 0.0,
            'sell_volume': 0.0,
            'vwap_sum': 0.0,
            'trade_count': 0,
            'start_time': None,
            'end_time': None,
        }
    
    def process_trade(self, timestamp: float, price: float, volume: float, 
                      side: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Process a trade, potentially emitting completed volume bars.
        
        Returns:
            List of completed bars (usually 0 or 1, but can be more for large trades)
        """
        if self.current_bar['open'] is None:
            self.current_bar['open'] = price
            self.current_bar['start_time'] = timestamp
        
        completed_bars = []
        remaining_volume = volume
        
        while remaining_volume > 0:
            space = self.volume_threshold - self.current_volume
            
            if remaining_volume >= space:
                # Complete current bar
                fill_vol = space
                self._update_bar(timestamp, price, fill_vol, side)
                
                # Finalize bar
                bar = self._finalize_bar()
                completed_bars.append(bar)
                
                # Start new bar
                self.current_bar = self._new_bar()
                self.current_bar['open'] = price
                self.current_bar['start_time'] = timestamp
                self.current_volume = 0.0
                
                remaining_volume -= fill_vol
            else:
                # Partial fill
                self._update_bar(timestamp, price, remaining_volume, side)
                remaining_volume = 0
        
        return completed_bars
    
    def _update_bar(self, timestamp: float, price: float, volume: float, 
                    side: Optional[str] = None):
        """Update current bar with trade data."""
        self.current_bar['high'] = max(self.current_bar['high'], price)
        self.current_bar['low'] = min(self.current_bar['low'], price)
        self.current_bar['close'] = price
        self.current_bar['volume'] += volume
        self.current_bar['vwap_sum'] += price * volume
        self.current_bar['trade_count'] += 1
        self.current_bar['end_time'] = timestamp
        self.current_volume += volume
        
        if side:
            if side.lower() == 'buy':
                self.current_bar['buy_volume'] += volume
            else:
                self.current_bar['sell_volume'] += volume
    
    def _finalize_bar(self) -> Dict[str, Any]:
        """Finalize current bar and compute derived stats."""
        bar = self.current_bar.copy()
        
        if bar['volume'] > 0:
            bar['vwap'] = bar['vwap_sum'] / bar['volume']
            bar['buy_ratio'] = bar['buy_volume'] / bar['volume']
        else:
            bar['vwap'] = bar['close']
            bar['buy_ratio'] = 0.5
        
        del bar['vwap_sum']  # Remove intermediate value
        
        # Calculate bar duration
        if bar['start_time'] and bar['end_time']:
            bar['duration'] = bar['end_time'] - bar['start_time']
        else:
            bar['duration'] = 0.0
        
        return bar
