"""
Signal generation framework.

Signals transform raw data into numeric arrays that strategies consume.
Each signal implements :meth:`generate` which takes a DataFrame and returns
a 1-D numpy array of the same length.

The :class:`SignalRegistry` acts as a catalogue that maps string names to
signal instances, enabling pluggable experimentation in notebooks and
production systems.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import polars as pl


# ═══════════════════════════════════════════════════════════════════
# Base
# ═══════════════════════════════════════════════════════════════════

class BaseSignal(ABC):
    """Abstract base class for all signals.

    Subclasses must implement :meth:`generate` and set :attr:`name`.
    """

    name: str = "base"
    description: str = ""

    @abstractmethod
    def generate(self, df: pl.DataFrame, prices: np.ndarray) -> np.ndarray:
        """Produce a signal array from data.

        Parameters
        ----------
        df : pl.DataFrame
            Source data (one day or concatenated days).
        prices : np.ndarray
            Pre-extracted mid-price array aligned with *df*.

        Returns
        -------
        np.ndarray
            Signal values, same length as *prices*.
        """

    def __repr__(self) -> str:
        attrs = ", ".join(
            f"{k}={v!r}"
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        )
        return f"{type(self).__name__}({attrs})"


# ═══════════════════════════════════════════════════════════════════
# Concrete signals
# ═══════════════════════════════════════════════════════════════════

class ImbalanceSignal(BaseSignal):
    """Rolling z-score of an orderbook imbalance column.

    Parameters
    ----------
    column : str
        Name of the imbalance column (e.g. ``"total_imbalance"``).
    lookback : int
        Rolling window length for mean/std calculation.
    """

    name = "imbalance_zscore"
    description = "Z-score of orderbook imbalance"

    def __init__(self, column: str = "total_imbalance", lookback: int = 600) -> None:
        self.column = column
        self.lookback = lookback

    def generate(self, df: pl.DataFrame, prices: np.ndarray) -> np.ndarray:
        values = df[self.column].to_numpy()
        n = len(values)
        z = np.zeros(n)
        for i in range(self.lookback, n):
            window = values[i - self.lookback : i]
            mean = np.nanmean(window)
            std = np.nanstd(window)
            if std > 0:
                z[i] = (values[i] - mean) / std
        return z


class ForwardReturnSignal(BaseSignal):
    """Compute forward returns over a fixed horizon.

    Useful as a *target* signal for regression models rather than an
    entry signal itself.

    Parameters
    ----------
    horizon : int
        Number of rows to look ahead.
    """

    name = "forward_return"
    description = "Forward return over N bars"

    def __init__(self, horizon: int = 60) -> None:
        self.horizon = horizon

    def generate(self, df: pl.DataFrame, prices: np.ndarray) -> np.ndarray:
        n = len(prices)
        fwd = np.zeros(n)
        h = self.horizon
        fwd[:-h] = (prices[h:] - prices[:-h]) / prices[:-h]
        return fwd


class PriceZScoreSignal(BaseSignal):
    """Rolling z-score of mid-price.

    Used for mean-reversion strategies.

    Parameters
    ----------
    lookback : int
        Window length for rolling mean/std.
    """

    name = "price_zscore"
    description = "Rolling z-score of mid-price"

    def __init__(self, lookback: int = 60) -> None:
        self.lookback = lookback

    def generate(self, df: pl.DataFrame, prices: np.ndarray) -> np.ndarray:
        n = len(prices)
        z = np.zeros(n)
        for i in range(self.lookback, n):
            window = prices[i - self.lookback : i]
            mean = np.mean(window)
            std = np.std(window)
            if std > 0:
                z[i] = (prices[i] - mean) / std
        return z


class PercentileSignal(BaseSignal):
    """Flag when a feature exceeds an extreme percentile.

    Returns +1 when the raw value ≥ upper percentile,
    -1 when ≤ lower percentile, 0 otherwise.

    Parameters
    ----------
    column : str
        Feature column name.
    upper_pct : float
        Upper percentile threshold (0-100).
    lower_pct : float | None
        Lower percentile threshold.  Defaults to ``100 - upper_pct``.
    """

    name = "percentile_extreme"
    description = "Extreme percentile flag"

    def __init__(
        self,
        column: str = "total_imbalance",
        upper_pct: float = 99.5,
        lower_pct: Optional[float] = None,
    ) -> None:
        self.column = column
        self.upper_pct = upper_pct
        self.lower_pct = lower_pct if lower_pct is not None else 100.0 - upper_pct

    def generate(self, df: pl.DataFrame, prices: np.ndarray) -> np.ndarray:
        values = df[self.column].to_numpy()
        upper_thresh = np.nanpercentile(values, self.upper_pct)
        lower_thresh = np.nanpercentile(values, self.lower_pct)
        result = np.zeros(len(values))
        result[values >= upper_thresh] = 1.0
        result[values <= lower_thresh] = -1.0
        return result


class OptimalTradeSignal(BaseSignal):
    """Hindsight-optimal buy/sell signal via peak-valley detection.

    This is a *labelling* signal used to create supervised targets,
    not a live trading signal.

    Parameters
    ----------
    min_profit_pct : float
        Minimum price move to count as trade.
    fee_pct : float
        Round-trip fee assumption for threshold calc.
    """

    name = "optimal_trade"
    description = "Hindsight-optimal buy/sell labels"

    def __init__(self, min_profit_pct: float = 0.002, fee_pct: float = 0.001) -> None:
        self.min_profit_pct = min_profit_pct
        self.fee_pct = fee_pct

    def generate(self, df: pl.DataFrame, prices: np.ndarray) -> np.ndarray:
        n = len(prices)
        signals = np.zeros(n, dtype=np.int8)
        if n < 2:
            return signals.astype(float)

        min_move = self.min_profit_pct + 2 * self.fee_pct

        looking_to_buy = True
        local_min_price = prices[0]
        local_min_idx = 0
        local_max_price = prices[0]
        local_max_idx = 0

        for i in range(1, n):
            price = prices[i]
            if looking_to_buy:
                if price < local_min_price:
                    local_min_price = price
                    local_min_idx = i
                if price > local_min_price * (1 + min_move):
                    signals[local_min_idx] = 1
                    looking_to_buy = False
                    local_max_price = price
                    local_max_idx = i
            else:
                if price > local_max_price:
                    local_max_price = price
                    local_max_idx = i
                if price < local_max_price * (1 - min_move):
                    signals[local_max_idx] = -1
                    looking_to_buy = True
                    local_min_price = price
                    local_min_idx = i

        return signals.astype(float)


# ═══════════════════════════════════════════════════════════════════
# Registry
# ═══════════════════════════════════════════════════════════════════

class SignalRegistry:
    """Named catalogue of signal instances.

    Enables selecting signals by string name in configs or notebooks.

    Examples
    --------
    >>> registry = SignalRegistry()
    >>> registry.register(ImbalanceSignal(lookback=300))
    >>> sig = registry.get("imbalance_zscore")
    >>> z = sig.generate(df, prices)
    """

    def __init__(self) -> None:
        self._signals: dict[str, BaseSignal] = {}

    def register(self, signal: BaseSignal, name: Optional[str] = None) -> None:
        key = name or signal.name
        self._signals[key] = signal

    def get(self, name: str) -> BaseSignal:
        if name not in self._signals:
            raise KeyError(
                f"Signal '{name}' not registered. "
                f"Available: {list(self._signals.keys())}"
            )
        return self._signals[name]

    def list(self) -> list[str]:
        return list(self._signals.keys())

    def __contains__(self, name: str) -> bool:
        return name in self._signals

    def __repr__(self) -> str:
        items = ", ".join(f"{k}: {v!r}" for k, v in self._signals.items())
        return f"SignalRegistry({{{items}}})"

    # ── factory: pre-populate with defaults ─────────────────────────

    @classmethod
    def default(cls) -> "SignalRegistry":
        """Return a registry pre-loaded with all built-in signals."""
        reg = cls()
        reg.register(ImbalanceSignal())
        reg.register(ForwardReturnSignal(horizon=10), "fwd_10s")
        reg.register(ForwardReturnSignal(horizon=30), "fwd_30s")
        reg.register(ForwardReturnSignal(horizon=60), "fwd_60s")
        reg.register(PriceZScoreSignal())
        reg.register(PercentileSignal())
        reg.register(OptimalTradeSignal())
        return reg
