"""
Strategy definitions.

A strategy maps signal values to trading actions (entry/exit decisions).
Each strategy implements :meth:`generate_positions` which returns a position
array: +1 = long, -1 = short, 0 = flat.

Strategies are **stateless** objects — all state lives in the position
array — making them trivially serializable and reproducible.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

import numpy as np


# ═══════════════════════════════════════════════════════════════════
# Base
# ═══════════════════════════════════════════════════════════════════

class BaseStrategy(ABC):
    """Abstract base class for all trading strategies.

    Every strategy must implement :meth:`generate_positions` which
    converts a DataFrame + signal array into entry/exit decisions.

    Attributes
    ----------
    name : str
        Human-readable strategy identifier.
    version : str
        Semantic version for tracking parameter changes.
    """

    name: str = "base"
    version: str = "0.1.0"

    @abstractmethod
    def generate_positions(
        self,
        prices: np.ndarray,
        signal: np.ndarray,
    ) -> np.ndarray:
        """Decide entry/exit for each bar.

        Parameters
        ----------
        prices : np.ndarray
            Mid-price series.
        signal : np.ndarray
            Numeric signal array (same length as *prices*).

        Returns
        -------
        np.ndarray
            Position array: +1 (long), -1 (short), 0 (flat).
            Same length as *prices*.
        """

    def params(self) -> dict:
        """Return strategy parameters as a serializable dict."""
        return {
            k: v for k, v in self.__dict__.items() if not k.startswith("_")
        }

    def __repr__(self) -> str:
        attrs = ", ".join(f"{k}={v!r}" for k, v in self.params().items())
        return f"{type(self).__name__}({attrs})"


# ═══════════════════════════════════════════════════════════════════
# Concrete strategies
# ═══════════════════════════════════════════════════════════════════

class ImbalanceStrategy(BaseStrategy):
    """Z-score threshold strategy on orderbook imbalance.

    Entry: ``|z| > entry_z``  (long if z > 0, short if z < 0)
    Exit:  ``|z| < exit_z``   or max hold reached.

    This is the **winning strategy** from notebook 02, producing
    +84.55% return (5d, 0 fee) with ρ=0.082 IC.

    Parameters
    ----------
    entry_z : float
        Z-score threshold to enter.
    exit_z : float
        Z-score threshold to exit.
    max_hold : int
        Maximum bars to hold before forced exit.
    """

    name = "imbalance_zscore"
    version = "1.0.0"

    def __init__(
        self,
        entry_z: float = 1.5,
        exit_z: float = 0.5,
        max_hold: int = 30,
    ) -> None:
        self.entry_z = entry_z
        self.exit_z = exit_z
        self.max_hold = max_hold

    def generate_positions(
        self,
        prices: np.ndarray,
        signal: np.ndarray,
    ) -> np.ndarray:
        n = len(prices)
        positions = np.zeros(n)
        position = 0
        entry_idx = 0

        for i in range(n):
            if position != 0:
                should_exit = False
                if i >= entry_idx + self.max_hold:
                    should_exit = True
                elif position == 1 and signal[i] < self.exit_z:
                    should_exit = True
                elif position == -1 and signal[i] > -self.exit_z:
                    should_exit = True

                if should_exit:
                    position = 0

            if position == 0:
                if signal[i] > self.entry_z:
                    position = 1
                    entry_idx = i
                elif signal[i] < -self.entry_z:
                    position = -1
                    entry_idx = i

            positions[i] = position

        return positions


class MeanReversionStrategy(BaseStrategy):
    """Price z-score mean reversion.

    Enter long when price z < -entry_z (oversold).
    Enter short when price z > entry_z (overbought).
    Exit when price z crosses back to exit_z.

    Parameters
    ----------
    entry_z : float
        Entry threshold (absolute z-score).
    exit_z : float
        Exit threshold.
    max_hold : int
        Maximum bars to hold.
    """

    name = "mean_reversion"
    version = "1.0.0"

    def __init__(
        self,
        entry_z: float = 2.0,
        exit_z: float = 0.5,
        max_hold: int = 300,
    ) -> None:
        self.entry_z = entry_z
        self.exit_z = exit_z
        self.max_hold = max_hold

    def generate_positions(
        self,
        prices: np.ndarray,
        signal: np.ndarray,
    ) -> np.ndarray:
        n = len(prices)
        positions = np.zeros(n)
        position = 0
        entry_idx = 0

        for i in range(n):
            if position != 0:
                should_exit = False
                if i >= entry_idx + self.max_hold:
                    should_exit = True
                elif position == 1 and signal[i] >= -self.exit_z:
                    should_exit = True
                elif position == -1 and signal[i] <= self.exit_z:
                    should_exit = True

                if should_exit:
                    position = 0

            if position == 0:
                if signal[i] < -self.entry_z:
                    position = 1  # Buy oversold
                    entry_idx = i
                elif signal[i] > self.entry_z:
                    position = -1  # Short overbought
                    entry_idx = i

            positions[i] = position

        return positions


class RegressionStrategy(BaseStrategy):
    """Trade when predicted return exceeds a threshold.

    Signal is expected to be a predicted forward return from an ML
    model.  Enter long when pred > threshold, short when < -threshold.

    Parameters
    ----------
    threshold : float
        Minimum predicted return to trigger a trade.
    hold_period : int
        Fixed holding period in bars.
    cooldown : int
        Minimum bars between trades.
    """

    name = "regression"
    version = "1.0.0"

    def __init__(
        self,
        threshold: float = 0.0005,
        hold_period: int = 60,
        cooldown: int = 30,
    ) -> None:
        self.threshold = threshold
        self.hold_period = hold_period
        self.cooldown = cooldown

    def generate_positions(
        self,
        prices: np.ndarray,
        signal: np.ndarray,
    ) -> np.ndarray:
        n = len(prices)
        positions = np.zeros(n)
        position = 0
        entry_idx = 0
        last_exit = -self.cooldown

        for i in range(n):
            # Exit after hold period
            if position != 0 and i >= entry_idx + self.hold_period:
                position = 0
                last_exit = i

            # Entry after cooldown
            if position == 0 and i >= last_exit + self.cooldown:
                if signal[i] > self.threshold:
                    position = 1
                    entry_idx = i
                elif signal[i] < -self.threshold:
                    position = -1
                    entry_idx = i

            positions[i] = position

        return positions


class DirectionStrategy(BaseStrategy):
    """Binary direction classifier strategy.

    Signal is expected to be a probability (0-1) from a direction
    classifier.  Go long above ``long_threshold``, short below
    ``short_threshold``.

    Parameters
    ----------
    long_threshold : float
        Probability above which to go long.
    short_threshold : float
        Probability below which to go short.
    hold_period : int
        Fixed bars to hold each position.
    """

    name = "direction"
    version = "1.0.0"

    def __init__(
        self,
        long_threshold: float = 0.6,
        short_threshold: float = 0.4,
        hold_period: int = 1,
    ) -> None:
        self.long_threshold = long_threshold
        self.short_threshold = short_threshold
        self.hold_period = hold_period

    def generate_positions(
        self,
        prices: np.ndarray,
        signal: np.ndarray,
    ) -> np.ndarray:
        n = len(prices)
        positions = np.zeros(n)
        position = 0
        entry_idx = 0

        for i in range(n):
            if position != 0 and i >= entry_idx + self.hold_period:
                position = 0

            if position == 0:
                if signal[i] >= self.long_threshold:
                    position = 1
                    entry_idx = i
                elif signal[i] <= self.short_threshold:
                    position = -1
                    entry_idx = i

            positions[i] = position

        return positions


class UltraSelectiveStrategy(BaseStrategy):
    """Only trade on extreme percentile signals.

    The signal is expected to be a raw feature (e.g. ``total_imbalance``).
    This strategy internally computes percentile thresholds and only
    enters positions at the extremes.

    Parameters
    ----------
    entry_percentile : float
        Upper percentile for long entry (0-100).
    hold_bars : int
        Fixed holding period.
    min_gap : int
        Minimum bars between trades.
    """

    name = "ultra_selective"
    version = "1.0.0"

    def __init__(
        self,
        entry_percentile: float = 99.5,
        hold_bars: int = 10,
        min_gap: int = 10,
    ) -> None:
        self.entry_percentile = entry_percentile
        self.hold_bars = hold_bars
        self.min_gap = min_gap

    def generate_positions(
        self,
        prices: np.ndarray,
        signal: np.ndarray,
    ) -> np.ndarray:
        long_thresh = np.nanpercentile(signal, self.entry_percentile)
        short_thresh = np.nanpercentile(signal, 100 - self.entry_percentile)

        n = len(prices)
        positions = np.zeros(n)
        position = 0
        entry_idx = -self.min_gap
        last_exit = -self.min_gap

        for i in range(n):
            if position != 0 and i >= entry_idx + self.hold_bars:
                position = 0
                last_exit = i

            if position == 0 and i >= last_exit + self.min_gap:
                if signal[i] >= long_thresh:
                    position = 1
                    entry_idx = i
                elif signal[i] <= short_thresh:
                    position = -1
                    entry_idx = i

            positions[i] = position

        return positions
