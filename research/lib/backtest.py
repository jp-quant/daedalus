"""
Backtesting engine.

Takes a price array and a position array, simulates PnL accounting
with realistic fee handling.  Produces a :class:`BacktestResult`
with equity curve, trade log, and summary metrics.

Design choices:
- Vectorized where possible, loop for position transitions.
- No look-ahead — the position array is consumed bar-by-bar.
- Fee is charged on both entry and exit.
- Supports long and short positions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Sequence

import numpy as np
import pandas as pd
import polars as pl

from research.lib.strategies import BaseStrategy
from research.lib.signals import BaseSignal


# ═══════════════════════════════════════════════════════════════════
# Data containers
# ═══════════════════════════════════════════════════════════════════

@dataclass
class TradeLog:
    """Individual trade record."""

    entry_idx: int
    exit_idx: int
    entry_price: float
    exit_price: float
    direction: int  # +1 long, -1 short
    gross_return: float
    net_return: float
    holding_period: int

    @property
    def is_winner(self) -> bool:
        return self.net_return > 0


@dataclass
class BacktestResult:
    """Complete backtest output.

    Attributes
    ----------
    equity_curve : np.ndarray
        Mark-to-market equity at each bar.
    trades : list[TradeLog]
        Detailed trade records.
    params : dict
        Strategy + engine parameters used.
    """

    equity_curve: np.ndarray
    trades: list[TradeLog]
    params: dict = field(default_factory=dict)

    # ── derived metrics (computed on demand) ────────────────────────

    @property
    def total_return_pct(self) -> float:
        if len(self.equity_curve) == 0:
            return 0.0
        return (self.equity_curve[-1] / self.equity_curve[0] - 1) * 100

    @property
    def n_trades(self) -> int:
        return len(self.trades)

    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        wins = sum(1 for t in self.trades if t.is_winner)
        return wins / len(self.trades)

    @property
    def avg_trade_return_pct(self) -> float:
        if not self.trades:
            return 0.0
        return float(np.mean([t.net_return for t in self.trades]) * 100)

    @property
    def sharpe(self) -> float:
        """Annualized Sharpe ratio (assumes 1-second bars, 24/7)."""
        if len(self.trades) < 2:
            return 0.0
        rets = [t.net_return for t in self.trades]
        mu = np.mean(rets)
        sigma = np.std(rets)
        if sigma == 0:
            return 0.0
        # Scale: seconds_per_year ≈ 365.25 * 86400
        bars_per_year = 365.25 * 86400
        avg_hold = np.mean([t.holding_period for t in self.trades])
        trades_per_year = bars_per_year / max(avg_hold, 1)
        return float(mu / sigma * np.sqrt(trades_per_year))

    @property
    def max_drawdown_pct(self) -> float:
        eq = self.equity_curve
        peak = np.maximum.accumulate(eq)
        dd = (eq - peak) / peak
        return float(np.min(dd) * 100)

    def summary(self) -> dict:
        """Return a flat dict of key performance metrics."""
        return {
            "total_return_pct": round(self.total_return_pct, 4),
            "n_trades": self.n_trades,
            "win_rate": round(self.win_rate, 4),
            "avg_trade_return_pct": round(self.avg_trade_return_pct, 6),
            "sharpe": round(self.sharpe, 2),
            "max_drawdown_pct": round(self.max_drawdown_pct, 4),
            "final_equity": round(float(self.equity_curve[-1]), 4)
            if len(self.equity_curve) > 0
            else 0,
        }

    def to_dataframe(self) -> pd.DataFrame:
        """Convert trade log to a Pandas DataFrame."""
        if not self.trades:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "entry_idx": [t.entry_idx for t in self.trades],
                "exit_idx": [t.exit_idx for t in self.trades],
                "entry_price": [t.entry_price for t in self.trades],
                "exit_price": [t.exit_price for t in self.trades],
                "direction": [t.direction for t in self.trades],
                "gross_return": [t.gross_return for t in self.trades],
                "net_return": [t.net_return for t in self.trades],
                "holding_period": [t.holding_period for t in self.trades],
            }
        )


# ═══════════════════════════════════════════════════════════════════
# Engine
# ═══════════════════════════════════════════════════════════════════

class BacktestEngine:
    """Event-driven backtesting engine.

    Processes a position array against a price array, tracking PnL
    with configurable fee and initial capital.

    Parameters
    ----------
    fee_pct : float
        Fee as decimal fraction per trade side.
        E.g. 0.0001 = 1 bps. Applied on both entry and exit.
    initial_capital : float
        Starting equity.
    """

    def __init__(
        self,
        fee_pct: float = 0.0001,
        initial_capital: float = 10_000.0,
    ) -> None:
        self.fee_pct = fee_pct
        self.initial_capital = initial_capital

    def run(
        self,
        prices: np.ndarray,
        positions: np.ndarray,
        *,
        strategy_params: Optional[dict] = None,
    ) -> BacktestResult:
        """Execute backtest from pre-computed position array.

        Parameters
        ----------
        prices : np.ndarray
            Mid-price series.
        positions : np.ndarray
            Position array (+1 long, -1 short, 0 flat).
        strategy_params : dict | None
            Metadata to store in result.

        Returns
        -------
        BacktestResult
        """
        n = len(prices)
        equity = self.initial_capital
        equity_curve = np.full(n, equity)
        trades: list[TradeLog] = []

        current_pos = 0
        entry_price = 0.0
        entry_idx = 0

        for i in range(n):
            target_pos = int(positions[i])

            # Position transition
            if current_pos != target_pos:
                # Close existing position
                if current_pos != 0:
                    exit_price = prices[i]
                    if current_pos == 1:
                        gross = exit_price / entry_price - 1
                    else:
                        gross = entry_price / exit_price - 1
                    net = gross - self.fee_pct  # exit fee
                    equity *= 1 + net
                    trades.append(
                        TradeLog(
                            entry_idx=entry_idx,
                            exit_idx=i,
                            entry_price=entry_price,
                            exit_price=exit_price,
                            direction=current_pos,
                            gross_return=gross,
                            net_return=net,
                            holding_period=i - entry_idx,
                        )
                    )

                # Open new position
                if target_pos != 0:
                    entry_price = prices[i]
                    entry_idx = i
                    equity *= 1 - self.fee_pct  # entry fee

                current_pos = target_pos

            equity_curve[i] = equity

        # Close any open position at end
        if current_pos != 0:
            exit_price = prices[-1]
            if current_pos == 1:
                gross = exit_price / entry_price - 1
            else:
                gross = entry_price / exit_price - 1
            net = gross - self.fee_pct
            equity *= 1 + net
            equity_curve[-1] = equity
            trades.append(
                TradeLog(
                    entry_idx=entry_idx,
                    exit_idx=n - 1,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    direction=current_pos,
                    gross_return=gross,
                    net_return=net,
                    holding_period=n - 1 - entry_idx,
                )
            )

        params = {
            "fee_pct": self.fee_pct,
            "initial_capital": self.initial_capital,
        }
        if strategy_params:
            params.update(strategy_params)

        return BacktestResult(
            equity_curve=equity_curve,
            trades=trades,
            params=params,
        )

    def run_strategy(
        self,
        prices: np.ndarray,
        signal: np.ndarray,
        strategy: BaseStrategy,
    ) -> BacktestResult:
        """Convenience: generate positions then backtest.

        Parameters
        ----------
        prices : np.ndarray
            Mid-price series.
        signal : np.ndarray
            Signal array to feed into the strategy.
        strategy : BaseStrategy
            Strategy instance.

        Returns
        -------
        BacktestResult
        """
        positions = strategy.generate_positions(prices, signal)
        return self.run(
            prices, positions, strategy_params=strategy.params()
        )

    def sweep_fees(
        self,
        prices: np.ndarray,
        positions: np.ndarray,
        fee_levels_bps: Sequence[float] = (0, 0.5, 1, 2, 3, 5, 10),
    ) -> pd.DataFrame:
        """Run backtest at multiple fee levels for sensitivity analysis.

        Parameters
        ----------
        prices : np.ndarray
        positions : np.ndarray
        fee_levels_bps : sequence of float
            Fee levels in basis points.

        Returns
        -------
        pd.DataFrame
            One row per fee level with summary metrics.
        """
        rows = []
        original_fee = self.fee_pct
        for bps in fee_levels_bps:
            self.fee_pct = bps / 10_000
            result = self.run(prices, positions)
            row = {"fee_bps": bps}
            row.update(result.summary())
            rows.append(row)
        self.fee_pct = original_fee
        return pd.DataFrame(rows)

    def sweep_parameter(
        self,
        prices: np.ndarray,
        signal: np.ndarray,
        strategy: BaseStrategy,
        param_name: str,
        param_values: Sequence,
    ) -> pd.DataFrame:
        """Sweep a single strategy parameter.

        Parameters
        ----------
        prices : np.ndarray
        signal : np.ndarray
        strategy : BaseStrategy
            Will be mutated by setting ``param_name`` to each value.
        param_name : str
            Strategy attribute to sweep.
        param_values : sequence
            Values to try.

        Returns
        -------
        pd.DataFrame
            One row per parameter value with summary metrics.
        """
        rows = []
        for val in param_values:
            setattr(strategy, param_name, val)
            result = self.run_strategy(prices, signal, strategy)
            row = {param_name: val}
            row.update(result.summary())
            rows.append(row)
        return pd.DataFrame(rows)
