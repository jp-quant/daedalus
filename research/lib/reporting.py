"""
Portfolio & Strategy Performance Reporting Framework
====================================================

Provides comprehensive performance analytics and interactive visualization
for any trading strategy or portfolio, given standardized trade/order data.

This is the **canonical reporting module** for all Daedalus research.
Every strategy evaluation, backtest comparison, and portfolio analysis
should flow through this framework for consistency and reproducibility.

Data Model (3 core DataFrames)
------------------------------
1. ``trades_df`` — Round-trip trade records (entry + exit).
   Required columns: entry_time, exit_time, symbol, side, entry_price,
   exit_price, quantity, gross_pnl, net_pnl, fee, return_pct,
   holding_bars
   Optional: signal_entry, signal_exit, trade_id, bar_entry, bar_exit

2. ``equity_df`` — Time series of portfolio/strategy equity.
   Required columns: timestamp (or bar), equity
   Optional: cash, position_value, drawdown, drawdown_pct

3. ``positions_df`` — Per-asset position snapshots over time.
   Required columns: timestamp (or bar), symbol, quantity, market_value
   Optional: avg_entry_price, unrealized_pnl, weight

Usage
-----
    from research.lib.reporting import PerformanceReport

    # From a trades DataFrame (minimum viable input)
    report = PerformanceReport.from_trades(trades_df, equity_df)
    report.summary()               # Dict of all metrics
    report.plot_equity()           # Interactive equity curve
    report.plot_drawdown()         # Drawdown chart
    report.plot_trade_analysis()   # P&L distribution, scatter, etc.
    report.plot_returns()          # Return distribution
    report.full_dashboard()        # All-in-one dashboard

    # From raw backtest engine output
    report = PerformanceReport.from_backtest_result(result, prices, timestamps)

Architecture Notes for Future Agents
-------------------------------------
- This module is AGNOSTIC to strategy type. It takes standardized DataFrames.
- The ``PerformanceReport`` class is the main entry point.
- Visualization uses Plotly (interactive) with matplotlib fallback (static).
- Metrics are computed lazily and cached via @property.
- The module is designed to be extended: add new metrics by adding methods.
- All DataFrames use snake_case column names.
- Timestamps can be datetime or integer bar indices — the module handles both.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

import numpy as np
import pandas as pd

# ═══════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════

_BARS_PER_DAY = 86_400  # 1-second bars, 24h
_BARS_PER_YEAR = _BARS_PER_DAY * 365.25
_PLOTLY_AVAILABLE = False

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.express as px

    _PLOTLY_AVAILABLE = True
except ImportError:
    pass

# Fallback
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    _MPL_AVAILABLE = True
except ImportError:
    _MPL_AVAILABLE = False


# ═══════════════════════════════════════════════════════════════════
# Data Model — Standardized DataFrame Schemas
# ═══════════════════════════════════════════════════════════════════

# These define the CANONICAL column names. Adapters can rename columns
# to match, but all internal logic uses these names.

TRADES_REQUIRED_COLS = [
    "entry_time",  # int (bar index) or datetime
    "exit_time",  # int (bar index) or datetime
    "symbol",  # str, e.g. "HBAR-USD"
    "side",  # str, "long" or "short"
    "entry_price",  # float
    "exit_price",  # float
    "quantity",  # float (units of base asset, or 1.0 if normalized)
    "gross_pnl",  # float (absolute P&L before fees)
    "net_pnl",  # float (absolute P&L after fees)
    "fee",  # float (total fees for round-trip)
    "return_pct",  # float (net return in %, e.g. 0.05 = 5%)
    "holding_bars",  # int
]

TRADES_OPTIONAL_COLS = [
    "trade_id",  # int or str
    "signal_entry",  # float (model probability at entry)
    "signal_exit",  # float (model probability at exit)
    "bar_entry",  # int (absolute bar index)
    "bar_exit",  # int (absolute bar index)
    "date",  # str, e.g. "2026-01-15"
    "slippage",  # float (estimated slippage cost)
]

EQUITY_REQUIRED_COLS = [
    "bar",  # int (bar index) or datetime
    "equity",  # float (NAV)
]

EQUITY_OPTIONAL_COLS = [
    "timestamp",  # datetime
    "cash",  # float
    "position_value",  # float
    "drawdown",  # float (absolute)
    "drawdown_pct",  # float (percentage, negative)
    "daily_return",  # float
]


# ═══════════════════════════════════════════════════════════════════
# Metrics Computation
# ═══════════════════════════════════════════════════════════════════


def compute_drawdown(equity: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Compute drawdown series from equity curve.

    Returns
    -------
    dd_abs : np.ndarray
        Absolute drawdown (equity - peak).
    dd_pct : np.ndarray
        Percentage drawdown ((equity - peak) / peak).
    """
    peak = np.maximum.accumulate(equity)
    dd_abs = equity - peak
    with np.errstate(divide="ignore", invalid="ignore"):
        dd_pct = np.where(peak > 0, dd_abs / peak, 0.0)
    return dd_abs, dd_pct


def compute_sortino(returns: np.ndarray, target: float = 0.0) -> float:
    """Sortino ratio (downside deviation only)."""
    excess = returns - target
    downside = returns[returns < target] - target
    if len(downside) == 0 or np.std(downside) == 0:
        return 0.0
    return float(np.mean(excess) / np.std(downside))


def compute_calmar(total_return: float, max_dd_pct: float) -> float:
    """Calmar ratio = annualized return / max drawdown."""
    if max_dd_pct == 0:
        return 0.0
    return abs(total_return / max_dd_pct)


def compute_profit_factor(trade_pnls: np.ndarray) -> float:
    """Profit factor = gross profits / gross losses."""
    profits = trade_pnls[trade_pnls > 0].sum()
    losses = abs(trade_pnls[trade_pnls < 0].sum())
    if losses == 0:
        return float("inf") if profits > 0 else 0.0
    return float(profits / losses)


def compute_consecutive_runs(wins: np.ndarray) -> tuple[int, int]:
    """Compute max consecutive wins and losses."""
    if len(wins) == 0:
        return 0, 0

    max_wins = max_losses = 0
    cur_wins = cur_losses = 0

    for w in wins:
        if w:
            cur_wins += 1
            cur_losses = 0
            max_wins = max(max_wins, cur_wins)
        else:
            cur_losses += 1
            cur_wins = 0
            max_losses = max(max_losses, cur_losses)

    return max_wins, max_losses


def compute_rolling_metric(
    values: np.ndarray, window: int, metric_fn
) -> np.ndarray:
    """Compute rolling metric over a window."""
    n = len(values)
    result = np.full(n, np.nan)
    for i in range(window, n + 1):
        result[i - 1] = metric_fn(values[i - window : i])
    return result


# ═══════════════════════════════════════════════════════════════════
# PerformanceReport — The Main Class
# ═══════════════════════════════════════════════════════════════════


class PerformanceReport:
    """Comprehensive performance report for a strategy or portfolio.

    This is the primary interface for all reporting. It takes standardized
    DataFrames and computes metrics + generates visualizations.

    Parameters
    ----------
    trades_df : pd.DataFrame
        Round-trip trade records with columns per TRADES_REQUIRED_COLS.
    equity_df : pd.DataFrame
        Time series of equity with columns per EQUITY_REQUIRED_COLS.
    positions_df : pd.DataFrame, optional
        Per-asset position snapshots over time.
    meta : dict, optional
        Strategy metadata (name, parameters, etc.).
    """

    def __init__(
        self,
        trades_df: pd.DataFrame,
        equity_df: pd.DataFrame,
        positions_df: Optional[pd.DataFrame] = None,
        meta: Optional[dict] = None,
    ):
        self.trades = trades_df.copy()
        self.equity = equity_df.copy()
        self.positions = positions_df.copy() if positions_df is not None else None
        self.meta = meta or {}
        self._metrics_cache: Optional[dict] = None

        # Ensure drawdown columns exist on equity
        if "drawdown_pct" not in self.equity.columns and "equity" in self.equity.columns:
            eq = self.equity["equity"].values
            dd_abs, dd_pct = compute_drawdown(eq)
            self.equity["drawdown"] = dd_abs
            self.equity["drawdown_pct"] = dd_pct

    # ── Factory Methods ─────────────────────────────────────────

    @classmethod
    def from_trades(
        cls,
        trades_df: pd.DataFrame,
        equity_df: pd.DataFrame,
        positions_df: Optional[pd.DataFrame] = None,
        meta: Optional[dict] = None,
    ) -> "PerformanceReport":
        """Create report from pre-built DataFrames."""
        return cls(trades_df, equity_df, positions_df, meta)

    @classmethod
    def from_backtest_result(
        cls,
        result,  # BacktestResult
        prices: np.ndarray,
        symbol: str = "UNKNOWN",
        timestamps: Optional[np.ndarray] = None,
        fee_bps: float = 0.1,
        date: str = "",
        meta: Optional[dict] = None,
    ) -> "PerformanceReport":
        """Create report directly from a BacktestResult object.

        This is the bridge from the existing backtest engine to the
        reporting framework.
        """
        # Build trades_df from TradeLog objects
        trade_records = []
        for i, t in enumerate(result.trades):
            entry_p = t.entry_price
            exit_p = t.exit_price
            side = "long" if t.direction == 1 else "short"

            # Gross P&L (per unit)
            if t.direction == 1:
                gross_pnl = exit_p - entry_p
            else:
                gross_pnl = entry_p - exit_p

            # Fee = fee_bps * 2 (entry + exit) * price
            fee_per_unit = (fee_bps / 10_000) * 2 * entry_p
            net_pnl = gross_pnl - fee_per_unit

            entry_time = timestamps[t.entry_idx] if timestamps is not None else t.entry_idx
            exit_time = timestamps[t.exit_idx] if timestamps is not None else t.exit_idx

            trade_records.append(
                {
                    "trade_id": i,
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                    "bar_entry": t.entry_idx,
                    "bar_exit": t.exit_idx,
                    "symbol": symbol,
                    "side": side,
                    "entry_price": entry_p,
                    "exit_price": exit_p,
                    "quantity": 1.0,
                    "gross_pnl": t.gross_return,
                    "net_pnl": t.net_return,
                    "fee": t.gross_return - t.net_return,
                    "return_pct": t.net_return * 100,
                    "holding_bars": t.holding_period,
                    "date": date,
                }
            )

        trades_df = pd.DataFrame(trade_records) if trade_records else pd.DataFrame(
            columns=TRADES_REQUIRED_COLS + ["trade_id", "bar_entry", "bar_exit", "date"]
        )

        # Build equity_df
        eq_data = {
            "bar": np.arange(len(result.equity_curve)),
            "equity": result.equity_curve,
        }
        if timestamps is not None and len(timestamps) == len(result.equity_curve):
            eq_data["timestamp"] = timestamps
        equity_df = pd.DataFrame(eq_data)

        return cls(trades_df, equity_df, meta=meta)

    @classmethod
    def combine(
        cls,
        reports: Dict[str, "PerformanceReport"],
        strategy_name: str = "combined",
    ) -> "PerformanceReport":
        """Combine multiple single-day/single-asset reports into one.

        Useful for aggregating daily backtests across multiple days/assets.
        """
        all_trades = []
        all_equity = []
        bar_offset = 0

        for label, rpt in reports.items():
            t = rpt.trades.copy()
            t["source"] = label
            all_trades.append(t)

            e = rpt.equity.copy()
            if "bar" in e.columns:
                e["bar"] = e["bar"] + bar_offset
                bar_offset = e["bar"].max() + 1
            all_equity.append(e)

        trades_df = pd.concat(all_trades, ignore_index=True)

        # Chain equity curves (compound across days)
        if all_equity:
            equity_pieces = []
            cum_factor = 1.0
            for e in all_equity:
                scaled = e.copy()
                initial_eq = scaled["equity"].iloc[0]
                if initial_eq > 0:
                    scaled["equity"] = scaled["equity"] / initial_eq * cum_factor
                    cum_factor = scaled["equity"].iloc[-1]
                equity_pieces.append(scaled)
            equity_df = pd.concat(equity_pieces, ignore_index=True)
        else:
            equity_df = pd.DataFrame(columns=["bar", "equity"])

        return cls(trades_df, equity_df, meta={"strategy": strategy_name})

    # ── Core Metrics ────────────────────────────────────────────

    def summary(self) -> dict:
        """Compute all performance metrics. Cached after first call."""
        if self._metrics_cache is not None:
            return self._metrics_cache

        m: dict[str, Any] = {}
        eq = self.equity["equity"].values
        trades = self.trades

        # ── Return metrics ──
        if len(eq) > 0:
            m["total_return_pct"] = (eq[-1] / eq[0] - 1) * 100
            m["final_equity"] = eq[-1]
            m["initial_equity"] = eq[0]
        else:
            m["total_return_pct"] = 0.0
            m["final_equity"] = 0.0
            m["initial_equity"] = 0.0

        # ── Trade metrics ──
        m["n_trades"] = len(trades)
        if len(trades) > 0:
            net_pnls = trades["net_pnl"].values
            gross_pnls = trades["gross_pnl"].values
            returns = trades["return_pct"].values
            wins = net_pnls > 0
            holdings = trades["holding_bars"].values

            m["win_rate"] = wins.mean()
            m["avg_return_pct"] = returns.mean()
            m["median_return_pct"] = float(np.median(returns))
            m["avg_win_pct"] = float(returns[wins].mean()) if wins.any() else 0.0
            m["avg_loss_pct"] = float(returns[~wins].mean()) if (~wins).any() else 0.0
            m["best_trade_pct"] = float(returns.max())
            m["worst_trade_pct"] = float(returns.min())
            m["profit_factor"] = compute_profit_factor(net_pnls)

            # Consecutive runs
            max_consec_wins, max_consec_losses = compute_consecutive_runs(wins)
            m["max_consecutive_wins"] = max_consec_wins
            m["max_consecutive_losses"] = max_consec_losses

            # Holding period
            m["avg_holding_bars"] = float(holdings.mean())
            m["median_holding_bars"] = float(np.median(holdings))

            # Fees
            if "fee" in trades.columns:
                m["total_fees"] = float(trades["fee"].sum())
            else:
                m["total_fees"] = 0.0

            # Long/short breakdown
            longs = trades[trades["side"] == "long"]
            shorts = trades[trades["side"] == "short"]
            m["n_long_trades"] = len(longs)
            m["n_short_trades"] = len(shorts)
            m["long_win_rate"] = (
                (longs["net_pnl"] > 0).mean() if len(longs) > 0 else 0.0
            )
            m["short_win_rate"] = (
                (shorts["net_pnl"] > 0).mean() if len(shorts) > 0 else 0.0
            )

            # Return distribution
            m["return_skew"] = float(pd.Series(returns).skew())
            m["return_kurtosis"] = float(pd.Series(returns).kurtosis())
            m["return_std"] = float(returns.std())

            # Sharpe (annualized, approximate)
            if m["return_std"] > 0 and m["avg_holding_bars"] > 0:
                trades_per_year = _BARS_PER_YEAR / m["avg_holding_bars"]
                m["sharpe"] = float(
                    (m["avg_return_pct"] / m["return_std"])
                    * np.sqrt(trades_per_year)
                    / 100  # convert from pct
                )
            else:
                m["sharpe"] = 0.0

            # Sortino
            m["sortino"] = compute_sortino(returns)

        else:
            for k in [
                "win_rate", "avg_return_pct", "median_return_pct",
                "avg_win_pct", "avg_loss_pct", "best_trade_pct",
                "worst_trade_pct", "profit_factor", "max_consecutive_wins",
                "max_consecutive_losses", "avg_holding_bars",
                "median_holding_bars", "total_fees", "n_long_trades",
                "n_short_trades", "long_win_rate", "short_win_rate",
                "return_skew", "return_kurtosis", "return_std",
                "sharpe", "sortino",
            ]:
                m[k] = 0.0

        # ── Drawdown metrics ──
        if len(eq) > 0:
            _, dd_pct = compute_drawdown(eq)
            m["max_drawdown_pct"] = float(dd_pct.min() * 100)
            # Drawdown duration (bars)
            in_dd = dd_pct < 0
            if in_dd.any():
                dd_runs = np.diff(np.where(np.concatenate(([False], in_dd, [False])))[0])
                m["max_dd_duration_bars"] = int(dd_runs.max()) if len(dd_runs) > 0 else 0
            else:
                m["max_dd_duration_bars"] = 0
            m["calmar"] = compute_calmar(m["total_return_pct"], abs(m["max_drawdown_pct"]))
        else:
            m["max_drawdown_pct"] = 0.0
            m["max_dd_duration_bars"] = 0
            m["calmar"] = 0.0

        # ── Daily metrics (if date info available) ──
        if "date" in trades.columns and len(trades) > 0:
            daily = trades.groupby("date")["net_pnl"].sum()
            m["n_trading_days"] = len(daily)
            m["profitable_days"] = int((daily > 0).sum())
            m["daily_win_rate"] = m["profitable_days"] / m["n_trading_days"]
            m["avg_daily_pnl"] = float(daily.mean())
            m["best_day_pnl"] = float(daily.max())
            m["worst_day_pnl"] = float(daily.min())
        elif "date" in trades.columns:
            m["n_trading_days"] = 0
            m["profitable_days"] = 0
            m["daily_win_rate"] = 0.0

        # ── Symbol breakdown ──
        if "symbol" in trades.columns and len(trades) > 0:
            m["n_symbols"] = trades["symbol"].nunique()
            m["symbols"] = list(trades["symbol"].unique())

        m.update(self.meta)
        self._metrics_cache = m
        return m

    def summary_table(self) -> pd.DataFrame:
        """Return summary as a single-row DataFrame for display."""
        s = self.summary()
        # Select key metrics for display
        display_keys = [
            "total_return_pct", "n_trades", "win_rate", "sharpe", "sortino",
            "max_drawdown_pct", "calmar", "profit_factor", "avg_return_pct",
            "avg_holding_bars", "total_fees", "return_skew",
        ]
        display = {k: s.get(k, None) for k in display_keys}
        return pd.DataFrame([display])

    def print_summary(self) -> None:
        """Pretty-print the full summary to stdout."""
        s = self.summary()
        name = s.get("strategy", "Strategy")
        print(f"\n{'═' * 70}")
        print(f"  PERFORMANCE REPORT: {name}")
        print(f"{'═' * 70}")

        sections = [
            ("Returns", [
                ("Total Return", f"{s['total_return_pct']:+,.2f}%"),
                ("Sharpe Ratio", f"{s['sharpe']:.2f}"),
                ("Sortino Ratio", f"{s['sortino']:.2f}"),
                ("Calmar Ratio", f"{s['calmar']:.2f}"),
                ("Max Drawdown", f"{s['max_drawdown_pct']:.2f}%"),
                ("Max DD Duration", f"{s['max_dd_duration_bars']:,} bars"),
            ]),
            ("Trades", [
                ("Total Trades", f"{s['n_trades']:,}"),
                ("Win Rate", f"{s['win_rate']:.1%}"),
                ("Profit Factor", f"{s['profit_factor']:.2f}"),
                ("Avg Return/Trade", f"{s['avg_return_pct']:.4f}%"),
                ("Best Trade", f"{s['best_trade_pct']:+.4f}%"),
                ("Worst Trade", f"{s['worst_trade_pct']:+.4f}%"),
                ("Max Consec. Wins", f"{s['max_consecutive_wins']}"),
                ("Max Consec. Losses", f"{s['max_consecutive_losses']}"),
            ]),
            ("Holdings", [
                ("Avg Hold (bars)", f"{s['avg_holding_bars']:.1f}"),
                ("Median Hold (bars)", f"{s['median_holding_bars']:.1f}"),
                ("Long Trades", f"{s['n_long_trades']:,} ({s['long_win_rate']:.1%} WR)"),
                ("Short Trades", f"{s['n_short_trades']:,} ({s['short_win_rate']:.1%} WR)"),
            ]),
            ("Costs", [
                ("Total Fees (frac)", f"{s['total_fees']:.6f}"),
            ]),
            ("Distribution", [
                ("Std Dev", f"{s['return_std']:.4f}%"),
                ("Skewness", f"{s['return_skew']:.2f}"),
                ("Kurtosis", f"{s['return_kurtosis']:.2f}"),
            ]),
        ]

        # Daily metrics if available
        if "n_trading_days" in s:
            sections.append(("Daily", [
                ("Trading Days", f"{s['n_trading_days']}"),
                ("Profitable Days", f"{s['profitable_days']}/{s['n_trading_days']}"),
                ("Daily Win Rate", f"{s['daily_win_rate']:.1%}"),
            ]))

        for section_name, items in sections:
            print(f"\n  ── {section_name} ──")
            for label, value in items:
                print(f"    {label:.<30s} {value}")

        print(f"\n{'═' * 70}\n")

    # ── Visualization: Plotly (Interactive) ─────────────────────

    def plot_equity(
        self,
        title: str = "Equity Curve",
        show_drawdown: bool = True,
        height: int = 500,
    ):
        """Interactive equity curve with optional drawdown overlay."""
        if not _PLOTLY_AVAILABLE:
            return self._plot_equity_mpl(title, show_drawdown)

        eq = self.equity
        x = eq["timestamp"] if "timestamp" in eq.columns else eq["bar"]

        if show_drawdown:
            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=[0.7, 0.3],
                subplot_titles=[title, "Drawdown (%)"],
            )
            fig.add_trace(
                go.Scatter(x=x, y=eq["equity"], name="Equity",
                           line=dict(color="#2196F3", width=1.5)),
                row=1, col=1,
            )
            fig.add_trace(
                go.Scatter(x=x, y=eq["drawdown_pct"] * 100,
                           name="Drawdown %",
                           fill="tozeroy",
                           line=dict(color="#F44336", width=1)),
                row=2, col=1,
            )
            fig.update_yaxes(title_text="Equity ($)", row=1, col=1)
            fig.update_yaxes(title_text="Drawdown %", row=2, col=1)
        else:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(x=x, y=eq["equity"], name="Equity",
                           line=dict(color="#2196F3", width=1.5))
            )
            fig.update_yaxes(title_text="Equity ($)")

        fig.update_layout(
            height=height, template="plotly_white",
            showlegend=True, hovermode="x unified",
        )
        fig.show()
        return fig

    def plot_trades(
        self,
        title: str = "Trade Analysis",
        height: int = 600,
    ):
        """Interactive trade analysis: P&L scatter + distribution."""
        if not _PLOTLY_AVAILABLE:
            return self._plot_trades_mpl(title)

        trades = self.trades
        if len(trades) == 0:
            print("No trades to plot.")
            return None

        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                "Trade P&L Over Time", "Return Distribution",
                "P&L by Holding Period", "Cumulative P&L",
            ],
            vertical_spacing=0.12, horizontal_spacing=0.1,
        )

        # 1) P&L scatter over time
        x_time = trades["bar_entry"] if "bar_entry" in trades.columns else trades["entry_time"]
        colors = ["#4CAF50" if p > 0 else "#F44336" for p in trades["net_pnl"]]
        fig.add_trace(
            go.Scatter(
                x=x_time, y=trades["return_pct"],
                mode="markers", marker=dict(color=colors, size=3, opacity=0.6),
                name="Trade Return",
            ),
            row=1, col=1,
        )

        # 2) Return distribution
        fig.add_trace(
            go.Histogram(
                x=trades["return_pct"], nbinsx=100,
                marker_color="#2196F3", opacity=0.7, name="Return Dist",
            ),
            row=1, col=2,
        )

        # 3) P&L vs holding period
        fig.add_trace(
            go.Scatter(
                x=trades["holding_bars"], y=trades["return_pct"],
                mode="markers", marker=dict(color=colors, size=3, opacity=0.5),
                name="Hold vs Return",
            ),
            row=2, col=1,
        )

        # 4) Cumulative P&L
        cum_pnl = trades["net_pnl"].cumsum()
        fig.add_trace(
            go.Scatter(
                x=np.arange(len(cum_pnl)), y=cum_pnl,
                line=dict(color="#2196F3", width=1.5), name="Cumulative P&L",
            ),
            row=2, col=2,
        )

        fig.update_layout(
            height=height, title_text=title,
            template="plotly_white", showlegend=False,
        )
        fig.show()
        return fig

    def plot_returns(
        self,
        title: str = "Return Analysis",
        height: int = 500,
    ):
        """Interactive return analysis: rolling metrics, QQ-like."""
        if not _PLOTLY_AVAILABLE:
            return self._plot_returns_mpl(title)

        trades = self.trades
        if len(trades) == 0:
            return None

        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=["Rolling Win Rate (50-trade window)", "Rolling Avg Return"],
        )

        returns = trades["return_pct"].values
        wins = (trades["net_pnl"] > 0).astype(float).values
        window = min(50, len(trades) // 3) if len(trades) > 10 else len(trades)
        if window > 0:
            rolling_wr = compute_rolling_metric(wins, window, np.mean)
            rolling_ret = compute_rolling_metric(returns, window, np.mean)
            x_idx = np.arange(len(returns))

            fig.add_trace(
                go.Scatter(x=x_idx, y=rolling_wr * 100, name="Win Rate %",
                           line=dict(color="#4CAF50", width=1.5)),
                row=1, col=1,
            )
            fig.add_trace(
                go.Scatter(x=x_idx, y=rolling_ret, name="Avg Return %",
                           line=dict(color="#2196F3", width=1.5)),
                row=1, col=2,
            )

        fig.update_layout(
            height=height, template="plotly_white", showlegend=False,
        )
        fig.show()
        return fig

    def plot_positions(
        self,
        title: str = "Position Analysis",
        height: int = 500,
    ):
        """Position size and exposure over time."""
        if self.positions is None or len(self.positions) == 0:
            print("No position data available.")
            return None

        if not _PLOTLY_AVAILABLE:
            return None

        pos = self.positions
        symbols = pos["symbol"].unique()

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            subplot_titles=["Position Value by Asset", "Portfolio Exposure"],
            vertical_spacing=0.08,
        )

        x_col = "timestamp" if "timestamp" in pos.columns else "bar"

        for sym in symbols:
            mask = pos["symbol"] == sym
            fig.add_trace(
                go.Scatter(
                    x=pos.loc[mask, x_col], y=pos.loc[mask, "market_value"],
                    name=sym.replace("-USD", ""),
                    stackgroup="one",
                ),
                row=1, col=1,
            )

        # Total exposure
        total = pos.groupby(x_col)["market_value"].sum().reset_index()
        fig.add_trace(
            go.Scatter(
                x=total[x_col], y=total["market_value"],
                name="Total Exposure",
                line=dict(color="#FF9800", width=2),
            ),
            row=2, col=1,
        )

        fig.update_layout(
            height=height, title_text=title,
            template="plotly_white",
        )
        fig.show()
        return fig

    def plot_daily_returns(
        self,
        title: str = "Daily Returns",
        height: int = 400,
    ):
        """Bar chart of daily P&L."""
        if "date" not in self.trades.columns or len(self.trades) == 0:
            print("No daily data available.")
            return None

        daily = self.trades.groupby("date")["net_pnl"].sum().reset_index()
        daily["color"] = daily["net_pnl"].apply(
            lambda x: "#4CAF50" if x > 0 else "#F44336"
        )

        if not _PLOTLY_AVAILABLE:
            return None

        fig = go.Figure(
            go.Bar(
                x=daily["date"], y=daily["net_pnl"],
                marker_color=daily["color"],
                name="Daily P&L",
            )
        )
        fig.update_layout(
            height=height, title=title,
            template="plotly_white",
            xaxis_title="Date", yaxis_title="Net P&L (frac)",
        )
        fig.show()
        return fig

    def plot_fee_analysis(
        self,
        title: str = "Fee & Cost Analysis",
        height: int = 400,
    ):
        """Cumulative fees over time."""
        if "fee" not in self.trades.columns or len(self.trades) == 0:
            return None

        if not _PLOTLY_AVAILABLE:
            return None

        cum_fees = self.trades["fee"].cumsum()
        cum_gross = self.trades["gross_pnl"].cumsum()
        cum_net = self.trades["net_pnl"].cumsum()

        fig = go.Figure()
        fig.add_trace(go.Scatter(y=cum_gross, name="Gross P&L",
                                  line=dict(color="#4CAF50")))
        fig.add_trace(go.Scatter(y=cum_net, name="Net P&L",
                                  line=dict(color="#2196F3")))
        fig.add_trace(go.Scatter(y=cum_fees, name="Cumulative Fees",
                                  line=dict(color="#F44336", dash="dot")))
        fig.update_layout(
            height=height, title=title,
            template="plotly_white",
            xaxis_title="Trade #", yaxis_title="Cumulative Value",
        )
        fig.show()
        return fig

    def full_dashboard(
        self,
        title: str = "Strategy Performance Dashboard",
        height: int = 1200,
    ):
        """Generate a comprehensive all-in-one dashboard.

        Returns a dict of all generated figures.
        """
        self.print_summary()

        figs = {}
        figs["equity"] = self.plot_equity(
            title=f"{title} — Equity Curve", height=500
        )
        figs["trades"] = self.plot_trades(
            title=f"{title} — Trade Analysis", height=600
        )
        figs["returns"] = self.plot_returns(
            title=f"{title} — Return Analysis", height=400
        )
        figs["daily"] = self.plot_daily_returns(
            title=f"{title} — Daily Returns", height=350
        )
        figs["fees"] = self.plot_fee_analysis(
            title=f"{title} — Fee Analysis", height=350
        )
        if self.positions is not None:
            figs["positions"] = self.plot_positions(
                title=f"{title} — Positions", height=500
            )

        return figs

    # ── Matplotlib Fallbacks ────────────────────────────────────

    def _plot_equity_mpl(self, title, show_drawdown):
        """Matplotlib fallback for equity curve."""
        if not _MPL_AVAILABLE:
            print("Neither Plotly nor Matplotlib available.")
            return None

        eq = self.equity
        x = eq["bar"].values

        if show_drawdown:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8),
                                            gridspec_kw={"height_ratios": [3, 1]},
                                            sharex=True)
            ax1.plot(x, eq["equity"], color="#2196F3", linewidth=1)
            ax1.set_ylabel("Equity")
            ax1.set_title(title)
            ax1.grid(True, alpha=0.3)

            ax2.fill_between(x, eq["drawdown_pct"] * 100, 0,
                             color="#F44336", alpha=0.4)
            ax2.set_ylabel("Drawdown %")
            ax2.set_xlabel("Bar")
            ax2.grid(True, alpha=0.3)
        else:
            fig, ax1 = plt.subplots(figsize=(14, 5))
            ax1.plot(x, eq["equity"], color="#2196F3", linewidth=1)
            ax1.set_title(title)
            ax1.set_ylabel("Equity")
            ax1.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.show()
        return fig

    def _plot_trades_mpl(self, title):
        """Matplotlib fallback for trade analysis."""
        if not _MPL_AVAILABLE:
            return None

        trades = self.trades
        if len(trades) == 0:
            return None

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        # P&L scatter
        colors = ["g" if p > 0 else "r" for p in trades["net_pnl"]]
        x_time = trades["bar_entry"] if "bar_entry" in trades.columns else range(len(trades))
        axes[0, 0].scatter(x_time, trades["return_pct"], c=colors, s=2, alpha=0.5)
        axes[0, 0].set_title("Trade Returns Over Time")
        axes[0, 0].axhline(y=0, color="k", linewidth=0.5)

        # Distribution
        axes[0, 1].hist(trades["return_pct"], bins=100, color="#2196F3", alpha=0.7)
        axes[0, 1].set_title("Return Distribution")
        axes[0, 1].axvline(x=0, color="k", linewidth=0.5)

        # Hold vs return
        axes[1, 0].scatter(trades["holding_bars"], trades["return_pct"],
                           c=colors, s=2, alpha=0.5)
        axes[1, 0].set_title("Holding Period vs Return")

        # Cumulative P&L
        axes[1, 1].plot(trades["net_pnl"].cumsum(), color="#2196F3")
        axes[1, 1].set_title("Cumulative P&L")

        plt.suptitle(title, fontsize=14)
        plt.tight_layout()
        plt.show()
        return fig

    def _plot_returns_mpl(self, title):
        """Matplotlib fallback for return analysis."""
        if not _MPL_AVAILABLE:
            return None

        trades = self.trades
        if len(trades) == 0:
            return None

        returns = trades["return_pct"].values
        wins = (trades["net_pnl"] > 0).astype(float).values
        window = min(50, len(trades) // 3) if len(trades) > 10 else len(trades)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

        if window > 0:
            rolling_wr = compute_rolling_metric(wins, window, np.mean)
            rolling_ret = compute_rolling_metric(returns, window, np.mean)

            ax1.plot(rolling_wr * 100, color="#4CAF50")
            ax1.set_title(f"Rolling Win Rate ({window}-trade)")
            ax1.set_ylabel("Win Rate %")
            ax1.axhline(y=50, color="k", linewidth=0.5, linestyle="--")

            ax2.plot(rolling_ret, color="#2196F3")
            ax2.set_title(f"Rolling Avg Return ({window}-trade)")
            ax2.set_ylabel("Return %")
            ax2.axhline(y=0, color="k", linewidth=0.5, linestyle="--")

        plt.suptitle(title, fontsize=14)
        plt.tight_layout()
        plt.show()
        return fig

    # ── Export ──────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Export report data as a dict of DataFrames + metrics."""
        return {
            "metrics": self.summary(),
            "trades": self.trades,
            "equity": self.equity,
            "positions": self.positions,
        }

    def save(self, path: str) -> None:
        """Save report data to disk (CSV + JSON)."""
        import json
        from pathlib import Path

        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)

        # Metrics
        with open(p / "metrics.json", "w") as f:
            # Filter out non-serializable values
            metrics = {
                k: v for k, v in self.summary().items()
                if isinstance(v, (int, float, str, bool, list))
            }
            json.dump(metrics, f, indent=2, default=str)

        # DataFrames
        self.trades.to_csv(p / "trades.csv", index=False)
        self.equity.to_csv(p / "equity.csv", index=False)
        if self.positions is not None:
            self.positions.to_csv(p / "positions.csv", index=False)

        print(f"Report saved to {p}/")


# ═══════════════════════════════════════════════════════════════════
# Multi-Strategy Comparison
# ═══════════════════════════════════════════════════════════════════


class StrategyComparison:
    """Compare multiple strategies/portfolios side by side.

    Usage:
        comp = StrategyComparison()
        comp.add("Strategy A", report_a)
        comp.add("Strategy B", report_b)
        comp.comparison_table()
        comp.plot_equity_comparison()
    """

    def __init__(self):
        self._reports: dict[str, PerformanceReport] = {}

    def add(self, name: str, report: PerformanceReport) -> None:
        """Register a strategy report."""
        self._reports[name] = report

    def comparison_table(self) -> pd.DataFrame:
        """Side-by-side metric comparison."""
        rows = []
        for name, rpt in self._reports.items():
            s = rpt.summary()
            rows.append({
                "Strategy": name,
                "Return %": f"{s['total_return_pct']:+,.1f}%",
                "Trades": s["n_trades"],
                "Win Rate": f"{s['win_rate']:.1%}",
                "Sharpe": f"{s['sharpe']:.2f}",
                "Sortino": f"{s['sortino']:.2f}",
                "Max DD %": f"{s['max_drawdown_pct']:.2f}%",
                "Profit Factor": f"{s['profit_factor']:.2f}",
                "Avg Hold": f"{s['avg_holding_bars']:.0f}",
            })
        return pd.DataFrame(rows).set_index("Strategy")

    def plot_equity_comparison(
        self, title: str = "Strategy Comparison — Equity Curves", height: int = 500,
    ):
        """Overlay equity curves from all registered strategies."""
        if not _PLOTLY_AVAILABLE:
            return self._plot_equity_comparison_mpl(title)

        fig = go.Figure()
        colors = px.colors.qualitative.Set2
        for i, (name, rpt) in enumerate(self._reports.items()):
            eq = rpt.equity
            x = eq["timestamp"] if "timestamp" in eq.columns else eq["bar"]
            # Normalize to starting equity = 1
            norm_eq = eq["equity"] / eq["equity"].iloc[0]
            fig.add_trace(
                go.Scatter(x=x, y=norm_eq, name=name,
                           line=dict(color=colors[i % len(colors)], width=2))
            )

        fig.update_layout(
            height=height, title=title,
            template="plotly_white",
            yaxis_title="Normalized Equity",
            hovermode="x unified",
        )
        fig.show()
        return fig

    def _plot_equity_comparison_mpl(self, title):
        if not _MPL_AVAILABLE:
            return None
        fig, ax = plt.subplots(figsize=(14, 6))
        for name, rpt in self._reports.items():
            eq = rpt.equity
            norm = eq["equity"] / eq["equity"].iloc[0]
            ax.plot(eq["bar"], norm, label=name, linewidth=1.5)
        ax.set_title(title)
        ax.set_ylabel("Normalized Equity")
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
        return fig
