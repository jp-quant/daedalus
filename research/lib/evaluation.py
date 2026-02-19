"""
Performance evaluation utilities.

Provides higher-level analysis on :class:`BacktestResult` objects:
correlation analysis, fee sensitivity tables, multi-day aggregation,
and formatted reporting.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd
import polars as pl

from research.lib.backtest import BacktestResult


class PerformanceAnalyzer:
    """Analyze and compare backtest results.

    Examples
    --------
    >>> analyzer = PerformanceAnalyzer()
    >>> analyzer.add("imbalance_z1.5", result1)
    >>> analyzer.add("imbalance_z2.0", result2)
    >>> print(analyzer.comparison_table())
    """

    def __init__(self) -> None:
        self._results: dict[str, BacktestResult] = {}

    def add(self, label: str, result: BacktestResult) -> None:
        """Register a backtest result under a label."""
        self._results[label] = result

    def comparison_table(self) -> pd.DataFrame:
        """Build a side-by-side comparison of all registered results."""
        rows = []
        for label, res in self._results.items():
            row = {"strategy": label}
            row.update(res.summary())
            rows.append(row)
        return pd.DataFrame(rows).set_index("strategy")

    @staticmethod
    def correlation_matrix(
        df: pl.DataFrame,
        feature_cols: list[str],
        prices: np.ndarray,
        horizons: Sequence[int] = (10, 30, 60, 120, 300),
    ) -> pd.DataFrame:
        """Compute correlations between features and forward returns.

        Parameters
        ----------
        df : pl.DataFrame
            Source data.
        feature_cols : list[str]
            Columns to correlate.
        prices : np.ndarray
            Price array.
        horizons : sequence of int
            Forward-return horizons in bars.

        Returns
        -------
        pd.DataFrame
            Rows = features, columns = horizons, values = correlation.
        """
        records: list[dict] = []
        for col in feature_cols:
            if col not in df.columns:
                continue
            values = df[col].to_numpy()
            row: dict = {"feature": col}
            for h in horizons:
                fwd = np.zeros(len(prices))
                fwd[:-h] = (prices[h:] - prices[:-h]) / prices[:-h]
                mask = ~(np.isnan(values) | np.isnan(fwd))
                if mask.sum() > 100:
                    corr = np.corrcoef(values[mask], fwd[mask])[0, 1]
                else:
                    corr = np.nan
                row[f"{h}s"] = corr
            records.append(row)
        return pd.DataFrame(records).set_index("feature")

    @staticmethod
    def fee_sensitivity_table(
        result_at_fees: dict[float, BacktestResult],
    ) -> pd.DataFrame:
        """Build a fee sensitivity table from multiple runs.

        Parameters
        ----------
        result_at_fees : dict[float, BacktestResult]
            Mapping of ``fee_bps -> BacktestResult``.

        Returns
        -------
        pd.DataFrame
        """
        rows = []
        for fee_bps, res in sorted(result_at_fees.items()):
            row = {"fee_bps": fee_bps}
            row.update(res.summary())
            rows.append(row)
        return pd.DataFrame(rows)

    @staticmethod
    def aggregate_daily(
        daily_results: dict[str, BacktestResult],
    ) -> dict:
        """Aggregate daily backtest results into a summary.

        Parameters
        ----------
        daily_results : dict[str, BacktestResult]
            Mapping of ``"YYYY-MM-DD" -> BacktestResult``.

        Returns
        -------
        dict
            Aggregated metrics.
        """
        if not daily_results:
            return {}

        total_return = 1.0
        total_trades = 0
        all_trade_returns = []

        for date, res in daily_results.items():
            total_return *= 1 + res.total_return_pct / 100
            total_trades += res.n_trades
            all_trade_returns.extend(t.net_return for t in res.trades)

        return {
            "total_return_pct": round((total_return - 1) * 100, 4),
            "total_trades": total_trades,
            "avg_win_rate": round(
                np.mean([r.win_rate for r in daily_results.values()]), 4
            ),
            "avg_trade_return_pct": round(
                float(np.mean(all_trade_returns) * 100) if all_trade_returns else 0, 6
            ),
            "days": len(daily_results),
        }
