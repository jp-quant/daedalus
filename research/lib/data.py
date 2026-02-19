"""
Data loading utilities for research.

Provides memory-efficient lazy loading of partitioned Parquet data
using Polars with Hive partitioning, following the same patterns
used in the main pipeline.
"""

from __future__ import annotations

import gc
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import polars as pl


class DataLoader:
    """Memory-efficient data loader for partitioned orderbook data.

    Wraps Polars lazy scan with Hive partition filtering so each
    day of data is loaded independently without pulling the full dataset
    into memory.

    Parameters
    ----------
    data_root : str | Path
        Root directory of the partitioned Parquet data.
        e.g. ``data/processed/silver/orderbook``
    exchange : str
        Exchange identifier. Default ``"coinbaseadvanced"``.
    symbol : str
        Trading pair. Default ``"BTC-USD"``.
    price_col : str | None
        Name of the mid-price column.  Auto-detected if ``None``.
    downsample : int
        Keep every *n*-th row to reduce memory.  ``1`` = full resolution.
    """

    # Ordered list of candidate price column names.
    _PRICE_CANDIDATES = ("mid_price", "mid", "microprice", "price")

    def __init__(
        self,
        data_root: str | Path,
        exchange: str = "coinbaseadvanced",
        symbol: str = "BTC-USD",
        price_col: Optional[str] = None,
        downsample: int = 1,
    ) -> None:
        self.data_root = Path(data_root)
        self.exchange = exchange
        self.symbol = symbol
        self._price_col = price_col
        self.downsample = max(1, downsample)
        self._schema: Optional[list[str]] = None

    # ── public API ──────────────────────────────────────────────────

    def load_day(
        self,
        year: int,
        month: int,
        day: int,
        *,
        columns: Optional[list[str]] = None,
        downsample: Optional[int] = None,
    ) -> pl.DataFrame:
        """Load a single day of data with optional column selection.

        Parameters
        ----------
        year, month, day : int
            Date partition keys.
        columns : list[str] | None
            Subset of columns to select.  ``None`` selects all.
        downsample : int | None
            Override instance-level downsampling for this call.

        Returns
        -------
        pl.DataFrame
            Sorted by ``timestamp``.
        """
        lf = pl.scan_parquet(
            str(self.data_root / "**/*.parquet"),
            hive_partitioning=True,
        )
        lf = lf.filter(
            (pl.col("exchange") == self.exchange)
            & (pl.col("symbol") == self.symbol)
            & (pl.col("year") == year)
            & (pl.col("month") == month)
            & (pl.col("day") == day)
        )
        if columns is not None:
            # Always include timestamp for sorting
            cols = list(dict.fromkeys(["timestamp"] + columns))
            lf = lf.select(cols)

        df = lf.collect().sort("timestamp")

        step = downsample or self.downsample
        if step > 1:
            df = df.gather_every(step)

        # Cache schema on first load
        if self._schema is None:
            self._schema = df.columns

        return df

    def load_days(
        self,
        dates: Sequence[tuple[int, int, int]],
        *,
        columns: Optional[list[str]] = None,
        downsample: Optional[int] = None,
    ) -> pl.DataFrame:
        """Concatenate multiple days into a single DataFrame."""
        frames: list[pl.DataFrame] = []
        for y, m, d in dates:
            df = self.load_day(y, m, d, columns=columns, downsample=downsample)
            if df.shape[0] > 0:
                frames.append(df)
            gc.collect()
        if not frames:
            raise ValueError("No data loaded for the specified dates")
        return pl.concat(frames)

    def iter_days(
        self,
        dates: Sequence[tuple[int, int, int]],
        *,
        columns: Optional[list[str]] = None,
        downsample: Optional[int] = None,
    ):
        """Iterate over days one at a time (memory efficient).

        Yields
        ------
        tuple[tuple[int,int,int], pl.DataFrame]
            ``(date_tuple, dataframe)`` for each non-empty day.
        """
        for date in dates:
            y, m, d = date
            df = self.load_day(y, m, d, columns=columns, downsample=downsample)
            if df.shape[0] > 0:
                yield date, df
            gc.collect()

    # ── price helpers ───────────────────────────────────────────────

    @property
    def price_col(self) -> str:
        """Return (and auto-detect) the mid-price column name."""
        if self._price_col is not None:
            return self._price_col
        if self._schema is not None:
            for candidate in self._PRICE_CANDIDATES:
                if candidate in self._schema:
                    self._price_col = candidate
                    return candidate
        # Fallback: will be resolved on first load
        return "mid_price"

    def get_prices(self, df: pl.DataFrame) -> np.ndarray:
        """Extract price array, auto-computing mid_price if needed."""
        col = self.price_col
        if col in df.columns:
            return df[col].to_numpy()
        if "best_bid" in df.columns and "best_ask" in df.columns:
            return ((df["best_bid"] + df["best_ask"]) / 2).to_numpy()
        raise KeyError(
            f"No price column found. Tried {self._PRICE_CANDIDATES} "
            "and best_bid/best_ask fallback."
        )

    def get_features(
        self,
        df: pl.DataFrame,
        feature_cols: list[str],
    ) -> np.ndarray:
        """Extract feature matrix, filtering to available columns.

        Returns
        -------
        np.ndarray
            ``(n_samples, n_features)`` float32 array with NaN rows removed.
        """
        available = [c for c in feature_cols if c in df.columns]
        if not available:
            raise ValueError("No requested feature columns found in DataFrame")
        X = df.select(available).to_numpy().astype(np.float32)
        return X

    # ── default feature sets ────────────────────────────────────────

    @staticmethod
    def default_feature_cols() -> list[str]:
        """Return the curated 56-feature set used across research notebooks."""
        return [
            # Core price
            "mid_price", "microprice", "micro_minus_mid", "spread", "relative_spread",
            # Imbalance
            "total_imbalance", "imbalance_L1", "imbalance_L3", "imbalance_L5",
            "imbalance_L10", "smart_depth_imbalance", "book_pressure",
            # Order flow
            "ofi", "mlofi", "ofi_sum_5s", "ofi_sum_15s", "ofi_sum_60s",
            "mlofi_sum_5s", "mlofi_sum_15s", "mlofi_sum_60s",
            "order_flow_toxicity", "vpin",
            # Momentum / velocity
            "mid_velocity", "mid_accel", "log_return",
            "mean_return_5s", "mean_return_15s", "mean_return_60s",
            # Volatility
            "rv_5s", "rv_15s", "rv_60s", "rv_300s",
            # Depth
            "total_bid_depth", "total_ask_depth", "smart_bid_depth", "smart_ask_depth",
            "bid_depth_decay_5", "ask_depth_decay_5",
            # Concentration & slope
            "bid_concentration", "ask_concentration", "bid_slope", "ask_slope",
            "center_of_gravity", "cog_vs_mid",
            # Volume band imbalances
            "imb_band_0_5bps", "imb_band_5_10bps", "imb_band_10_25bps",
            # Spread dynamics
            "spread_percentile", "mean_spread_5s", "mean_spread_15s",
            # Trade flow
            "tfi_5s", "tfi_15s", "tfi_60s",
            # Liquidity
            "kyle_lambda", "amihud_like", "lambda_like",
        ]
