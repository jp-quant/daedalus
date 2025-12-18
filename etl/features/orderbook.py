"""
Orderbook Feature Extraction
============================

Vectorized structural feature extraction using Polars.
These features depend only on the current snapshot, not history.

Features extracted:
- L0-LN prices and sizes (configurable depth)
- Mid price, spread, relative spread
- Microprice (volume-weighted fair value)
- Multi-level imbalance (L1, L3, L5, L10)
- Band-based depth (0-5bps, 5-10bps, etc.)
- VWAP bid/ask (top 5 levels)
- Smart depth (exponential decay weighted)
- Book slope (simple + regression-based)
- Center of gravity
- Book convexity and depth decay
- Concentration (Herfindahl index)
- Lambda-like and Amihud-like liquidity proxies

References:
- Cont et al. (2014): OFI price impact
- Kyle & Obizhaeva (2016): Microstructure invariance
- Ghysels & Nguyen: Book slope / elasticity
- López de Prado (2018): Volume clocks
"""

from typing import List, Optional

import polars as pl

from etl.core.enums import FeatureCategory


def extract_structural_features(
    df: pl.LazyFrame,
    max_levels: int = 20,
    bands_bps: Optional[List[int]] = None,
    categories: Optional[set[FeatureCategory]] = None,
) -> pl.LazyFrame:
    """
    Extract comprehensive structural orderbook features using Polars.
    
    This is a fully vectorized implementation - very fast for batch processing.
    For streaming/row-by-row processing, use snapshot.py functions.
    
    Args:
        df: LazyFrame with orderbook data. Expected columns:
            - bids: List[Struct{price: f64, size: f64}]
            - asks: List[Struct{price: f64, size: f64}]
            - timestamp: i64 (milliseconds)
        max_levels: Maximum depth levels to process (default 20)
        bands_bps: Basis point bands for depth calculation
                   Default: [5, 10, 25, 50, 100]
        categories: Feature categories to compute. If None, compute all.
        
    Returns:
        LazyFrame with structural features added
    """
    if bands_bps is None:
        bands_bps = [5, 10, 25, 50, 100]
    
    if categories is None:
        categories = {FeatureCategory.STRUCTURAL}
    
    # Extract Level 0-N prices and sizes from nested arrays
    # Use list.get(i, null_on_oob=True) to handle shorter orderbooks gracefully
    level_exprs = []
    for i in range(max_levels):
        level_exprs.extend([
            pl.col("bids").list.get(i, null_on_oob=True).struct.field("price").fill_null(0.0).alias(f"bid_price_L{i}"),
            pl.col("bids").list.get(i, null_on_oob=True).struct.field("size").fill_null(0.0).alias(f"bid_size_L{i}"),
            pl.col("asks").list.get(i, null_on_oob=True).struct.field("price").fill_null(0.0).alias(f"ask_price_L{i}"),
            pl.col("asks").list.get(i, null_on_oob=True).struct.field("size").fill_null(0.0).alias(f"ask_size_L{i}"),
        ])
    
    df = df.with_columns(level_exprs)
    
    # Core price metrics (always computed)
    df = _add_core_price_metrics(df)
    
    # Total depth
    df = _add_total_depth(df, max_levels)
    
    # VWAP
    df = _add_vwap(df, min(5, max_levels))
    
    # Smart depth (exponential decay weighted)
    df = _add_smart_depth(df, min(20, max_levels))
    
    # Book slope
    if max_levels >= 5:
        df = _add_book_slope(df)
    
    # Center of gravity
    df = _add_center_of_gravity(df, min(20, max_levels))
    
    # Depth decay rate
    if max_levels >= 5:
        df = _add_depth_decay(df)
    
    # Concentration (Herfindahl index)
    df = _add_concentration(df, max_levels)
    
    # Liquidity proxies (lambda-like, amihud-like)
    df = _add_liquidity_proxies(df, max_levels)
    
    # Multi-level imbalance
    df = _add_multi_level_imbalance(df, max_levels)
    
    # Band-based depth
    df = _add_band_depth(df, bands_bps, max_levels)
    
    # Micro minus mid
    df = df.with_columns([
        (pl.col("microprice") - pl.col("mid_price")).alias("micro_minus_mid"),
    ])
    
    return df


def _add_core_price_metrics(df: pl.LazyFrame) -> pl.LazyFrame:
    """Add best bid/ask, mid price, spread, microprice, L1 imbalance."""
    df = df.with_columns([
        pl.col("bid_price_L0").alias("best_bid"),
        pl.col("ask_price_L0").alias("best_ask"),
        pl.col("bid_size_L0").alias("bid_size_L1"),  # Alias for compatibility
        pl.col("ask_size_L0").alias("ask_size_L1"),
        ((pl.col("bid_price_L0") + pl.col("ask_price_L0")) / 2).alias("mid_price"),
        (pl.col("ask_price_L0") - pl.col("bid_price_L0")).alias("spread"),
    ])
    
    df = df.with_columns([
        (pl.col("spread") / pl.col("mid_price")).alias("relative_spread"),
        (
            (pl.col("bid_price_L0") * pl.col("ask_size_L0") + 
             pl.col("ask_price_L0") * pl.col("bid_size_L0")) /
            (pl.col("bid_size_L0") + pl.col("ask_size_L0"))
        ).alias("microprice"),
        (
            (pl.col("bid_size_L0") - pl.col("ask_size_L0")) /
            (pl.col("bid_size_L0") + pl.col("ask_size_L0"))
        ).alias("imbalance_L1"),
    ])
    
    return df


def _add_total_depth(df: pl.LazyFrame, max_levels: int) -> pl.LazyFrame:
    """Add total bid/ask depth and book pressure."""
    bid_depth_expr = pl.sum_horizontal([pl.col(f"bid_size_L{i}") for i in range(max_levels)])
    ask_depth_expr = pl.sum_horizontal([pl.col(f"ask_size_L{i}") for i in range(max_levels)])
    
    df = df.with_columns([
        bid_depth_expr.alias("total_bid_depth"),
        ask_depth_expr.alias("total_ask_depth"),
        bid_depth_expr.alias("total_bid_volume"),
        ask_depth_expr.alias("total_ask_volume"),
    ])
    
    df = df.with_columns([
        (
            (pl.col("total_bid_depth") - pl.col("total_ask_depth")) /
            (pl.col("total_bid_depth") + pl.col("total_ask_depth"))
        ).alias("total_imbalance"),
        (
            (pl.col("total_bid_depth") - pl.col("total_ask_depth")) /
            (pl.col("total_bid_depth") + pl.col("total_ask_depth"))
        ).alias("book_pressure"),
    ])
    
    return df


def _add_vwap(df: pl.LazyFrame, vwap_levels: int) -> pl.LazyFrame:
    """Add volume-weighted average price for top N levels."""
    bid_vwap_num = pl.sum_horizontal([
        pl.col(f"bid_price_L{i}") * pl.col(f"bid_size_L{i}") 
        for i in range(vwap_levels)
    ])
    bid_vwap_den = pl.sum_horizontal([
        pl.col(f"bid_size_L{i}") for i in range(vwap_levels)
    ])
    ask_vwap_num = pl.sum_horizontal([
        pl.col(f"ask_price_L{i}") * pl.col(f"ask_size_L{i}") 
        for i in range(vwap_levels)
    ])
    ask_vwap_den = pl.sum_horizontal([
        pl.col(f"ask_size_L{i}") for i in range(vwap_levels)
    ])
    
    df = df.with_columns([
        (bid_vwap_num / bid_vwap_den).alias("vwap_bid_5"),
        (ask_vwap_num / ask_vwap_den).alias("vwap_ask_5"),
    ])
    
    df = df.with_columns([
        (pl.col("vwap_ask_5") - pl.col("vwap_bid_5")).alias("vwap_spread"),
    ])
    
    return df


def _add_smart_depth(df: pl.LazyFrame, depth_levels: int) -> pl.LazyFrame:
    """
    Add smart depth with exponential decay weighting.
    
    Smart depth weights liquidity by proximity to mid price,
    capturing the intuition that near-mid liquidity matters more.
    """
    k = 100  # Decay constant
    
    smart_bid_exprs = []
    smart_ask_exprs = []
    for i in range(depth_levels):
        # Bid: weight by exp(-k * (mid - bid_price) / mid)
        smart_bid_exprs.append(
            pl.col(f"bid_size_L{i}") * 
            ((-k * (pl.col("mid_price") - pl.col(f"bid_price_L{i}")) / pl.col("mid_price")).exp())
        )
        # Ask: weight by exp(-k * (ask_price - mid) / mid)
        smart_ask_exprs.append(
            pl.col(f"ask_size_L{i}") * 
            ((-k * (pl.col(f"ask_price_L{i}") - pl.col("mid_price")) / pl.col("mid_price")).exp())
        )
    
    df = df.with_columns([
        pl.sum_horizontal(smart_bid_exprs).alias("smart_bid_depth"),
        pl.sum_horizontal(smart_ask_exprs).alias("smart_ask_depth"),
    ])
    
    df = df.with_columns([
        (
            (pl.col("smart_bid_depth") - pl.col("smart_ask_depth")) /
            (pl.col("smart_bid_depth") + pl.col("smart_ask_depth"))
        ).alias("smart_depth_imbalance"),
    ])
    
    return df


def _add_book_slope(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add book slope features.
    
    Slope measures liquidity resilience - how quickly volume accumulates
    with price distance from mid.
    
    Reference: Ghysels & Nguyen
    """
    bid_cum_vol_5 = pl.sum_horizontal([pl.col(f"bid_size_L{i}") for i in range(5)])
    ask_cum_vol_5 = pl.sum_horizontal([pl.col(f"ask_size_L{i}") for i in range(5)])
    
    df = df.with_columns([
        (
            (pl.col("bid_price_L0") - pl.col("bid_price_L4")).abs() / bid_cum_vol_5
        ).alias("bid_slope_simple"),
        (
            (pl.col("ask_price_L4") - pl.col("ask_price_L0")).abs() / ask_cum_vol_5
        ).alias("ask_slope_simple"),
    ])
    
    # Legacy aliases
    df = df.with_columns([
        pl.col("bid_slope_simple").alias("bid_slope"),
        pl.col("ask_slope_simple").alias("ask_slope"),
    ])
    
    return df


def _add_center_of_gravity(df: pl.LazyFrame, cog_levels: int) -> pl.LazyFrame:
    """
    Add center of gravity (volume-weighted price center).
    
    CoG > mid => upward pressure (buy pressure)
    CoG < mid => downward pressure (sell pressure)
    """
    cog_num = pl.sum_horizontal([
        pl.col(f"bid_price_L{i}") * pl.col(f"bid_size_L{i}") for i in range(cog_levels)
    ] + [
        pl.col(f"ask_price_L{i}") * pl.col(f"ask_size_L{i}") for i in range(cog_levels)
    ])
    cog_den = pl.sum_horizontal([
        pl.col(f"bid_size_L{i}") for i in range(cog_levels)
    ] + [
        pl.col(f"ask_size_L{i}") for i in range(cog_levels)
    ])
    
    df = df.with_columns([
        (cog_num / cog_den).alias("center_of_gravity"),
    ])
    
    df = df.with_columns([
        ((pl.col("center_of_gravity") - pl.col("mid_price")) / pl.col("mid_price") * 10000).alias("cog_vs_mid"),
    ])
    
    return df


def _add_depth_decay(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add depth decay rate features.
    
    Measures how volume decreases at deeper levels.
    Higher ratio = more uniform depth, lower = concentrated at top.
    """
    df = df.with_columns([
        (pl.col("bid_size_L4") / pl.col("bid_size_L0")).alias("bid_depth_decay_5"),
        (pl.col("ask_size_L4") / pl.col("ask_size_L0")).alias("ask_depth_decay_5"),
    ])
    
    df = df.with_columns([
        ((pl.col("bid_depth_decay_5") + pl.col("ask_depth_decay_5")) / 2).alias("avg_depth_decay_5"),
    ])
    
    return df


def _add_concentration(df: pl.LazyFrame, max_levels: int) -> pl.LazyFrame:
    """
    Add concentration (Herfindahl Index) features.
    
    Higher = more concentrated (less diverse depth).
    """
    bid_shares_sq = [
        (pl.col(f"bid_size_L{i}") / pl.col("total_bid_depth")).pow(2)
        for i in range(max_levels)
    ]
    ask_shares_sq = [
        (pl.col(f"ask_size_L{i}") / pl.col("total_ask_depth")).pow(2)
        for i in range(max_levels)
    ]
    
    df = df.with_columns([
        pl.sum_horizontal(bid_shares_sq).alias("bid_concentration"),
        pl.sum_horizontal(ask_shares_sq).alias("ask_concentration"),
    ])
    
    return df


def _add_liquidity_proxies(df: pl.LazyFrame, max_levels: int) -> pl.LazyFrame:
    """
    Add liquidity proxy features (lambda-like, amihud-like).
    
    Lambda: spread / near_volume (0-5bps approximation)
    Amihud: spread / wider_volume (0-10bps approximation)
    Higher values = less liquid.
    """
    near_vol = pl.sum_horizontal([
        pl.col(f"bid_size_L{i}") + pl.col(f"ask_size_L{i}") 
        for i in range(min(3, max_levels))
    ])
    wider_vol = pl.sum_horizontal([
        pl.col(f"bid_size_L{i}") + pl.col(f"ask_size_L{i}") 
        for i in range(min(6, max_levels))
    ])
    
    df = df.with_columns([
        (pl.col("spread") / near_vol).alias("lambda_like"),
        (pl.col("spread") / wider_vol).alias("amihud_like"),
    ])
    
    return df


def _add_multi_level_imbalance(df: pl.LazyFrame, max_levels: int) -> pl.LazyFrame:
    """Add imbalance at different depth levels."""
    for levels in [3, 5, 10]:
        if levels <= max_levels:
            bid_depth_n = pl.sum_horizontal([pl.col(f"bid_size_L{i}") for i in range(levels)])
            ask_depth_n = pl.sum_horizontal([pl.col(f"ask_size_L{i}") for i in range(levels)])
            df = df.with_columns([
                ((bid_depth_n - ask_depth_n) / (bid_depth_n + ask_depth_n)).alias(f"imbalance_L{levels}"),
            ])
    
    return df


def _add_band_depth(df: pl.LazyFrame, bands_bps: List[int], max_levels: int) -> pl.LazyFrame:
    """
    Add band-based depth features.
    
    Approximates volume within each basis point band from mid price.
    Uses level indices mapped to typical bps distances.
    """
    # Approximate mapping from levels to bps bands
    # This is an approximation - actual bps depends on price levels
    band_level_mapping = {
        (0, 5): (0, 2),     # 0-5bps ≈ levels 0-2
        (5, 10): (2, 4),    # 5-10bps ≈ levels 2-4
        (10, 25): (4, 7),   # 10-25bps ≈ levels 4-7
        (25, 50): (7, 12),  # 25-50bps ≈ levels 7-12
        (50, 100): (12, 20) # 50-100bps ≈ levels 12-20
    }
    
    prev_bp = 0
    for bp in bands_bps:
        level_range = band_level_mapping.get((prev_bp, bp))
        if level_range:
            start_lvl, end_lvl = level_range
            end_lvl = min(end_lvl, max_levels)
            
            if start_lvl < max_levels:
                bid_band_vol = pl.sum_horizontal([
                    pl.col(f"bid_size_L{i}") for i in range(start_lvl, end_lvl)
                ])
                ask_band_vol = pl.sum_horizontal([
                    pl.col(f"ask_size_L{i}") for i in range(start_lvl, end_lvl)
                ])
                
                band_key = f"{prev_bp}_{bp}bps"
                df = df.with_columns([
                    bid_band_vol.alias(f"bid_vol_band_{band_key}"),
                    ask_band_vol.alias(f"ask_vol_band_{band_key}"),
                    ((bid_band_vol - ask_band_vol) / (bid_band_vol + ask_band_vol)).alias(f"imb_band_{band_key}"),
                ])
        prev_bp = bp
    
    return df


def compute_rolling_features(
    df: pl.LazyFrame,
    horizons: Optional[List[int]] = None,
) -> pl.LazyFrame:
    """
    Compute rolling features using Polars rolling operations.
    
    These are features that look back over a time window but don't
    require complex inter-record state (like OFI).
    
    Args:
        df: LazyFrame with structural features and capture_ts column
        horizons: Rolling window sizes in seconds.
                  Default: [5, 15, 60, 300, 900]
        
    Returns:
        LazyFrame with rolling features added
    """
    if horizons is None:
        horizons = [5, 15, 60, 300, 900]
    
    time_col = "capture_ts"

    existing_cols = set(df.collect_schema().names())
    
    # Ensure sorted by time
    df = df.sort(time_col)
    
    # Log returns
    if "log_return" not in existing_cols:
        df = df.with_columns([
            (pl.col("mid_price").log() - pl.col("mid_price").shift(1).log()).alias("log_return")
        ])
    
    # Rolling stats for each horizon using group_by_dynamic
    for h in horizons:
        window_str = f"{h}s"

        agg_exprs = []
        rv_name = f"rv_{h}s"
        mean_ret_name = f"mean_return_{h}s"
        mean_spread_name = f"mean_spread_{h}s"

        if rv_name not in existing_cols:
            agg_exprs.append(pl.col("log_return").std().alias(rv_name))
        if mean_ret_name not in existing_cols:
            agg_exprs.append(pl.col("log_return").mean().alias(mean_ret_name))
        if mean_spread_name not in existing_cols:
            agg_exprs.append(pl.col("relative_spread").mean().alias(mean_spread_name))

        if not agg_exprs:
            continue
        
        # Compute rolling stats via group_by_dynamic and join back
        rolling_stats = (
            df.group_by_dynamic(
                time_col,
                every="1s",
                period=window_str,
                closed="left",
                label="right",
            )
            .agg(agg_exprs)
        )
        
        # Join rolling stats back to original dataframe
        df = df.join_asof(
            rolling_stats,
            on=time_col,
            strategy="backward",
        )
    
    return df
