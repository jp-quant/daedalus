"""
Snapshot feature extraction module.

Extract STATIC/STRUCTURAL features from a single orderbook snapshot without
requiring historical state. This is Layer 1 of feature engineering.

Feature Categories:
1. Best bid/ask (L0) - Top of book prices and sizes
2. Multi-level depth (L1 to L{max_levels-1}) - Deeper orderbook levels
3. Mid price, spread, relative spread - Core price metrics
4. Microprice - Volume-weighted fair value
5. Imbalance at each level - Bid/ask size ratios
6. Cumulative depth at price bands - Liquidity distribution (0-5bps, 5-10bps, etc.)
7. Book depth - Number of valid levels

Output: Dict with 60+ features per snapshot

Called by: SymbolState.process_snapshot() in state.py for dynamic feature layering
"""
import numpy as np
from typing import Dict, List, Optional, Any, Union

def extract_orderbook_features(
    snapshot: Dict[str, Any],
    max_levels: int = 10,
    bands_bps: Optional[List[int]] = None,
    keep_raw_arrays: bool = False,
) -> Dict[str, Any]:
    """
    Extract comprehensive structural features from a single orderbook snapshot.

    Args:
        snapshot: Dictionary containing 'data' with 'bids', 'asks' (lists of [price, size]),
                  'timestamp' (ms), 'symbol', 'exchange'.
        max_levels: Number of top levels to extract individual features for.
        bands_bps: List of basis points for depth aggregation (e.g. [5, 10, 25]).
                   Defaults to [5, 10, 25, 50].
        keep_raw_arrays: If True, include the original 'bids' and 'asks' arrays in the output.

    Returns:
        Flat dictionary of features.
    """
    if bands_bps is None:
        bands_bps = [5, 10, 25, 50]

    # Extract basic data
    # Handle both direct keys and nested 'data' key structure if present
    # The user spec says snapshot has 'data' with bids/asks, but also 'symbol'/'exchange' at top level.
    # However, sometimes 'data' might be the snapshot itself in some systems. 
    # We'll assume the spec: snapshot['data']['bids']
    
    data = snapshot.get('data', snapshot)
    bids = data.get('bids', [])
    asks = data.get('asks', [])
    
    # Convert to numpy arrays for easier slicing/math
    # Shape: (N, 2) -> col 0 = price, col 1 = size
    bids_arr = np.array(bids, dtype=np.float64) if bids else np.empty((0, 2))
    asks_arr = np.array(asks, dtype=np.float64) if asks else np.empty((0, 2))

    # Ensure we have valid data
    has_bids = len(bids_arr) > 0
    has_asks = len(asks_arr) > 0
    
    # Initialize feature dict
    features = {
        'timestamp': snapshot.get('timestamp', data.get('timestamp')),
        'date': snapshot.get('date', data.get('date')),
        'symbol': snapshot.get('symbol', data.get('symbol')),
        'exchange': snapshot.get('exchange', data.get('exchange')),
        'nonce': snapshot.get('nonce', data.get('nonce')),
    }

    # --- L1 & Spread ---
    best_bid = bids_arr[0, 0] if has_bids else np.nan
    best_ask = asks_arr[0, 0] if has_asks else np.nan
    bid_size_l1 = bids_arr[0, 1] if has_bids else 0.0
    ask_size_l1 = asks_arr[0, 1] if has_asks else 0.0

    mid_price = np.nan
    if has_bids and has_asks:
        mid_price = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid
        relative_spread = spread / mid_price if mid_price > 0 else np.nan
        
        total_l1_vol = bid_size_l1 + ask_size_l1
        imbalance_l1 = (bid_size_l1 - ask_size_l1) / total_l1_vol if total_l1_vol > 0 else 0.0
        microprice = (best_bid * ask_size_l1 + best_ask * bid_size_l1) / total_l1_vol if total_l1_vol > 0 else mid_price
    else:
        spread = np.nan
        relative_spread = np.nan
        imbalance_l1 = np.nan
        microprice = np.nan

    features.update({
        'best_bid': best_bid,
        'best_ask': best_ask,
        'mid_price': mid_price,
        'spread': spread,
        'relative_spread': relative_spread,
        'bid_size_L1': bid_size_l1,
        'ask_size_L1': ask_size_l1,
        'imbalance_L1': imbalance_l1,
        'microprice': microprice,
    })

    # --- Per-level Features ---
    for i in range(max_levels):
        # Bids
        if i < len(bids_arr):
            features[f'bid_price_L{i}'] = bids_arr[i, 0]
            features[f'bid_size_L{i}'] = bids_arr[i, 1]
        else:
            features[f'bid_price_L{i}'] = np.nan
            features[f'bid_size_L{i}'] = 0.0
        
        # Asks
        if i < len(asks_arr):
            features[f'ask_price_L{i}'] = asks_arr[i, 0]
            features[f'ask_size_L{i}'] = asks_arr[i, 1]
        else:
            features[f'ask_price_L{i}'] = np.nan
            features[f'ask_size_L{i}'] = 0.0

    # --- Distance-band Depth ---
    if has_bids and has_asks and mid_price > 0:
        # Calculate distances in bps
        # Bids: (mid - price) / mid * 10000
        bid_dists = (mid_price - bids_arr[:, 0]) / mid_price * 10000
        # Asks: (price - mid) / mid * 10000
        ask_dists = (asks_arr[:, 0] - mid_price) / mid_price * 10000

        prev_bp = 0
        for bp in bands_bps:
            # Mask for current band: prev_bp <= dist < bp
            # Note: usually bands are cumulative or incremental. 
            # Spec implies incremental buckets: "0-5 bps, 5-10, 10-25"
            
            bid_mask = (bid_dists >= prev_bp) & (bid_dists < bp)
            ask_mask = (ask_dists >= prev_bp) & (ask_dists < bp)
            
            bid_vol = np.sum(bids_arr[bid_mask, 1])
            ask_vol = np.sum(asks_arr[ask_mask, 1])
            
            total_vol = bid_vol + ask_vol
            imb = (bid_vol - ask_vol) / total_vol if total_vol > 0 else 0.0
            
            band_key = f"{prev_bp}_{bp}bps"
            features[f'bid_vol_band_{band_key}'] = bid_vol
            features[f'ask_vol_band_{band_key}'] = ask_vol
            features[f'imb_band_{band_key}'] = imb
            
            prev_bp = bp
    else:
        # Fill with 0/NaN if no mid price
        prev_bp = 0
        for bp in bands_bps:
            band_key = f"{prev_bp}_{bp}bps"
            features[f'bid_vol_band_{band_key}'] = 0.0
            features[f'ask_vol_band_{band_key}'] = 0.0
            features[f'imb_band_{band_key}'] = np.nan
            prev_bp = bp

    # --- Shape & Concentration (Top N) ---
    # Limit to max_levels for these calcs to avoid processing full depth
    bids_top = bids_arr[:max_levels]
    asks_top = asks_arr[:max_levels]
    
    # Cumulative Volume & 50% Depth
    if len(bids_top) > 0:
        bid_cum_vol = np.cumsum(bids_top[:, 1])
        total_bid_top = bid_cum_vol[-1]
        # Find index where cum vol >= 50% of total top-N vol
        bid_50_idx = np.searchsorted(bid_cum_vol, total_bid_top * 0.5)
        features['bid_50pct_depth_levels'] = int(bid_50_idx)
        
        # Concentration (Herfindahl)
        if total_bid_top > 0:
            bid_shares = bids_top[:, 1] / total_bid_top
            features['bid_concentration'] = np.sum(bid_shares ** 2)
        else:
            features['bid_concentration'] = 0.0
    else:
        features['bid_50pct_depth_levels'] = np.nan
        features['bid_concentration'] = np.nan

    if len(asks_top) > 0:
        ask_cum_vol = np.cumsum(asks_top[:, 1])
        total_ask_top = ask_cum_vol[-1]
        ask_50_idx = np.searchsorted(ask_cum_vol, total_ask_top * 0.5)
        features['ask_50pct_depth_levels'] = int(ask_50_idx)
        
        if total_ask_top > 0:
            ask_shares = asks_top[:, 1] / total_ask_top
            features['ask_concentration'] = np.sum(ask_shares ** 2)
        else:
            features['ask_concentration'] = 0.0
    else:
        features['ask_50pct_depth_levels'] = np.nan
        features['ask_concentration'] = np.nan

    # --- Liquidity / Impact Proxies ---
    
    # VWAP (Top 5)
    vwap_levels = 5
    
    # Bid VWAP
    if len(bids_arr) >= 1:
        b_slice = bids_arr[:vwap_levels]
        b_vol_sum = np.sum(b_slice[:, 1])
        features['vwap_bid_5'] = np.sum(b_slice[:, 0] * b_slice[:, 1]) / b_vol_sum if b_vol_sum > 0 else np.nan
    else:
        features['vwap_bid_5'] = np.nan
        
    # Ask VWAP
    if len(asks_arr) >= 1:
        a_slice = asks_arr[:vwap_levels]
        a_vol_sum = np.sum(a_slice[:, 1])
        features['vwap_ask_5'] = np.sum(a_slice[:, 0] * a_slice[:, 1]) / a_vol_sum if a_vol_sum > 0 else np.nan
    else:
        features['vwap_ask_5'] = np.nan
        
    if not np.isnan(features['vwap_bid_5']) and not np.isnan(features['vwap_ask_5']):
        features['vwap_spread'] = features['vwap_ask_5'] - features['vwap_bid_5']
    else:
        features['vwap_spread'] = np.nan

    # Smart Depth (Exponential decay)
    # k = 100 (configurable in theory, hardcoded for now as per spec default)
    k = 100
    if has_bids and mid_price > 0:
        # dists already calc'd above: bid_dists
        # We use the full array available or a reasonable subset (e.g. top 50) to save time
        # Spec doesn't strictly limit, but top 50 is usually enough for 'smart depth'
        limit = 50
        b_slice = bids_arr[:limit]
        dists = (mid_price - b_slice[:, 0]) / mid_price  # raw distance, not bps
        weights = np.exp(-k * dists)
        features['smart_bid_depth'] = np.sum(b_slice[:, 1] * weights)
    else:
        features['smart_bid_depth'] = 0.0
        
    if has_asks and mid_price > 0:
        limit = 50
        a_slice = asks_arr[:limit]
        dists = (a_slice[:, 0] - mid_price) / mid_price
        weights = np.exp(-k * dists)
        features['smart_ask_depth'] = np.sum(a_slice[:, 1] * weights)
    else:
        features['smart_ask_depth'] = 0.0
        
    total_smart = features['smart_bid_depth'] + features['smart_ask_depth']
    features['smart_depth_imbalance'] = (
        (features['smart_bid_depth'] - features['smart_ask_depth']) / total_smart 
        if total_smart > 0 else 0.0
    )

    # Lambda / Amihud
    # Lambda: spread / near_vol (0-5bps)
    # Amihud: spread / slightly_wider_vol (0-10bps)
    # We can reuse the band volumes if they match, or re-calc.
    # Let's re-calc specifically for these definitions to be safe.
    
    if mid_price > 0 and not np.isnan(spread):
        # 0-5 bps vol
        vol_0_5 = (
            features.get('bid_vol_band_0_5bps', 0) + 
            features.get('ask_vol_band_0_5bps', 0)
        )
        features['lambda_like'] = spread / vol_0_5 if vol_0_5 > 0 else np.nan
        
        # 0-10 bps vol (sum of 0-5 and 5-10 if bands are [5, 10...])
        vol_0_10 = vol_0_5 + (
            features.get('bid_vol_band_5_10bps', 0) + 
            features.get('ask_vol_band_5_10bps', 0)
        )
        features['amihud_like'] = spread / vol_0_10 if vol_0_10 > 0 else np.nan
    else:
        features['lambda_like'] = np.nan
        features['amihud_like'] = np.nan

    # ===============================================================
    # ADVANCED SHAPE FEATURES (Research Priority)
    # Based on: Ghysels & Nguyen, López de Prado, Kyle & Obizhaeva
    # ===============================================================
    
    # --- Order Book Slope / Elasticity ---
    # Measures how quickly volume accumulates with price distance
    # Steep slope = lots of liquidity near mid (resilient)
    # Shallow slope = thin near top, liquidity far out (fragile)
    
    # Method 1: Simple slope (price change per unit volume for top 5 levels)
    if len(bids_arr) >= 5:
        price_diff = bids_arr[4, 0] - bids_arr[0, 0]  # Negative for bids
        cum_vol = np.sum(bids_arr[:5, 1])
        features['bid_slope_simple'] = abs(price_diff) / cum_vol if cum_vol > 0 else np.nan
    else:
        features['bid_slope_simple'] = np.nan
        
    if len(asks_arr) >= 5:
        price_diff = asks_arr[4, 0] - asks_arr[0, 0]  # Positive
        cum_vol = np.sum(asks_arr[:5, 1])
        features['ask_slope_simple'] = abs(price_diff) / cum_vol if cum_vol > 0 else np.nan
    else:
        features['ask_slope_simple'] = np.nan
    
    # Method 2: Regression-based slope (more robust)
    # Regress cumulative volume against price distance from mid
    slope_levels = min(10, len(bids_arr), len(asks_arr))
    
    if slope_levels >= 3 and mid_price > 0:
        # Bid side: distance is (mid - bid_price) / mid * 10000 (bps)
        bid_distances = (mid_price - bids_arr[:slope_levels, 0]) / mid_price * 10000
        bid_cum_vol = np.cumsum(bids_arr[:slope_levels, 1])
        
        # Linear regression: cum_vol = slope * distance + intercept
        # slope tells us "volume added per bps away from mid"
        try:
            bid_slope_coef = np.polyfit(bid_distances, bid_cum_vol, 1)[0]
            features['bid_slope_regression'] = bid_slope_coef
        except:
            features['bid_slope_regression'] = np.nan
        
        # Ask side
        ask_distances = (asks_arr[:slope_levels, 0] - mid_price) / mid_price * 10000
        ask_cum_vol = np.cumsum(asks_arr[:slope_levels, 1])
        
        try:
            ask_slope_coef = np.polyfit(ask_distances, ask_cum_vol, 1)[0]
            features['ask_slope_regression'] = ask_slope_coef
        except:
            features['ask_slope_regression'] = np.nan
        
        # Slope asymmetry: bid_slope / ask_slope
        # > 1 means more bid support (bullish), < 1 means more ask resistance (bearish)
        if features['ask_slope_regression'] and features['ask_slope_regression'] > 0:
            features['slope_asymmetry'] = features['bid_slope_regression'] / features['ask_slope_regression']
        else:
            features['slope_asymmetry'] = np.nan
    else:
        features['bid_slope_regression'] = np.nan
        features['ask_slope_regression'] = np.nan
        features['slope_asymmetry'] = np.nan
    
    # --- Center of Gravity (CoG) / Volume-Weighted Mid ---
    # The "center of mass" of the order book
    # If CoG > mid_price, book skewed with buy orders below (upward pull)
    # If CoG < mid_price, book skewed with sell orders above (downward pull)
    
    cog_levels = min(20, len(bids_arr), len(asks_arr))
    if cog_levels >= 1 and mid_price > 0:
        bid_prices = bids_arr[:cog_levels, 0]
        bid_sizes = bids_arr[:cog_levels, 1]
        ask_prices = asks_arr[:cog_levels, 0]
        ask_sizes = asks_arr[:cog_levels, 1]
        
        total_vol = np.sum(bid_sizes) + np.sum(ask_sizes)
        if total_vol > 0:
            cog = (np.sum(bid_prices * bid_sizes) + np.sum(ask_prices * ask_sizes)) / total_vol
            features['center_of_gravity'] = cog
            features['cog_vs_mid'] = (cog - mid_price) / mid_price * 10000  # bps
        else:
            features['center_of_gravity'] = np.nan
            features['cog_vs_mid'] = np.nan
    else:
        features['center_of_gravity'] = np.nan
        features['cog_vs_mid'] = np.nan
    
    # --- Book Convexity / Concavity ---
    # Measures liquidity distribution shape
    # Convex: Most liquidity near mid (stable, mean-reverting)
    # Concave: Liquidity scattered far from mid (fragile, momentum-prone)
    
    if mid_price > 0 and 5 in bands_bps and 50 in bands_bps:
        # Compare near vs far liquidity
        near_vol = features.get('bid_vol_band_0_5bps', 0) + features.get('ask_vol_band_0_5bps', 0)
        
        # Calculate total volume up to 50bps
        total_50bps_vol = 0
        prev_bp = 0
        for bp in bands_bps:
            if bp <= 50:
                band_key = f"{prev_bp}_{bp}bps"
                total_50bps_vol += features.get(f'bid_vol_band_{band_key}', 0)
                total_50bps_vol += features.get(f'ask_vol_band_{band_key}', 0)
            prev_bp = bp
        
        if total_50bps_vol > 0:
            # Convexity: fraction of volume in nearest band
            features['book_convexity'] = near_vol / total_50bps_vol
        else:
            features['book_convexity'] = np.nan
    else:
        features['book_convexity'] = np.nan
    
    # --- Depth Decay Rate ---
    # How fast does volume decrease as we go deeper?
    # Calculated as the ratio of volume at level N vs level 1
    
    if len(bids_arr) >= 5 and len(asks_arr) >= 5:
        # Average decay: (size_L5 + size_L10) / (2 * size_L1)
        bid_decay = bids_arr[4, 1] / bids_arr[0, 1] if bids_arr[0, 1] > 0 else np.nan
        ask_decay = asks_arr[4, 1] / asks_arr[0, 1] if asks_arr[0, 1] > 0 else np.nan
        
        features['bid_depth_decay_5'] = bid_decay
        features['ask_depth_decay_5'] = ask_decay
        
        if not np.isnan(bid_decay) and not np.isnan(ask_decay):
            features['avg_depth_decay_5'] = (bid_decay + ask_decay) / 2
        else:
            features['avg_depth_decay_5'] = np.nan
    else:
        features['bid_depth_decay_5'] = np.nan
        features['ask_depth_decay_5'] = np.nan
        features['avg_depth_decay_5'] = np.nan
    
    # --- Total Book Pressure ---
    # Overall directional pressure based on full book
    # Positive = more bid volume (bullish), Negative = more ask volume (bearish)
    
    if has_bids and has_asks:
        total_bid_vol = np.sum(bids_arr[:max_levels, 1])
        total_ask_vol = np.sum(asks_arr[:max_levels, 1])
        total_vol = total_bid_vol + total_ask_vol
        
        if total_vol > 0:
            features['book_pressure'] = (total_bid_vol - total_ask_vol) / total_vol
            features['total_bid_volume'] = total_bid_vol
            features['total_ask_volume'] = total_ask_vol
        else:
            features['book_pressure'] = 0.0
            features['total_bid_volume'] = 0.0
            features['total_ask_volume'] = 0.0
    else:
        features['book_pressure'] = np.nan
        features['total_bid_volume'] = np.nan
        features['total_ask_volume'] = np.nan
    
    # --- Legacy slopes for backward compatibility ---
    features['bid_slope'] = features.get('bid_slope_simple', np.nan)
    features['ask_slope'] = features.get('ask_slope_simple', np.nan)

    # Optional: Keep raw arrays
    if keep_raw_arrays:
        features['bids'] = bids
        features['asks'] = asks

    return features
