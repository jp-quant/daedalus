"""
Symbol Normalization Utilities
==============================

Handles quote currency equivalence for crypto trading pairs.

In crypto markets, the same asset is often quoted in different stablecoin
denominations across exchanges and channels:
- Coinbase Advanced: orderbook uses BTC/USDC, but trades/ticker return BTC/USD
- Binance: BTC/USDT, BTC/BUSD, BTC/USDC
- Kraken: BTC/USD

For analysis, modeling, and feature engineering, these are all effectively
the same underlying asset pair. This module provides utilities to normalize
quote currencies so that data from different partitions can be joined and
processed together seamlessly.

Key Functions:
    normalize_symbol:       "BTC-USDC" → "BTC-USD"
    get_symbol_variants:    "BTC-USD" → ["BTC-BUSD", "BTC-DAI", "BTC-USD", ...]
    symbols_match:          ("BTC-USDC", "BTC-USD") → True
    normalize_symbol_expr:  Polars expression for vectorized column normalization

Usage:
    # Scalar normalization
    from etl.utils.symbol import normalize_symbol, symbols_match
    
    normalize_symbol("BTC-USDC")     # → "BTC-USD"
    normalize_symbol("ETH-USDT")     # → "ETH-USD"
    symbols_match("BTC-USDC", "BTC-USD")  # → True
    
    # Vectorized Polars column normalization
    from etl.utils.symbol import normalize_symbol_expr
    
    df = df.with_columns(normalize_symbol_expr("symbol"))
    # All BTC-USDC, BTC-USDT, etc. → BTC-USD
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, List, Optional, Set

# Polars import for expression builders
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

if TYPE_CHECKING:
    import polars as pl


# =============================================================================
# Constants
# =============================================================================

# Stablecoin quote currencies that are equivalent to USD.
# These are all USD-pegged stablecoins encountered across major exchanges.
STABLECOIN_QUOTES: Set[str] = {
    "USDC",   # Circle (USD Coin)
    "USDT",   # Tether
    "BUSD",   # Binance USD (deprecated but still in historical data)
    "DAI",    # MakerDAO (soft-pegged)
    "TUSD",   # TrueUSD
    "GUSD",   # Gemini Dollar
    "USDP",   # Paxos Dollar
    "PYUSD",  # PayPal USD
    "FRAX",   # Frax (fractional-algorithmic)
    "LUSD",   # Liquity USD
    "UST",    # Terra USD (deprecated but in historical data)
}

# The canonical quote currency to normalize to
CANONICAL_QUOTE = "USD"

# All quotes in the equivalence class (including canonical)
ALL_USD_QUOTES: Set[str] = STABLECOIN_QUOTES | {CANONICAL_QUOTE}

# Default separator in sanitized symbols (BTC-USD, not BTC/USD)
SYMBOL_SEPARATOR = "-"

# Pre-compiled regex for matching stablecoin suffixes.
# Sorted by length descending so longer matches take priority (e.g., PYUSD before USD).
_STABLECOIN_PATTERN = re.compile(
    r"-(" + "|".join(
        re.escape(s) for s in sorted(STABLECOIN_QUOTES, key=len, reverse=True)
    ) + r")$"
)


# =============================================================================
# Scalar Functions
# =============================================================================

def normalize_symbol(
    symbol: str,
    separator: str = SYMBOL_SEPARATOR,
) -> str:
    """
    Normalize a trading pair by converting stablecoin quotes to canonical USD.
    
    Args:
        symbol: Trading pair (e.g., "BTC-USDC", "ETH-USDT", "SOL/USDC")
        separator: Separator character in the symbol (default "-")
    
    Returns:
        Normalized symbol with canonical USD quote.
    
    Examples:
        >>> normalize_symbol("BTC-USDC")
        'BTC-USD'
        >>> normalize_symbol("ETH-USDT")
        'ETH-USD'
        >>> normalize_symbol("SOL-USD")
        'SOL-USD'
        >>> normalize_symbol("BTC/USDC", separator="/")
        'BTC/USD'
        >>> normalize_symbol("AAPL")  # no separator → unchanged
        'AAPL'
    """
    if separator not in symbol:
        return symbol
    
    parts = symbol.rsplit(separator, 1)
    if len(parts) != 2:
        return symbol
    
    base, quote = parts
    if quote in STABLECOIN_QUOTES:
        return f"{base}{separator}{CANONICAL_QUOTE}"
    
    return symbol


def get_symbol_variants(
    symbol: str,
    separator: str = SYMBOL_SEPARATOR,
) -> List[str]:
    """
    Get all quote-currency variants for a symbol.
    
    Given any symbol with a USD-equivalent quote, returns all possible
    variants (including the canonical USD form).
    
    Args:
        symbol: Trading pair (e.g., "BTC-USD" or "BTC-USDC")
        separator: Separator character in the symbol
    
    Returns:
        Sorted list of all equivalent symbol variants.
        If the symbol doesn't have a recognized USD quote, returns [symbol].
    
    Examples:
        >>> get_symbol_variants("BTC-USD")
        ['BTC-BUSD', 'BTC-DAI', 'BTC-FRAX', 'BTC-GUSD', 'BTC-LUSD',
         'BTC-PYUSD', 'BTC-TUSD', 'BTC-USD', 'BTC-USDC', 'BTC-USDP',
         'BTC-USDT', 'BTC-UST']
        >>> get_symbol_variants("BTC-EUR")
        ['BTC-EUR']
    """
    if separator not in symbol:
        return [symbol]
    
    parts = symbol.rsplit(separator, 1)
    if len(parts) != 2:
        return [symbol]
    
    base, quote = parts
    if quote not in ALL_USD_QUOTES:
        return [symbol]
    
    return sorted(f"{base}{separator}{q}" for q in ALL_USD_QUOTES)


def symbols_match(
    a: str,
    b: str,
    separator: str = SYMBOL_SEPARATOR,
) -> bool:
    """
    Check if two symbols are equivalent (same base, equivalent USD quote).
    
    Args:
        a: First symbol (e.g., "BTC-USDC")
        b: Second symbol (e.g., "BTC-USD")
        separator: Separator character
    
    Returns:
        True if symbols have the same base and equivalent USD quotes.
    
    Examples:
        >>> symbols_match("BTC-USDC", "BTC-USD")
        True
        >>> symbols_match("ETH-USDT", "ETH-USDC")
        True
        >>> symbols_match("BTC-USD", "ETH-USD")
        False
        >>> symbols_match("BTC-EUR", "BTC-USD")
        False
    """
    return normalize_symbol(a, separator) == normalize_symbol(b, separator)


def get_base_currency(
    symbol: str,
    separator: str = SYMBOL_SEPARATOR,
) -> Optional[str]:
    """
    Extract the base currency from a trading pair.
    
    Args:
        symbol: Trading pair (e.g., "BTC-USD")
        separator: Separator character
    
    Returns:
        Base currency string, or None if separator not found.
    
    Examples:
        >>> get_base_currency("BTC-USD")
        'BTC'
        >>> get_base_currency("ETH-USDC")
        'ETH'
    """
    if separator in symbol:
        return symbol.rsplit(separator, 1)[0]
    return None


def get_quote_currency(
    symbol: str,
    separator: str = SYMBOL_SEPARATOR,
) -> Optional[str]:
    """
    Extract the quote currency from a trading pair.
    
    Args:
        symbol: Trading pair (e.g., "BTC-USD")
        separator: Separator character
    
    Returns:
        Quote currency string, or None if separator not found.
    
    Examples:
        >>> get_quote_currency("BTC-USD")
        'USD'
        >>> get_quote_currency("ETH-USDC")
        'USDC'
    """
    if separator in symbol:
        return symbol.rsplit(separator, 1)[1]
    return None


def is_usd_quoted(
    symbol: str,
    separator: str = SYMBOL_SEPARATOR,
) -> bool:
    """
    Check if a symbol is quoted in any USD-equivalent currency.
    
    Args:
        symbol: Trading pair
        separator: Separator character
    
    Returns:
        True if quoted in USD, USDC, USDT, or any other stablecoin.
    
    Examples:
        >>> is_usd_quoted("BTC-USD")
        True
        >>> is_usd_quoted("BTC-USDC")
        True
        >>> is_usd_quoted("BTC-EUR")
        False
    """
    quote = get_quote_currency(symbol, separator)
    return quote is not None and quote in ALL_USD_QUOTES


# =============================================================================
# Polars Expression Builders (Vectorized)
# =============================================================================

def normalize_symbol_expr(col: str = "symbol") -> pl.Expr:
    """
    Build a Polars expression that normalizes quote currencies in a symbol column.
    
    Converts stablecoin suffixes (-USDC, -USDT, -BUSD, etc.) to canonical -USD.
    This is fully vectorized and efficient for large DataFrames.
    
    Args:
        col: Name of the symbol column (default "symbol")
    
    Returns:
        Polars expression that normalizes the column in-place (same alias).
    
    Usage:
        # In a with_columns call
        df = df.with_columns(normalize_symbol_expr("symbol"))
        
        # In a lazy pipeline
        lf = lf.with_columns(normalize_symbol_expr())
    
    Note:
        Uses regex replacement with alternation, sorted by length descending
        to prevent partial matches (e.g., PYUSD must match before USD).
    """
    if not HAS_POLARS:
        raise RuntimeError("Polars is required for normalize_symbol_expr")
    
    # Build regex: -(PYUSD|USDC|USDT|BUSD|USDP|FRAX|GUSD|LUSD|TUSD|DAI|UST)$
    # Sorted by length descending to prevent partial match issues
    stables = sorted(STABLECOIN_QUOTES, key=len, reverse=True)
    pattern = "-(" + "|".join(re.escape(s) for s in stables) + ")$"
    
    return pl.col(col).str.replace(pattern, f"-{CANONICAL_QUOTE}").alias(col)


def symbol_matches_expr(
    symbol: str,
    col: str = "symbol",
    normalize: bool = True,
) -> pl.Expr:
    """
    Build a Polars filter expression that matches a symbol with quote normalization.
    
    When normalize=True, matches all quote variants of the symbol.
    When normalize=False, does exact match only.
    
    Args:
        symbol: Symbol to match (e.g., "BTC-USDC" or "BTC-USD")
        col: Column name to filter on
        normalize: Whether to match all USD-equivalent variants
    
    Returns:
        Polars boolean expression for filtering.
    
    Examples:
        # Matches BTC-USD, BTC-USDC, BTC-USDT, etc.
        df.filter(symbol_matches_expr("BTC-USDC"))
        
        # Exact match only
        df.filter(symbol_matches_expr("BTC-USDC", normalize=False))
    """
    if not HAS_POLARS:
        raise RuntimeError("Polars is required for symbol_matches_expr")
    
    if normalize and is_usd_quoted(symbol):
        variants = get_symbol_variants(symbol)
        return pl.col(col).is_in(variants)
    else:
        return pl.col(col) == symbol
