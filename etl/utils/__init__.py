"""
ETL Utilities
=============

Parquet file management utilities that are independent of ETL transforms.

Modules:
- crud: ParquetCRUD - Delete, Update, Upsert operations for Parquet datasets
- compaction: ParquetCompactor - File consolidation and optimization
- repartition: Repartitioner - Partition schema migration
- time_utils: Timestamp parsing utilities
- resampling: ASOF-style time series resampling utilities
- symbol: Quote currency normalization (USD/USDC/USDT equivalence)
"""

from etl.utils.crud import ParquetCRUD
from etl.utils.compaction import ParquetCompactor
from etl.utils.repartition import Repartitioner
from etl.utils.time_utils import add_time_fields, parse_timestamp_fields
from etl.utils.resampling import (
    resample_orderbook,
    resample_timeseries,
    parse_duration_to_ms,
    get_resampling_stats,
)
from etl.utils.symbol import (
    normalize_symbol,
    get_symbol_variants,
    symbols_match,
    is_usd_quoted,
    get_base_currency,
    get_quote_currency,
    normalize_symbol_expr,
    symbol_matches_expr,
    STABLECOIN_QUOTES,
    CANONICAL_QUOTE,
    ALL_USD_QUOTES,
)

__all__ = [
    "ParquetCRUD",
    "ParquetCompactor", 
    "Repartitioner",
    "add_time_fields",
    "parse_timestamp_fields",
    # Resampling utilities
    "resample_orderbook",
    "resample_timeseries",
    "parse_duration_to_ms",
    "get_resampling_stats",
    # Symbol normalization utilities
    "normalize_symbol",
    "get_symbol_variants",
    "symbols_match",
    "is_usd_quoted",
    "get_base_currency",
    "get_quote_currency",
    "normalize_symbol_expr",
    "symbol_matches_expr",
    "STABLECOIN_QUOTES",
    "CANONICAL_QUOTE",
    "ALL_USD_QUOTES",
]
