"""
Shared utilities for Daedalus market data pipeline.

This module provides common functionality used by both:
- ingestion/ (raw data collection)
- etl/ (data transformation)

Key Design Principles:
1. NO cross-imports between ingestion and etl
2. Both layers import from shared/ for common logic
3. Keeps partitioning logic consistent across the pipeline
"""

from shared.partitioning import (
    DEFAULT_PARTITION_COLUMNS,
    PROHIBITED_PATH_CHARS,
    sanitize_partition_value,
    extract_datetime_components,
    build_partition_path,
    format_partition_value,
    parse_partition_path,
    get_partition_cols_for_granularity,
    PartitionConfig,
)

__all__ = [
    "DEFAULT_PARTITION_COLUMNS",
    "PROHIBITED_PATH_CHARS",
    "sanitize_partition_value",
    "extract_datetime_components",
    "build_partition_path",
    "format_partition_value",
    "parse_partition_path",
    "get_partition_cols_for_granularity",
    "PartitionConfig",
]

# Polars-dependent functions (only available if polars is installed)
try:
    from shared.partitioning import (
        partition_dataframe,
        sanitize_dataframe_partition_values,
        add_datetime_partition_columns,
        ensure_partition_columns_exist,
    )
    __all__.extend([
        "partition_dataframe",
        "sanitize_dataframe_partition_values",
        "add_datetime_partition_columns",
        "ensure_partition_columns_exist",
    ])
except ImportError:
    pass  # Polars not available

