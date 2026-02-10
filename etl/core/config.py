"""
Configuration Models for ETL Framework
======================================

Declarative configuration classes for defining transform inputs and outputs.
Inspired by GlueETL pattern: INPUTS dict → transform() → OUTPUTS dict
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Union, List

import polars as pl

from etl.core.enums import (
    CompressionCodec,
    DataFormat,
    FeatureCategory,
    PartitionGranularity,
    ProcessingMode,
    StorageBackendType,
    WriteMode,
)


@dataclass
class StorageConfig:
    """
    Storage backend configuration.
    
    Supports both local and S3 storage. The framework is agnostic
    to the actual backend - this config is passed to StorageBackend.
    """
    backend_type: StorageBackendType = StorageBackendType.LOCAL
    base_path: Optional[str] = None
    
    # S3-specific options
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    region: Optional[str] = None
    
    def __post_init__(self):
        if self.backend_type == StorageBackendType.S3:
            if not self.bucket:
                raise ValueError("S3 storage requires 'bucket' to be specified")


@dataclass
class InputConfig:
    """
    Configuration for a single input source.
    
    Example:
        InputConfig(
            name="bronze_orderbook",
            path="/data/bronze/orderbook",
            format=DataFormat.PARQUET,
            partition_cols=["exchange", "symbol", "year", "month", "day"],
        )
    """
    # Identity
    name: str  # Logical name for this input (key in INPUTS dict)
    
    # Location
    path: str  # Base path or pattern
    format: DataFormat = DataFormat.PARQUET
    storage: Optional[StorageConfig] = None  # If None, uses transform-level storage
    
    # Partitioning
    partition_cols: list[str] = field(default_factory=list)
    
    # Filtering (applied at read time)
    filters: Optional[list[tuple[str, str, Any]]] = None  # [(col, op, val), ...]
    
    # Schema handling
    schema: Optional[dict[str, pl.DataType]] = None  # Expected schema (optional validation)
    columns: Optional[list[str]] = None  # Columns to select (projection pushdown)
    
    # Reader options
    reader_options: dict[str, Any] = field(default_factory=dict)
    
    def validate(self) -> None:
        """Validate configuration."""
        if not self.name:
            raise ValueError("InputConfig requires 'name'")
        if not self.path:
            raise ValueError(f"InputConfig '{self.name}' requires 'path'")


@dataclass
class OutputConfig:
    """
    Configuration for a single output destination.
    
    Example:
        OutputConfig(
            name="silver_features",
            path="/data/silver/features",
            format=DataFormat.PARQUET,
            partition_cols=["exchange", "symbol", "year", "month", "day"],
            mode=WriteMode.OVERWRITE_PARTITION,
        )
    """
    # Identity
    name: str  # Logical name for this output (key in OUTPUTS dict)
    
    # Location
    path: str  # Base path
    format: DataFormat = DataFormat.PARQUET
    storage: Optional[StorageConfig] = None  # If None, uses transform-level storage
    
    # Partitioning
    partition_cols: list[str] = field(default_factory=list)
    
    # Write behavior
    mode: WriteMode = WriteMode.APPEND
    key_cols: Optional[list[str]] = None  # Required for MERGE mode
    
    # Parquet options
    compression: CompressionCodec = CompressionCodec.ZSTD
    compression_level: int = 3
    row_group_size: Optional[int] = None
    
    # Writer options
    writer_options: dict[str, Any] = field(default_factory=dict)
    
    def validate(self) -> None:
        """Validate configuration."""
        if not self.name:
            raise ValueError("OutputConfig requires 'name'")
        if not self.path:
            raise ValueError(f"OutputConfig '{self.name}' requires 'path'")
        if self.mode == WriteMode.MERGE and not self.key_cols:
            raise ValueError(f"OutputConfig '{self.name}' with MERGE mode requires 'key_cols'")


@dataclass
class TransformConfig:
    """
    Configuration for a transform.
    
    Defines inputs, outputs, and processing parameters.
    The transform itself contains the actual logic.
    """
    # Identity
    name: str  # Unique transform name
    description: Optional[str] = None
    
    # I/O declarations
    inputs: dict[str, InputConfig] = field(default_factory=dict)
    outputs: dict[str, OutputConfig] = field(default_factory=dict)
    
    # Processing mode
    mode: ProcessingMode = ProcessingMode.BATCH
    
    # Default storage (can be overridden per input/output)
    storage: Optional[StorageConfig] = None
    
    # Execution parameters
    parallelism: int = 1  # Number of parallel workers
    batch_size: Optional[int] = None  # Rows per batch (for large datasets)
    
    # State management (for stateful transforms)
    state_path: Optional[str] = None  # Where to persist state
    checkpoint_interval: Optional[int] = None  # Checkpoint every N batches
    
    # Dependencies
    depends_on: list[str] = field(default_factory=list)  # Other transforms that must run first
    
    def validate(self) -> None:
        """Validate configuration."""
        if not self.name:
            raise ValueError("TransformConfig requires 'name'")
        
        for input_config in self.inputs.values():
            input_config.validate()
        
        for output_config in self.outputs.values():
            output_config.validate()
    
    def add_input(self, config: InputConfig) -> "TransformConfig":
        """Add an input configuration."""
        self.inputs[config.name] = config
        return self
    
    def add_output(self, config: OutputConfig) -> "TransformConfig":
        """Add an output configuration."""
        self.outputs[config.name] = config
        return self


@dataclass
class FeatureConfig:
    """
    Configuration for feature computation.
    
    Controls which feature categories are computed and their parameters.
    This is the central configuration used by TransformExecutor and all
    ETL transforms.
    
    Research References:
        - Cont et al. (2014): OFI price impact
        - Kyle & Obizhaeva (2016): Microstructure invariance
        - Zhang et al. (2019): DeepLOB - 20 levels captures "walls"
        - Xu et al. (2019): Multi-level OFI with decay
        - Easley et al. (2012): VPIN for flow toxicity
        - López de Prado (2018): Volume clocks
    """
    # Feature categories to compute
    categories: set[FeatureCategory] = field(default_factory=lambda: {
        FeatureCategory.STRUCTURAL,
        FeatureCategory.DYNAMIC,
    })
    
    # ==========================================================================
    # ORDERBOOK DEPTH PARAMETERS
    # ==========================================================================
    depth_levels: int = 20  # Number of price levels to process (Zhang: 20 captures "walls")
    ofi_levels: int = 10    # Levels for multi-level OFI (Xu: 10 with decay)
    bands_bps: list[int] = field(default_factory=lambda: [5, 10, 25, 50, 100])  # Liquidity bands
    
    # ==========================================================================
    # ROLLING WINDOW PARAMETERS
    # ==========================================================================
    rolling_windows: list[int] = field(default_factory=lambda: [5, 15, 60, 300, 900])  # Seconds
    
    # ==========================================================================
    # BAR AGGREGATION PARAMETERS
    # ==========================================================================
    bar_interval_seconds: int = 60
    bar_durations: list[int] = field(default_factory=lambda: [60, 300, 900, 3600])
    
    # ==========================================================================
    # OFI / MLOFI PARAMETERS
    # ==========================================================================
    ofi_decay_alpha: float = 0.5  # Exponential decay for MLOFI: w_i = exp(-alpha * i)
    
    # ==========================================================================
    # KYLE'S LAMBDA PARAMETERS
    # ==========================================================================
    kyle_lambda_window: int = 300  # Rolling window in seconds for price impact
    
    # ==========================================================================
    # VPIN PARAMETERS
    # ==========================================================================
    enable_vpin: bool = True
    vpin_bucket_size: float = 1.0  # Volume bucket size
    vpin_window_buckets: int = 50  # Number of buckets in rolling window
    
    # ==========================================================================
    # SPREAD REGIME PARAMETERS
    # ==========================================================================
    use_dynamic_spread_regime: bool = True
    spread_regime_window: int = 300  # Seconds for percentile calculation
    spread_tight_percentile: float = 0.2  # Bottom 20% = tight
    spread_wide_percentile: float = 0.8   # Top 20% = wide
    spread_tight_threshold_bps: float = 5.0   # Fallback static threshold
    spread_wide_threshold_bps: float = 20.0   # Fallback static threshold
    
    # ==========================================================================
    # STATEFUL FEATURE TOGGLES
    # ==========================================================================
    enable_stateful: bool = True  # Enable OFI, MLOFI, regime tracking, etc.
    
    # ==========================================================================
    # RESAMPLING CONFIGURATION (ASOF semantics)
    # ==========================================================================
    # If set, resamples data to this frequency before feature extraction
    # Uses ASOF semantics: "at time T, what was the last known state?"
    # This prevents lookahead bias and is safe for backtesting/ML
    resample_frequency: Optional[str] = None  # e.g., "1m", "5m", "1s" (None = no resampling)
    resample_max_staleness: Optional[str] = None  # e.g., "5m" - flag data older than this
    
    # ==========================================================================
    # STORAGE OPTIMIZATION
    # ==========================================================================
    drop_raw_book_arrays: bool = True  # Drop raw bids/asks arrays after extraction
    
    # ==========================================================================
    # QUOTE CURRENCY NORMALIZATION
    # ==========================================================================
    # When True, stablecoin quote currencies (USDC, USDT, BUSD, etc.) are
    # normalized to canonical USD before feature computation. This allows
    # seamless joining of data across different quote currencies:
    #   - Orderbook from symbol=BTC-USDC + Trades from symbol=BTC-USD
    #   - Output features will use the canonical form (BTC-USD)
    normalize_quotes: bool = True
    
    def has_category(self, category: FeatureCategory) -> bool:
        """Check if a feature category is enabled."""
        return category in self.categories


@dataclass
class FilterSpec:
    """
    Declarative filter specification for partition pruning.
    
    Supports both exact matches and date ranges. Use this to efficiently
    filter large datasets at read time via partition pruning.
    
    Attributes:
        exchange: Filter by exchange (exact match).
        symbol: Filter by symbol (exact match, or expanded with normalize_quotes).
        year: Filter by year (exact match or use start_date/end_date).
        month: Filter by month (exact match or use start_date/end_date).
        day: Filter by day (exact match or use start_date/end_date).
        start_date: Inclusive start date for range queries (YYYY-MM-DD string).
        end_date: Inclusive end date for range queries (YYYY-MM-DD string).
        normalize_quotes: When True and symbol is set, matches all quote currency
            variants (USD, USDC, USDT, etc.) instead of exact symbol match.
            This enables loading data from different quote partitions.
    
    Example:
        # Exact partition match
        filter_spec = FilterSpec(exchange="binanceus", symbol="BTC/USDT", year=2025, month=12)
        
        # Match all USD-equivalent quote variants (BTC-USD, BTC-USDC, BTC-USDT, ...)
        filter_spec = FilterSpec(
            exchange="coinbaseadvanced",
            symbol="BTC-USDC",
            normalize_quotes=True,  # Also matches BTC-USD, BTC-USDT, etc.
        )
        
        # Date range (for multi-day batch processing)
        filter_spec = FilterSpec(
            exchange="binanceus",
            start_date="2025-12-01",
            end_date="2025-12-15",
        )
        
        # Use with executor
        result = executor.execute(transform, filter_spec=filter_spec)
    """
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    year: Optional[int] = None
    month: Optional[int] = None
    day: Optional[int] = None
    hour: Optional[int] = None
    start_date: Optional[str] = None  # YYYY-MM-DD format
    end_date: Optional[str] = None    # YYYY-MM-DD format
    normalize_quotes: bool = False     # Expand symbol to all USD-equivalent variants
    
    def to_polars_filter(self) -> Optional[pl.Expr]:
        """
        Convert to Polars filter expression.
        
        When normalize_quotes is True and a symbol is specified, the filter
        matches all quote currency variants (USD, USDC, USDT, etc.) using
        ``is_in`` instead of exact equality. This enables loading data from
        partitions with different stablecoin quote currencies.
        
        Returns:
            Combined filter expression or None if no filters specified.
        """
        conditions = []
        
        if self.exchange is not None:
            conditions.append(pl.col("exchange") == self.exchange)
        if self.symbol is not None:
            if self.normalize_quotes:
                from etl.utils.symbol import get_symbol_variants, is_usd_quoted
                if is_usd_quoted(self.symbol):
                    variants = get_symbol_variants(self.symbol)
                    conditions.append(pl.col("symbol").is_in(variants))
                else:
                    conditions.append(pl.col("symbol") == self.symbol)
            else:
                conditions.append(pl.col("symbol") == self.symbol)
        
        # Handle date range vs exact date filters
        if self.start_date or self.end_date:
            # Use date range filtering with capture_ts
            if self.start_date:
                from datetime import datetime
                start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
                conditions.append(pl.col("capture_ts") >= start_dt)
            if self.end_date:
                from datetime import datetime, timedelta
                end_dt = datetime.strptime(self.end_date, "%Y-%m-%d") + timedelta(days=1)
                conditions.append(pl.col("capture_ts") < end_dt)
        else:
            # Use exact year/month/day filters
            if self.year is not None:
                conditions.append(pl.col("year") == self.year)
            if self.month is not None:
                conditions.append(pl.col("month") == self.month)
            if self.day is not None:
                conditions.append(pl.col("day") == self.day)
            if self.hour is not None:
                conditions.append(pl.col("hour") == self.hour)
        if not conditions:
            return None
        
        result = conditions[0]
        for cond in conditions[1:]:
            result = result & cond
        return result
    
    def to_partition_filters(self) -> list[tuple[str, str, Any]]:
        """
        Convert to partition filter tuples for PyArrow.
        
        Note: Date ranges are NOT pushed down to partition pruning
        (they require timestamp column access). Use year/month/day
        for partition-level pruning, or ensure your Parquet reader
        supports predicate pushdown on capture_ts.
        
        Note: When normalize_quotes is True, symbol filtering uses
        ``in`` operator with all quote variants instead of exact match.
        
        Returns:
            List of (column, operator, value) tuples.
        """
        filters = []
        
        if self.exchange is not None:
            filters.append(("exchange", "=", self.exchange))
        if self.symbol is not None:
            if self.normalize_quotes:
                from etl.utils.symbol import get_symbol_variants, is_usd_quoted
                if is_usd_quoted(self.symbol):
                    variants = get_symbol_variants(self.symbol)
                    filters.append(("symbol", "in", variants))
                else:
                    filters.append(("symbol", "=", self.symbol))
            else:
                filters.append(("symbol", "=", self.symbol))
        if self.year is not None:
            filters.append(("year", "=", self.year))
        if self.month is not None:
            filters.append(("month", "=", self.month))
        if self.day is not None:
            filters.append(("day", "=", self.day))
        if self.hour is not None:
            filters.append(("hour", "=", self.hour))
        return filters
    
    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary of non-None values.
        
        Useful for logging, partition path building, and context passing.
        
        Returns:
            Dictionary with only the set (non-None) filter values.
        """
        result = {}
        if self.exchange is not None:
            result["exchange"] = self.exchange
        if self.symbol is not None:
            result["symbol"] = self.symbol
        if self.year is not None:
            result["year"] = self.year
        if self.month is not None:
            result["month"] = self.month
        if self.day is not None:
            result["day"] = self.day
        if self.hour is not None:
            result["hour"] = self.hour
        if self.start_date is not None:
            result["start_date"] = self.start_date
        if self.end_date is not None:
            result["end_date"] = self.end_date
        if self.normalize_quotes:
            result["normalize_quotes"] = True
        return result
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FilterSpec":
        """
        Create FilterSpec from dictionary.
        
        Args:
            data: Dictionary with filter values.
        
        Returns:
            FilterSpec instance.
        """
        return cls(
            exchange=data.get("exchange"),
            symbol=data.get("symbol"),
            year=data.get("year"),
            month=data.get("month"),
            day=data.get("day"),
            hour=data.get("hour"),
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            normalize_quotes=data.get("normalize_quotes", False),
        )
    
    def __repr__(self) -> str:
        """Human-readable representation."""
        parts = []
        for key, val in self.to_dict().items():
            parts.append(f"{key}={val!r}")
        return f"FilterSpec({', '.join(parts)})" if parts else "FilterSpec()"
