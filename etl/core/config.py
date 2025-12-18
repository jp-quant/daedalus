"""
Configuration Models for ETL Framework
======================================

Declarative configuration classes for defining transform inputs and outputs.
Inspired by GlueETL pattern: INPUTS dict → transform() → OUTPUTS dict
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Union

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
    """
    # Feature categories to compute
    categories: set[FeatureCategory] = field(default_factory=lambda: {
        FeatureCategory.STRUCTURAL,
        FeatureCategory.DYNAMIC,
    })
    
    # Orderbook depth parameters
    depth_levels: int = 10  # Number of price levels to process
    
    # Rolling window parameters
    rolling_windows: list[int] = field(default_factory=lambda: [60, 300, 900])  # Seconds
    
    # Bar aggregation parameters
    bar_interval_seconds: int = 60
    
    # Advanced feature parameters
    vpin_bucket_size: int = 100  # Volume buckets for VPIN
    
    # OFI parameters
    ofi_decay_alpha: float = 0.94  # Exponential decay for MLOFI
    
    # Spread regime thresholds (in basis points)
    spread_tight_threshold_bps: float = 5.0
    spread_wide_threshold_bps: float = 20.0
    
    def has_category(self, category: FeatureCategory) -> bool:
        """Check if a feature category is enabled."""
        return category in self.categories


@dataclass
class FilterSpec:
    """
    Declarative filter specification.
    
    Used to build partition pruning filters without string manipulation.
    """
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    year: Optional[int] = None
    month: Optional[int] = None
    day: Optional[int] = None
    
    def to_polars_filter(self) -> Optional[pl.Expr]:
        """Convert to Polars filter expression."""
        conditions = []
        
        if self.exchange is not None:
            conditions.append(pl.col("exchange") == self.exchange)
        if self.symbol is not None:
            conditions.append(pl.col("symbol") == self.symbol)
        if self.year is not None:
            conditions.append(pl.col("year") == self.year)
        if self.month is not None:
            conditions.append(pl.col("month") == self.month)
        if self.day is not None:
            conditions.append(pl.col("day") == self.day)
        
        if not conditions:
            return None
        
        result = conditions[0]
        for cond in conditions[1:]:
            result = result & cond
        return result
    
    def to_partition_filters(self) -> list[tuple[str, str, Any]]:
        """Convert to partition filter tuples for PyArrow."""
        filters = []
        
        if self.exchange is not None:
            filters.append(("exchange", "=", self.exchange))
        if self.symbol is not None:
            filters.append(("symbol", "=", self.symbol))
        if self.year is not None:
            filters.append(("year", "=", self.year))
        if self.month is not None:
            filters.append(("month", "=", self.month))
        if self.day is not None:
            filters.append(("day", "=", self.day))
        
        return filters
