"""
Enums for ETL Framework
=======================

Type-safe enumerations for all ETL configuration options.
Eliminates hardcoded strings throughout the codebase.
"""

from enum import Enum, auto


class DataTier(str, Enum):
    """
    Medallion architecture data tiers.
    
    - BRONZE: Raw ingested data
    - SILVER: Cleaned and feature-enriched data
    - GOLD: Aggregated/business-ready data
    """
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    
    def __str__(self) -> str:
        return self.value


class StorageBackendType(str, Enum):
    """
    Storage backend types.
    
    Used to identify which storage implementation is active
    without string comparisons.
    """
    LOCAL = "local"
    S3 = "s3"
    
    def __str__(self) -> str:
        return self.value


class DataFormat(str, Enum):
    """
    Data file formats supported by the ETL framework.
    """
    PARQUET = "parquet"
    NDJSON = "ndjson"
    CSV = "csv"
    JSON = "json"
    
    def __str__(self) -> str:
        return self.value


class CompressionCodec(str, Enum):
    """
    Compression codecs for Parquet files.
    
    Reference: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html
    """
    ZSTD = "zstd"
    SNAPPY = "snappy"
    GZIP = "gzip"
    LZ4 = "lz4"
    BROTLI = "brotli"
    UNCOMPRESSED = "uncompressed"
    
    def __str__(self) -> str:
        return self.value


class WriteMode(str, Enum):
    """
    Write modes for transform outputs.
    
    - APPEND: Add new files to existing partition (default)
    - OVERWRITE: Replace all files in affected partitions
    - OVERWRITE_PARTITION: Replace only the specific partitions being written
    - MERGE: Upsert based on key columns (requires key_cols in OutputConfig)
    """
    APPEND = "append"
    OVERWRITE = "overwrite"
    OVERWRITE_PARTITION = "overwrite_partition"
    MERGE = "merge"
    
    def __str__(self) -> str:
        return self.value


class ProcessingMode(str, Enum):
    """
    Processing modes for transforms.
    
    - BATCH: Standard batch processing (default)
    - STREAMING: Future - continuous processing with checkpoints
    - HYBRID: Combines vectorized + stateful processing
    """
    BATCH = "batch"
    STREAMING = "streaming"  # Future expansion
    HYBRID = "hybrid"
    
    def __str__(self) -> str:
        return self.value


class PartitionGranularity(str, Enum):
    """
    Time-based partition granularity options.
    """
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    
    def __str__(self) -> str:
        return self.value


class FeatureCategory(str, Enum):
    """
    Categories of features for orderbook processing.
    Used for selective feature computation.
    """
    STRUCTURAL = "structural"      # Static per-snapshot features
    DYNAMIC = "dynamic"            # Delta-based features (OFI, returns)
    ROLLING = "rolling"            # Time-windowed statistics
    BARS = "bars"                  # Aggregated time bars
    ADVANCED = "advanced"          # VPIN, Kyle's Lambda, etc.
    
    def __str__(self) -> str:
        return self.value


class SpreadRegime(str, Enum):
    """
    Spread regime classifications.
    """
    TIGHT = "tight"
    NORMAL = "normal"
    WIDE = "wide"
    
    def __str__(self) -> str:
        return self.value


class TradeSide(str, Enum):
    """
    Trade side (aggressor side).
    """
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"
    
    def __str__(self) -> str:
        return self.value
