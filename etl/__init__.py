"""
Daedalus ETL Layer
==================

Transform raw market data into structured datasets with rich features.

Architecture:
    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
    │   Bronze    │ -> │   Silver    │ -> │    Gold     │
    │  (raw data) │    │ (features)  │    │ (aggregates)│
    └─────────────┘    └─────────────┘    └─────────────┘

Modules:
    core/       - Framework foundation (base classes, enums, executor)
    features/   - Feature extraction (structural, rolling, stateful)
    transforms/ - Transform implementations (Bronze -> Silver -> Gold)
    utils/      - Parquet utilities (CRUD, repartition, compaction)
    legacy/     - Archived NDJSON-based pipeline (do not import)

Usage:
    from etl.core import (
        BaseTransform,
        TransformConfig,
        TransformExecutor,
        InputConfig,
        OutputConfig,
    )
    from etl.transforms import OrderbookFeatureTransform
    from etl.features import extract_structural_features
    
    # Create transform configuration
    config = TransformConfig(
        name="my_transform",
        inputs={"bronze": InputConfig(name="bronze", path="bronze/orderbook")},
        outputs={"silver": OutputConfig(name="silver", path="silver/features")},
    )
    
    # Create and execute transform
    transform = OrderbookFeatureTransform(config)
    executor = TransformExecutor()
    result = executor.execute(transform)
"""

# Core framework
from etl.core import (
    BaseTransform,
    CompressionCodec,
    DataFormat,
    FeatureCategory,
    FeatureConfig,
    FilterSpec,
    InputConfig,
    OutputConfig,
    PartitionGranularity,
    ProcessingMode,
    StatefulTransform,
    StorageBackendType,
    StorageConfig,
    TransformConfig,
    TransformContext,
    TransformExecutor,
    TransformRegistry,
    WriteMode,
    get_registry,
    register_transform,
)

# Feature extraction
from etl.features import (
    BarBuilder,
    KyleLambdaEstimator,
    RegimeStats,
    RollingSum,
    RollingWelford,
    StateConfig,
    StatefulFeatureProcessor,
    StatefulProcessorConfig,
    SymbolState,
    VolumeBarBuilder,
    VPINCalculator,
    Welford,
    compute_rolling_features,
    extract_orderbook_features,
    extract_structural_features,
)

# Transforms
from etl.transforms import OrderbookFeatureTransform

# Utilities
from etl.utils import ParquetCompactor, ParquetCRUD, Repartitioner

__all__ = [
    # Core - Enums
    "StorageBackendType",
    "DataFormat",
    "CompressionCodec",
    "WriteMode",
    "ProcessingMode",
    "PartitionGranularity",
    "FeatureCategory",
    # Core - Config
    "StorageConfig",
    "InputConfig",
    "OutputConfig",
    "TransformConfig",
    "FeatureConfig",
    "FilterSpec",
    # Core - Base classes
    "BaseTransform",
    "StatefulTransform",
    "TransformContext",
    # Core - Execution
    "TransformExecutor",
    "TransformRegistry",
    "get_registry",
    "register_transform",
    # Features - Streaming
    "Welford",
    "RollingWelford",
    "RollingSum",
    "RegimeStats",
    "KyleLambdaEstimator",
    "VPINCalculator",
    "VolumeBarBuilder",
    # Features - State
    "SymbolState",
    "StateConfig",
    "BarBuilder",
    # Features - Extraction
    "extract_orderbook_features",
    "extract_structural_features",
    "compute_rolling_features",
    # Features - Batch processing
    "StatefulFeatureProcessor",
    "StatefulProcessorConfig",
    # Transforms
    "OrderbookFeatureTransform",
    # Utilities
    "ParquetCRUD",
    "Repartitioner",
    "ParquetCompactor",
]

