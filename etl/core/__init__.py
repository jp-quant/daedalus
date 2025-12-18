"""
ETL Core Framework
==================

The core framework for building ETL transforms. Provides:
- BaseTransform: Abstract base class for all transforms
- InputConfig/OutputConfig: Declarative I/O configuration
- TransformExecutor: Runs transforms with storage backend support
- Enums for type-safe configuration

Design Philosophy:
- No hardcoded strings - use enums
- No logic derived from naming conventions
- Explicit configuration over convention
- Framework-agnostic (not tied to "channels" or specific data sources)
- Scaffolded for future real-time support
"""

from etl.core.enums import (
    CompressionCodec,
    DataFormat,
    FeatureCategory,
    PartitionGranularity,
    ProcessingMode,
    SpreadRegime,
    StorageBackendType,
    TradeSide,
    WriteMode,
)
from etl.core.config import (
    FeatureConfig,
    FilterSpec,
    InputConfig,
    OutputConfig,
    StorageConfig,
    TransformConfig,
)
from etl.core.base import BaseTransform, StatefulTransform, TransformContext
from etl.core.executor import BatchExecutor, TransformExecutor
from etl.core.registry import TransformRegistry, get_registry, register_transform

__all__ = [
    # Enums
    "StorageBackendType",
    "DataFormat",
    "CompressionCodec",
    "WriteMode",
    "ProcessingMode",
    "PartitionGranularity",
    "FeatureCategory",
    "SpreadRegime",
    "TradeSide",
    # Config
    "StorageConfig",
    "InputConfig",
    "OutputConfig",
    "TransformConfig",
    "FeatureConfig",
    "FilterSpec",
    # Base
    "BaseTransform",
    "StatefulTransform",
    "TransformContext",
    # Executor
    "TransformExecutor",
    "BatchExecutor",
    # Registry
    "TransformRegistry",
    "get_registry",
    "register_transform",
]
