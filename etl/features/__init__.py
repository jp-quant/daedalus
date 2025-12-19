"""
Features Module
===============

Feature extraction and computation for orderbook data.

Modules:
- snapshot: Row-by-row structural feature extraction (for streaming/dict data)
- streaming: Online algorithms for rolling statistics
- state: Stateful symbol processing with bar building  
- orderbook: Vectorized structural features (Polars, for batch)
- stateful: Stateful feature processor for batch processing
"""

# Row-by-row extraction (for dict-based streaming)
from .snapshot import extract_orderbook_features

# Streaming statistics
from .streaming import (
    KyleLambdaEstimator,
    RegimeStats,
    RollingPercentile,
    RollingSum,
    RollingWelford,
    TimeBasedRollingSum,
    VolumeBarBuilder,
    VPINCalculator,
    Welford,
)

# Stateful symbol processing (for real-time)
from .state import BarBuilder, StateConfig, SymbolState

# Vectorized batch processing (Polars)
from .orderbook import (
    compute_rolling_features,
    extract_structural_features,
)

# Stateful batch processing
from .stateful import StatefulFeatureProcessor, StatefulProcessorConfig

__all__ = [
    # Snapshot extraction (dict-based)
    "extract_orderbook_features",
    # Streaming statistics
    "Welford",
    "TimeBasedRollingSum",
    "RollingWelford",
    "RollingSum",
    "RegimeStats",
    "RollingPercentile",
    "KyleLambdaEstimator",
    "VPINCalculator",
    "VolumeBarBuilder",
    # State management
    "SymbolState",
    "StateConfig",
    "BarBuilder",
    # Vectorized (Polars)
    "extract_structural_features",
    "compute_rolling_features",
    # Stateful batch
    "StatefulFeatureProcessor",
    "StatefulProcessorConfig",
]
