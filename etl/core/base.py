"""
Base Transform Class
====================

Abstract base class for all ETL transforms.
Follows the GlueETL pattern: INPUTS dict → transform() → OUTPUTS dict
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, TypeVar

import polars as pl

from etl.core.config import FeatureConfig, TransformConfig
from etl.core.enums import ProcessingMode

# Type alias for transform state
StateDict = dict[str, Any]


@dataclass
class TransformContext:
    """
    Context passed to transforms during execution.
    
    Contains metadata about the current execution that transforms
    may need but shouldn't have to manage themselves.
    """
    # Execution metadata
    execution_id: str  # Unique ID for this execution
    execution_time: datetime = field(default_factory=datetime.now)
    
    # Processing context
    batch_index: int = 0  # Current batch number (for batched execution)
    total_batches: Optional[int] = None  # Total batches if known
    
    # Partition context (when processing specific partitions)
    partition_values: Optional[dict[str, Any]] = None
    
    # Feature configuration
    feature_config: FeatureConfig = field(default_factory=FeatureConfig)
    
    # Custom metadata
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseTransform(ABC):
    """
    Abstract base class for all transforms.
    
    Transforms implement the core ETL logic. They:
    1. Receive resolved inputs as a dictionary of DataFrames/LazyFrames
    2. Apply transformation logic
    3. Return outputs as a dictionary of DataFrames/LazyFrames
    
    Example:
        class OrderbookFeatureTransform(BaseTransform):
            def transform(self, inputs: dict[str, pl.LazyFrame], context: TransformContext) -> dict[str, pl.LazyFrame]:
                bronze = inputs["bronze_orderbook"]
                features = self.compute_features(bronze)
                return {"silver_features": features}
    """
    
    def __init__(self, config: TransformConfig):
        """
        Initialize transform with configuration.
        
        Args:
            config: Transform configuration with I/O declarations
        """
        self.config = config
        self._state: StateDict = {}
        self._initialized = False
    
    @property
    def name(self) -> str:
        """Transform name from config."""
        return self.config.name
    
    @property
    def processing_mode(self) -> ProcessingMode:
        """Processing mode from config."""
        return self.config.mode
    
    # =========================================================================
    # Core Transform Methods
    # =========================================================================
    
    @abstractmethod
    def transform(
        self,
        inputs: dict[str, pl.LazyFrame],
        context: TransformContext,
    ) -> dict[str, pl.LazyFrame]:
        """
        Apply transformation logic.
        
        This is the main method that subclasses must implement.
        
        Args:
            inputs: Dictionary mapping input names to LazyFrames.
                    Keys match InputConfig.name from the config.
            context: Execution context with metadata.
        
        Returns:
            Dictionary mapping output names to LazyFrames.
            Keys must match OutputConfig.name from the config.
        
        Raises:
            ValueError: If required inputs are missing or invalid.
            RuntimeError: If transformation fails.
        """
        pass
    
    def validate_inputs(self, inputs: dict[str, pl.LazyFrame]) -> None:
        """
        Validate that all required inputs are present and valid.
        
        Called before transform(). Override to add custom validation.
        
        Args:
            inputs: Dictionary of input LazyFrames.
        
        Raises:
            ValueError: If validation fails.
        """
        required_inputs = set(self.config.inputs.keys())
        provided_inputs = set(inputs.keys())
        
        missing = required_inputs - provided_inputs
        if missing:
            raise ValueError(
                f"Transform '{self.name}' missing required inputs: {missing}"
            )
    
    def validate_outputs(self, outputs: dict[str, pl.LazyFrame]) -> None:
        """
        Validate that all declared outputs are produced.
        
        Called after transform(). Override to add custom validation.
        
        Args:
            outputs: Dictionary of output LazyFrames.
        
        Raises:
            ValueError: If validation fails.
        """
        expected_outputs = set(self.config.outputs.keys())
        produced_outputs = set(outputs.keys())
        
        missing = expected_outputs - produced_outputs
        if missing:
            raise ValueError(
                f"Transform '{self.name}' did not produce expected outputs: {missing}"
            )
    
    # =========================================================================
    # Lifecycle Methods
    # =========================================================================
    
    def initialize(self, context: TransformContext) -> None:
        """
        Initialize the transform before execution.
        
        Called once before any batches are processed.
        Override to perform setup like loading models, caches, etc.
        
        Args:
            context: Execution context.
        """
        self._initialized = True
    
    def finalize(self, context: TransformContext) -> None:
        """
        Finalize the transform after execution.
        
        Called once after all batches are processed.
        Override to perform cleanup, final aggregations, etc.
        
        Args:
            context: Execution context.
        """
        pass
    
    # =========================================================================
    # State Management (for stateful transforms)
    # =========================================================================
    
    def get_state(self) -> StateDict:
        """
        Get current transform state.
        
        State is serializable and can be persisted/restored for:
        - Checkpoint recovery
        - Streaming continuity
        - Incremental processing
        
        Returns:
            Dictionary of state values.
        """
        return self._state.copy()
    
    def set_state(self, state: StateDict) -> None:
        """
        Restore transform state.
        
        Called when resuming from checkpoint or restoring streaming state.
        
        Args:
            state: Previously saved state dictionary.
        """
        self._state = state.copy()
    
    def update_state(self, key: str, value: Any) -> None:
        """
        Update a single state value.
        
        Args:
            key: State key.
            value: State value.
        """
        self._state[key] = value
    
    def get_state_value(self, key: str, default: Any = None) -> Any:
        """
        Get a single state value.
        
        Args:
            key: State key.
            default: Default value if key not found.
        
        Returns:
            State value or default.
        """
        return self._state.get(key, default)
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def add_partition_columns(
        self,
        df: pl.LazyFrame,
        timestamp_col: str = "timestamp",
    ) -> pl.LazyFrame:
        """
        Add standard partition columns (year, month, day) from timestamp.
        
        Args:
            df: Input LazyFrame.
            timestamp_col: Name of the timestamp column.
        
        Returns:
            LazyFrame with partition columns added.
        """
        return df.with_columns([
            pl.col(timestamp_col).dt.year().alias("year"),
            pl.col(timestamp_col).dt.month().alias("month"),
            pl.col(timestamp_col).dt.day().alias("day"),
        ])
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"


class StatefulTransform(BaseTransform):
    """
    Base class for stateful transforms that maintain state across batches.
    
    Provides additional infrastructure for:
    - Checkpointing
    - Incremental processing
    - Watermark tracking
    
    Example:
        class StreamingOFITransform(StatefulTransform):
            def transform(self, inputs, context):
                # Access previous state
                prev_bids = self.get_state_value("prev_bids", {})
                # ... compute OFI using previous state
                # Update state for next batch
                self.update_state("prev_bids", current_bids)
                return outputs
    """
    
    def __init__(self, config: TransformConfig):
        super().__init__(config)
        self._watermark: Optional[datetime] = None
        self._checkpoint_count = 0
    
    @property
    def watermark(self) -> Optional[datetime]:
        """Current watermark (latest processed timestamp)."""
        return self._watermark
    
    def update_watermark(self, timestamp: datetime) -> None:
        """
        Update the watermark if the new timestamp is later.
        
        Args:
            timestamp: New potential watermark.
        """
        if self._watermark is None or timestamp > self._watermark:
            self._watermark = timestamp
    
    def get_state(self) -> StateDict:
        """Get state including watermark."""
        state = super().get_state()
        state["_watermark"] = self._watermark.isoformat() if self._watermark else None
        state["_checkpoint_count"] = self._checkpoint_count
        return state
    
    def set_state(self, state: StateDict) -> None:
        """Restore state including watermark."""
        watermark_str = state.pop("_watermark", None)
        self._watermark = datetime.fromisoformat(watermark_str) if watermark_str else None
        self._checkpoint_count = state.pop("_checkpoint_count", 0)
        super().set_state(state)
    
    def should_checkpoint(self) -> bool:
        """
        Check if it's time to create a checkpoint.
        
        Uses checkpoint_interval from config.
        
        Returns:
            True if checkpoint should be created.
        """
        interval = self.config.checkpoint_interval
        if interval is None:
            return False
        return self._checkpoint_count >= interval
    
    def mark_checkpoint(self) -> None:
        """Reset checkpoint counter after creating a checkpoint."""
        self._checkpoint_count = 0
    
    def increment_checkpoint_counter(self) -> None:
        """Increment checkpoint counter (called after each batch)."""
        self._checkpoint_count += 1


# Type variable for transform factory functions
T = TypeVar("T", bound=BaseTransform)
