"""
Unit Tests for ETL Core Module
==============================

Tests for enums, config, base classes, and registry.
"""

import pytest
from datetime import datetime

import polars as pl

from etl.core.enums import (
    DataTier,
    StorageBackendType,
    DataFormat,
    CompressionCodec,
    WriteMode,
    ProcessingMode,
    PartitionGranularity,
    FeatureCategory,
    SpreadRegime,
    TradeSide,
)
from etl.core.config import (
    TransformConfig,
    InputConfig,
    OutputConfig,
    FilterSpec,
    FeatureConfig,
)
from etl.core.base import (
    BaseTransform,
    StatefulTransform,
    TransformContext,
)
from etl.core.registry import (
    TransformRegistry,
    register_transform,
    get_registry,
)


class TestEnums:
    """Tests for enum values and conversions."""
    
    def test_data_tier_values(self):
        """Test DataTier enum values."""
        assert DataTier.BRONZE.value == "bronze"
        assert DataTier.SILVER.value == "silver"
        assert DataTier.GOLD.value == "gold"
    
    def test_storage_backend_type_values(self):
        """Test StorageBackendType enum values."""
        assert StorageBackendType.LOCAL.value == "local"
        assert StorageBackendType.S3.value == "s3"
    
    def test_data_format_values(self):
        """Test DataFormat enum values."""
        assert DataFormat.PARQUET.value == "parquet"
        assert DataFormat.NDJSON.value == "ndjson"
        assert DataFormat.CSV.value == "csv"
    
    def test_compression_codec_values(self):
        """Test CompressionCodec enum values."""
        assert CompressionCodec.ZSTD.value == "zstd"
        assert CompressionCodec.SNAPPY.value == "snappy"
        assert CompressionCodec.GZIP.value == "gzip"
    
    def test_write_mode_values(self):
        """Test WriteMode enum values."""
        assert WriteMode.APPEND.value == "append"
        assert WriteMode.OVERWRITE.value == "overwrite"
        assert WriteMode.MERGE.value == "merge"
    
    def test_processing_mode_values(self):
        """Test ProcessingMode enum values."""
        assert ProcessingMode.BATCH.value == "batch"
        assert ProcessingMode.STREAMING.value == "streaming"
        assert ProcessingMode.HYBRID.value == "hybrid"
    
    def test_feature_category_values(self):
        """Test FeatureCategory enum values."""
        assert FeatureCategory.STRUCTURAL.value == "structural"
        assert FeatureCategory.DYNAMIC.value == "dynamic"
        assert FeatureCategory.ROLLING.value == "rolling"
    
    def test_spread_regime_values(self):
        """Test SpreadRegime enum values."""
        assert SpreadRegime.TIGHT.value == "tight"
        assert SpreadRegime.NORMAL.value == "normal"
        assert SpreadRegime.WIDE.value == "wide"
    
    def test_trade_side_values(self):
        """Test TradeSide enum values."""
        assert TradeSide.BUY.value == "buy"
        assert TradeSide.SELL.value == "sell"
        assert TradeSide.UNKNOWN.value == "unknown"
    
    def test_enum_str_conversion(self):
        """Test that enums convert to strings properly."""
        assert str(DataTier.BRONZE) == "bronze"
        assert str(CompressionCodec.ZSTD) == "zstd"


class TestConfig:
    """Tests for configuration classes."""
    
    def test_input_config_defaults(self):
        """Test InputConfig default values."""
        config = InputConfig(name="test", path="test/path")
        
        assert config.name == "test"
        assert config.path == "test/path"
        assert config.format == DataFormat.PARQUET
        assert config.filters is None
        assert config.columns is None
    
    def test_output_config_defaults(self):
        """Test OutputConfig default values."""
        config = OutputConfig(name="test", path="test/path")
        
        assert config.name == "test"
        assert config.path == "test/path"
        assert config.format == DataFormat.PARQUET
        assert config.mode == WriteMode.APPEND
        assert config.compression == CompressionCodec.ZSTD
    
    def test_transform_config_creation(self):
        """Test TransformConfig creation."""
        config = TransformConfig(
            name="test_transform",
            inputs={
                "bronze": InputConfig(name="bronze", path="bronze/data")
            },
            outputs={
                "silver": OutputConfig(name="silver", path="silver/data")
            },
        )
        
        assert config.name == "test_transform"
        assert len(config.inputs) == 1
        assert len(config.outputs) == 1
        assert "bronze" in config.inputs
        assert "silver" in config.outputs
    
    def test_feature_config_defaults(self):
        """Test FeatureConfig default values."""
        config = FeatureConfig()
        
        assert config.depth_levels == 10
        assert 60 in config.rolling_windows
        assert FeatureCategory.STRUCTURAL in config.categories
    
    def test_filter_spec(self):
        """Test FilterSpec configuration."""
        filter_spec = FilterSpec(
            exchange="binance",
            symbol="BTC/USDT",
            year=2025,
        )
        
        assert filter_spec.exchange == "binance"
        assert filter_spec.symbol == "BTC/USDT"
        assert filter_spec.year == 2025
        
        # Test to_partition_filters
        filters = filter_spec.to_partition_filters()
        assert ("exchange", "=", "binance") in filters


class TestTransformContext:
    """Tests for TransformContext."""
    
    def test_context_creation(self):
        """Test TransformContext creation."""
        context = TransformContext(
            execution_id="test-123",
            batch_index=0,
            total_batches=10,
        )
        
        assert context.execution_id == "test-123"
        assert context.batch_index == 0
        assert context.total_batches == 10
        assert isinstance(context.execution_time, datetime)
    
    def test_context_partition_values(self):
        """Test TransformContext with partition values."""
        context = TransformContext(
            execution_id="test-456",
            partition_values={"date": "2025-01-01", "exchange": "binance"},
        )
        
        assert context.partition_values["date"] == "2025-01-01"
        assert context.partition_values["exchange"] == "binance"
    
    def test_context_feature_config(self):
        """Test TransformContext with FeatureConfig."""
        feature_config = FeatureConfig(depth_levels=10)
        context = TransformContext(
            execution_id="test-789",
            feature_config=feature_config,
        )
        
        assert context.feature_config.depth_levels == 10


class TestBaseTransform:
    """Tests for BaseTransform abstract class."""
    
    def test_simple_transform_implementation(self):
        """Test implementing a simple transform."""
        
        class SimpleTransform(BaseTransform):
            def transform(
                self,
                inputs: dict[str, pl.LazyFrame],
                context: TransformContext,
            ) -> dict[str, pl.LazyFrame]:
                bronze = inputs["bronze"]
                silver = bronze.with_columns([
                    (pl.col("value") * 2).alias("doubled")
                ])
                return {"silver": silver}
        
        config = TransformConfig(
            name="simple",
            inputs={"bronze": InputConfig(name="bronze", path="bronze")},
            outputs={"silver": OutputConfig(name="silver", path="silver")},
        )
        
        transform = SimpleTransform(config)
        assert transform.name == "simple"
        assert transform.processing_mode == ProcessingMode.BATCH
    
    def test_validate_inputs_missing(self):
        """Test input validation fails for missing inputs."""
        
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(
            name="test",
            inputs={
                "input1": InputConfig(name="input1", path="path1"),
                "input2": InputConfig(name="input2", path="path2"),
            },
            outputs={},
        )
        
        transform = TestTransform(config)
        
        with pytest.raises(ValueError, match="missing required inputs"):
            transform.validate_inputs({"input1": pl.LazyFrame()})


class TestBaseTransformLifecycle:
    """Tests for BaseTransform lifecycle methods."""
    
    def test_validate_outputs_success(self):
        """Test output validation passes with all expected outputs."""
        from etl.core.config import InputConfig, OutputConfig
        
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {"output1": pl.LazyFrame(), "output2": pl.LazyFrame()}
        
        config = TransformConfig(
            name="test",
            inputs={"input1": InputConfig(name="input1", path="path1")},
            outputs={
                "output1": OutputConfig(name="output1", path="path1"),
                "output2": OutputConfig(name="output2", path="path2"),
            },
        )
        
        transform = TestTransform(config)
        outputs = transform.transform({}, None)
        
        # Should not raise
        transform.validate_outputs(outputs)
    
    def test_validate_outputs_missing(self):
        """Test output validation fails for missing outputs."""
        from etl.core.config import InputConfig, OutputConfig
        
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {"output1": pl.LazyFrame()}  # Missing output2
        
        config = TransformConfig(
            name="test",
            inputs={},
            outputs={
                "output1": OutputConfig(name="output1", path="path1"),
                "output2": OutputConfig(name="output2", path="path2"),
            },
        )
        
        transform = TestTransform(config)
        outputs = transform.transform({}, None)
        
        with pytest.raises(ValueError, match="did not produce expected outputs"):
            transform.validate_outputs(outputs)
    
    def test_initialize_sets_flag(self):
        """Test initialize sets _initialized flag."""
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestTransform(config)
        
        assert transform._initialized == False
        
        context = TransformContext(execution_id="test-123")
        transform.initialize(context)
        
        assert transform._initialized == True
    
    def test_finalize_callable(self):
        """Test finalize can be called without error."""
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestTransform(config)
        context = TransformContext(execution_id="test-123")
        
        # Should not raise
        transform.finalize(context)
    
    def test_add_partition_columns(self):
        """Test add_partition_columns adds year, month, day."""
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestTransform(config)
        
        # Create test data with timestamp column
        df = pl.LazyFrame({
            "timestamp": [
                datetime(2025, 1, 15, 10, 30),
                datetime(2025, 6, 20, 14, 45),
            ]
        })
        
        result = transform.add_partition_columns(df, "timestamp").collect()
        
        assert "year" in result.columns
        assert "month" in result.columns
        assert "day" in result.columns
        assert result["year"][0] == 2025
        assert result["month"][0] == 1
        assert result["day"][0] == 15
        assert result["month"][1] == 6


class TestBaseTransformStateManagement:
    """Tests for BaseTransform state management methods."""
    
    def test_get_state_empty(self):
        """Test get_state returns empty dict initially."""
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestTransform(config)
        
        state = transform.get_state()
        assert state == {}
    
    def test_set_state(self):
        """Test set_state restores state."""
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestTransform(config)
        
        transform.set_state({"key1": "value1", "key2": 42})
        
        assert transform.get_state() == {"key1": "value1", "key2": 42}
    
    def test_update_state(self):
        """Test update_state updates single value."""
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestTransform(config)
        
        transform.update_state("key1", "value1")
        transform.update_state("key2", 42)
        
        assert transform.get_state_value("key1") == "value1"
        assert transform.get_state_value("key2") == 42
    
    def test_get_state_value_default(self):
        """Test get_state_value returns default if key not found."""
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestTransform(config)
        
        assert transform.get_state_value("nonexistent") is None
        assert transform.get_state_value("nonexistent", "default") == "default"
    
    def test_state_is_copied(self):
        """Test that get/set_state copies the dictionary."""
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestTransform(config)
        
        original = {"key": "value"}
        transform.set_state(original)
        
        # Modify original
        original["key"] = "modified"
        
        # Transform state should not be affected
        assert transform.get_state_value("key") == "value"


class TestStatefulTransform:
    """Tests for StatefulTransform class."""
    
    def test_watermark_initial_none(self):
        """Test watermark is None initially."""
        class TestStatefulTransform(StatefulTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestStatefulTransform(config)
        
        assert transform.watermark is None
    
    def test_update_watermark(self):
        """Test update_watermark updates watermark."""
        class TestStatefulTransform(StatefulTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestStatefulTransform(config)
        
        ts1 = datetime(2025, 1, 1, 10, 0)
        ts2 = datetime(2025, 1, 1, 11, 0)
        
        transform.update_watermark(ts1)
        assert transform.watermark == ts1
        
        transform.update_watermark(ts2)
        assert transform.watermark == ts2
    
    def test_update_watermark_no_regression(self):
        """Test watermark doesn't regress to earlier timestamp."""
        class TestStatefulTransform(StatefulTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestStatefulTransform(config)
        
        ts1 = datetime(2025, 1, 1, 10, 0)
        ts2 = datetime(2025, 1, 1, 9, 0)  # Earlier
        
        transform.update_watermark(ts1)
        transform.update_watermark(ts2)
        
        assert transform.watermark == ts1  # Should still be ts1
    
    def test_checkpoint_counter(self):
        """Test checkpoint counter increments."""
        class TestStatefulTransform(StatefulTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestStatefulTransform(config)
        
        assert transform._checkpoint_count == 0
        
        transform.increment_checkpoint_counter()
        assert transform._checkpoint_count == 1
        
        transform.increment_checkpoint_counter()
        transform.increment_checkpoint_counter()
        assert transform._checkpoint_count == 3
    
    def test_should_checkpoint_no_interval(self):
        """Test should_checkpoint returns False if no interval set."""
        class TestStatefulTransform(StatefulTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestStatefulTransform(config)
        
        for _ in range(100):
            transform.increment_checkpoint_counter()
        
        assert transform.should_checkpoint() == False
    
    def test_should_checkpoint_with_interval(self):
        """Test should_checkpoint returns True when counter >= interval."""
        class TestStatefulTransform(StatefulTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={}, checkpoint_interval=5)
        transform = TestStatefulTransform(config)
        
        assert transform.should_checkpoint() == False
        
        for _ in range(5):
            transform.increment_checkpoint_counter()
        
        assert transform.should_checkpoint() == True
    
    def test_mark_checkpoint_resets_counter(self):
        """Test mark_checkpoint resets counter to 0."""
        class TestStatefulTransform(StatefulTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={}, checkpoint_interval=5)
        transform = TestStatefulTransform(config)
        
        for _ in range(5):
            transform.increment_checkpoint_counter()
        
        assert transform._checkpoint_count == 5
        
        transform.mark_checkpoint()
        assert transform._checkpoint_count == 0
    
    def test_stateful_get_state_includes_watermark(self):
        """Test get_state includes watermark and checkpoint count."""
        class TestStatefulTransform(StatefulTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestStatefulTransform(config)
        
        ts = datetime(2025, 1, 1, 10, 0)
        transform.update_watermark(ts)
        transform.increment_checkpoint_counter()
        transform.update_state("custom_key", "custom_value")
        
        state = transform.get_state()
        
        assert "_watermark" in state
        assert state["_watermark"] == ts.isoformat()
        assert "_checkpoint_count" in state
        assert state["_checkpoint_count"] == 1
        assert state["custom_key"] == "custom_value"
    
    def test_stateful_set_state_restores_watermark(self):
        """Test set_state restores watermark and checkpoint count."""
        class TestStatefulTransform(StatefulTransform):
            def transform(self, inputs, context):
                return {}
        
        config = TransformConfig(name="test", inputs={}, outputs={})
        transform = TestStatefulTransform(config)
        
        saved_state = {
            "_watermark": "2025-01-01T10:00:00",
            "_checkpoint_count": 7,
            "custom_key": "custom_value",
        }
        
        transform.set_state(saved_state)
        
        assert transform.watermark == datetime(2025, 1, 1, 10, 0)
        assert transform._checkpoint_count == 7
        assert transform.get_state_value("custom_key") == "custom_value"


class TestTransformRegistry:
    """Tests for TransformRegistry."""
    
    def test_registry_register_class(self):
        """Test registering a transform class."""
        registry = TransformRegistry()
        
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        registry.register("test_transform", TestTransform)
        
        assert registry.has_transform("test_transform")
        assert registry.get_transform_class("test_transform") == TestTransform
    
    def test_registry_create_instance(self):
        """Test creating transform instance from registry."""
        registry = TransformRegistry()
        
        class TestTransform(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        registry.register("my_transform", TestTransform)
        
        config = TransformConfig(
            name="my_transform",
            inputs={},
            outputs={},
        )
        
        transform = registry.create("my_transform", config)
        assert isinstance(transform, TestTransform)
    
    def test_registry_duplicate_registration(self):
        """Test that duplicate registration raises error."""
        registry = TransformRegistry()
        
        class Transform1(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        class Transform2(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        registry.register("same_name", Transform1)
        
        with pytest.raises(ValueError, match="already registered"):
            registry.register("same_name", Transform2)
    
    def test_registry_list(self):
        """Test listing registered transforms."""
        registry = TransformRegistry()
        
        class Transform1(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        class Transform2(BaseTransform):
            def transform(self, inputs, context):
                return {}
        
        registry.register("transform_a", Transform1)
        registry.register("transform_b", Transform2)
        
        names = registry.list_transforms()
        assert "transform_a" in names
        assert "transform_b" in names
    
    def test_global_registry_decorator(self):
        """Test the @register_transform decorator."""
        # This tests the module-level decorator
        registry = get_registry()
        
        # Check if orderbook_features is registered (from imports)
        # It should be registered when etl.transforms is imported
        assert registry.has_transform("orderbook_features")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
