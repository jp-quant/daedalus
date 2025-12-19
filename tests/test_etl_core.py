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
        
        assert config.depth_levels == 20  # Zhang et al. 2019: 20 levels captures "walls"
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


class TestFeatureConfigComplete:
    """Tests for complete FeatureConfig parameters."""
    
    def test_feature_config_all_defaults(self):
        """Test FeatureConfig has all expected default values."""
        config = FeatureConfig()
        
        # Orderbook depth
        assert config.depth_levels == 20
        assert config.ofi_levels == 10
        assert config.bands_bps == [5, 10, 25, 50, 100]
        
        # Rolling windows
        assert config.rolling_windows == [5, 15, 60, 300, 900]
        
        # Bar durations
        assert config.bar_interval_seconds == 60
        assert config.bar_durations == [60, 300, 900, 3600]
        
        # OFI/MLOFI
        assert config.ofi_decay_alpha == 0.5
        
        # Kyle's Lambda
        assert config.kyle_lambda_window == 300
        
        # VPIN
        assert config.enable_vpin == True
        assert config.vpin_bucket_size == 1.0
        assert config.vpin_window_buckets == 50
        
        # Spread regime
        assert config.use_dynamic_spread_regime == True
        assert config.spread_regime_window == 300
        assert config.spread_tight_percentile == 0.2
        assert config.spread_wide_percentile == 0.8
        assert config.spread_tight_threshold_bps == 5.0
        assert config.spread_wide_threshold_bps == 20.0
        
        # Stateful toggle
        assert config.enable_stateful == True
        
        # Storage optimization
        assert config.drop_raw_book_arrays == True  # Default drops raw arrays
    
    def test_feature_config_custom_values(self):
        """Test FeatureConfig with custom values."""
        config = FeatureConfig(
            categories={FeatureCategory.STRUCTURAL, FeatureCategory.ADVANCED},
            depth_levels=30,
            ofi_levels=15,
            bands_bps=[10, 20, 50],
            rolling_windows=[10, 30, 120],
            ofi_decay_alpha=0.7,
            kyle_lambda_window=600,
            enable_vpin=False,
            vpin_bucket_size=2.5,
            spread_tight_threshold_bps=3.0,
        )
        
        assert config.depth_levels == 30
        assert config.ofi_levels == 15
        assert config.bands_bps == [10, 20, 50]
        assert config.rolling_windows == [10, 30, 120]
        assert config.ofi_decay_alpha == 0.7
        assert config.kyle_lambda_window == 600
        assert config.enable_vpin == False
        assert config.vpin_bucket_size == 2.5
        assert config.spread_tight_threshold_bps == 3.0
    
    def test_has_category(self):
        """Test has_category method."""
        config = FeatureConfig(
            categories={FeatureCategory.STRUCTURAL, FeatureCategory.DYNAMIC}
        )
        
        assert config.has_category(FeatureCategory.STRUCTURAL)
        assert config.has_category(FeatureCategory.DYNAMIC)
        assert not config.has_category(FeatureCategory.ROLLING)
        assert not config.has_category(FeatureCategory.ADVANCED)


class TestFilterSpecComplete:
    """Tests for complete FilterSpec functionality."""
    
    def test_filter_spec_exact_match(self):
        """Test FilterSpec with exact partition match."""
        filter_spec = FilterSpec(
            exchange="binanceus",
            symbol="BTC/USDT",
            year=2025,
            month=12,
            day=18,
        )
        
        filters = filter_spec.to_partition_filters()
        assert ("exchange", "=", "binanceus") in filters
        assert ("symbol", "=", "BTC/USDT") in filters
        assert ("year", "=", 2025) in filters
        assert ("month", "=", 12) in filters
        assert ("day", "=", 18) in filters
    
    def test_filter_spec_date_range(self):
        """Test FilterSpec with date range."""
        filter_spec = FilterSpec(
            exchange="binanceus",
            start_date="2025-12-01",
            end_date="2025-12-15",
        )
        
        # Date range should generate polars filter
        expr = filter_spec.to_polars_filter()
        assert expr is not None
        
        # Partition filters should not include date range
        filters = filter_spec.to_partition_filters()
        assert ("exchange", "=", "binanceus") in filters
    
    def test_filter_spec_polars_filter_exact(self):
        """Test to_polars_filter with exact values."""
        filter_spec = FilterSpec(
            exchange="binanceus",
            symbol="ETH/USDT",
        )
        
        expr = filter_spec.to_polars_filter()
        assert expr is not None
        
        # Test with actual data
        df = pl.DataFrame({
            "exchange": ["binanceus", "binanceus", "coinbase"],
            "symbol": ["ETH/USDT", "BTC/USDT", "ETH/USDT"],
            "value": [1, 2, 3],
        })
        
        filtered = df.lazy().filter(expr).collect()
        assert len(filtered) == 1
        assert filtered["value"][0] == 1
    
    def test_filter_spec_empty(self):
        """Test empty FilterSpec."""
        filter_spec = FilterSpec()
        
        assert filter_spec.to_polars_filter() is None
        assert filter_spec.to_partition_filters() == []


class TestFeatureConfigBridge:
    """Tests for config bridge methods (YAML → ETL framework)."""
    
    def test_to_feature_config_default(self):
        """Test DaedalusConfig.to_feature_config with defaults."""
        from config.config import DaedalusConfig, FeatureConfigOptions
        
        config = DaedalusConfig(
            features=FeatureConfigOptions()
        )
        
        feature_config = config.to_feature_config()
        
        # Check defaults mapped correctly
        assert feature_config.depth_levels == 20
        assert feature_config.ofi_levels == 10
        assert feature_config.bands_bps == [5, 10, 25, 50, 100]
        assert feature_config.rolling_windows == [5, 15, 60, 300, 900]
        assert feature_config.ofi_decay_alpha == 0.5
        assert feature_config.enable_vpin == True
        assert feature_config.vpin_bucket_size == 1.0
        assert feature_config.kyle_lambda_window == 300
    
    def test_to_feature_config_custom(self):
        """Test DaedalusConfig.to_feature_config with custom values."""
        from config.config import DaedalusConfig, FeatureConfigOptions
        
        config = DaedalusConfig(
            features=FeatureConfigOptions(
                categories=["structural", "advanced"],
                depth_levels=30,
                ofi_levels=15,
                rolling_windows=[10, 60, 300],
                ofi_decay_alpha=0.8,
                enable_vpin=False,
                vpin_bucket_size=2.0,
            )
        )
        
        feature_config = config.to_feature_config()
        
        assert feature_config.depth_levels == 30
        assert feature_config.ofi_levels == 15
        assert feature_config.rolling_windows == [10, 60, 300]
        assert feature_config.ofi_decay_alpha == 0.8
        assert feature_config.enable_vpin == False
        assert feature_config.vpin_bucket_size == 2.0
        
        # Check categories
        assert FeatureCategory.STRUCTURAL in feature_config.categories
        assert FeatureCategory.ADVANCED in feature_config.categories
        assert FeatureCategory.DYNAMIC not in feature_config.categories
    
    def test_to_stateful_processor_config(self):
        """Test DaedalusConfig.to_stateful_processor_config."""
        from config.config import DaedalusConfig, FeatureConfigOptions
        
        config = DaedalusConfig(
            features=FeatureConfigOptions(
                rolling_windows=[5, 30, 120],
                ofi_levels=8,
                ofi_decay_alpha=0.6,
                kyle_lambda_window=600,
                enable_vpin=True,
                vpin_bucket_size=1.5,
                vpin_window_buckets=40,
            )
        )
        
        processor_config = config.to_stateful_processor_config()
        
        assert processor_config.horizons == [5, 30, 120]
        assert processor_config.ofi_levels == 8
        assert processor_config.ofi_decay_alpha == 0.6
        assert processor_config.kyle_lambda_window == 600
        assert processor_config.enable_vpin == True
        assert processor_config.vpin_bucket_volume == 1.5
        assert processor_config.vpin_window_buckets == 40
    
    def test_to_state_config(self):
        """Test DaedalusConfig.to_state_config."""
        from config.config import DaedalusConfig, FeatureConfigOptions
        
        config = DaedalusConfig(
            features=FeatureConfigOptions(
                depth_levels=25,
                rolling_windows=[5, 30, 120],
                bar_durations=[60, 300, 600],
                ofi_levels=12,
                ofi_decay_alpha=0.4,
                bands_bps=[5, 15, 30],
                kyle_lambda_window=450,
            )
        )
        
        state_config = config.to_state_config()
        
        assert state_config.max_levels == 25
        assert state_config.horizons == [5, 30, 120]
        assert state_config.bar_durations == [60, 300, 600]
        assert state_config.ofi_levels == 12
        assert state_config.ofi_decay_alpha == 0.4
        assert state_config.bands_bps == [5, 15, 30]
        assert state_config.kyle_lambda_window == 450


class TestFeatureCategoryEnum:
    """Tests for FeatureCategory enum including aliases."""
    
    def test_bars_aggregates_alias(self):
        """Test that AGGREGATES is an alias for BARS."""
        assert FeatureCategory.BARS.value == "bars"
        assert FeatureCategory.AGGREGATES.value == "bars"
        assert FeatureCategory.BARS == FeatureCategory.AGGREGATES
    
    def test_all_categories(self):
        """Test all FeatureCategory values exist."""
        categories = [
            FeatureCategory.STRUCTURAL,
            FeatureCategory.DYNAMIC,
            FeatureCategory.ROLLING,
            FeatureCategory.BARS,
            FeatureCategory.ADVANCED,
        ]
        
        values = {"structural", "dynamic", "rolling", "bars", "advanced"}
        for cat in categories:
            assert cat.value in values


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
