"""Configuration management for Daedalus."""
import os
import yaml
from pathlib import Path
from typing import Optional, Dict, Any, Literal
from pydantic import BaseModel, Field


class CoinbaseConfig(BaseModel):
    """Coinbase Advanced Trade API configuration."""
    api_key: str = Field(..., description="Coinbase Advanced Trade API key")
    api_secret: str = Field(..., description="Coinbase Advanced Trade API secret (PEM format)")
    product_ids: list[str] = Field(default_factory=lambda: ["BTC-USD", "ETH-USD"])
    channels: list[str] = Field(default_factory=lambda: ["ticker", "level2", "market_trades"])
    ws_url: str = "wss://advanced-trade-ws.coinbase.com"
    level2_batch_size: int = Field(default=10, description="Max products per level2 subscription (Coinbase limit)")


class CcxtExchangeConfig(BaseModel):
    """Configuration for a single CCXT exchange."""
    api_key: str = ""
    api_secret: str = ""
    password: str = ""
    # Map of method name (e.g. watchTicker) to list of symbols
    channels: Dict[str, list[str]] = Field(default_factory=dict)
    # Extra ccxt config options
    options: Dict[str, Any] = Field(default_factory=dict)
    # Limit number of bids/asks in orderbook snapshots (None = unlimited)
    max_orderbook_depth: Optional[int] = None


class CcxtConfig(BaseModel):
    """CCXT configuration."""
    # Map of exchange_id to its config
    exchanges: Dict[str, CcxtExchangeConfig] = Field(default_factory=dict)


class S3Config(BaseModel):
    """S3-specific configuration."""
    bucket: str = ""
    region: Optional[str] = None  # Auto-detected if None
    aws_access_key_id: Optional[str] = None  # Uses environment/IAM role if None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    endpoint_url: Optional[str] = None  # For S3-compatible services (MinIO, etc.)
    max_pool_connections: int = 50  # boto3 connection pool size (default 10 too low for threading)


class PathConfig(BaseModel):
    """Customizable path structure for data organization."""
    # Ingestion layer paths (relative to storage root)
    raw_dir: str = "raw"  # Base directory for raw ingestion data
    active_subdir: str = "active"  # Subdirectory for actively writing segments
    ready_subdir: str = "ready"  # Subdirectory for segments ready for ETL
    processing_subdir: str = "processing"  # Subdirectory for ETL in-progress
    
    # ETL layer paths (relative to storage root)
    processed_dir: str = "processed"  # Base directory for processed/transformed data
    
    # Data tier paths (customizable naming)
    # Default: Medallion architecture (bronze/silver/gold)
    # Change these to use your own naming convention
    tier_raw: str = "bronze"      # Raw data tier (ingested data)
    tier_features: str = "silver"  # Feature-enriched data tier
    tier_aggregates: str = "gold"  # Aggregated/business-ready data tier
    
    # State management paths
    state_dir: str = "temp/state"  # Directory for state checkpoints


class StateManagementConfig(BaseModel):
    """Configuration for ETL state persistence and recovery."""
    enabled: bool = Field(default=True, description="Enable state persistence")
    auto_save_on_shutdown: bool = Field(default=True, description="Auto-save state on graceful shutdown")
    checkpoint_interval_seconds: int = Field(default=60, description="Seconds between auto-checkpoints")
    max_state_files: int = Field(default=5, description="Max state files to keep (for rotation)")
    state_dir: str = Field(default="temp/state", description="Directory for state files (relative to storage root)")


class StorageLayerConfig(BaseModel):
    """Storage configuration for a specific layer (ingestion or ETL)."""
    backend: Literal["local", "s3"] = "local"
    base_dir: str = "./data"
    s3: Optional[S3Config] = Field(default_factory=S3Config)


class SyncConfig(BaseModel):
    """
    Sync job configuration for data synchronization between storage backends.
    
    This is separate from ETL storage because:
    - ETL may run fully local for performance
    - Sync job handles local→S3 uploads on a schedule
    - Allows different sync strategies (upload, download, bidirectional)
    """
    enabled: bool = Field(default=False, description="Enable sync job")
    
    # Source storage (where to read from)
    source: StorageLayerConfig = Field(
        default_factory=lambda: StorageLayerConfig(backend="local", base_dir="./data")
    )
    
    # Destination storage (where to write to)
    destination: StorageLayerConfig = Field(
        default_factory=lambda: StorageLayerConfig(
            backend="s3",
            base_dir="market-data-vault",
            s3=S3Config(bucket="market-data-vault")
        )
    )
    
    # Sync options
    compact_before_upload: bool = Field(default=True, description="Compact parquet files before upload")
    delete_after_transfer: bool = Field(default=True, description="Delete source files after successful transfer")
    target_file_size_mb: int = Field(default=100, description="Target file size for compaction")
    max_workers: int = Field(default=5, description="Parallel transfer threads")
    interval_seconds: int = Field(default=300, description="Seconds between sync runs in continuous mode")
    
    # Paths to sync (if empty, uses defaults)
    paths: list[str] = Field(
        default_factory=list,
        description="Specific paths to sync. Empty = sync all processed data"
    )


class StorageConfig(BaseModel):
    """
    Explicit storage configuration per layer.
    
    Each layer (ingestion, ETL input, ETL output) has its own storage backend.
    This provides maximum flexibility:
    - All local: Fast development/testing
    - All S3: Fully cloud-native
    - Hybrid: Ingest to local (fast), ETL to S3 (durable)
    - Multi-bucket: Different S3 buckets for raw vs processed
    
    All paths are relative to base_dir (local) or bucket (S3).
    """
    # Ingestion layer storage (where raw segments are written)
    ingestion_storage: StorageLayerConfig = Field(
        default_factory=lambda: StorageLayerConfig(backend="local", base_dir="./data")
    )
    
    # ETL input storage (where ETL reads raw segments from)
    etl_storage_input: StorageLayerConfig = Field(
        default_factory=lambda: StorageLayerConfig(backend="local", base_dir="./data")
    )
    
    # ETL output storage (where processed data is written)
    etl_storage_output: StorageLayerConfig = Field(
        default_factory=lambda: StorageLayerConfig(backend="local", base_dir="./data")
    )
    
    # Sync job configuration (for local→S3 uploads)
    sync: SyncConfig = Field(default_factory=SyncConfig)
    
    # Path structure configuration (applies to all storage backends)
    paths: PathConfig = Field(default_factory=PathConfig)


class IngestionConfig(BaseModel):
    """Ingestion layer configuration."""
    # Raw landing format: "ndjson" (legacy) or "parquet" (recommended)
    raw_format: Literal["ndjson", "parquet"] = Field(
        default="ndjson",
        description="Raw data landing format. 'parquet' is 5-10x smaller with ZSTD compression."
    )
    batch_size: int = 100
    flush_interval_seconds: float = 5.0
    queue_maxsize: int = 10000
    enable_fsync: bool = True
    auto_reconnect: bool = True
    max_reconnect_attempts: int = 10
    reconnect_delay: float = 5.0
    segment_max_mb: int = 100  # Max size in MB before rotating segment
    # Parquet-specific options
    parquet_compression: str = Field(default="zstd", description="Parquet compression codec")
    parquet_compression_level: int = Field(default=3, description="Compression level (1-22 for zstd)")
    # Partitioning options for raw data landing
    partition_by: list[str] = Field(
        default=["exchange", "symbol"],
        description=(
            "Columns to partition raw data by (Hive-style). "
            "Default ['exchange', 'symbol'] creates paths like: "
            "orderbook/exchange=binanceus/symbol=BTC~USDT/segment_*.parquet"
        )
    )
    enable_date_partition: bool = Field(
        default=False,
        description="Add date partition (year/month/day) for time-based organization"
    )


class ChannelETLConfig(BaseModel):
    """
    Per-channel ETL configuration.
    
    .. deprecated:: 2025.12
        This class is deprecated. Use :class:`FeatureConfigOptions` instead for
        unified feature configuration across all ETL pipelines.
        
        Migration guide:
        - Move `processor_options` parameters to the `features:` section in config.yaml
        - Use `config.to_feature_config()` to get FeatureConfig for TransformExecutor
        - Use `config.to_stateful_processor_config()` for StatefulFeatureProcessor
        - Use `config.to_state_config()` for streaming SymbolState
    """
    enabled: bool = True
    partition_cols: Optional[list[str]] = None  # e.g., ["product_id", "date"]
    processor_options: Dict[str, Any] = Field(default_factory=dict)


class ETLConfig(BaseModel):
    """
    ETL layer configuration.
    
    .. deprecated:: 2025.12
        The per-channel `processor_options` pattern is deprecated.
        Use :class:`FeatureConfigOptions` (via `config.features`) instead for
        unified feature configuration.
        
        The `state`, `compression`, and `delete_after_processing` options
        remain valid for ETL orchestration settings.
    """
    compression: str = "zstd"
    schedule_cron: Optional[str] = None  # e.g., "0 * * * *" for hourly
    delete_after_processing: bool = True  # Delete raw segments after ETL
    
    # State management (still valid - used for checkpoint/resume)
    state: StateManagementConfig = Field(default_factory=StateManagementConfig)
    
    # Channel-specific configuration
    # DEPRECATED: Use config.features instead for feature parameters
    # These defaults are kept for backwards compatibility only
    channels: Dict[str, ChannelETLConfig] = Field(default_factory=lambda: {
        "ticker": ChannelETLConfig(
            partition_cols=["exchange", "symbol", "date"],
            processor_options={"add_derived_fields": True}
        ),
        "orderbook": ChannelETLConfig(
            partition_cols=["exchange", "symbol", "date"],
            processor_options={
                # DEPRECATED: Use config.features instead
                # These are kept for backwards compatibility only
                "compute_features": True,
                "max_levels": 20,
                "bands_bps": [5, 10, 25, 50, 100],
                "horizons": [5, 15, 60, 300, 900],
                "bar_durations": [60, 300, 900, 3600],
                "enable_stateful": True,
                "ofi_levels": 10,
                "ofi_decay_alpha": 0.5,
                "use_dynamic_spread_regime": True,
                "spread_regime_window": 300,
                "spread_tight_percentile": 0.2,
                "spread_wide_percentile": 0.8,
                "tight_spread_threshold": 0.0001,
                "kyle_lambda_window": 300,
                "enable_vpin": True,
                "vpin_bucket_volume": 1.0,
                "vpin_window_buckets": 50,
            }
        ),
        "trades": ChannelETLConfig(
            partition_cols=["exchange", "symbol", "date"],
            processor_options={"add_derived_fields": True}
        ),
    })


class FeatureConfigOptions(BaseModel):
    """
    Feature computation configuration (loaded from YAML).
    
    Maps to etl.core.config.FeatureConfig for the new ETL framework.
    These settings control which features are computed and their parameters.
    
    This is the UNIFIED feature configuration that should be used by all
    ETL scripts. It replaces the per-channel processor_options pattern.
    
    Research References:
        - Cont et al. (2014): OFI price impact
        - Kyle & Obizhaeva (2016): Microstructure invariance
        - Zhang et al. (2019): DeepLOB - 20 levels captures "walls"
        - Xu et al. (2019): Multi-level OFI with decay
        - Easley et al. (2012): VPIN for flow toxicity
        - López de Prado (2018): Volume clocks
    """
    # Feature categories to enable
    # Options: structural, dynamic, rolling, bars, advanced
    categories: list[str] = Field(
        default=["structural", "dynamic", "rolling"],
        description="Feature categories to compute. Options: structural, dynamic, rolling, bars, advanced"
    )
    
    # ==========================================================================
    # ORDERBOOK DEPTH PARAMETERS
    # ==========================================================================
    depth_levels: int = Field(
        default=20,
        description="Number of orderbook price levels to process (Zhang et al. 2019: 20 captures 'walls')"
    )
    
    ofi_levels: int = Field(
        default=10,
        description="Number of levels for multi-level OFI calculation (Xu et al. 2019: 10 levels with decay)"
    )
    
    bands_bps: list[int] = Field(
        default=[5, 10, 25, 50, 100],
        description="Liquidity bands in basis points for depth aggregation (wider for crypto volatility)"
    )
    
    # ==========================================================================
    # ROLLING WINDOW PARAMETERS
    # ==========================================================================
    rolling_windows: list[int] = Field(
        default=[5, 15, 60, 300, 900],
        description="Rolling window sizes in seconds [5s micro, 15s short, 60s standard, 5min medium, 15min trend]"
    )
    
    # ==========================================================================
    # BAR AGGREGATION PARAMETERS
    # ==========================================================================
    bar_interval_seconds: int = Field(
        default=60,
        description="Default bar interval in seconds (used if bar_durations not specified)"
    )
    bar_durations: list[int] = Field(
        default=[60, 300, 900, 3600],
        description="Bar durations in seconds [1min, 5min, 15min, 1hr] for gold-tier aggregation"
    )
    
    # ==========================================================================
    # OFI / MLOFI PARAMETERS (Cont 2014, Xu 2019)
    # ==========================================================================
    ofi_decay_alpha: float = Field(
        default=0.5,
        description="Exponential decay alpha for MLOFI level weighting: w_i = exp(-alpha * i)"
    )
    
    # ==========================================================================
    # KYLE'S LAMBDA PARAMETERS (Kyle & Obizhaeva 2016)
    # ==========================================================================
    kyle_lambda_window: int = Field(
        default=300,
        description="Rolling window in seconds for Kyle's Lambda (price impact) estimation"
    )
    
    # ==========================================================================
    # VPIN PARAMETERS (Easley et al. 2012)
    # ==========================================================================
    enable_vpin: bool = Field(
        default=True,
        description="Enable VPIN (Volume-Synchronized Probability of Informed Trading) calculation"
    )
    
    vpin_bucket_size: float = Field(
        default=1.0,
        description="VPIN volume bucket size (normalize by typical trade size)"
    )
    
    vpin_window_buckets: int = Field(
        default=50,
        description="Number of volume buckets in VPIN rolling window"
    )
    
    # ==========================================================================
    # SPREAD REGIME PARAMETERS
    # ==========================================================================
    use_dynamic_spread_regime: bool = Field(
        default=True,
        description="Use dynamic percentile-based spread regime detection"
    )
    
    spread_regime_window: int = Field(
        default=300,
        description="Window in seconds for spread percentile calculation"
    )
    
    spread_tight_percentile: float = Field(
        default=0.2,
        description="Percentile threshold for 'tight' spread regime (bottom 20%)"
    )
    
    spread_wide_percentile: float = Field(
        default=0.8,
        description="Percentile threshold for 'wide' spread regime (top 20%)"
    )
    
    spread_tight_threshold_bps: float = Field(
        default=5.0,
        description="Fallback static threshold: spread below this (bps) is 'tight'"
    )
    spread_wide_threshold_bps: float = Field(
        default=20.0,
        description="Fallback static threshold: spread above this (bps) is 'wide'"
    )
    
    # ==========================================================================
    # STATEFUL FEATURE TOGGLES
    # ==========================================================================
    enable_stateful: bool = Field(
        default=True,
        description="Enable stateful features (OFI, MLOFI, regime tracking, Kyle's Lambda)"
    )
    
    # ==========================================================================
    # STORAGE OPTIMIZATION
    # ==========================================================================
    drop_raw_book_arrays: bool = Field(
        default=True,
        description=(
            "Drop raw bids/asks arrays after feature extraction. "
            "Set to True (default) to reduce silver tier storage ~60-80%. "
            "Set to False if downstream processes need the full orderbook arrays."
        )
    )


class DaedalusConfig(BaseModel):
    """Root configuration for Daedalus."""
    # Data sources
    coinbase: Optional[CoinbaseConfig] = None
    ccxt: Optional[CcxtConfig] = None
    
    # Storage backend
    storage: StorageConfig = Field(default_factory=StorageConfig)
    
    # Ingestion settings
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    
    # ETL settings
    etl: ETLConfig = Field(default_factory=ETLConfig)
    
    # Feature computation settings (for new ETL framework)
    features: FeatureConfigOptions = Field(default_factory=FeatureConfigOptions)
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    def to_feature_config(self) -> "FeatureConfig":
        """
        Convert feature options to etl.core.config.FeatureConfig.
        
        This bridges the YAML config to the ETL framework's FeatureConfig.
        
        Returns:
            FeatureConfig instance for use with TransformExecutor.
        """
        from etl.core.config import FeatureConfig
        from etl.core.enums import FeatureCategory
        
        # Map string category names to enum values
        category_map = {
            "structural": FeatureCategory.STRUCTURAL,
            "dynamic": FeatureCategory.DYNAMIC,
            "rolling": FeatureCategory.ROLLING,
            "bars": FeatureCategory.BARS,
            "aggregates": FeatureCategory.BARS,  # Alias
            "advanced": FeatureCategory.ADVANCED,
        }
        
        categories = set()
        for cat_name in self.features.categories:
            if cat_name.lower() in category_map:
                categories.add(category_map[cat_name.lower()])
        
        return FeatureConfig(
            categories=categories,
            depth_levels=self.features.depth_levels,
            ofi_levels=self.features.ofi_levels,
            bands_bps=self.features.bands_bps,
            rolling_windows=self.features.rolling_windows,
            bar_interval_seconds=self.features.bar_interval_seconds,
            bar_durations=self.features.bar_durations,
            ofi_decay_alpha=self.features.ofi_decay_alpha,
            kyle_lambda_window=self.features.kyle_lambda_window,
            enable_vpin=self.features.enable_vpin,
            vpin_bucket_size=self.features.vpin_bucket_size,
            vpin_window_buckets=self.features.vpin_window_buckets,
            use_dynamic_spread_regime=self.features.use_dynamic_spread_regime,
            spread_regime_window=self.features.spread_regime_window,
            spread_tight_percentile=self.features.spread_tight_percentile,
            spread_wide_percentile=self.features.spread_wide_percentile,
            spread_tight_threshold_bps=self.features.spread_tight_threshold_bps,
            spread_wide_threshold_bps=self.features.spread_wide_threshold_bps,
            enable_stateful=self.features.enable_stateful,
            drop_raw_book_arrays=self.features.drop_raw_book_arrays,
        )
    
    def to_stateful_processor_config(self) -> "StatefulProcessorConfig":
        """
        Convert feature options to StatefulProcessorConfig for batch processing.
        
        This bridges the YAML config to the stateful feature processor.
        
        Returns:
            StatefulProcessorConfig instance for use with StatefulFeatureProcessor.
        """
        from etl.features.stateful import StatefulProcessorConfig
        
        return StatefulProcessorConfig(
            horizons=self.features.rolling_windows,
            ofi_levels=self.features.ofi_levels,
            ofi_decay_alpha=self.features.ofi_decay_alpha,
            spread_regime_window=self.features.spread_regime_window,
            spread_tight_percentile=self.features.spread_tight_percentile,
            spread_wide_percentile=self.features.spread_wide_percentile,
            use_dynamic_spread_regime=self.features.use_dynamic_spread_regime,
            tight_spread_threshold=self.features.spread_tight_threshold_bps / 10000,  # bps to ratio
            kyle_lambda_window=self.features.kyle_lambda_window,
            enable_vpin=self.features.enable_vpin,
            vpin_bucket_volume=self.features.vpin_bucket_size,
            vpin_window_buckets=self.features.vpin_window_buckets,
        )
    
    def to_state_config(self) -> "StateConfig":
        """
        Convert feature options to StateConfig for streaming processing.
        
        This bridges the YAML config to the streaming state processor.
        
        Returns:
            StateConfig instance for use with SymbolState.
        """
        from etl.features.state import StateConfig
        
        return StateConfig(
            horizons=self.features.rolling_windows,
            bar_durations=self.features.bar_durations,
            max_levels=self.features.depth_levels,
            ofi_levels=self.features.ofi_levels,
            ofi_decay_alpha=self.features.ofi_decay_alpha,
            bands_bps=self.features.bands_bps,
            use_dynamic_spread_regime=self.features.use_dynamic_spread_regime,
            spread_regime_window=self.features.spread_regime_window,
            spread_tight_percentile=self.features.spread_tight_percentile,
            spread_wide_percentile=self.features.spread_wide_percentile,
            tight_spread_threshold=self.features.spread_tight_threshold_bps / 10000,  # bps to ratio
            kyle_lambda_window=self.features.kyle_lambda_window,
            vpin_bucket_count=self.features.vpin_window_buckets,
            vpin_window_buckets=self.features.vpin_window_buckets,
        )


def load_config(config_path: Optional[str] = None) -> DaedalusConfig:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config file. If None, looks for:
            1. DAEDALUS_CONFIG environment variable
            2. ./config/config.yaml
            3. ~/.daedalus/config.yaml
    
    Returns:
        DaedalusConfig instance
    """
    if config_path is None:
        # Check environment variable
        config_path = os.environ.get("DAEDALUS_CONFIG")
        
        if config_path is None:
            # Check default locations
            candidates = [
                Path("./config/config.yaml"),
                Path.home() / ".daedalus" / "config.yaml",
            ]
            for candidate in candidates:
                if candidate.exists():
                    config_path = str(candidate)
                    break
    
    if config_path is None:
        raise FileNotFoundError(
            "No config file found. Set DAEDALUS_CONFIG or create config/config.yaml"
        )
    
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        yaml_data = yaml.safe_load(f)
    
    return DaedalusConfig(**yaml_data)


def save_example_config(output_path: str = "./config/config.example.yaml"):
    """
    Save an example configuration file.
    
    Args:
        output_path: Where to save the example config
    """
    example = {
        "coinbase": {
            "api_key": "organizations/xxx/apiKeys/xxx",
            "api_secret": "-----BEGIN EC PRIVATE KEY-----\\n...\\n-----END EC PRIVATE KEY-----\\n",
            "product_ids": ["BTC-USD", "ETH-USD"],
            "channels": ["ticker", "level2", "market_trades"],
        },
        "ingestion": {
            "output_dir": "./data/raw",
            "batch_size": 100,
            "flush_interval_seconds": 5.0,
            "queue_maxsize": 10000,
            "enable_fsync": True,
        },
        "etl": {
            "input_dir": "./data/raw",
            "output_dir": "./data/processed",
            "compression": "zstd",
        },
        "log_level": "INFO",
    }
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        yaml.dump(example, f, default_flow_style=False, sort_keys=False)
    
    print(f"Example config saved to {output_path}")
