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


class ChannelETLConfig(BaseModel):
    """Per-channel ETL configuration."""
    enabled: bool = True
    partition_cols: Optional[list[str]] = None  # e.g., ["product_id", "date"]
    processor_options: Dict[str, Any] = Field(default_factory=dict)


class ETLConfig(BaseModel):
    """ETL layer configuration."""
    compression: str = "zstd"
    schedule_cron: Optional[str] = None  # e.g., "0 * * * *" for hourly
    delete_after_processing: bool = True  # Delete raw segments after ETL
    
    # Channel-specific configuration
    channels: Dict[str, ChannelETLConfig] = Field(default_factory=lambda: {
        "ticker": ChannelETLConfig(
            partition_cols=["exchange", "symbol", "date"],
            processor_options={"add_derived_fields": True}
        ),
        "orderbook": ChannelETLConfig(
            partition_cols=["exchange", "symbol", "date"],
            processor_options={"add_derived_fields": True}
        ),
        "trades": ChannelETLConfig(
            partition_cols=["exchange", "symbol", "date"],
            processor_options={"add_derived_fields": True}
        ),
    })


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
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


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
