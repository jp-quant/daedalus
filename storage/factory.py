"""
Storage factory for creating storage backends and path utilities.

Provides unified interface for creating appropriate storage backends
based on configuration.
"""
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import DaedalusConfig

from storage.base import StorageBackend, LocalStorage, S3Storage

logger = logging.getLogger(__name__)


def _create_backend_from_layer_config(
    layer_config,
    layer_name: str
) -> StorageBackend:
    """
    Create storage backend from layer-specific config.
    
    Args:
        layer_config: StorageLayerConfig
        layer_name: Name for logging (e.g., "ingestion", "etl_input")
    
    Returns:
        StorageBackend instance
    """
    backend = layer_config.backend
    base_dir = layer_config.base_dir
    
    if backend == "local":
        logger.info(f"[{layer_name}] Initializing local storage: {base_dir}")
        return LocalStorage(base_path=base_dir)
    
    elif backend == "s3":
        if not layer_config.s3:
            raise ValueError(f"[{layer_name}] S3 backend selected but no S3 configuration provided")
        
        # Use s3.bucket if specified, otherwise base_dir
        bucket = layer_config.s3.bucket or base_dir
        
        logger.info(f"[{layer_name}] Initializing S3 storage: {bucket}")
        return S3Storage(
            bucket=bucket,
            region=layer_config.s3.region,
            aws_access_key_id=layer_config.s3.aws_access_key_id,
            aws_secret_access_key=layer_config.s3.aws_secret_access_key,
            aws_session_token=layer_config.s3.aws_session_token,
            endpoint_url=layer_config.s3.endpoint_url,
            max_pool_connections=layer_config.s3.max_pool_connections,
        )
    
    else:
        raise ValueError(f"[{layer_name}] Unknown storage backend: {backend}")


def create_ingestion_storage(config: "DaedalusConfig") -> StorageBackend:
    """
    Create storage backend for ingestion layer.
    
    Args:
        config: Daedalus configuration
    
    Returns:
        StorageBackend instance for ingestion
    """
    return _create_backend_from_layer_config(
        config.storage.ingestion_storage,
        "ingestion"
    )


def create_etl_storage_input(config: "DaedalusConfig") -> StorageBackend:
    """
    Create storage backend for ETL input (reading raw data).
    
    Args:
        config: Daedalus configuration
    
    Returns:
        StorageBackend instance for ETL input
    """
    return _create_backend_from_layer_config(
        config.storage.etl_storage_input,
        "etl_input"
    )


def create_etl_storage_output(config: "DaedalusConfig") -> StorageBackend:
    """
    Create storage backend for ETL output (writing processed data).
    
    Args:
        config: Daedalus configuration
    
    Returns:
        StorageBackend instance for ETL output
    """
    return _create_backend_from_layer_config(
        config.storage.etl_storage_output,
        "etl_output"
    )


def create_sync_source_storage(config: "DaedalusConfig") -> StorageBackend:
    """
    Create storage backend for sync job source (where to read from).
    
    Args:
        config: Daedalus configuration
    
    Returns:
        StorageBackend instance for sync source
    """
    return _create_backend_from_layer_config(
        config.storage.sync.source,
        "sync_source"
    )


def create_sync_destination_storage(config: "DaedalusConfig") -> StorageBackend:
    """
    Create storage backend for sync job destination (where to write to).
    
    Args:
        config: Daedalus configuration
    
    Returns:
        StorageBackend instance for sync destination
    """
    return _create_backend_from_layer_config(
        config.storage.sync.destination,
        "sync_destination"
    )


def get_ingestion_path(config: "DaedalusConfig", source_name: str, state: str = "active") -> str:
    """
    Get path for ingestion layer data.
    
    Args:
        config: Daedalus configuration
        source_name: Data source name (e.g., "coinbase")
        state: "active" or "ready"
    
    Returns:
        Relative path from storage root
    """
    paths = config.storage.paths
    
    if state == "active":
        subdir = paths.active_subdir
    elif state == "ready":
        subdir = paths.ready_subdir
    else:
        raise ValueError(f"Unknown state: {state}")
    
    # Build path: raw/active/coinbase or raw/ready/coinbase
    return f"{paths.raw_dir}/{subdir}/{source_name}"


def get_etl_input_path(config: "DaedalusConfig", source_name: str) -> str:
    """
    Get path for ETL input (reads from ready/ directory).
    
    Args:
        config: Daedalus configuration
        source_name: Data source name
    
    Returns:
        Relative path from storage root
    """
    return get_ingestion_path(config, source_name, state="ready")


def get_etl_output_path(config: "DaedalusConfig", source_name: str) -> str:
    """
    Get path for ETL output (processed data).
    
    Args:
        config: Daedalus configuration
        source_name: Data source name
    
    Returns:
        Relative path from storage root
    """
    paths = config.storage.paths
    return f"{paths.processed_dir}/{source_name}"


def get_processing_path(config: "DaedalusConfig", source_name: str) -> str:
    """
    Get path for ETL processing (temporary in-progress directory).
    
    Args:
        config: Daedalus configuration
        source_name: Data source name
    
    Returns:
        Relative path from storage root
    """
    paths = config.storage.paths
    return f"{paths.raw_dir}/{paths.processing_subdir}/{source_name}"
