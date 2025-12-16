"""
Examples of using explicit storage configuration with Daedalus.

Demonstrates how the explicit per-layer storage architecture works
with both local and S3 backends.
"""
import logging
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import load_config
from storage.factory import (
    create_ingestion_storage,
    create_etl_storage_input,
    create_etl_storage_output,
    get_ingestion_path,
    get_etl_output_path
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Example 1: Explicit Storage Configuration
# =====================================

def example_explicit_storage():
    """
    Demonstrate explicit per-layer storage configuration.
    
    Each layer can use different backends for maximum flexibility.
    """
    logger.info("=" * 80)
    logger.info("Example 1: Explicit Storage Configuration")
    logger.info("=" * 80)
    
    # Load config
    config = load_config()
    
    # Create storage backends for each layer
    ingestion_storage = create_ingestion_storage(config)
    etl_input_storage = create_etl_storage_input(config)
    etl_output_storage = create_etl_storage_output(config)
    
    logger.info(f"Ingestion storage: {ingestion_storage.backend_type} @ {ingestion_storage.base_path}")
    logger.info(f"ETL input storage: {etl_input_storage.backend_type} @ {etl_input_storage.base_path}")
    logger.info(f"ETL output storage: {etl_output_storage.backend_type} @ {etl_output_storage.base_path}")
    
    # Test write to ingestion storage
    test_data = b"Hello from explicit storage!"
    test_path = "test/explicit_example.txt"
    
    full_path = ingestion_storage.write_bytes(test_data, test_path)
    logger.info(f"✓ Written to: {full_path}")
    
    # Test read
    read_data = ingestion_storage.read_bytes(test_path)
    logger.info(f"✓ Read back: {read_data.decode()}")
    
    # Cleanup
    ingestion_storage.delete(test_path)
    logger.info("✓ Cleaned up")
    
    logger.info("✓ Example 1 complete\n")


# Example 2: Path Resolution
# =====================================

def example_path_resolution():
    """
    Demonstrate path resolution for different layers.
    """
    logger.info("=" * 80)
    logger.info("Example 2: Path Resolution")
    logger.info("=" * 80)
    
    config = load_config()
    
    # Get paths for ingestion
    active_path = get_ingestion_path(config, "coinbase", state="active")
    ready_path = get_ingestion_path(config, "coinbase", state="ready")
    
    logger.info(f"Ingestion active path:  {active_path}")
    logger.info(f"Ingestion ready path:   {ready_path}")
    
    # Get paths for ETL
    etl_output_path = get_etl_output_path(config, "coinbase")
    
    logger.info(f"ETL output path:        {etl_output_path}")
    
    logger.info("✓ Example 2 complete\n")


# Example 3: Working with Different Backends
# =====================================

def example_backend_flexibility():
    """
    Show how to work with different storage backends.
    """
    logger.info("=" * 80)
    logger.info("Example 3: Backend Flexibility")
    logger.info("=" * 80)
    
    config = load_config()
    
    # Get storage for each layer
    ingestion = create_ingestion_storage(config)
    etl_output = create_etl_storage_output(config)
    
    # Show configuration
    logger.info("Current configuration:")
    logger.info(f"  Ingestion: {ingestion.backend_type}")
    logger.info(f"  ETL Output: {etl_output.backend_type}")
    
    # Example: Hybrid mode (local ingestion, S3 output)
    logger.info("\nTo enable hybrid mode, update config.yaml:")
    logger.info("""
    storage:
      ingestion_storage:
        backend: "local"
        base_dir: "F:/"
      
      etl_storage_output:
        backend: "s3"
        base_dir: "market-data-vault"
        s3:
          bucket: "market-data-vault"
    """)
    
    logger.info("✓ Example 3 complete\n")


def main():
    """Run all examples."""
    try:
        # Example 1: Explicit storage
        example_explicit_storage()
        
        # Example 2: Path resolution
        example_path_resolution()
        
        # Example 3: Backend flexibility
        example_backend_flexibility()
        
    except Exception as e:
        logger.error(f"Error running examples: {e}", exc_info=True)


if __name__ == "__main__":
    main()
