"""
Examples of using unified storage with Daedalus.

Demonstrates the new unified storage architecture that works
seamlessly with both local filesystem and S3.
"""
import logging
import asyncio
import polars as pl
from pathlib import Path

from config import load_config
from storage.factory import create_storage_backend, get_ingestion_path, get_etl_output_path
from ingestion.orchestrators import IngestionPipeline
from etl.job import ETLJob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# =============================================================================
# Example 1: Unified Storage Backend Usage
# =============================================================================

def example_unified_storage():
    """
    Demonstrate unified storage backend that works with both local and S3.
    
    Just change storage.backend in config.yaml to switch!
    """
    logger.info("=" * 80)
    logger.info("Example 1: Unified Storage Backend")
    logger.info("=" * 80)
    
    # Load config (reads storage.backend setting)
    config = load_config()
    
    # Create storage backend (auto-selects LocalStorage or S3Storage)
    storage = create_storage_backend(config)
    
    logger.info(f"  Storage backend: {storage.backend_type}")
    logger.info(f"  Base path: {storage.base_path}")
    
    # Write some data
    test_data = b"Hello from unified storage!"
    test_path = "test/unified_example.txt"
    
    full_path = storage.write_bytes(test_data, test_path)
    logger.info(f"  Wrote to: {full_path}")
    
    # Read it back
    content = storage.read_bytes(test_path)
    logger.info(f"  Read back: {content.decode()}")
    
    # Check existence
    exists = storage.exists(test_path)
    logger.info(f"  File exists: {exists}")
    
    # List files
    files = storage.list_files(path="test", pattern="*.txt")
    logger.info(f"  Found {len(files)} files in test/")
    
    # Clean up
    storage.delete(test_path)
    logger.info(f"  Deleted {test_path}")


# =============================================================================
# Example 2: Path Resolution
# =============================================================================

def example_path_resolution():
    """
    Show how paths are resolved from config.
    
    All paths are relative to storage root (base_dir or bucket).
    """
    logger.info("=" * 80)
    logger.info("Example 2: Path Resolution")
    logger.info("=" * 80)
    
    config = load_config()
    storage = create_storage_backend(config)
    
    # Get ingestion paths for coinbase
    active_path = get_ingestion_path(config, "coinbase", state="active")
    ready_path = get_ingestion_path(config, "coinbase", state="ready")
    
    # Get ETL output path
    output_path = get_etl_output_path(config, "coinbase")
    
    logger.info(f"  Backend: {storage.backend_type}")
    logger.info(f"  Active:  {storage.get_full_path(active_path)}")
    logger.info(f"  Ready:   {storage.get_full_path(ready_path)}")
    logger.info(f"  Output:  {storage.get_full_path(output_path)}")
    
    # Paths work consistently across local and S3
    logger.info(f"\n  Relative paths:")
    logger.info(f"    Active:  {active_path}")
    logger.info(f"    Ready:   {ready_path}")
    logger.info(f"    Output:  {output_path}")


# =============================================================================
# Example 3: Ingestion with Unified Storage
# =============================================================================

async def example_unified_ingestion():
    """
    Run ingestion with unified storage.
    
    Same code works for local and S3!
    """
    logger.info("=" * 80)
    logger.info("Example 3: Unified Ingestion")
    logger.info("=" * 80)
    
    config = load_config()
    
    # Create pipeline (auto-uses correct storage)
    pipeline = IngestionPipeline(config)
    
    logger.info(f"  Storage: {pipeline.storage.backend_type}")
    logger.info(f"  Sources: {len(pipeline.collectors)} configured")
    
    # Start pipeline
    # await pipeline.start()
    # await pipeline.wait_for_shutdown()
    # await pipeline.stop()
    
    logger.info("  (Pipeline setup complete - uncomment to run)")


# =============================================================================
# Example 4: ETL with Unified Storage
# =============================================================================

def example_unified_etl():
    """
    Run ETL with unified storage.
    
    Same code works for local and S3!
    """
    logger.info("=" * 80)
    logger.info("Example 4: Unified ETL")
    logger.info("=" * 80)
    
    config = load_config()
    
    # Create storage backend
    storage = create_storage_backend(config)
    
    # Get paths
    input_path = get_ingestion_path(config, "coinbase", state="ready")
    output_path = get_etl_output_path(config, "coinbase")
    
    logger.info(f"  Storage: {storage.backend_type}")
    logger.info(f"  Input:   {input_path}")
    logger.info(f"  Output:  {output_path}")
    
    # Convert channel config
    channel_config = {
        channel_name: {
            "partition_cols": channel_cfg.partition_cols,
            "processor_options": channel_cfg.processor_options,
        }
        for channel_name, channel_cfg in config.etl.channels.items()
        if channel_cfg.enabled
    }
    
    # Create ETL job
    job = ETLJob(
        storage=storage,
        input_path=input_path,
        output_path=output_path,
        delete_after_processing=config.etl.delete_after_processing,
        channel_config=channel_config,
    )
    
    # Process segments
    # job.process_all()
    
    logger.info("  (ETL job setup complete - uncomment to run)")


# =============================================================================
# Example 5: Polars Integration
# =============================================================================

def example_polars_integration():
    """
    Read/write Parquet files with Polars using unified storage.
    
    Works with both local and S3!
    """
    logger.info("=" * 80)
    logger.info("Example 5: Polars Integration")
    logger.info("=" * 80)
    
    config = load_config()
    storage = create_storage_backend(config)
    
    # Create sample data
    df = pl.DataFrame({
        "product_id": ["BTC-USD", "ETH-USD"] * 50,
        "price": [50000.0, 3000.0] * 50,
        "timestamp": pl.datetime_range(
            start=pl.datetime(2025, 12, 3),
            end=pl.datetime(2025, 12, 3, 1, 39),
            interval="1m"
        )[:100]
    })
    
    # Write to storage
    test_path = "test/sample_data.parquet"
    full_path = storage.get_full_path(test_path)
    
    logger.info(f"  Writing {len(df)} rows to {full_path}")
    
    # Get storage options for Polars
    storage_options = storage.get_storage_options()
    
    if storage_options:
        # S3: use storage options
        df.write_parquet(full_path, storage_options=storage_options)
    else:
        # Local: direct write
        df.write_parquet(full_path)
    
    # Read back
    logger.info(f"  Reading from {full_path}")
    
    if storage_options:
        df_read = pl.read_parquet(full_path, storage_options=storage_options)
    else:
        df_read = pl.read_parquet(full_path)
    
    logger.info(f"  Read {len(df_read)} rows")
    logger.info(f"  Schema: {df_read.schema}")


# =============================================================================
# Example 6: Configuration Examples
# =============================================================================

def example_configuration_scenarios():
    """
    Show different configuration scenarios.
    
    See config/config.examples.yaml for full examples.
    """
    logger.info("=" * 80)
    logger.info("Example 6: Configuration Scenarios")
    logger.info("=" * 80)
    
    logger.info("""
    SCENARIO 1: All-Local Storage
    ------------------------------
    storage:
      backend: "local"
      base_dir: "F:/"
    
    Result: F:/raw/active/coinbase/segment_*.ndjson
    
    
    SCENARIO 2: All-S3 Storage
    ---------------------------
    storage:
      backend: "s3"
      base_dir: "my-bucket"
      s3:
        bucket: "my-bucket"
        region: "us-east-1"
    
    Result: s3://my-bucket/raw/active/coinbase/segment_*.ndjson
    
    
    SCENARIO 3: Custom Paths
    -------------------------
    storage:
      backend: "local"
      base_dir: "./data"
      paths:
        raw_dir: "ingested"
        active_subdir: "streaming"
        ready_subdir: "completed"
        processed_dir: "curated"
    
    Result: ./data/ingested/streaming/coinbase/segment_*.ndjson
    
    
    TO SWITCH: Just change storage.backend in config.yaml!
    No code changes needed!
    """)


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    # Run synchronous examples
    example_unified_storage()
    print()
    example_path_resolution()
    print()
    example_unified_etl()
    print()
    example_polars_integration()
    print()
    example_configuration_scenarios()
    
    # Run async example
    # asyncio.run(example_unified_ingestion())
    
    logger.info("\n" + "=" * 80)
    logger.info("All examples completed!")
    logger.info("=" * 80)
    logger.info("\nNEXT STEPS:")
    logger.info("1. Review config/config.examples.yaml for full configuration examples")
    logger.info("2. Edit config/config.yaml to set storage.backend ('local' or 's3')")
    logger.info("3. Run: python scripts/run_ingestion.py")
    logger.info("4. Run: python scripts/run_etl_watcher.py")



# =============================================================================
# Example 1: Direct S3 Storage Manager Usage
# =============================================================================

def example_s3_manager_direct():
    """
    Use S3StorageManager directly for basic operations.
    
    Assumes: AWS credentials in environment or IAM role attached.
    """
    logger.info("=" * 80)
    logger.info("Example 1: Direct S3 Storage Manager Usage")
    logger.info("=" * 80)
    
    # Initialize S3 manager
    s3 = S3StorageManager(
        bucket="my-data-lake",
        prefix="daedalus",  # All operations under s3://my-data-lake/daedalus/
        region="us-east-1",  # Optional, auto-detected if None
    )
    
    # Write bytes
    data = b"Hello, S3 from Daedalus!"
    s3_path = s3.write_bytes(data, "test/hello.txt")
    logger.info(f"  Wrote to: {s3_path}")
    
    # Read bytes
    content = s3.read_bytes("test/hello.txt")
    logger.info(f"  Read back: {content.decode()}")
    
    # Check existence
    exists = s3.exists("test/hello.txt")
    logger.info(f"  File exists: {exists}")
    
    # List objects
    objects = s3.list_objects(prefix="test", suffix=".txt")
    logger.info(f"  Found {len(objects)} objects in test/")
    
    # Delete
    s3.delete("test/hello.txt")
    logger.info(f"  Deleted test/hello.txt")


# =============================================================================
# Example 2: S3 + Polars Integration
# =============================================================================

def example_s3_polars_integration():
    """
    Use S3 with Polars for reading/writing Parquet files.
    
    s3fs provides seamless integration with Polars.
    """
    logger.info("=" * 80)
    logger.info("Example 2: S3 + Polars Integration")
    logger.info("=" * 80)
    
    # Initialize S3 manager
    s3 = S3StorageManager(
        bucket="my-data-lake",
        prefix="daedalus"
    )
    
    # Create sample data
    df = pl.DataFrame({
        "product_id": ["BTC-USD", "ETH-USD", "SOL-USD"] * 100,
        "price": [50000.0, 3000.0, 100.0] * 100,
        "timestamp": pl.datetime_range(
            start=pl.datetime(2025, 12, 1),
            end=pl.datetime(2025, 12, 1, 4, 59),
            interval="1m"
        )[:300]
    })
    
    # Write to S3 using Polars
    s3_path = s3.get_full_path("test/sample_data.parquet")
    logger.info(f"  Writing {len(df)} rows to {s3_path}")
    
    df.write_parquet(
        s3_path,
        storage_options=s3.get_storage_options()
    )
    
    # Read from S3 using Polars
    logger.info(f"  Reading back from {s3_path}")
    df_read = pl.read_parquet(
        s3_path,
        storage_options=s3.get_storage_options()
    )
    
    logger.info(f"  Read {len(df_read)} rows")
    logger.info(f"  Schema: {df_read.schema}")
    
    # Scan with lazy evaluation (efficient for large datasets)
    logger.info(f"  Lazy scan with filter")
    lf = pl.scan_parquet(
        s3_path,
        storage_options=s3.get_storage_options()
    )
    
    btc_data = lf.filter(pl.col("product_id") == "BTC-USD").collect()
    logger.info(f"  BTC-USD records: {len(btc_data)}")


# =============================================================================
# Example 3: S3 Log Writer (Ingestion)
# =============================================================================

async def example_s3_log_writer():
    """
    Use S3LogWriter for ingestion layer (writes NDJSON segments to S3).
    
    Same interface as LogWriter but writes to S3.
    """
    logger.info("=" * 80)
    logger.info("Example 3: S3 Log Writer (Ingestion)")
    logger.info("=" * 80)
    
    # Initialize S3 manager
    s3 = S3StorageManager(
        bucket="my-data-lake",
        prefix="daedalus"
    )
    
    # Create S3 log writer
    writer = S3LogWriter(
        s3_manager=s3,
        source_name="coinbase",
        batch_size=10,
        flush_interval_seconds=2.0,
        segment_max_mb=1,  # Small for demo
    )
    
    # Start writer
    await writer.start()
    logger.info("  S3 log writer started")
    
    # Write some records
    for i in range(50):
        record = {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": 50000.0 + i,
            "sequence": i,
        }
        await writer.write(record)
    
    logger.info("  Wrote 50 records")
    
    # Check stats
    stats = writer.get_stats()
    logger.info(f"  Stats: {stats}")
    
    # Stop writer (triggers final flush and segment finalization)
    await writer.stop()
    logger.info("  S3 log writer stopped")
    
    # List segments in ready/
    ready_segments = s3.list_objects(
        prefix="raw/ready/coinbase",
        suffix=".ndjson"
    )
    logger.info(f"  Found {len(ready_segments)} segments in ready/")
    for seg in ready_segments[:3]:
        logger.info(f"    - {seg['Key']} ({seg['Size'] / 1024:.1f} KB)")


# =============================================================================
# Example 4: S3 Parquet Writer (ETL)
# =============================================================================

def example_s3_parquet_writer():
    """
    Use S3ParquetWriter for ETL layer (writes partitioned Parquet to S3).
    
    Same interface as ParquetWriter but writes to S3.
    """
    logger.info("=" * 80)
    logger.info("Example 4: S3 Parquet Writer (ETL)")
    logger.info("=" * 80)
    
    # Initialize S3 manager
    s3 = S3StorageManager(
        bucket="my-data-lake",
        prefix="daedalus"
    )
    
    # Create S3 parquet writer
    writer = S3ParquetWriter(
        s3_manager=s3,
        base_prefix="processed/coinbase",
        compression="zstd"
    )
    
    # Create sample processed data
    df = pl.DataFrame({
        "product_id": ["BTC-USD"] * 100 + ["ETH-USD"] * 100,
        "date": ["2025-12-01"] * 100 + ["2025-12-02"] * 100,
        "hour": [14] * 50 + [15] * 50 + [9] * 50 + [10] * 50,
        "price": list(range(200)),
        "volume": [100.0 + i for i in range(200)],
    }).to_pandas()
    
    # Write with partitioning
    logger.info(f"  Writing {len(df)} rows with partitioning")
    writer.write(
        data=df,
        output_path="ticker",  # Writes to: processed/coinbase/ticker/
        partition_cols=["product_id", "date", "hour"]
    )
    
    logger.info(f"  Stats: {writer.get_stats()}")
    
    # List created files
    files = s3.list_objects(
        prefix="processed/coinbase/ticker",
        suffix=".parquet"
    )
    logger.info(f"  Created {len(files)} parquet files:")
    for f in files[:5]:
        logger.info(f"    - {f['Key']} ({f['Size'] / 1024:.1f} KB)")


# =============================================================================
# Example 5: Storage Factory Pattern (Recommended)
# =============================================================================

async def example_storage_factory():
    """
    Use StorageFactory for automatic local/S3 writer selection.
    
    This is the recommended approach - write code once, switch backends via config.
    """
    logger.info("=" * 80)
    logger.info("Example 5: Storage Factory Pattern")
    logger.info("=" * 80)
    
    # Load config (reads storage.backend setting)
    config = load_config()
    
    # Create factory
    factory = StorageFactory(config)
    logger.info(f"  Storage backend: {factory.storage_backend}")
    
    # Create log writer (auto-selects LogWriter or S3LogWriter)
    log_writer = factory.create_log_writer(source_name="coinbase")
    logger.info(f"  Created log writer: {type(log_writer).__name__}")
    
    # Use log writer (same code works for both local and S3)
    await log_writer.start()
    
    for i in range(10):
        await log_writer.write({
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": 50000.0 + i,
        })
    
    await log_writer.stop()
    logger.info(f"  Log writer stats: {log_writer.get_stats()}")
    
    # Create parquet writer (auto-selects ParquetWriter or S3ParquetWriter)
    parquet_writer = factory.create_parquet_writer()
    logger.info(f"  Created parquet writer: {type(parquet_writer).__name__}")
    
    # Use parquet writer (same code works for both local and S3)
    df = pl.DataFrame({
        "product_id": ["BTC-USD"] * 10,
        "price": list(range(10)),
    }).to_pandas()
    
    parquet_writer.write(
        data=df,
        output_path="test/data",
        partition_cols=["product_id"]
    )
    logger.info(f"  Parquet writer stats: {parquet_writer.get_stats()}")


# =============================================================================
# Example 6: Config-Based S3 Setup
# =============================================================================

def example_config_based_setup():
    """
    Show how to configure S3 in config.yaml.
    
    This prints example configuration that you should add to config.yaml.
    """
    logger.info("=" * 80)
    logger.info("Example 6: Config-Based S3 Setup")
    logger.info("=" * 80)
    
    example_config = """
# In config/config.yaml:

storage:
  backend: "s3"  # Change from "local" to "s3"
  
  # S3 configuration
  s3:
    enabled: true
    bucket: "my-data-lake-bucket"
    prefix: "daedalus"  # All data under s3://bucket/daedalus/
    region: "us-east-1"  # Optional, auto-detected if null
    
    # Option 1: Use IAM role (recommended for EC2/ECS)
    aws_access_key_id: null
    aws_secret_access_key: null
    
    # Option 2: Use explicit credentials (for local testing)
    # aws_access_key_id: "AKIAIOSFODNN7EXAMPLE"
    # aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    
    # Option 3: Use environment variables
    # export AWS_ACCESS_KEY_ID=...
    # export AWS_SECRET_ACCESS_KEY=...
    # (leave both null in config)

# All ingestion/ETL paths now interpreted as S3 prefixes
ingestion:
  # output_dir ignored when backend="s3"
  batch_size: 100
  segment_max_mb: 100

etl:
  # input_dir/output_dir ignored when backend="s3"
  compression: "zstd"
    """
    
    logger.info(example_config)
    
    logger.info("\n  After configuring, all existing code automatically uses S3:")
    logger.info("    - run_ingestion.py → writes to S3")
    logger.info("    - run_etl.py → reads from S3, writes to S3")
    logger.info("    - No code changes needed!")


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    import asyncio
    
    # Choose which example to run
    
    # Synchronous examples
    # example_s3_manager_direct()
    # example_s3_polars_integration()
    # example_s3_parquet_writer()
    # example_config_based_setup()
    
    # Async examples
    # asyncio.run(example_s3_log_writer())
    # asyncio.run(example_storage_factory())
    
    logger.info("\nS3 storage examples ready!")
    logger.info("Uncomment one example to run.")
    logger.info("\nREQUIREMENTS:")
    logger.info("  pip install boto3 s3fs")
    logger.info("\nCREDENTIALS:")
    logger.info("  - Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables, OR")
    logger.info("  - Configure AWS CLI (aws configure), OR")
    logger.info("  - Use IAM role (EC2/ECS/Lambda)")
