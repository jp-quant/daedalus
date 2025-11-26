"""
Example: Migrating to new ETL architecture

This file demonstrates how to use the new pipeline-based ETL system
with practical examples.
"""

# ============================================================================
# Example 1: Basic ETL Job (Backwards Compatible)
# ============================================================================

def example_basic_etl():
    """Same as before, but now uses pipelines internally."""
    from etl.job import ETLJob
    
    # Create job - API unchanged
    job = ETLJob(
        input_dir="data/raw/ready/coinbase",
        output_dir="data/processed",
        source="coinbase",
        delete_after_processing=True,
    )
    
    # Process all segments
    job.process_all()
    
    print("ETL complete!")


# ============================================================================
# Example 2: Custom Channel Configuration
# ============================================================================

def example_custom_config():
    """Enable advanced processing for specific channels."""
    from etl.job import ETLJob
    
    # Configure per-channel behavior
    channel_config = {
        "level2": {
            # Partition by product and date for efficient queries
            "partition_cols": ["product_id", "date"],
            
            # Enable advanced features
            "processor_options": {
                "add_derived_fields": True,
                "reconstruct_lob": True,      # Build orderbook state
                "compute_features": True,     # Microstructure features
            }
        },
        "market_trades": {
            "partition_cols": ["product_id", "date"],
            "processor_options": {
                "add_derived_fields": True,
            }
        },
        "ticker": {
            # Time-based partitioning for ticker data
            "partition_cols": ["date", "hour"],
            "processor_options": {
                "add_derived_fields": True,
            }
        },
    }
    
    job = ETLJob(
        input_dir="data/raw/ready/coinbase",
        output_dir="data/processed",
        source="coinbase",
        channel_config=channel_config,
    )
    
    # Process specific date range
    from datetime import datetime
    job.process_date_range(
        start_date=datetime(2025, 11, 20),
        end_date=datetime(2025, 11, 26),
    )


# ============================================================================
# Example 3: Custom Pipeline for Specific Channel
# ============================================================================

def example_custom_pipeline():
    """Build a custom pipeline for specialized processing."""
    from pathlib import Path
    from etl.readers import NDJSONReader
    from etl.processors import RawParser
    from etl.processors.coinbase import Level2Processor
    from etl.writers import ParquetWriter
    from etl.orchestrators import ETLPipeline
    
    # Create custom pipeline
    pipeline = ETLPipeline(
        reader=NDJSONReader(),
        processors=[
            RawParser(source="coinbase", channel="level2"),
            Level2Processor(
                reconstruct_lob=True,
                compute_features=True,
            ),
        ],
        writer=ParquetWriter(),
    )
    
    # Process multiple segments
    segment_dir = Path("data/raw/ready/coinbase")
    for segment_file in segment_dir.glob("segment_*.ndjson"):
        pipeline.execute(
            input_path=segment_file,
            output_path="data/processed/coinbase/level2",
            partition_cols=["product_id", "date"],
        )
        
        # Stats available after each segment
        print(f"Processed {segment_file.name}")
        print(f"  Reader: {pipeline.reader.get_stats()}")
        print(f"  Processor: {pipeline.processor.get_stats()}")
        print(f"  Writer: {pipeline.writer.get_stats()}")
        
        # Reset for next segment
        pipeline.reset_stats()


# ============================================================================
# Example 4: Reprocessing Parquet Data (Add Features)
# ============================================================================

def example_reprocessing():
    """Reprocess existing Parquet files to add derived features."""
    from etl.readers import ParquetReader
    from etl.processors.coinbase import Level2Processor
    from etl.writers import ParquetWriter
    from etl.orchestrators import ETLPipeline
    
    # Reprocessing pipeline
    # Uses Polars for efficient lazy evaluation
    pipeline = ETLPipeline(
        reader=ParquetReader(use_polars=True),
        processors=[
            Level2Processor(
                reconstruct_lob=True,
                compute_features=True,
            ),
        ],
        writer=ParquetWriter(),
    )
    
    # Reprocess existing data
    pipeline.execute(
        input_path="data/processed/coinbase/level2",
        output_path="data/processed/coinbase/level2_features",
        partition_cols=["product_id", "date"],
    )


# ============================================================================
# Example 5: Custom Processor - VWAP Computation
# ============================================================================

def example_custom_processor():
    """Create a custom processor for VWAP computation."""
    from etl.processors import BaseProcessor
    from etl.readers import ParquetReader
    from etl.writers import ParquetWriter
    from etl.orchestrators import ETLPipeline
    import polars as pl
    
    class VWAPProcessor(BaseProcessor):
        """Compute VWAP from trades data."""
        
        def __init__(self, window_minutes: int = 5):
            super().__init__()
            self.window_minutes = window_minutes
        
        def process_batch(self, records: list[dict]) -> list[dict]:
            """Compute VWAP using Polars for efficiency."""
            if not records:
                return []
            
            df = pl.DataFrame(records)
            
            # Compute VWAP and OHLCV
            result = (
                df
                .with_columns([
                    pl.col("time").str.to_datetime().alias("timestamp"),
                    (pl.col("price") * pl.col("size")).alias("value"),
                ])
                .groupby_dynamic(
                    "timestamp",
                    every=f"{self.window_minutes}m",
                    by=["product_id"],
                )
                .agg([
                    (pl.col("value").sum() / pl.col("size").sum()).alias("vwap"),
                    pl.col("size").sum().alias("volume"),
                    pl.col("price").min().alias("low"),
                    pl.col("price").max().alias("high"),
                    pl.col("price").first().alias("open"),
                    pl.col("price").last().alias("close"),
                    pl.col("timestamp").first().alias("window_start"),
                ])
                .with_columns([
                    pl.col("timestamp").dt.date().alias("date"),
                ])
            )
            
            self.stats["records_processed"] = len(records)
            self.stats["records_output"] = len(result)
            
            return result.to_dicts()
    
    # Use custom processor in pipeline
    pipeline = ETLPipeline(
        reader=ParquetReader(use_polars=True),
        processors=[VWAPProcessor(window_minutes=5)],
        writer=ParquetWriter(),
    )
    
    # Compute 5-minute VWAP bars
    pipeline.execute(
        input_path="data/processed/coinbase/market_trades",
        output_path="data/processed/coinbase/vwap_5m",
        partition_cols=["product_id", "date"],
    )


# ============================================================================
# Example 6: Query Partitioned Data
# ============================================================================

def example_query_data():
    """Query partitioned Parquet data efficiently."""
    import duckdb
    import polars as pl
    import pandas as pd
    
    # Method 1: DuckDB (SQL interface, partition pruning)
    print("=== DuckDB Query ===")
    df_duckdb = duckdb.query("""
        SELECT 
            product_id,
            AVG(mid_price) as avg_mid_price,
            AVG(spread_bps) as avg_spread_bps,
            COUNT(*) as num_updates
        FROM 'data/processed/coinbase/level2/**/*.parquet'
        WHERE product_id = 'BTC-USD' 
          AND date = '2025-11-26'
        GROUP BY product_id
    """).to_df()
    print(df_duckdb)
    
    # Method 2: Polars (Lazy, memory efficient)
    print("\n=== Polars Query ===")
    lf = pl.scan_parquet("data/processed/coinbase/level2/**/*.parquet")
    
    df_polars = (
        lf
        .filter(pl.col("product_id") == "BTC-USD")
        .filter(pl.col("date") == "2025-11-26")
        .groupby("product_id")
        .agg([
            pl.col("mid_price").mean().alias("avg_mid_price"),
            pl.col("spread_bps").mean().alias("avg_spread_bps"),
            pl.count().alias("num_updates"),
        ])
        .collect()
    )
    print(df_polars)
    
    # Method 3: Pandas (Familiar, with partition filtering)
    print("\n=== Pandas Query ===")
    df_pandas = pd.read_parquet(
        "data/processed/coinbase/level2",
        filters=[
            ("product_id", "==", "BTC-USD"),
            ("date", "==", "2025-11-26"),
        ]
    )
    
    summary = df_pandas.groupby("product_id").agg({
        "mid_price": "mean",
        "spread_bps": "mean",
        "product_id": "count",
    })
    summary.columns = ["avg_mid_price", "avg_spread_bps", "num_updates"]
    print(summary)


# ============================================================================
# Example 7: Multi-Channel Processing
# ============================================================================

def example_segment_pipeline():
    """Process all channels at once using CoinbaseSegmentPipeline."""
    from pathlib import Path
    from etl.orchestrators import CoinbaseSegmentPipeline
    
    # Create Coinbase segment pipeline
    pipeline = CoinbaseSegmentPipeline(
        output_dir="data/processed",
        channel_config={
            "level2": {
                "partition_cols": ["product_id", "date"],
                "processor_options": {
                    "reconstruct_lob": True,
                    "compute_features": False,
                },
            },
            "market_trades": {
                "partition_cols": ["product_id", "date"],
            },
            "ticker": {
                "partition_cols": ["date", "hour"],
            },
        },
    )
    
    # Process all segments in directory
    segment_dir = Path("data/raw/ready/coinbase")
    for segment_file in sorted(segment_dir.glob("segment_*.ndjson")):
        print(f"\nProcessing {segment_file.name}")
        
        # Process all channels
        pipeline.process_segment(segment_file)
        
        # Get stats per channel
        stats = pipeline.get_stats()
        for channel, channel_stats in stats.items():
            print(f"  {channel}:")
            print(f"    Records processed: {channel_stats['processor']['records_processed']}")
            print(f"    Records written: {channel_stats['writer']['records_written']}")


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    # Run examples
    print("ETL Architecture Examples\n")
    
    print("1. Basic ETL Job")
    print("-" * 60)
    # example_basic_etl()
    
    print("\n2. Custom Channel Configuration")
    print("-" * 60)
    # example_custom_config()
    
    print("\n3. Custom Pipeline")
    print("-" * 60)
    # example_custom_pipeline()
    
    print("\n4. Reprocessing Parquet Data")
    print("-" * 60)
    # example_reprocessing()
    
    print("\n5. Custom VWAP Processor")
    print("-" * 60)
    # example_custom_processor()
    
    print("\n6. Query Partitioned Data")
    print("-" * 60)
    # example_query_data()
    
    print("\n7. Multi-Channel Segment Pipeline")
    print("-" * 60)
    # example_segment_pipeline()
    
    print("\nUncomment examples above to run them!")
