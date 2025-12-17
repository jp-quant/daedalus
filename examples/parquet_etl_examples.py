"""
Parquet ETL Pipeline Examples.

Demonstrates the new Polars-based ETL pipeline for processing
channel-separated Parquet files (orderbook, trades, ticker).
"""
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

# =============================================================================
# EXAMPLE 1: Simple Lazy Scan Pattern
# =============================================================================

def example_lazy_scan():
    """
    Basic pattern for lazy scanning Parquet files.
    
    This is the foundation - lazily scan, transform, collect.
    """
    # Lazy scan all orderbook files
    orderbook_lf = pl.scan_parquet("data/raw/ready/ccxt/orderbook/*.parquet")
    
    # Add derived features (all lazy - no execution yet)
    orderbook_lf = orderbook_lf.with_columns([
        # Best bid/ask from nested struct arrays
        pl.col("bids").list.get(0).struct.field("price").alias("best_bid"),
        pl.col("asks").list.get(0).struct.field("price").alias("best_ask"),
    ])
    
    orderbook_lf = orderbook_lf.with_columns([
        ((pl.col("best_bid") + pl.col("best_ask")) / 2).alias("mid_price"),
        (pl.col("best_ask") - pl.col("best_bid")).alias("spread"),
    ])
    
    # Filter for specific symbol (still lazy)
    btc_lf = orderbook_lf.filter(pl.col("symbol") == "BTC/USDT")
    
    # Now collect to execute
    result = btc_lf.collect()
    print(f"Loaded {len(result)} BTC/USDT orderbook snapshots")
    return result


# =============================================================================
# EXAMPLE 2: Merge Orderbook + Trades Chronologically
# =============================================================================

def example_merge_channels():
    """
    Merge orderbook and trades for chronological processing.
    
    This is needed for TFI (Trade Flow Imbalance) calculation.
    """
    # Load both channels
    orderbook_lf = pl.scan_parquet("data/raw/ready/ccxt/orderbook/*.parquet")
    trades_lf = pl.scan_parquet("data/raw/ready/ccxt/trades/*.parquet")
    
    # Add channel marker
    orderbook_lf = orderbook_lf.with_columns(pl.lit("orderbook").alias("channel"))
    trades_lf = trades_lf.with_columns(pl.lit("trades").alias("channel"))
    
    # Select common columns for union
    orderbook_slim = orderbook_lf.select([
        "capture_ts", "exchange", "symbol", "channel"
    ])
    trades_slim = trades_lf.select([
        "capture_ts", "exchange", "symbol", "channel"
    ])
    
    # Union and sort
    merged = pl.concat([orderbook_slim, trades_slim]).sort("capture_ts")
    
    # Collect
    result = merged.collect()
    print(f"Merged timeline: {len(result)} events")
    
    # Count by channel
    print(result.group_by("channel").count())
    return result


# =============================================================================
# EXAMPLE 3: Rolling Features with group_by_dynamic
# =============================================================================

def example_bar_aggregation():
    """
    Create time bars using Polars group_by_dynamic.
    
    This is the vectorized approach to bar aggregation.
    """
    # Scan and compute basic features
    orderbook_lf = pl.scan_parquet("data/raw/ready/ccxt/orderbook/*.parquet")
    
    # Extract prices
    orderbook_lf = orderbook_lf.with_columns([
        pl.col("bids").list.get(0).struct.field("price").alias("best_bid"),
        pl.col("asks").list.get(0).struct.field("price").alias("best_ask"),
        pl.col("bids").list.get(0).struct.field("size").alias("bid_size"),
        pl.col("asks").list.get(0).struct.field("size").alias("ask_size"),
    ])
    
    orderbook_lf = orderbook_lf.with_columns([
        ((pl.col("best_bid") + pl.col("best_ask")) / 2).alias("mid_price"),
        (pl.col("best_ask") - pl.col("best_bid")).alias("spread"),
        (
            (pl.col("bid_size") - pl.col("ask_size")) /
            (pl.col("bid_size") + pl.col("ask_size"))
        ).alias("imbalance"),
    ])
    
    # Compute log returns (requires collecting first for shift)
    df = orderbook_lf.sort("capture_ts").collect()
    
    df = df.with_columns([
        (pl.col("mid_price").log() - pl.col("mid_price").shift(1).log()).alias("log_return")
    ])
    
    # Create 1-minute bars
    bars_1m = df.group_by_dynamic(
        "capture_ts",
        every="1m",
        period="1m",
        closed="left",
        label="left",
        group_by=["exchange", "symbol"],
    ).agg([
        # OHLC
        pl.col("mid_price").first().alias("open"),
        pl.col("mid_price").max().alias("high"),
        pl.col("mid_price").min().alias("low"),
        pl.col("mid_price").last().alias("close"),
        
        # Spread stats
        pl.col("spread").mean().alias("avg_spread"),
        pl.col("spread").min().alias("min_spread"),
        pl.col("spread").max().alias("max_spread"),
        
        # Imbalance
        pl.col("imbalance").mean().alias("avg_imbalance"),
        
        # Realized variance
        pl.col("log_return").var().alias("realized_variance"),
        
        # Tick count
        pl.count().alias("tick_count"),
    ])
    
    # Add derived columns
    bars_1m = bars_1m.with_columns([
        # Range
        (pl.col("high") - pl.col("low")).alias("range"),
        
        # Return
        ((pl.col("close") - pl.col("open")) / pl.col("open")).alias("bar_return"),
        
        # Realized vol (annualized assuming 1-min bars)
        (pl.col("realized_variance").sqrt() * (525600 ** 0.5)).alias("rv_annualized"),
    ])
    
    print(f"Created {len(bars_1m)} 1-minute bars")
    return bars_1m


# =============================================================================
# EXAMPLE 4: Full Hybrid Pipeline (Local Storage)
# =============================================================================

def example_full_pipeline():
    """
    Full hybrid pipeline using the new ParquetETLPipeline.
    
    Combines vectorized structural features with stateful features.
    Uses local filesystem storage.
    """
    from etl.parquet_etl_pipeline import ParquetETLPipeline, ParquetETLConfig
    
    # Configure
    config = ParquetETLConfig(
        horizons=[5, 15, 60, 300, 900],
        bar_durations=[60, 300, 900, 3600],
        max_levels=20,
        ofi_levels=10,
        mode='hybrid',
    )
    
    # Local pipeline (no storage backend = direct file paths)
    pipeline = ParquetETLPipeline(config=config)
    
    # Process ticker (fully vectorized)
    ticker_df = pipeline.process_ticker(
        input_path="data/raw/ready/ccxt/ticker",
        output_path="data/silver/ticker",
    )
    print(f"Processed {len(ticker_df)} ticker records")
    
    # Process trades (fully vectorized)
    trades_df = pipeline.process_trades(
        input_path="data/raw/ready/ccxt/trades",
        output_path="data/silver/trades",
    )
    print(f"Processed {len(trades_df)} trade records")
    
    # Process orderbook with trades (hybrid)
    hf_df, bars_df = pipeline.process_orderbook_with_trades(
        orderbook_path="data/raw/ready/ccxt/orderbook",
        trades_path="data/raw/ready/ccxt/trades",
        output_hf_path="data/silver/orderbook/hf",
        output_bars_path="data/silver/orderbook/bars",
    )
    print(f"Produced {len(hf_df)} HF feature rows")
    print(f"Produced {len(bars_df)} bar aggregates")
    
    return hf_df, bars_df


# =============================================================================
# EXAMPLE 5: Full Pipeline with S3 Storage Backend
# =============================================================================

def example_s3_pipeline():
    """
    Full hybrid pipeline with S3 storage backends.
    
    Reads from S3 and writes to S3 using StorageBackend abstraction.
    """
    from etl.parquet_etl_pipeline import ParquetETLPipeline, ParquetETLConfig
    from storage.base import S3Storage
    
    # Configure S3 storage backends
    storage_input = S3Storage(
        bucket="my-market-data-raw",
        region="us-east-1",
    )
    
    storage_output = S3Storage(
        bucket="my-market-data-processed",
        region="us-east-1",
    )
    
    # Configure ETL
    config = ParquetETLConfig(
        horizons=[5, 15, 60, 300, 900],
        bar_durations=[60, 300, 900, 3600],
        max_levels=20,
        mode='hybrid',
    )
    
    # Create pipeline with S3 storage
    pipeline = ParquetETLPipeline(
        config=config,
        storage_input=storage_input,
        storage_output=storage_output,
    )
    
    # Process - paths are relative to S3 bucket roots
    hf_df, bars_df = pipeline.process_orderbook_with_trades(
        orderbook_path="raw/ready/ccxt/orderbook",
        trades_path="raw/ready/ccxt/trades",
        output_hf_path="processed/ccxt/orderbook/hf",
        output_bars_path="processed/ccxt/orderbook/bars",
    )
    
    print(f"Processed to S3: {len(hf_df)} HF rows, {len(bars_df)} bars")
    return hf_df, bars_df


# =============================================================================
# EXAMPLE 6: Config-Driven Pipeline (Recommended)
# =============================================================================

def example_config_driven_pipeline():
    """
    Config-driven pipeline using Daedalus configuration.
    
    This is the recommended approach for production use.
    Loads storage backends and paths from config.yaml.
    """
    from config import load_config
    from storage.factory import (
        create_etl_storage_input,
        create_etl_storage_output,
        get_etl_input_path,
        get_etl_output_path,
    )
    from etl.parquet_etl_pipeline import ParquetETLPipeline, ParquetETLConfig
    
    # Load configuration
    config = load_config()  # Uses config/config.yaml
    
    # Create storage backends from config
    storage_input = create_etl_storage_input(config)
    storage_output = create_etl_storage_output(config)
    
    # Get paths from config
    input_base = get_etl_input_path(config, "ccxt")
    output_base = get_etl_output_path(config, "ccxt")
    
    print(f"Input: {storage_input.backend_type} @ {input_base}")
    print(f"Output: {storage_output.backend_type} @ {output_base}")
    
    # Create ETL config from Daedalus config
    etl_config = ParquetETLConfig(
        compression=config.etl.compression,
    )
    
    # Apply channel-specific partition cols if configured
    if config.etl.channels.get("orderbook"):
        etl_config.orderbook_partition_cols = (
            config.etl.channels["orderbook"].partition_cols or
            etl_config.orderbook_partition_cols
        )
    
    # Create pipeline
    pipeline = ParquetETLPipeline(
        config=etl_config,
        storage_input=storage_input,
        storage_output=storage_output,
    )
    
    # Process
    hf_df, bars_df = pipeline.process_orderbook_with_trades(
        orderbook_path=f"{input_base}/orderbook",
        trades_path=f"{input_base}/trades",
        output_hf_path=f"{output_base}/orderbook/hf",
        output_bars_path=f"{output_base}/orderbook/bars",
    )
    
    return hf_df, bars_df


# =============================================================================
# EXAMPLE 7: DuckDB Alternative for SQL Users
# =============================================================================

def example_duckdb_processing():
    """
    Alternative approach using DuckDB for SQL-friendly processing.
    
    DuckDB has excellent Parquet support and can be more intuitive
    for those familiar with SQL.
    """
    import duckdb
    
    # Connect and enable Parquet
    con = duckdb.connect()
    
    # Create views for each channel
    con.execute("""
        CREATE VIEW orderbook AS 
        SELECT * FROM read_parquet('data/raw/ready/ccxt/orderbook/*.parquet')
    """)
    
    con.execute("""
        CREATE VIEW trades AS 
        SELECT * FROM read_parquet('data/raw/ready/ccxt/trades/*.parquet')
    """)
    
    # Query with SQL - compute basic features
    features = con.execute("""
        WITH base AS (
            SELECT 
                capture_ts,
                exchange,
                symbol,
                bids[1].price AS best_bid,
                asks[1].price AS best_ask,
                bids[1].size AS bid_size,
                asks[1].size AS ask_size
            FROM orderbook
        )
        SELECT 
            capture_ts,
            exchange,
            symbol,
            best_bid,
            best_ask,
            (best_bid + best_ask) / 2 AS mid_price,
            best_ask - best_bid AS spread,
            (best_ask - best_bid) / ((best_bid + best_ask) / 2) AS relative_spread,
            (bid_size - ask_size) / (bid_size + ask_size) AS imbalance
        FROM base
        WHERE symbol = 'BTC/USDT'
        ORDER BY capture_ts
    """).fetchdf()
    
    print(f"DuckDB processed {len(features)} rows")
    
    # Can also do bar aggregation in SQL
    bars = con.execute("""
        WITH base AS (
            SELECT 
                capture_ts,
                exchange,
                symbol,
                (bids[1].price + asks[1].price) / 2 AS mid_price,
                asks[1].price - bids[1].price AS spread
            FROM orderbook
        )
        SELECT 
            exchange,
            symbol,
            time_bucket(INTERVAL '1 minute', capture_ts) AS bar_time,
            FIRST(mid_price) AS open,
            MAX(mid_price) AS high,
            MIN(mid_price) AS low,
            LAST(mid_price) AS close,
            AVG(spread) AS avg_spread,
            COUNT(*) AS tick_count
        FROM base
        WHERE symbol = 'BTC/USDT'
        GROUP BY exchange, symbol, time_bucket(INTERVAL '1 minute', capture_ts)
        ORDER BY bar_time
    """).fetchdf()
    
    print(f"Created {len(bars)} bars with DuckDB")
    
    return features, bars


# =============================================================================
# EXAMPLE 8: Incremental Processing Pattern
# =============================================================================

def example_incremental_processing():
    """
    Pattern for incremental processing of new segments.
    
    Instead of reprocessing everything, process only new segments.
    """
    from etl.parquet_etl_pipeline import ParquetETLPipeline, ParquetETLConfig
    
    # Track processed segments
    processed_segments_file = Path("data/state/processed_segments.txt")
    processed = set()
    if processed_segments_file.exists():
        processed = set(processed_segments_file.read_text().strip().split("\n"))
    
    # Find new segments
    orderbook_dir = Path("data/raw/ready/ccxt/orderbook")
    all_segments = set(f.stem for f in orderbook_dir.glob("*.parquet"))
    new_segments = all_segments - processed
    
    if not new_segments:
        print("No new segments to process")
        return
    
    print(f"Processing {len(new_segments)} new segments")
    
    # Process new segments only
    config = ParquetETLConfig()
    pipeline = ParquetETLPipeline(config=config)
    
    for segment in sorted(new_segments):
        # Filter to single segment
        orderbook_lf = pl.scan_parquet(orderbook_dir / f"{segment}.parquet")
        trades_lf = pl.scan_parquet(f"data/raw/ready/ccxt/trades/{segment}.parquet")
        
        # Process...
        # (would call pipeline methods here)
        
        # Mark as processed
        processed.add(segment)
    
    # Save state
    processed_segments_file.parent.mkdir(parents=True, exist_ok=True)
    processed_segments_file.write_text("\n".join(sorted(processed)))
    
    print(f"Processed {len(new_segments)} segments")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Parquet ETL Pipeline Examples")
    print("=" * 60)
    
    # Uncomment to run examples (requires data to exist)
    
    # print("\n--- Example 1: Lazy Scan ---")
    # example_lazy_scan()
    
    # print("\n--- Example 2: Merge Channels ---")
    # example_merge_channels()
    
    # print("\n--- Example 3: Bar Aggregation ---")
    # example_bar_aggregation()
    
    # print("\n--- Example 4: Full Pipeline (Local) ---")
    # example_full_pipeline()
    
    # print("\n--- Example 5: S3 Pipeline ---")
    # example_s3_pipeline()
    
    # print("\n--- Example 6: Config-Driven Pipeline ---")
    # example_config_driven_pipeline()
    
    # print("\n--- Example 7: DuckDB ---")
    # example_duckdb_processing()
    
    # print("\n--- Example 8: Incremental Processing ---")
    # example_incremental_processing()
    
    print("\nExamples available - uncomment to run with your data")
