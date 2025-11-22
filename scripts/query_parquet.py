"""Query examples for partitioned Parquet files."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def query_with_duckdb():
    """
    Query Parquet with DuckDB (recommended for local analytics).
    
    DuckDB is serverless, fast, and SQL-compatible.
    Install: pip install duckdb
    """
    import duckdb
    
    # Example 1: Query single day across all hours
    query = """
    SELECT 
        COUNT(*) as total_records,
        MIN(timestamp) as first_ts,
        MAX(timestamp) as last_ts,
        AVG(price) as avg_price
    FROM 'F:/processed/coinbase/ticker/date=2025-11-20/**/*.parquet'
    WHERE product_id = 'BTC-USD'
    """
    
    result = duckdb.query(query).to_df()
    print("\n=== DuckDB Query: BTC-USD Stats for 2025-11-20 ===")
    print(result)
    
    # Example 2: Query specific hour range
    query2 = """
    SELECT 
        product_id,
        COUNT(*) as tick_count,
        MIN(price) as low,
        MAX(price) as high
    FROM 'F:/processed/coinbase/ticker/date=2025-11-20/hour={14,15,16}/*.parquet'
    GROUP BY product_id
    ORDER BY tick_count DESC
    LIMIT 10
    """
    
    result2 = duckdb.query(query2).to_df()
    print("\n=== DuckDB Query: Top 10 Products by Activity (Hours 14-16) ===")
    print(result2)
    
    # Example 3: Aggregation across multiple days
    query3 = """
    SELECT 
        DATE_TRUNC('hour', timestamp) as hour_bucket,
        product_id,
        COUNT(*) as ticks,
        AVG(price) as avg_price
    FROM 'F:/processed/coinbase/ticker/date=2025-11-*/**/*.parquet'
    WHERE product_id IN ('BTC-USD', 'ETH-USD', 'SOL-USD')
    GROUP BY hour_bucket, product_id
    ORDER BY hour_bucket, product_id
    """
    
    result3 = duckdb.query(query3).to_df()
    print("\n=== DuckDB Query: Hourly Aggregates for Top 3 Products ===")
    print(result3.head(20))


def query_with_polars():
    """
    Query Parquet with Polars (blazing fast, lazy evaluation).
    
    Polars is optimized for DataFrames and has excellent Parquet support.
    Install: pip install polars
    """
    import polars as pl
    
    # Example 1: Lazy scan (doesn't load until needed)
    df = (
        pl.scan_parquet("F:/processed/coinbase/ticker/**/*.parquet")
        .filter(pl.col("product_id") == "BTC-USD")
        .filter(pl.col("date") == "2025-11-20")
        .select(["timestamp", "price", "volume_24h"])
        .collect()
    )
    
    print("\n=== Polars Query: BTC-USD Ticks on 2025-11-20 ===")
    print(df.head(10))
    
    # Example 2: Aggregation with lazy evaluation
    hourly_stats = (
        pl.scan_parquet("F:/processed/coinbase/ticker/**/*.parquet")
        .filter(pl.col("product_id").is_in(["BTC-USD", "ETH-USD"]))
        .group_by(["date", "hour", "product_id"])
        .agg([
            pl.count().alias("tick_count"),
            pl.col("price").mean().alias("avg_price"),
            pl.col("price").min().alias("low"),
            pl.col("price").max().alias("high"),
        ])
        .collect()
    )
    
    print("\n=== Polars Query: Hourly Stats ===")
    print(hourly_stats)


def query_with_pandas():
    """
    Query Parquet with Pandas (traditional, but loads everything into memory).
    
    WARNING: Not recommended for large datasets (GB+).
    Use for small queries or when DuckDB/Polars aren't available.
    """
    import pandas as pd
    
    # Example: Read specific partition
    df = pd.read_parquet("F:/processed/coinbase/ticker/date=2025-11-20/hour=14/")
    
    print("\n=== Pandas Query: Hour 14 Data ===")
    print(f"Total rows: {len(df)}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    print("\nFirst 10 rows:")
    print(df.head(10))
    
    # Filter and aggregate
    btc_stats = (
        df[df['product_id'] == 'BTC-USD']
        .agg({
            'price': ['mean', 'min', 'max'],
            'volume_24h': 'mean'
        })
    )
    
    print("\n=== Pandas Query: BTC-USD Stats ===")
    print(btc_stats)


def query_with_spark():
    """
    Query Parquet with PySpark (for distributed processing on clusters).
    
    Use when:
    - Dataset is too large for single machine (TB+)
    - Running on Spark cluster (EMR, Databricks, etc.)
    
    Install: pip install pyspark
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("FluxForge Query") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()
    
    # Read partitioned data (Spark automatically discovers partitions)
    df = spark.read.parquet("F:/processed/coinbase/ticker")
    
    print("\n=== Spark Query: Schema ===")
    df.printSchema()
    
    # Example query with partition pruning
    result = df \
        .filter("date = '2025-11-20' AND hour >= 14 AND hour <= 16") \
        .filter("product_id IN ('BTC-USD', 'ETH-USD')") \
        .groupBy("hour", "product_id") \
        .agg(
            {"price": "avg", "*": "count"}
        ) \
        .orderBy("hour", "product_id")
    
    print("\n=== Spark Query: Hourly Aggregates ===")
    result.show()
    
    spark.stop()


def list_partitions():
    """List all available partitions."""
    import os
    
    base_path = Path("F:/processed/coinbase/ticker")
    
    if not base_path.exists():
        print(f"Path not found: {base_path}")
        return
    
    print("\n=== Available Partitions ===")
    
    for date_dir in sorted(base_path.glob("date=*")):
        date = date_dir.name.split("=")[1]
        
        hours = []
        for hour_dir in sorted(date_dir.glob("hour=*")):
            hour = hour_dir.name.split("=")[1]
            file_count = len(list(hour_dir.glob("*.parquet")))
            
            # Calculate total size
            total_size = sum(f.stat().st_size for f in hour_dir.glob("*.parquet"))
            size_mb = total_size / 1024**2
            
            hours.append(f"  hour={hour}: {file_count} files, {size_mb:.1f} MB")
        
        print(f"\ndate={date}:")
        for h in hours:
            print(h)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Query FluxForge Parquet files")
    parser.add_argument(
        "--engine",
        choices=["duckdb", "polars", "pandas", "spark", "list"],
        default="list",
        help="Query engine to use (default: list partitions)"
    )
    
    args = parser.parse_args()
    
    try:
        if args.engine == "duckdb":
            query_with_duckdb()
        elif args.engine == "polars":
            query_with_polars()
        elif args.engine == "pandas":
            query_with_pandas()
        elif args.engine == "spark":
            query_with_spark()
        else:
            list_partitions()
    
    except ImportError as e:
        print(f"\nError: {e}")
        print("\nInstall required package:")
        print(f"  pip install {args.engine}")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
