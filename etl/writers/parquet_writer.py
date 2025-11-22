"""Parquet writer for ETL output with scalable partitioning."""
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger(__name__)


class ParquetWriter:
    """
    Write processed records to Parquet files with scalable partitioning.
    
    Strategy:
    - Partitions by date AND hour (keeps files small ~100MB each)
    - Micro-batches within hour (UUID-based filenames, no overwrites)
    - Schema evolution via PyArrow schema merging
    - Designed for distributed queries (Spark, DuckDB, Polars)
    
    Directory structure:
    processed/
      coinbase/
        ticker/
          date=2025-11-20/
            hour=14/
              part-20251120T140512-abc123.parquet
              part-20251120T140830-def456.parquet
            hour=15/
              part-20251120T150120-ghi789.parquet
          date=2025-11-21/
            hour=09/
              part-20251121T090045-jkl012.parquet
    
    Query examples:
    - DuckDB: SELECT * FROM 'processed/coinbase/ticker/**/*.parquet' WHERE date='2025-11-20'
    - Spark: spark.read.parquet("processed/coinbase/ticker").filter("date='2025-11-20' AND hour=14")
    - Polars: pl.scan_parquet("processed/coinbase/ticker/**/*.parquet")
    """
    
    def __init__(
        self,
        output_dir: str,
        compression: str = "snappy"
    ):
        """
        Initialize Parquet writer.
        
        Args:
            output_dir: Base directory for Parquet files
            compression: Compression codec (snappy, gzip, zstd, etc.)
        """
        self.output_dir = Path(output_dir)
        self.compression = compression
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            f"[ParquetWriter] Initialized: output_dir={output_dir}, "
            f"compression={compression}"
        )
    
    def write(
        self,
        records: List[Dict[str, Any]],
        source: str,
        channel: str,
        date_str: Optional[str] = None,
        partition_cols: Optional[List[str]] = None
    ):
        """
        Write records to partitioned Parquet files (scalable, no in-memory merge).
        
        Strategy:
        - Caller provides structured records with partition columns already populated
        - Groups by partition columns for efficient writes
        - Writes micro-batches with unique filenames (UUID-based)
        - NO loading of existing files (append-only, scales to TB+)
        - Schema stored in each file for evolution compatibility
        
        Args:
            records: List of parsed records (must contain partition_cols if specified)
            source: Data source (coinbase, databento, etc.)
            channel: Channel name (ticker, level2, etc.)
            partition_cols: Column names to partition by (e.g., ['year', 'month', 'day', 'hour']).
                          If None, writes all records to a single file without partitioning.
        
        Raises:
            ValueError: If partition columns are specified but missing from records
        """
        if not records:
            logger.warning(f"[ParquetWriter] No records to write for {source}/{channel}")
            return
        
        try:
            # Convert to DataFrame once
            df = pd.DataFrame(records)
            
            # Validate partition columns exist (if specified)
            if partition_cols:
                missing_cols = set(partition_cols) - set(df.columns)
                if missing_cols:
                    raise ValueError(
                        f"Partition columns {missing_cols} not found in records. "
                        f"Available columns: {list(df.columns)}"
                    )
            
            # Group by partition columns
            if partition_cols:
                grouped = df.groupby(partition_cols, dropna=False)
            else:
                # No partitioning - single group
                grouped = [(tuple(), df)]
            
            total_written = 0
            for partition_values, partition_df in grouped:
                # Build partition path (e.g., year=2025/month=11/day=20/hour=14)
                partition_dir = self.output_dir / source / channel
                
                if partition_cols:
                    # Ensure partition_values is iterable
                    if not isinstance(partition_values, tuple):
                        partition_values = (partition_values,)
                    
                    for col, val in zip(partition_cols, partition_values):
                        partition_dir = partition_dir / f"{col}={val}"
                
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                # Generate unique filename (timestamp + UUID to avoid collisions)
                if date_str is None:
                    date_str = datetime.now().strftime("%Y%m%dT%H%M%S")
                else:
                    date_str = str(date_str)
                import uuid
                unique_id = str(uuid.uuid4())[:8]
                filename = f"part_{date_str}_{unique_id}.parquet"
                
                output_file = partition_dir / filename
                
                # Drop partition columns from data (stored in directory path)
                if partition_cols:
                    data_df = partition_df.drop(columns=partition_cols, errors='ignore')
                else:
                    data_df = partition_df
                
                # Convert to Arrow Table and write directly
                table = pa.Table.from_pandas(data_df)
                pq.write_table(
                    table,
                    output_file,
                    compression=self.compression
                )
                
                total_written += len(partition_df)
                
                logger.info(
                    f"[ParquetWriter] Wrote {len(partition_df)} records to "
                    f"{output_file.relative_to(self.output_dir)} "
                    f"({data_df.memory_usage(deep=True).sum() / 1024:.1f} KB)"
                )
            
            logger.info(
                f"[ParquetWriter] Total: {total_written} records written for "
                f"{source}/{channel}"
            )
        
        except Exception as e:
            logger.error(
                f"[ParquetWriter] Error writing to Parquet: {e}",
                exc_info=True
            )
            raise
