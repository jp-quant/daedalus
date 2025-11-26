"""Parquet writer for ETL output with scalable partitioning."""
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import uuid

from .base_writer import BaseWriter

logger = logging.getLogger(__name__)


class ParquetWriter(BaseWriter):
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
        output_dir: Optional[str] = None,
        compression: str = "snappy"
    ):
        """
        Initialize Parquet writer.
        
        Args:
            output_dir: Base directory for Parquet files (optional, can be specified per write)
            compression: Compression codec (snappy, gzip, zstd, etc.)
        """
        super().__init__()
        self.output_dir = Path(output_dir) if output_dir else None
        self.compression = compression
        
        if self.output_dir:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            f"[ParquetWriter] Initialized: output_dir={output_dir}, "
            f"compression={compression}"
        )
    
    def write(
        self,
        data: Union[List[Dict[str, Any]], pd.DataFrame],
        output_path: Union[Path, str],
        partition_cols: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Write records to Parquet with flexible partitioning.
        
        Strategy:
        - Auto-detects data format (list[dict] or DataFrame)
        - If partition_cols specified: creates Hive-style partitions
        - Otherwise: flat write with UUID filename
        - NO loading of existing files (append-only, scales to TB+)
        
        Args:
            data: Records as list[dict] or DataFrame
            output_path: Directory to write to
            partition_cols: Columns to partition by (e.g., ['product_id', 'date'])
                          Creates directory structure: product_id=BTC-USD/date=2025-11-22/
            **kwargs: Additional options (unused, for future extensibility)
        
        Example:
            # Flat write
            writer.write(records, "output/coinbase/ticker")
            
            # Partitioned write
            writer.write(records, "output/coinbase/level2", 
                        partition_cols=["product_id", "date"])
        """
        if not data:
            logger.warning(f"[ParquetWriter] No data to write")
            return
        
        output_path = Path(output_path)
        
        # Convert to DataFrame if needed
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
        
        if df.empty:
            logger.warning(f"[ParquetWriter] Empty DataFrame, skipping write")
            return
        
        try:
            if partition_cols:
                self._write_partitioned(df, output_path, partition_cols)
            else:
                self._write_flat(df, output_path)
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"[ParquetWriter] Error writing to Parquet: {e}", exc_info=True)
            raise
    
    def _write_partitioned(
        self,
        df: pd.DataFrame,
        output_path: Path,
        partition_cols: List[str]
    ):
        """
        Write with Hive-style partitioning.
        
        Creates directory structure like:
          output_path/
            product_id=BTC-USD/
              date=2025-11-22/
                part-uuid.parquet
            product_id=ETH-USD/
              date=2025-11-22/
                part-uuid.parquet
        """
        # Validate partition columns exist
        missing_cols = set(partition_cols) - set(df.columns)
        if missing_cols:
            raise ValueError(
                f"Partition columns {missing_cols} not found in data. "
                f"Available columns: {list(df.columns)}"
            )
        
        # Group by partition columns
        grouped = df.groupby(partition_cols, dropna=False)
        
        for partition_values, partition_df in grouped:
            # Ensure partition_values is iterable
            if not isinstance(partition_values, tuple):
                partition_values = (partition_values,)
            
            # Build partition path
            partition_path = output_path
            for col, val in zip(partition_cols, partition_values):
                partition_path = partition_path / f"{col}={val}"
            
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Drop partition columns from data (stored in directory path)
            data_df = partition_df.drop(columns=partition_cols, errors='ignore')
            
            # Generate unique filename
            filename = self._generate_unique_filename()
            output_file = partition_path / filename
            
            # Write to Parquet
            table = pa.Table.from_pandas(data_df)
            pq.write_table(
                table,
                output_file,
                compression=self.compression
            )
            
            self.stats["records_written"] += len(partition_df)
            self.stats["files_written"] += 1
            
            logger.info(
                f"[ParquetWriter] Wrote {len(partition_df)} records to "
                f"{output_file.relative_to(output_path.parent) if output_path.parent != output_path else output_file.name} "
                f"({data_df.memory_usage(deep=True).sum() / 1024:.1f} KB)"
            )
        
        logger.info(
            f"[ParquetWriter] Total: {self.stats['records_written']} records written "
            f"across {self.stats['files_written']} files"
        )
    
    def _write_flat(
        self,
        df: pd.DataFrame,
        output_path: Path
    ):
        """
        Write without partitioning (single file with UUID name).
        """
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate unique filename
        filename = self._generate_unique_filename()
        output_file = output_path / filename
        
        # Write to Parquet
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            output_file,
            compression=self.compression
        )
        
        self.stats["records_written"] += len(df)
        self.stats["files_written"] += 1
        
        logger.info(
            f"[ParquetWriter] Wrote {len(df)} records to {output_file.name} "
            f"({df.memory_usage(deep=True).sum() / 1024:.1f} KB)"
        )
    
    def _generate_unique_filename(self) -> str:
        """Generate unique filename with timestamp and UUID."""
        timestamp = datetime.now().strftime("%Y%m%d")
        unique_id = str(uuid.uuid4())[:8]
        return f"part_{timestamp}_{unique_id}.parquet"
