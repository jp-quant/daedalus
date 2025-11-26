"""Parquet reader for reprocessing structured data."""
from pathlib import Path
from typing import Iterator, Union, Optional
import logging

from .base_reader import BaseReader

logger = logging.getLogger(__name__)


class ParquetReader(BaseReader):
    """
    Read Parquet files with optional lazy evaluation.
    
    Supports both pandas and polars for different use cases:
    - pandas: Small files, eager evaluation
    - polars: Large files, lazy evaluation for memory efficiency
    """
    
    def __init__(self, use_polars: bool = True):
        """
        Initialize Parquet reader.
        
        Args:
            use_polars: If True, use polars for lazy evaluation (recommended for large files)
        """
        super().__init__()
        self.use_polars = use_polars
        
        if use_polars:
            try:
                import polars as pl
                self.pl = pl
            except ImportError:
                logger.warning(
                    "Polars not installed, falling back to pandas. "
                    "Install polars for better performance: pip install polars"
                )
                self.use_polars = False
                import pandas as pd
                self.pd = pd
        else:
            import pandas as pd
            self.pd = pd
    
    def read(self, source: Union[Path, str]) -> Iterator[dict]:
        """
        Read Parquet file and yield records.
        
        Args:
            source: Path to Parquet file or directory
            
        Yields:
            Records as dictionaries
        """
        source = Path(source)
        
        if not source.exists():
            logger.error(f"Source not found: {source}")
            return
        
        # Handle directory (read all parquet files)
        if source.is_dir():
            parquet_files = sorted(source.glob("**/*.parquet"))
            logger.info(f"[ParquetReader] Found {len(parquet_files)} parquet files in {source}")
            
            for file in parquet_files:
                yield from self._read_file(file)
        else:
            yield from self._read_file(source)
    
    def _read_file(self, file_path: Path) -> Iterator[dict]:
        """Read a single parquet file."""
        try:
            self.stats["files_processed"] += 1
            
            if self.use_polars:
                # Use polars for efficient reading
                df = self.pl.read_parquet(file_path)
                records = df.to_dicts()
            else:
                # Use pandas
                df = self.pd.read_parquet(file_path)
                records = df.to_dict('records')
            
            for record in records:
                self.stats["records_read"] += 1
                yield record
            
            logger.debug(f"[ParquetReader] Read {len(records)} records from {file_path.name}")
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Error reading parquet file {file_path}: {e}", exc_info=True)
    
    def read_lazy(self, source: Union[Path, str]):
        """
        Read Parquet as lazy DataFrame (polars only).
        
        Returns LazyFrame for memory-efficient processing of large files.
        
        Args:
            source: Path to Parquet file(s)
            
        Returns:
            Polars LazyFrame
        """
        if not self.use_polars:
            raise NotImplementedError("Lazy reading requires polars")
        
        source = Path(source)
        
        if source.is_dir():
            # Scan directory pattern
            pattern = str(source / "**/*.parquet")
            return self.pl.scan_parquet(pattern)
        else:
            return self.pl.scan_parquet(source)
