"""Abstract base reader interface."""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterator, Union
import logging

logger = logging.getLogger(__name__)


class BaseReader(ABC):
    """
    Abstract base class for all ETL readers.
    
    Readers are responsible for:
    - Loading data from various sources (NDJSON, Parquet, CSV, etc.)
    - Yielding records in a standard format
    - Handling errors gracefully
    """
    
    def __init__(self):
        """Initialize reader."""
        self.stats = {
            "records_read": 0,
            "errors": 0,
            "files_processed": 0,
        }
    
    @abstractmethod
    def read(self, source: Union[Path, str]) -> Iterator[dict]:
        """
        Read data from source and yield records.
        
        Args:
            source: Path to file or directory to read from
            
        Yields:
            Individual records as dictionaries
        """
        pass
    
    def read_batch(self, source: Union[Path, str], batch_size: int = 1000) -> Iterator[list[dict]]:
        """
        Read data in batches for efficiency.
        
        Args:
            source: Path to file or directory to read from
            batch_size: Number of records per batch
            
        Yields:
            Batches of records
        """
        batch = []
        for record in self.read(source):
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield remaining records
        if batch:
            yield batch
    
    def get_stats(self) -> dict:
        """Get reader statistics."""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset statistics counters."""
        self.stats = {
            "records_read": 0,
            "errors": 0,
            "files_processed": 0,
        }
