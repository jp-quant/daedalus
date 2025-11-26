"""Abstract base writer interface."""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Union, Optional
import logging

logger = logging.getLogger(__name__)


class BaseWriter(ABC):
    """
    Abstract base class for all ETL writers.
    
    Writers are responsible for:
    - Writing data to various sinks (Parquet, database, etc.)
    - Handling partitioning strategies
    - Ensuring data durability
    """
    
    def __init__(self):
        """Initialize writer."""
        self.stats = {
            "records_written": 0,
            "files_written": 0,
            "errors": 0,
        }
    
    @abstractmethod
    def write(
        self,
        data: Any,
        output_path: Union[Path, str],
        **kwargs
    ):
        """
        Write data to sink.
        
        Args:
            data: Data to write (can be list[dict], DataFrame, etc.)
            output_path: Path to write to
            **kwargs: Additional writer-specific options
        """
        pass
    
    def get_stats(self) -> dict:
        """Get writer statistics."""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset statistics counters."""
        self.stats = {
            "records_written": 0,
            "files_written": 0,
            "errors": 0,
        }
