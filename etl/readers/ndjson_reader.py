"""NDJSON reader for raw segment files."""
import json
from pathlib import Path
from typing import Iterator, Union
import logging

from .base_reader import BaseReader
from ingestion.utils.serialization import from_ndjson

logger = logging.getLogger(__name__)


class NDJSONReader(BaseReader):
    """
    Read NDJSON (newline-delimited JSON) files.
    
    Designed for reading raw segment files from ingestion layer.
    """
    
    def __init__(self, max_errors: int = 100):
        """
        Initialize NDJSON reader.
        
        Args:
            max_errors: Maximum errors to log before suppressing
        """
        super().__init__()
        self.max_errors = max_errors
        self._error_count = 0
    
    def read(self, source: Union[Path, str]) -> Iterator[dict]:
        """
        Read NDJSON file line by line.
        
        Args:
            source: Path to NDJSON file
            
        Yields:
            Raw records as dictionaries
        """
        source = Path(source)
        
        if not source.exists():
            logger.error(f"Source file not found: {source}")
            return
        
        if not source.is_file():
            logger.error(f"Source is not a file: {source}")
            return
        
        self.stats["files_processed"] += 1
        line_num = 0
        
        try:
            with open(source, 'r', encoding='utf-8') as f:
                for line in f:
                    line_num += 1
                    
                    # Skip empty lines
                    if not line.strip():
                        continue
                    
                    try:
                        # Parse NDJSON line
                        record = from_ndjson(line)
                        self.stats["records_read"] += 1
                        yield record
                    
                    except Exception as e:
                        self.stats["errors"] += 1
                        self._error_count += 1
                        
                        if self._error_count <= self.max_errors:
                            logger.error(
                                f"Error parsing line {line_num} in {source.name}: {e}"
                            )
                        elif self._error_count == self.max_errors + 1:
                            logger.warning(
                                f"Max errors ({self.max_errors}) reached, "
                                "suppressing further error logs"
                            )
        
        except Exception as e:
            logger.error(f"Error reading file {source}: {e}", exc_info=True)
        
        logger.info(
            f"[NDJSONReader] Read {self.stats['records_read']} records "
            f"from {source.name} ({self.stats['errors']} errors)"
        )
