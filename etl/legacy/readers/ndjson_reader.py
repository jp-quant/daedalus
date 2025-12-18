"""NDJSON reader for raw segment files with storage backend support."""
import io
from pathlib import Path
from typing import Iterator, Union, Optional, TYPE_CHECKING
import logging

from .base_reader import BaseReader
from ingestion.utils.serialization import from_ndjson

if TYPE_CHECKING:
    from storage.base import StorageBackend

logger = logging.getLogger(__name__)


class NDJSONReader(BaseReader):
    """
    Read NDJSON (newline-delimited JSON) files from any storage backend.
    
    Supports:
    - Local filesystem paths (Path or str)
    - S3 paths via StorageBackend
    - Streaming for memory efficiency
    
    Designed for reading raw segment files from ingestion layer.
    
    Examples:
        # Local file read (no storage backend)
        reader = NDJSONReader()
        for record in reader.read("data/segment.ndjson"):
            process(record)
        
        # With storage backend (local or S3)
        from storage.factory import create_etl_storage_input
        storage = create_etl_storage_input(config)
        reader = NDJSONReader(storage=storage)
        for record in reader.read("raw/ready/ccxt/segment_001.ndjson"):
            process(record)
    """
    
    def __init__(
        self,
        storage: Optional["StorageBackend"] = None,
        max_errors: int = 100,
        chunk_size: int = 8 * 1024 * 1024,  # 8MB chunks for S3 streaming
    ):
        """
        Initialize NDJSON reader.
        
        Args:
            storage: Optional storage backend for cloud/remote storage.
                    If None, reads from local filesystem directly.
            max_errors: Maximum errors to log before suppressing
            chunk_size: Chunk size for streaming S3 reads (default 8MB)
        """
        super().__init__()
        self.storage = storage
        self.max_errors = max_errors
        self.chunk_size = chunk_size
        self._error_count = 0
    
    def read(self, source: Union[Path, str]) -> Iterator[dict]:
        """
        Read NDJSON file line by line from local or remote storage.
        
        Args:
            source: Path to NDJSON file. Can be:
                   - Local filesystem path (if storage is None)
                   - Relative path from storage root (if storage is provided)
            
        Yields:
            Raw records as dictionaries
        """
        source_str = str(source)
        
        # Route to appropriate reader based on storage backend
        if self.storage is None:
            # Direct local filesystem read
            yield from self._read_local(Path(source))
        elif self.storage.backend_type == "local":
            # Local storage backend - resolve to full path
            full_path = Path(self.storage.get_full_path(source_str))
            yield from self._read_local(full_path)
        elif self.storage.backend_type == "s3":
            # S3 storage - stream from cloud
            yield from self._read_s3(source_str)
        else:
            # Unknown backend - try to get full path and read locally
            logger.warning(f"Unknown storage backend: {self.storage.backend_type}, attempting local read")
            full_path = Path(self.storage.get_full_path(source_str))
            yield from self._read_local(full_path)
    
    def _read_local(self, source: Path) -> Iterator[dict]:
        """Read NDJSON from local filesystem."""
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
                        self._handle_parse_error(line_num, source.name, e)
        
        except Exception as e:
            logger.error(f"Error reading file {source}: {e}", exc_info=True)
        
        logger.info(
            f"[NDJSONReader] Read {self.stats['records_read']} records "
            f"from {source.name} ({self.stats['errors']} errors)"
        )
    
    def _read_s3(self, source: str) -> Iterator[dict]:
        """
        Read NDJSON from S3 storage with streaming.
        
        Uses chunked reading to handle large files without loading entirely into memory.
        """
        self.stats["files_processed"] += 1
        line_num = 0
        source_name = source.split("/")[-1] if "/" in source else source
        
        try:
            # Get S3 filesystem from storage
            s3fs = self.storage.get_filesystem()
            full_path = self.storage.get_full_path(source)
            
            # Open file in streaming mode
            with s3fs.open(full_path, 'rb') as f:
                # Use a buffer to handle lines split across chunks
                buffer = ""
                
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        # Process any remaining data in buffer
                        if buffer.strip():
                            line_num += 1
                            try:
                                record = from_ndjson(buffer)
                                self.stats["records_read"] += 1
                                yield record
                            except Exception as e:
                                self._handle_parse_error(line_num, source_name, e)
                        break
                    
                    # Decode chunk and add to buffer
                    buffer += chunk.decode('utf-8')
                    
                    # Process complete lines
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        line_num += 1
                        
                        if not line.strip():
                            continue
                        
                        try:
                            record = from_ndjson(line)
                            self.stats["records_read"] += 1
                            yield record
                        except Exception as e:
                            self._handle_parse_error(line_num, source_name, e)
        
        except Exception as e:
            logger.error(f"Error reading S3 file {source}: {e}", exc_info=True)
        
        logger.info(
            f"[NDJSONReader] Read {self.stats['records_read']} records "
            f"from s3://{source_name} ({self.stats['errors']} errors)"
        )
    
    def _read_s3_full(self, source: str) -> Iterator[dict]:
        """
        Read NDJSON from S3 by downloading entire file.
        
        Fallback method for smaller files or when streaming isn't available.
        """
        self.stats["files_processed"] += 1
        line_num = 0
        source_name = source.split("/")[-1] if "/" in source else source
        
        try:
            # Download entire file
            data = self.storage.read_bytes(source)
            content = data.decode('utf-8')
            
            for line in content.split('\n'):
                line_num += 1
                
                if not line.strip():
                    continue
                
                try:
                    record = from_ndjson(line)
                    self.stats["records_read"] += 1
                    yield record
                except Exception as e:
                    self._handle_parse_error(line_num, source_name, e)
        
        except Exception as e:
            logger.error(f"Error reading S3 file {source}: {e}", exc_info=True)
        
        logger.info(
            f"[NDJSONReader] Read {self.stats['records_read']} records "
            f"from s3://{source_name} ({self.stats['errors']} errors)"
        )
    
    def _handle_parse_error(self, line_num: int, source_name: str, error: Exception):
        """Handle parsing errors with rate limiting."""
        self.stats["errors"] += 1
        self._error_count += 1
        
        if self._error_count <= self.max_errors:
            logger.error(f"Error parsing line {line_num} in {source_name}: {error}")
        elif self._error_count == self.max_errors + 1:
            logger.warning(
                f"Max errors ({self.max_errors}) reached, "
                "suppressing further error logs"
            )
    
    def reset_stats(self):
        """Reset reader statistics for reuse."""
        self.stats = {
            "records_read": 0,
            "errors": 0,
            "files_processed": 0,
        }
        self._error_count = 0
