"""ETL job runner - orchestrates segment processing using pipelines."""
import logging
import os
from pathlib import Path
from typing import Optional
from datetime import datetime

from storage.base import StorageBackend
from etl.orchestrators.coinbase_segment_pipeline import CoinbaseSegmentPipeline
from etl.orchestrators.ccxt_segment_pipeline import CcxtSegmentPipeline

logger = logging.getLogger(__name__)


class ETLJob:
    """
    Coinbase ETL job that processes raw NDJSON segment logs into structured Parquet files.
    
    Uses CoinbaseSegmentPipeline for flexible, composable processing:
    - Handles Coinbase channel routing and processing
    - Each channel gets its own processor and partitioning strategy
    - Easily extensible for new Coinbase channels
    
    Workflow:
    1. Scan ready/ directory for closed segments
    2. Move segment to processing/ (atomic, prevents double-processing)
    3. Route to CoinbaseSegmentPipeline for processing
    4. Delete processed segment (or move to archive)
    """
    
    def __init__(
        self,
        storage_input: StorageBackend,
        storage_output: StorageBackend,
        input_path: str,
        output_path: str,
        source: str = "coinbase",
        delete_after_processing: bool = True,
        processing_path: Optional[str] = None,
        channel_config: Optional[dict] = None,
        compression: str = "zstd",
    ):
        """
        Initialize ETL job.
        
        Args:
            storage_input: Storage backend for reading raw data
            storage_output: Storage backend for writing processed data
            input_path: Path containing ready NDJSON segments (relative to storage_input root)
            output_path: Path for Parquet output (relative to storage_output root)
            source: Data source (coinbase, ccxt, etc.)
            delete_after_processing: Delete raw segments after successful ETL
            processing_path: Temp path during processing (relative to storage_input root)
            channel_config: Channel-specific configuration for pipelines
            compression: Parquet compression codec
        """
        self.storage_input = storage_input
        self.storage_output = storage_output
        self.input_path = input_path
        self.output_path = output_path
        self.source = source
        self.delete_after_processing = delete_after_processing
        self.compression = compression
        
        # Processing path (for atomic move) - always on input storage
        if processing_path:
            self.processing_path = processing_path
        else:
            # Default: sibling of input_path
            self.processing_path = storage_input.join_path(
                str(Path(input_path).parent),
                "processing",
                source
            )
        
        # Ensure processing directory exists
        self.storage_input.mkdir(self.processing_path)
        
        # Initialize pipeline based on source
        # Pass both input_storage (for reading NDJSON) and storage_output (for writing Parquet)
        if source == "coinbase":
            self.pipeline = CoinbaseSegmentPipeline(
                storage=storage_output,
                output_base_path=output_path,
                channel_config=channel_config,
                compression=compression,
                input_storage=storage_input,
            )
        elif source == "ccxt" or source.startswith("ccxt_"):
            self.pipeline = CcxtSegmentPipeline(
                storage=storage_output,
                output_base_path=output_path,
                channel_config=channel_config,
                compression=compression,
                input_storage=storage_input,
            )
        else:
            raise ValueError(f"Unsupported source: {source}")
        
        logger.info(
            f"[ETLJob] Initialized {source} ETL: "
            f"input_storage={storage_input.backend_type}, "
            f"output_storage={storage_output.backend_type}, "
            f"input={input_path}, output={output_path}, "
            f"delete_after={delete_after_processing}"
        )
    
    def process_segment(self, segment_file: Path) -> bool:
        """
        Process a single NDJSON segment file using pipelines.
        
        Args:
            segment_file: Path to segment file in ready/ (can be relative or absolute)
            
        Returns:
            True if successful, False otherwise
        """
        # Handle both Path and string inputs
        if isinstance(segment_file, str):
            segment_file = Path(segment_file)
        
        # Get segment filename
        segment_name = segment_file.name if isinstance(segment_file, Path) else str(segment_file).split('/')[-1]
        
        # Processing file path (on input storage)
        processing_file_path = self.storage_input.join_path(self.processing_path, segment_name)
        
        try:
            # Move to processing/ (atomic for local, copy+delete for S3)
            if self.storage_input.backend_type == "local":
                # Local: atomic rename
                src = Path(self.storage_input.get_full_path(
                    self.storage_input.join_path(self.input_path, segment_name)
                ))
                dst = Path(self.storage_input.get_full_path(processing_file_path))
                
                # Ensure processing directory exists
                dst.parent.mkdir(parents=True, exist_ok=True)
                
                src.rename(dst)
                logger.info(f"[ETLJob] Processing segment: {segment_name}")
            else:
                # S3: copy to processing, then delete from ready
                segment_path = self.storage_input.join_path(self.input_path, segment_name)
                data = self.storage_input.read_bytes(segment_path)
                self.storage_input.write_bytes(data, processing_file_path)
                self.storage_input.delete(segment_path)
                logger.info(f"[ETLJob] Processing segment: {segment_name}")
        
        except FileNotFoundError:
            logger.warning(f"[ETLJob] Segment already processed or missing: {segment_name}")
            return False
        except Exception as e:
            logger.error(f"[ETLJob] Failed to move segment to processing/: {e}")
            return False
        
        try:
            # Process using segment pipeline (handles all channels)
            # NDJSONReader now supports StorageBackend, so we pass the relative path
            # and let the reader handle local vs S3 reading
            self.pipeline.process_segment(processing_file_path)
            
            logger.info(f"[ETLJob] Processed {segment_name} successfully")
            
            # Delete or retain processed segment
            if self.delete_after_processing:
                try:
                    self.storage_input.delete(processing_file_path)
                    logger.info(f"[ETLJob] Deleted processed segment: {segment_name}")
                except Exception as e:
                    logger.error(f"[ETLJob] Failed to delete segment: {e}")
            else:
                logger.info(f"[ETLJob] Segment retained in processing/: {segment_name}")
            
            return True
        
        except Exception as e:
            logger.error(f"[ETLJob] Error processing segment {segment_name}: {e}", exc_info=True)
            return False
    
    def _extract_date_from_segment(self, filename: str) -> str:
        """
        Extract date string from segment filename.
        
        Args:
            filename: Segment filename (e.g., segment_20251120T14_00012.ndjson)
            
        Returns:
            Date string in YYYY-MM-DD format
        """
        try:
            # Extract date part: segment_20251120T14_00012.ndjson -> 20251120
            parts = filename.split('_')
            if len(parts) >= 2:
                date_time_str = parts[1]  # 20251120T14
                date_part = date_time_str.split('T')[0]  # 20251120
                
                # Convert to YYYY-MM-DD
                year = date_part[:4]
                month = date_part[4:6]
                day = date_part[6:8]
                return f"{year}-{month}-{day}"
        except Exception as e:
            logger.warning(f"[ETLJob] Failed to extract date from {filename}: {e}")
        
        # Fallback to current date
        return datetime.now().strftime("%Y-%m-%d")
    
    def process_all(self):
        """Process all available segment files in ready/ directory."""
        logger.info(f"[ETLJob] Scanning for segments in {self.input_path}")
        
        # List all segment files from input storage
        try:
            files = self.storage_input.list_files(path=self.input_path, pattern="segment_*.ndjson")
        except Exception as e:
            logger.error(f"[ETLJob] Failed to list segments: {e}")
            return
        
        if not files:
            logger.info(f"[ETLJob] No segments found in {self.input_path}")
            return
        
        # Sort by path/name
        segment_files = sorted([f["path"] for f in files])
        
        logger.info(f"[ETLJob] Found {len(segment_files)} segment(s) to process")
        
        success_count = 0
        for segment_path in segment_files:
            # Extract filename from path
            segment_name = segment_path.split('/')[-1] if '/' in segment_path else Path(segment_path).name
            if self.process_segment(Path(segment_path)):
                success_count += 1
        
        # Print stats
        logger.info(
            f"[ETLJob] Processed {success_count}/{len(segment_files)} segments successfully"
        )
        logger.info(f"[ETLJob] Pipeline stats: {self.pipeline.get_stats()}")
    
    def process_date_range(
        self,
        start_date: datetime,
        end_date: Optional[datetime] = None
    ):
        """
        Process segments within a date range.
        
        Note: With segment-based approach, this filters segments by date in filename.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive). If None, only processes start_date.
        """
        if end_date is None:
            end_date = start_date
        
        # List all segments from input storage
        try:
            files = self.storage_input.list_files(path=self.input_path, pattern="segment_*.ndjson")
        except Exception as e:
            logger.error(f"[ETLJob] Failed to list segments: {e}")
            return
        
        if not files:
            logger.info(f"[ETLJob] No segments found in {self.input_path}")
            return
        
        # Sort files by path
        all_segments = sorted([Path(f["path"]) for f in files])
        
        # Filter by date range
        segments_to_process = []
        for segment_file in all_segments:
            segment_date_str = self._extract_date_from_segment(segment_file.name)
            try:
                segment_date = datetime.strptime(segment_date_str, "%Y-%m-%d")
                if start_date <= segment_date <= end_date:
                    segments_to_process.append(segment_file)
            except ValueError:
                logger.warning(f"[ETLJob] Could not parse date from {segment_file.name}")
                continue
        
        if not segments_to_process:
            logger.info(
                f"[ETLJob] No segments found for date range "
                f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            )
            return
        
        logger.info(
            f"[ETLJob] Found {len(segments_to_process)} segment(s) in date range"
        )
        
        success_count = 0
        for segment_file in segments_to_process:
            if self.process_segment(segment_file):
                success_count += 1
        
        logger.info(
            f"[ETLJob] Processed {success_count}/{len(segments_to_process)} segments successfully"
        )
        logger.info(f"[ETLJob] Pipeline stats: {self.pipeline.get_stats()}")
