"""ETL job runner - orchestrates segment processing using pipelines."""
import logging
import os
from pathlib import Path
from typing import Optional
from datetime import datetime

from etl.orchestrators.coinbase_segment_pipeline import CoinbaseSegmentPipeline

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
        input_dir: str,
        output_dir: str,
        delete_after_processing: bool = True,
        processing_dir: Optional[str] = None,
        channel_config: Optional[dict] = None,
    ):
        """
        Initialize Coinbase ETL job.
        
        Args:
            input_dir: Directory containing ready NDJSON segments (ready/)
            output_dir: Directory for Parquet output
            delete_after_processing: Delete raw segments after successful ETL
            processing_dir: Temp directory during processing
            channel_config: Channel-specific configuration for pipelines
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.source = "coinbase"
        self.delete_after_processing = delete_after_processing
        
        # Processing directory (for atomic move)
        if processing_dir:
            self.processing_dir = Path(processing_dir) / "coinbase"
        else:
            self.processing_dir = self.input_dir.parent / "processing" / "coinbase"
        
        self.processing_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Coinbase segment pipeline
        self.pipeline = CoinbaseSegmentPipeline(
            output_dir=output_dir,
            channel_config=channel_config,
        )
        
        logger.info(
            f"[ETLJob] Initialized Coinbase ETL: "
            f"input_dir={input_dir}, output_dir={output_dir}, "
            f"delete_after={delete_after_processing}"
        )
    
    def process_segment(self, segment_file: Path) -> bool:
        """
        Process a single NDJSON segment file using pipelines.
        
        Args:
            segment_file: Path to segment file in ready/
            
        Returns:
            True if successful, False otherwise
        """
        # Move to processing/ directory (atomic, prevents double-processing)
        processing_file = self.processing_dir / segment_file.name
        
        try:
            os.rename(segment_file, processing_file)
            logger.info(f"[ETLJob] Processing segment: {segment_file.name}")
        except FileNotFoundError:
            logger.warning(f"[ETLJob] Segment already processed or missing: {segment_file.name}")
            return False
        except Exception as e:
            logger.error(f"[ETLJob] Failed to move segment to processing/: {e}")
            return False
        
        try:
            # Process using segment pipeline (handles all channels)
            self.pipeline.process_segment(processing_file)
            
            logger.info(f"[ETLJob] Processed {segment_file.name} successfully")
            
            # Delete or archive processed segment
            if self.delete_after_processing:
                try:
                    processing_file.unlink()
                    logger.info(f"[ETLJob] Deleted processed segment: {segment_file.name}")
                except Exception as e:
                    logger.error(f"[ETLJob] Failed to delete segment: {e}")
            else:
                logger.info(f"[ETLJob] Segment retained in processing/: {segment_file.name}")
            
            return True
        
        except Exception as e:
            logger.error(f"[ETLJob] Error processing segment {segment_file.name}: {e}", exc_info=True)
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
        # Only read from input_dir (ready/), never from active/
        if not self.input_dir.exists():
            logger.error(f"[ETLJob] Input directory not found: {self.input_dir}")
            return
        
        # Find all segment files (ignore non-segment files)
        segment_files = sorted(self.input_dir.glob("segment_*.ndjson"))
        
        if not segment_files:
            logger.info(f"[ETLJob] No segments found in {self.input_dir}")
            return
        
        logger.info(f"[ETLJob] Found {len(segment_files)} segment(s) to process")
        
        success_count = 0
        for segment_file in segment_files:
            if self.process_segment(segment_file):
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
        
        if not self.input_dir.exists():
            logger.error(f"[ETLJob] Input directory not found: {self.input_dir}")
            return
        
        # Find all segments
        all_segments = sorted(self.input_dir.glob("segment_*.ndjson"))
        
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
