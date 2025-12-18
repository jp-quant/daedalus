"""Coinbase-specific segment pipeline for processing NDJSON segments."""
import logging
from pathlib import Path
from typing import Optional, Dict, List

from storage.base import StorageBackend
from etl.readers.ndjson_reader import NDJSONReader
from etl.processors.raw_processor import RawProcessor
from etl.processors.coinbase import Level2Processor, TradesProcessor, TickerProcessor
from etl.writers.parquet_writer import ParquetWriter
from .pipeline import ETLPipeline

logger = logging.getLogger(__name__)


class CoinbaseSegmentPipeline:
    """
    Coinbase-specific pipeline for processing NDJSON segments with channel routing.
    
    Supports separate storage backends for input (NDJSON) and output (Parquet):
    - Input storage: Where raw NDJSON segments are read from
    - Output storage: Where processed Parquet files are written to
    
    This enables hybrid storage patterns (read from local, write to S3).
    
    Automatically:
    - Reads NDJSON segments
    - Parses raw Coinbase records
    - Routes to Coinbase channel-specific processors
    - Writes with appropriate partitioning per channel
    
    Example:
        storage = create_storage_backend(config)
        pipeline = CoinbaseSegmentPipeline(
            storage=storage,
            output_base_path="processed/coinbase"
        )
        
        pipeline.process_segment("raw/ready/coinbase/segment_001.ndjson")
    """
    
    def __init__(
        self,
        storage: StorageBackend,
        output_base_path: str,
        channel_config: Optional[Dict[str, Dict]] = None,
        compression: str = "zstd",
        input_storage: Optional[StorageBackend] = None,
    ):
        """
        Initialize Coinbase segment pipeline.
        
        Args:
            storage: Output storage backend for Parquet files
            output_base_path: Base output path for processed data (relative to storage root)
            channel_config: Per-channel configuration
                {
                    "level2": {
                        "partition_cols": ["product_id", "date"],
                        "processor_options": {"reconstruct_lob": True}
                    },
                    "market_trades": {
                        "partition_cols": ["product_id", "date"]
                    }
                }
            compression: Parquet compression codec
            input_storage: Optional separate input storage for NDJSON.
                          If None, reader is created without storage (expects local paths).
        """
        self.storage = storage
        self.input_storage = input_storage
        self.source = "coinbase"
        self.output_base_path = output_base_path
        self.channel_config = channel_config or self._get_default_config()
        self.compression = compression
        
        # Create channel-specific pipelines
        self.pipelines = self._create_pipelines()
        
        logger.info(
            f"[CoinbaseSegmentPipeline] Initialized, "
            f"output_storage={storage.backend_type}, "
            f"input_storage={input_storage.backend_type if input_storage else 'local'}, "
            f"channels={list(self.pipelines.keys())}"
        )
    
    def _get_default_config(self) -> Dict[str, Dict]:
        """Get default configuration per channel."""
        return {
            "orderbook": {
                "partition_cols": ["product_id", "date"],
                "processor_options": {
                    "add_derived_fields": True,
                    "reconstruct_lob": False,  # Expensive, off by default
                    "compute_features": False,
                }
            },
            "trades": {
                "partition_cols": ["product_id", "date"],
                "processor_options": {
                    "add_derived_fields": True,
                }
            },
            "ticker": {
                "partition_cols": ["date", "hour"],
                "processor_options": {
                    "add_derived_fields": True,
                }
            },
        }
    
    def _create_pipelines(self) -> Dict[str, ETLPipeline]:
        """Create channel-specific pipelines."""
        pipelines = {}
        
        for channel, config in self.channel_config.items():
            # Get processor class for channel
            processor_class = self._get_processor_class(channel)
            if not processor_class:
                logger.warning(f"No processor for channel: {channel}")
                continue
            
            # Create processor with options
            processor_options = config.get("processor_options", {})
            processor = processor_class(**processor_options)
            
            # Create pipeline: NDJSON → RawProcessor → ChannelProcessor → Parquet
            # Use input_storage for reader, output storage for writer
            pipeline = ETLPipeline(
                reader=NDJSONReader(storage=self.input_storage),
                processors=[
                    RawProcessor(source=self.source, channel=channel),
                    processor,
                ],
                writer=ParquetWriter(storage=self.storage, compression=self.compression),
            )
            
            pipelines[channel] = pipeline
        
        return pipelines
    
    def _get_processor_class(self, channel: str):
        """Get Coinbase processor class for channel."""
        processors = {
            "orderbook": Level2Processor,
            "trades": TradesProcessor,
            "ticker": TickerProcessor,
        }
        return processors.get(channel)
    
    def process_segment(
        self,
        segment_path: Path,
        channels: Optional[List[str]] = None,
    ):
        """
        Process a single segment file.
        
        Args:
            segment_path: Path to NDJSON segment (relative to storage root or absolute local path)
            channels: List of channels to process (if None, process all)
        """
        segment_path = Path(segment_path)
        logger.info(f"[CoinbaseSegmentPipeline] Processing segment: {segment_path.name}")
        
        # Determine which channels to process
        if channels is None:
            channels = list(self.pipelines.keys())
        
        # Process each channel
        for channel in channels:
            if channel not in self.pipelines:
                logger.warning(f"No pipeline for channel: {channel}")
                continue
            
            try:
                # Get channel config
                config = self.channel_config.get(channel, {})
                partition_cols = config.get("partition_cols")
                
                # Output path: output_base_path/channel/ (relative to storage root)
                output_path = self.storage.join_path(self.output_base_path, channel)
                
                # Execute pipeline
                self.pipelines[channel].execute(
                    input_path=segment_path,
                    output_path=output_path,
                    partition_cols=partition_cols,
                )
                
                logger.info(
                    f"[CoinbaseSegmentPipeline] Completed channel {channel} for {segment_path.name}"
                )
            
            except Exception as e:
                logger.error(
                    f"[CoinbaseSegmentPipeline] Error processing channel {channel}: {e}",
                    exc_info=True
                )
        
        logger.info(f"[CoinbaseSegmentPipeline] Completed segment: {segment_path.name}")
    
    def get_stats(self) -> Dict[str, Dict]:
        """Get statistics from all channel pipelines."""
        stats = {}
        for channel, pipeline in self.pipelines.items():
            stats[channel] = {
                "reader": pipeline.reader.get_stats(),
                "processor": pipeline.processor.get_stats(),
                "writer": pipeline.writer.get_stats(),
            }
        return stats
    
    def reset_stats(self):
        """Reset statistics for all pipelines."""
        for pipeline in self.pipelines.values():
            pipeline.reset_stats()
