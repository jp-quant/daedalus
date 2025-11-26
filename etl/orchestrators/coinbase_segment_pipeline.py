"""Coinbase-specific segment pipeline for processing NDJSON segments."""
import logging
from pathlib import Path
from typing import Optional, Dict, List

from etl.readers.ndjson_reader import NDJSONReader
from etl.processors.raw_parser import RawParser
from etl.processors.coinbase import Level2Processor, TradesProcessor, TickerProcessor
from etl.writers.parquet_writer import ParquetWriter
from .pipeline import ETLPipeline

logger = logging.getLogger(__name__)


class CoinbaseSegmentPipeline:
    """
    Coinbase-specific pipeline for processing NDJSON segments with channel routing.
    
    Automatically:
    - Reads NDJSON segments
    - Parses raw Coinbase records
    - Routes to Coinbase channel-specific processors
    - Writes with appropriate partitioning per channel
    
    Example:
        pipeline = CoinbaseSegmentPipeline(
            output_dir="data/processed"
        )
        
        pipeline.process_segment("data/segments/segment_001.ndjson")
    """
    
    def __init__(
        self,
        output_dir: str,
        channel_config: Optional[Dict[str, Dict]] = None,
    ):
        """
        Initialize Coinbase segment pipeline.
        
        Args:
            output_dir: Base output directory for processed data
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
        """
        self.source = "coinbase"
        self.output_dir = Path(output_dir)
        self.channel_config = channel_config or self._get_default_config()
        
        # Create channel-specific pipelines
        self.pipelines = self._create_pipelines()
        
        logger.info(
            f"[CoinbaseSegmentPipeline] Initialized, "
            f"channels={list(self.pipelines.keys())}"
        )
    
    def _get_default_config(self) -> Dict[str, Dict]:
        """Get default configuration per channel."""
        return {
            "level2": {
                "partition_cols": ["product_id", "date"],
                "processor_options": {
                    "add_derived_fields": True,
                    "reconstruct_lob": False,  # Expensive, off by default
                    "compute_features": False,
                }
            },
            "market_trades": {
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
            
            # Create pipeline: NDJSON → RawParser → ChannelProcessor → Parquet
            pipeline = ETLPipeline(
                reader=NDJSONReader(),
                processors=[
                    RawParser(source=self.source, channel=channel),
                    processor,
                ],
                writer=ParquetWriter(),
            )
            
            pipelines[channel] = pipeline
        
        return pipelines
    
    def _get_processor_class(self, channel: str):
        """Get Coinbase processor class for channel."""
        processors = {
            "level2": Level2Processor,
            "market_trades": TradesProcessor,
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
            segment_path: Path to NDJSON segment
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
                
                # Output path: output_dir/source/channel/
                output_path = self.output_dir / self.source / channel
                
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
