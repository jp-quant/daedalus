"""CCXT-specific segment pipeline for processing NDJSON segments."""
import logging
from pathlib import Path
from typing import Optional, Dict, List

from storage.base import StorageBackend
from etl.readers.ndjson_reader import NDJSONReader
from etl.processors.raw_processor import RawProcessor
from etl.processors.ccxt.ticker_processor import CcxtTickerProcessor
from etl.processors.ccxt.trades_processor import CcxtTradesProcessor
from etl.processors.ccxt.advanced_orderbook_processor import CcxtAdvancedOrderbookProcessor
from etl.writers.parquet_writer import ParquetWriter
from .pipeline import ETLPipeline
from .multi_output_pipeline import MultiOutputETLPipeline

logger = logging.getLogger(__name__)


class CcxtSegmentPipeline:
    """
    CCXT-specific pipeline for processing NDJSON segments.
    
    Supports separate storage backends for input (NDJSON) and output (Parquet):
    - Input storage: Where raw NDJSON segments are read from
    - Output storage: Where processed Parquet files are written to
    
    This enables hybrid storage patterns (read from local, write to S3).
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
        Initialize CCXT segment pipeline.
        
        Args:
            storage: Output storage backend for Parquet files
            output_base_path: Base path for output files
            channel_config: Per-channel configuration
            compression: Parquet compression codec
            input_storage: Optional separate input storage for NDJSON.
                          If None, reader is created without storage (expects local paths).
        """
        self.storage = storage
        self.input_storage = input_storage
        self.source = "ccxt"
        self.output_base_path = output_base_path
        self.channel_config = channel_config or self._get_default_config()
        self.compression = compression
        
        # Create channel-specific pipelines
        self.pipelines = self._create_pipelines()
        
        logger.info(
            f"[CcxtSegmentPipeline] Initialized, "
            f"output_storage={storage.backend_type}, "
            f"input_storage={input_storage.backend_type if input_storage else 'local'}, "
            f"channels={list(self.pipelines.keys())}"
        )
    
    def _get_default_config(self) -> Dict[str, Dict]:
        """Get default configuration per channel."""
        return {
            "ticker": {
                "partition_cols": ["exchange", "symbol", "date"],
                "processor_options": {}
            },
            "trades": {
                "partition_cols": ["exchange", "symbol", "date"],
                "processor_options": {}
            },
            "orderbook": {
                "partition_cols": ["exchange", "symbol", "date"],
                "processor_options": {}
            }
        }
    
    def _create_pipelines(self) -> Dict[str, Dict]:
        """Create channel-specific pipelines."""
        pipelines = {}
        
        for channel, config in self.channel_config.items():
            
            if channel == "orderbook":
                # Special handling for orderbook with multi-output
                # We also ingest 'trades' here to calculate TFI/Aggressor features
                # Extract processor_options from config and pass to processor
                processor_options = config.get("processor_options", {})
                logger.debug(f"Creating CcxtAdvancedOrderbookProcessor with options: {processor_options}")
                processor = CcxtAdvancedOrderbookProcessor(**processor_options)
                
                pipeline = MultiOutputETLPipeline(
                    reader=NDJSONReader(storage=self.input_storage),
                    processors=[
                        RawProcessor(source=self.source, channel=["orderbook", "trades"]),
                        processor,
                    ],
                    writers={
                        'hf': ParquetWriter(self.storage, compression=self.compression),
                        'bars': ParquetWriter(self.storage, compression=self.compression)
                    }
                )
                
                pipelines[channel] = {
                    "pipeline": pipeline,
                    "output_path": f"{self.output_base_path}/{channel}", # Base path
                    "partition_cols": config.get("partition_cols", ["exchange", "symbol", "date"])
                }
                
            else:
                # Standard pipeline
                processor_class = self._get_processor_class(channel)
                if not processor_class:
                    logger.warning(f"No processor for channel: {channel}")
                    continue
                
                # Extract processor_options (if processor supports it)
                processor_options = config.get("processor_options", {})
                try:
                    processor = processor_class(**processor_options)
                except TypeError:
                    # Processor doesn't accept options, use default constructor
                    logger.debug(f"{processor_class.__name__} doesn't accept options, using defaults")
                    processor = processor_class()
                
                pipeline = ETLPipeline(
                    reader=NDJSONReader(storage=self.input_storage),
                    processors=[
                        RawProcessor(source=self.source, channel=channel),
                        processor,
                    ],
                    writer=ParquetWriter(
                        storage=self.storage,
                        compression=self.compression
                    )
                )
                
                pipelines[channel] = {
                    "pipeline": pipeline,
                    "output_path": f"{self.output_base_path}/{channel}",
                    "partition_cols": config.get("partition_cols", ["exchange", "symbol", "date"])
                }
            
        return pipelines

    def _get_processor_class(self, channel: str):
        processors = {
            "ticker": CcxtTickerProcessor,
            "trades": CcxtTradesProcessor,
            # "orderbook": CcxtOrderbookProcessor, # Handled separately
        }
        return processors.get(channel)

    def process_segment(self, segment_path: Path, channels: Optional[List[str]] = None):
        """Process a single segment file."""
        segment_path = Path(segment_path)
        logger.info(f"[CcxtSegmentPipeline] Processing segment: {segment_path.name}")
        
        results = {}
        
        target_channels = channels or self.pipelines.keys()
        
        for channel in target_channels:
            if channel not in self.pipelines:
                continue
                
            pipeline_config = self.pipelines[channel]
            pipeline = pipeline_config["pipeline"]
            output_path = pipeline_config["output_path"]
            partition_cols = pipeline_config["partition_cols"]
            
            try:
                if isinstance(pipeline, MultiOutputETLPipeline):
                    # Multi-output execution
                    output_paths = {
                        'hf': f"{output_path}/hf",
                        'bars': f"{output_path}/bars"
                    }
                    pipeline.execute(
                        input_path=str(segment_path),
                        output_paths=output_paths,
                        partition_cols=partition_cols
                    )
                    
                    # Aggregate stats
                    stats = {}
                    for key, writer in pipeline.writers.items():
                        s = writer.get_stats()
                        stats[key] = s
                        logger.info(f"  Channel {channel}/{key}: {s.get('records_written', 0)} records")
                    results[channel] = stats
                    
                else:
                    # Standard execution
                    pipeline.execute(
                        input_path=str(segment_path),
                        output_path=output_path,
                        partition_cols=partition_cols
                    )
                    
                    stats = pipeline.writer.get_stats()
                    results[channel] = stats
                    logger.info(f"  Channel {channel}: {stats.get('records_written', 0)} records")
                
                # Reset stats for next run
                pipeline.reset_stats()
                
            except Exception as e:
                logger.error(f"  Channel {channel} failed: {e}", exc_info=True)
                results[channel] = {"error": str(e)}
                
        return results
