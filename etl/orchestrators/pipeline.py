"""Generic ETL pipeline for composing reader → processor → writer."""
import logging
from pathlib import Path
from typing import Optional, List, Union

from etl.readers.base_reader import BaseReader
from etl.processors.base_processor import BaseProcessor, ProcessorChain
from etl.writers.base_writer import BaseWriter

logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Composable ETL pipeline: reader → processors → writer.
    
    Example:
        pipeline = ETLPipeline(
            reader=NDJSONReader(),
            processors=[RawParser("coinbase"), Level2Processor()],
            writer=ParquetWriter()
        )
        
        pipeline.execute(
            input_path="data/segment.ndjson",
            output_path="output/level2",
            partition_cols=["product_id", "date"]
        )
    """
    
    def __init__(
        self,
        reader: BaseReader,
        processors: Union[BaseProcessor, List[BaseProcessor]],
        writer: BaseWriter,
    ):
        """
        Initialize ETL pipeline.
        
        Args:
            reader: Reader for loading data
            processors: Single processor or list of processors (chained)
            writer: Writer for outputting data
        """
        self.reader = reader
        
        # Create processor chain if multiple processors
        if isinstance(processors, list):
            self.processor = ProcessorChain(processors)
        else:
            self.processor = processors
        
        self.writer = writer
        
        logger.info(
            f"[ETLPipeline] Initialized: "
            f"reader={reader.__class__.__name__}, "
            f"processors={self._get_processor_names()}, "
            f"writer={writer.__class__.__name__}"
        )
    
    def execute(
        self,
        input_path: Union[Path, str],
        output_path: Union[Path, str],
        partition_cols: Optional[List[str]] = None,
        batch_size: int = 1000,
    ):
        """
        Execute pipeline: read → process → write.
        
        Args:
            input_path: Path to input data
            output_path: Path to write output
            partition_cols: Partition columns for writer (optional)
            batch_size: Records per batch for processing
        """
        logger.info(
            f"[ETLPipeline] Executing: {input_path} → {output_path} "
            f"(partition_cols={partition_cols})"
        )
        
        try:
            # Read and process in batches
            all_processed = []
            
            for batch in self.reader.read_batch(input_path, batch_size):
                # Process batch through processor chain
                processed_batch = self.processor.process_batch(batch)
                
                if processed_batch:
                    all_processed.extend(processed_batch)
            
            # Write all processed records
            if all_processed:
                self.writer.write(
                    data=all_processed,
                    output_path=output_path,
                    partition_cols=partition_cols,
                )
                
                logger.info(
                    f"[ETLPipeline] Success: Processed {len(all_processed)} records"
                )
            else:
                logger.warning(f"[ETLPipeline] No records to write")
            
            # Log stats
            self._log_stats()
        
        except Exception as e:
            logger.error(f"[ETLPipeline] Error executing pipeline: {e}", exc_info=True)
            raise
    
    def _get_processor_names(self) -> str:
        """Get processor names for logging."""
        if isinstance(self.processor, ProcessorChain):
            names = [p.__class__.__name__ for p in self.processor.processors]
            return " → ".join(names)
        else:
            return self.processor.__class__.__name__
    
    def _log_stats(self):
        """Log statistics from all components."""
        logger.info(f"[ETLPipeline] Reader stats: {self.reader.get_stats()}")
        logger.info(f"[ETLPipeline] Processor stats: {self.processor.get_stats()}")
        logger.info(f"[ETLPipeline] Writer stats: {self.writer.get_stats()}")
    
    def reset_stats(self):
        """Reset statistics for all components."""
        self.reader.reset_stats()
        self.processor.reset_stats()
        self.writer.reset_stats()
