"""
Multi-output ETL pipeline.
Supports processors that produce multiple data streams (e.g. HF features + Bars).
"""
import logging
from pathlib import Path
from typing import Optional, List, Union, Dict, Any

from etl.readers.base_reader import BaseReader
from etl.processors.base_processor import BaseProcessor, ProcessorChain
from etl.writers.base_writer import BaseWriter

logger = logging.getLogger(__name__)


class MultiOutputETLPipeline:
    """
    ETL pipeline that supports multiple output streams.
    
    The processor (or chain) must return a dictionary of streams,
    or a list of such dictionaries (if processing batches).
    
    Example:
        pipeline = MultiOutputETLPipeline(
            reader=NDJSONReader(),
            processors=[RawParser(), AdvancedProcessor()],
            writers={
                'hf': ParquetWriter(),
                'bars': ParquetWriter()
            }
        )
    """
    
    def __init__(
        self,
        reader: BaseReader,
        processors: Union[BaseProcessor, List[BaseProcessor]],
        writers: Dict[str, BaseWriter],
    ):
        self.reader = reader
        
        if isinstance(processors, list):
            self.processor = ProcessorChain(processors)
        else:
            self.processor = processors
        
        self.writers = writers
        
        logger.info(
            f"[MultiOutputETLPipeline] Initialized: "
            f"reader={reader.__class__.__name__}, "
            f"writers={list(writers.keys())}"
        )
    
    def execute(
        self,
        input_path: Union[Path, str],
        output_paths: Dict[str, str],
        partition_cols: Optional[List[str]] = None,
        batch_size: int = 1000,
    ):
        """
        Execute pipeline.
        
        Args:
            input_path: Input file path
            output_paths: Dict mapping stream name to output path
            partition_cols: Partition columns
            batch_size: Batch size
        """
        logger.info(f"[MultiOutputETLPipeline] Executing: {input_path}")
        
        try:
            for batch in self.reader.read_batch(input_path, batch_size):
                processed_batch = self.processor.process_batch(batch)
                
                if not processed_batch:
                    continue
                
                # Aggregate streams
                streams: Dict[str, List[Any]] = {k: [] for k in self.writers.keys()}
                
                for item in processed_batch:
                    if not isinstance(item, dict):
                        continue
                        
                    for key in streams:
                        if key in item:
                            val = item[key]
                            if isinstance(val, list):
                                streams[key].extend(val)
                            elif val is not None:
                                streams[key].append(val)
                
                # Write streams
                for key, writer in self.writers.items():
                    if streams[key]:
                        out_path = output_paths.get(key)
                        if out_path:
                            writer.write(
                                data=streams[key],
                                output_path=out_path,
                                partition_cols=partition_cols
                            )
                            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            raise

    def reset_stats(self):
        self.reader.reset_stats()
        self.processor.reset_stats()
        for writer in self.writers.values():
            writer.reset_stats()
