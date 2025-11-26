"""Abstract base processor interface."""
from abc import ABC, abstractmethod
from typing import Any, Iterator, Union
import logging

logger = logging.getLogger(__name__)


class BaseProcessor(ABC):
    """
    Abstract base class for all ETL processors.
    
    Processors are responsible for:
    - Transforming data (parse, aggregate, derive features)
    - Being composable (chain multiple processors)
    - Supporting both eager and lazy evaluation
    """
    
    def __init__(self):
        """Initialize processor."""
        self.stats = {
            "records_processed": 0,
            "records_output": 0,
            "errors": 0,
        }
    
    @abstractmethod
    def process(self, data: Any) -> Any:
        """
        Transform data.
        
        Args:
            data: Input data (can be dict, list[dict], DataFrame, etc.)
            
        Returns:
            Transformed data in same or different format
        """
        pass
    
    def process_record(self, record: dict) -> Union[dict, list[dict], None]:
        """
        Process a single record.
        
        Args:
            record: Single record as dictionary
            
        Returns:
            Transformed record(s) or None to filter out
        """
        return self.process(record)
    
    def process_batch(self, records: list[dict]) -> list[dict]:
        """
        Process a batch of records.
        
        Default implementation processes records individually.
        Override for batch-optimized processing.
        
        Args:
            records: List of records
            
        Returns:
            List of transformed records
        """
        result = []
        for record in records:
            try:
                processed = self.process_record(record)
                if processed is None:
                    continue
                
                # Handle both single record and list of records
                if isinstance(processed, list):
                    result.extend(processed)
                else:
                    result.append(processed)
                
                self.stats["records_processed"] += 1
            except Exception as e:
                self.stats["errors"] += 1
                logger.error(f"Error processing record: {e}", exc_info=True)
        
        self.stats["records_output"] = len(result)
        return result
    
    def get_stats(self) -> dict:
        """Get processor statistics."""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset statistics counters."""
        self.stats = {
            "records_processed": 0,
            "records_output": 0,
            "errors": 0,
        }


class ProcessorChain(BaseProcessor):
    """
    Chain multiple processors together.
    
    Data flows through processors sequentially:
    input → processor1 → processor2 → ... → output
    """
    
    def __init__(self, processors: list[BaseProcessor]):
        """
        Initialize processor chain.
        
        Args:
            processors: List of processors to chain
        """
        super().__init__()
        self.processors = processors
    
    def process(self, data: Any) -> Any:
        """Apply all processors sequentially."""
        result = data
        for processor in self.processors:
            result = processor.process(result)
        return result
    
    def process_batch(self, records: list[dict]) -> list[dict]:
        """Process batch through all processors."""
        result = records
        for processor in self.processors:
            result = processor.process_batch(result)
        return result
    
    def get_stats(self) -> dict:
        """Get combined stats from all processors."""
        combined = {
            "processors": [],
            "total_records_processed": 0,
            "total_records_output": 0,
            "total_errors": 0,
        }
        
        for i, processor in enumerate(self.processors):
            stats = processor.get_stats()
            combined["processors"].append({
                "index": i,
                "class": processor.__class__.__name__,
                "stats": stats,
            })
            combined["total_records_processed"] += stats.get("records_processed", 0)
            combined["total_records_output"] += stats.get("records_output", 0)
            combined["total_errors"] += stats.get("errors", 0)
        
        return combined
