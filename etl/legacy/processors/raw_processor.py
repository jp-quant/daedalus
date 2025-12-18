"""Raw parser processor - converts NDJSON to structured records."""
import logging
from typing import Dict, Any, Optional, List, Union

from .base_processor import BaseProcessor
from etl.legacy.parsers.coinbase_parser import CoinbaseParser
from etl.legacy.parsers.ccxt_parser import CcxtParser

logger = logging.getLogger(__name__)


class RawProcessor(BaseProcessor):
    """
    Parse raw NDJSON records into structured format.
    
    This processor acts as a bridge between ingestion and ETL:
    - Takes raw records from NDJSON reader
    - Routes to source-specific parser (Coinbase, CCXT)
    - Returns normalized records ready for channel-specific processing
    """
    
    def __init__(self, source: str, channel: Union[str, List[str], None] = None):
        """
        Initialize raw processor.
        
        Args:
            source: Data source (coinbase, ccxt)
            channel: Optional channel filter (single string or list of strings)
        """
        super().__init__()
        self.source = source
        self.channel_filter = channel
        
        # Normalize channel filter to list or None
        if isinstance(self.channel_filter, str):
            self.channel_filter = [self.channel_filter]
        
        # Initialize source-specific parser
        if source == "coinbase":
            self.parser = CoinbaseParser()
        elif source == "ccxt":
            self.parser = CcxtParser()
        else:
            raise ValueError(f"Unsupported source: {source}")
        
        logger.info(
            f"[RawProcessor] Initialized for source={source}, channel_filter={channel}"
        )
    
    def process(self, data: Any) -> Any:
        """
        Process data (delegates to process_record or process_batch).
        
        Args:
            data: Either single record (dict) or batch (list[dict])
            
        Returns:
            Parsed records
        """
        if isinstance(data, dict):
            return self.process_record(data)
        elif isinstance(data, list):
            return self.process_batch(data)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
    
    def process_record(self, record: dict) -> Union[dict, list[dict], None]:
        """
        Parse a single raw record.
        
        Args:
            record: Raw NDJSON record
            
        Returns:
            Parsed record(s), or None if filtered/failed
        """
        try:
            # Parse using source-specific parser
            parsed = self.parser.parse_record(record)
            
            if not parsed:
                return None
            
            # Handle single record or list
            if isinstance(parsed, dict):
                parsed = [parsed]
            
            # Apply channel filter if specified
            if self.channel_filter:
                parsed = [
                    r for r in parsed 
                    if r.get("channel") in self.channel_filter
                ]
            
            if not parsed:
                return None
            
            self.stats["records_processed"] += 1
            self.stats["records_output"] += len(parsed)
            
            return parsed if len(parsed) > 1 else parsed[0]
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Error parsing record: {e}", exc_info=True)
            return None
    
    def get_stats(self) -> dict:
        """Get combined stats from processor and parser."""
        stats = super().get_stats()
        stats["parser_stats"] = self.parser.get_stats()
        return stats
