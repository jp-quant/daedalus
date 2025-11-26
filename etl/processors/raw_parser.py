"""Raw parser processor - converts NDJSON to structured records."""
import logging
from typing import Dict, Any, Optional, List, Union

from .base_processor import BaseProcessor
from etl.parsers.coinbase_parser import CoinbaseParser

logger = logging.getLogger(__name__)


class RawParser(BaseProcessor):
    """
    Parse raw NDJSON records into structured format.
    
    This processor acts as a bridge between ingestion and ETL:
    - Takes raw records from NDJSON reader
    - Routes to source-specific parser (Coinbase, Databento, etc.)
    - Returns normalized records ready for channel-specific processing
    """
    
    def __init__(self, source: str, channel: Optional[str] = None):
        """
        Initialize raw parser.
        
        Args:
            source: Data source (coinbase, databento, ibkr)
            channel: Optional channel filter (only parse specific channel)
        """
        super().__init__()
        self.source = source
        self.channel_filter = channel
        
        # Initialize source-specific parser
        if source == "coinbase":
            self.parser = CoinbaseParser()
        else:
            raise ValueError(f"Unsupported source: {source}")
        
        logger.info(
            f"[RawParser] Initialized for source={source}, channel_filter={channel}"
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
                    if r.get("channel") == self.channel_filter
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
