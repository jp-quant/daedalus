"""Coinbase ticker processor - normalizes and enriches ticker data."""
import logging
from typing import Dict, Any

from ..base_processor import BaseProcessor
from ..time_utils import add_time_fields

logger = logging.getLogger(__name__)


class TickerProcessor(BaseProcessor):
    """
    Process Coinbase ticker data.
    
    Coinbase-specific implementation for processing ticker messages.
    Other exchanges would have their own TickerProcessor implementations.
    
    Capabilities:
    - Basic validation and normalization
    - Add timestamp partitioning fields
    - Compute derived metrics (percent changes, etc.)
    """
    
    def __init__(
        self,
        add_derived_fields: bool = True,
        **kwargs
    ):
        """
        Initialize ticker processor.
        
        Args:
            add_derived_fields: Add basic derived fields
        """
        super().__init__()
        self.add_derived_fields = add_derived_fields
        
        logger.info(f"[CoinbaseTickerProcessor] Initialized: add_derived_fields={add_derived_fields}")
    
    def process(self, data: Any) -> Any:
        """Process ticker data."""
        if isinstance(data, dict):
            return self.process_record(data)
        elif isinstance(data, list):
            return self.process_batch(data)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
    
    def process_record(self, record: dict) -> dict:
        """
        Process a single ticker record.
        
        Args:
            record: Parsed ticker record
            
        Returns:
            Enhanced record with derived fields
        """
        try:
            # Add basic derived fields
            if self.add_derived_fields:
                record = self._add_derived_fields(record)
            
            self.stats["records_processed"] += 1
            self.stats["records_output"] += 1
            
            return record
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Error processing ticker record: {e}", exc_info=True)
            return record
    
    def _add_derived_fields(self, record: dict) -> dict:
        """Add basic derived fields."""
        # Extract timestamp components for partitioning (year, month, day, hour, minute, second, microsecond, date)
        record = add_time_fields(
            record,
            timestamp_field="server_timestamp",
            fallback_field="capture_timestamp"
        )
        
        # Compute additional metrics
        if "best_bid" in record and "best_ask" in record:
            best_bid = record["best_bid"]
            best_ask = record["best_ask"]
            
            if best_bid > 0 and best_ask > 0:
                record["mid_price"] = (best_bid + best_ask) / 2
                record["spread"] = best_ask - best_bid
                record["spread_bps"] = (record["spread"] / record["mid_price"]) * 10000
        
        # Range metrics
        if "high_24h" in record and "low_24h" in record:
            high_24h = record["high_24h"]
            low_24h = record["low_24h"]
            
            if high_24h > 0 and low_24h > 0:
                record["range_24h"] = high_24h - low_24h
                if low_24h > 0:
                    record["range_24h_pct"] = (record["range_24h"] / low_24h) * 100
        
        return record
