"""Coinbase market trades processor - aggregates and enriches trade data."""
import logging
from typing import Dict, Any
from datetime import datetime

from ..base_processor import BaseProcessor

logger = logging.getLogger(__name__)


class TradesProcessor(BaseProcessor):
    """
    Process Coinbase market_trades data.
    
    Coinbase-specific implementation for processing trade messages.
    Other exchanges would have their own TradesProcessor implementations.
    
    Capabilities:
    - Basic validation and normalization
    - Add timestamp partitioning fields
    - Trade direction inference
    - Future: Trade aggregation (VWAP, volume profiles, etc.)
    """
    
    def __init__(
        self,
        add_derived_fields: bool = True,
        infer_aggressor: bool = False,
    ):
        """
        Initialize trades processor.
        
        Args:
            add_derived_fields: Add basic derived fields (date, hour, etc.)
            infer_aggressor: Try to infer trade aggressor side
        """
        super().__init__()
        self.add_derived_fields = add_derived_fields
        self.infer_aggressor = infer_aggressor
        
        logger.info(
            f"[CoinbaseTradesProcessor] Initialized: add_derived_fields={add_derived_fields}, "
            f"infer_aggressor={infer_aggressor}"
        )
    
    def process(self, data: Any) -> Any:
        """Process trades data."""
        if isinstance(data, dict):
            return self.process_record(data)
        elif isinstance(data, list):
            return self.process_batch(data)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
    
    def process_record(self, record: dict) -> dict:
        """
        Process a single trade record.
        
        Args:
            record: Parsed trade record
            
        Returns:
            Enhanced record with derived fields
        """
        try:
            # Add basic derived fields
            if self.add_derived_fields:
                record = self._add_derived_fields(record)
            
            # Infer aggressor side (if enabled)
            if self.infer_aggressor:
                record = self._infer_aggressor(record)
            
            self.stats["records_processed"] += 1
            self.stats["records_output"] += 1
            
            return record
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Error processing trade record: {e}", exc_info=True)
            return record
    
    def _add_derived_fields(self, record: dict) -> dict:
        """Add basic derived fields."""
        # Extract timestamp components for partitioning
        time_field = record.get("time") or record.get("server_timestamp")
        if time_field:
            try:
                dt = datetime.fromisoformat(time_field.replace('Z', '+00:00'))
                record["year"] = dt.year
                record["month"] = dt.month
                record["day"] = dt.day
                record["hour"] = dt.hour
                record["date"] = dt.strftime("%Y-%m-%d")
                record["timestamp"] = dt
            except Exception as e:
                logger.debug(f"Failed to parse time: {e}")
        
        # Trade value (price * size)
        if "price" in record and "size" in record:
            record["value"] = record["price"] * record["size"]
        
        # Standardize side field
        side = record.get("side", "").upper()
        if side in ("BUY", "B"):
            record["side_normalized"] = "buy"
        elif side in ("SELL", "S"):
            record["side_normalized"] = "sell"
        else:
            record["side_normalized"] = side.lower()
        
        return record
    
    def _infer_aggressor(self, record: dict) -> dict:
        """
        Infer trade aggressor side.
        
        TODO: Implement aggressor inference using:
        - Order book state at trade time
        - Trade price vs mid-price
        - Trade side (if available)
        """
        # Placeholder - requires LOB state
        return record
